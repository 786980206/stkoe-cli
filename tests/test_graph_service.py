"""GraphService 测试：table/index/panel 资产基于 graph 的登记/读取/事件。

验证：
- table add 建 graph 节点 + 物理指纹（graph.db 普通表），meta/list/get/scan 走 graph；
- 表数据变化（scan）→ 版本时间戳递增 + 下游（panel）置脏（notify_change）；
- index 独立主体（Index 节点 + symbol/datetime 列）；
- panel（原 dataset）：建节点 + DEPENDS 边，get 实时 join；
- 旧 catalog.db 不再产生（登记全部进 graph.db）。
"""
from __future__ import annotations

import os
import shutil

import polars as pl
import pytest

from stkoe.graph.service import GraphService


@pytest.fixture()
def svc():
    base = os.path.join(os.environ.get("TEMP", "."), "gql_svc_test")
    shutil.rmtree(base, ignore_errors=True)
    for sub in ("table", "index"):
        for d in ("index", "m1", "m2"):
            os.makedirs(os.path.join(base, sub, d), exist_ok=True)
    # index 资产物理目录为 index/
    pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                  "code": [1, 2]}).write_parquet(os.path.join(base, "index", "index", "data.parquet"))
    pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                  "price": [1.5, 2.5]}).write_parquet(os.path.join(base, "table", "m1", "data.parquet"))
    pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                  "vol": [100, 200]}).write_parquet(os.path.join(base, "table", "m2", "data.parquet"))
    s = GraphService(base)
    yield s
    s.close()
    shutil.rmtree(base, ignore_errors=True)


class TestTableGraph:
    def test_add_creates_node_and_fingerprint(self, svc):
        r = svc.table_add("m1")
        assert r["implicit_registered"] is True
        assert r["changed"] is True
        assert r["version_after"] > 0  # 时间戳版本
        node = svc.store.get_node("table:m1")
        assert node["type"] == "table"
        assert {c["name"] for c in node["columns"]} == {"sym", "date", "price"}
        files = svc.store.fingerprint_get("table:m1")
        assert files["data.parquet"]["size"] > 0

    def test_duplicate_add_raises(self, svc):
        svc.table_add("m1")
        from stkoe.table.controller import TableExistsError

        with pytest.raises(TableExistsError):
            svc.table_add("m1")

    def test_get_and_meta(self, svc):
        svc.table_add("m1")
        df, total = svc.table_get("m1", count_total=True)
        assert df.height == 2 and total == 2
        assert df.columns == ["sym", "date", "price"]
        m = svc.table_meta("m1")
        assert m["name"] == "m1"
        assert m["layout"] == "single"
        assert len(m["files"]) == 1
        assert {c["name"] for c in m["columns"]} == {"sym", "date", "price"}

    def test_scan_idempotent_then_change_bumps_and_propagates(self, svc):
        svc.table_add("m1")
        svc.index_add("index", symbol_col="sym", datetime_col="date")
        svc.panel_add("ds1", "index", ["m1"])
        svc.graph.resolve_all()  # settle
        v0 = svc.table_meta("m1")["version"]

        # 无变化：不 bump
        r = svc.table_scan("m1")
        assert r["changed"] is False
        assert r["version_after"] == r["version_before"]

        # 追加文件：changed=True + 版本递增 + 下游 panel 置脏
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"],
                      "price": [3.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "more.parquet"))
        r2 = svc.table_scan("m1")
        assert r2["changed"] is True
        assert r2["version_after"] > v0
        pnode = svc.store.get_node("panel:ds1")
        assert pnode["valid"] is False  # notify_change 传播置脏
        # index 的 version_list 记录事件
        m1 = svc.store.get_node("table:m1")
        assert str(m1["version"]) in m1["version_list"]

    def test_set_col_delete(self, svc):
        svc.table_add("m1")
        m = svc.table_set("m1", display_name="表1", custom="x")
        assert m["display_name"] == "表1"
        assert m["extra"]["custom"] == "x"
        m2 = svc.table_col("m1", "price", display_name="价格", tags="a, b")
        col = next(c for c in m2["columns"] if c["name"] == "price")
        assert col["display_name"] == "价格"
        assert col["tags"] == ["a", "b"]
        svc.table_delete("m1")
        assert svc.store.get_node("table:m1") is None
        assert svc.store.fingerprint_get("table:m1") == {}

    def test_old_catalog_db_not_created(self, svc):
        """登记全部进 catalog.db（新结构：图节点/边 + 物理指纹表），旧 graph.db 名不再产生。"""
        svc.table_add("m1")
        assert (svc.data_dir / "catalog.db").exists()
        assert not (svc.data_dir / "graph.db").exists()

    def test_delete_clears_fingerprint_persistently(self, svc):
        """delete 清指纹必须持久化（跨连接验证）。

        回归：Python 3.13 默认 isolation_level=''（legacy 模式），txn() 外的 DELETE
        隐式事务不提交、close 时回滚 → 曾致 delete 后指纹残留（新进程可见）。
        """
        svc.table_add("m1")
        svc.close()
        # 新连接（模拟新进程/CLI 调用链）
        svc2 = GraphService(svc.data_dir)
        svc2.table_delete("m1")
        svc2.close()
        svc3 = GraphService(svc.data_dir)
        assert svc3.store.get_node("table:m1") is None
        assert svc3.store.fingerprint_get("table:m1") == {}
        svc3.close()


class TestIndexGraph:
    def test_index_independent(self, svc):
        r = svc.index_add("index", symbol_col="sym", datetime_col="date")
        assert r["name"] == "index"
        node = svc.store.get_node("index:index")
        assert node["type"] == "index"
        assert node["symbol_col"] == "sym"
        assert node["datetime_col"] == "date"
        # index 与 table 是不同 label
        svc.table_add("m1")
        assert {n["type"] for n in svc.store.list_nodes()} == {"index", "table"}

    def test_index_crud(self, svc):
        svc.index_add("index")
        m = svc.index_meta("index")
        assert m["name"] == "index"
        df, total = svc.index_get("index", count_total=True)
        assert df.height == 2 and total == 2
        svc.index_set("index", display_name="索引")
        assert svc.index_meta("index")["display_name"] == "索引"
        svc.index_delete("index")
        assert svc.store.get_node("index:index") is None


class TestPanelGraph:
    def test_panel_add_edges(self, svc):
        svc.table_add("m1")
        svc.table_add("m2")
        svc.index_add("index")
        p = svc.panel_add("ds1", "index", ["m1", "m2"])
        assert p["name"] == "ds1"
        assert p["index"] == "index:index"
        assert p["tables"] == {"m1": "asof_join", "m2": "asof_join"}  # 缺省 asof join
        # DEPENDS 边
        targets = sorted(d["target"] for d in svc.graph.deps_of("panel", "ds1"))
        assert targets == ["index:index", "table:m1", "table:m2"]
        # 边 detail 带 join 类型
        deps = {d["target"]: (d.get("detail") or {}).get("join")
                for d in svc.graph.deps_of("panel", "ds1")}
        assert deps == {"index:index": None, "table:m1": "asof_join", "table:m2": "asof_join"}

    def test_panel_add_join_types(self, svc):
        """成员表可配置 join：table1:asof / table1:left / 缺省 asof"""
        svc.table_add("m1")
        svc.table_add("m2")
        svc.index_add("index")
        p = svc.panel_add("ds1", "index", ["m1:asof", "m2:left", "m1"])
        assert p["tables"] == {"m1": "asof_join", "m2": "left_join"}
        deps = {d["target"]: (d.get("detail") or {}).get("join")
                for d in svc.graph.deps_of("panel", "ds1")}
        assert deps == {"index:index": None, "table:m1": "asof_join",
                        "table:m2": "left_join"}
        # dict 形态与 (name, join) 元组形态等价
        p2 = svc.panel_add("ds2", "index", {"m1": "left", "m2": "asof_join"})
        assert p2["tables"] == {"m1": "left_join", "m2": "asof_join"}
        p3 = svc.panel_add("ds3", "index", [("m1", "left"), ("m2", "asof")])
        assert p3["tables"] == {"m1": "left_join", "m2": "asof_join"}

    def test_panel_add_unknown_join_error(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        import pytest

        with pytest.raises(ValueError):
            svc.panel_add("ds1", "index", ["m1:inner"])

    def test_panel_get_asof_join_backward(self, svc):
        """asof join：member 无精确日期行时取最近过去日期（backward）"""
        import shutil
        import os as _os

        root = _os.path.join(_os.environ.get("TEMP", "."), "gql_asof_svc")
        shutil.rmtree(root, ignore_errors=True)
        for sub, d in (("index", "idx"), ("table", "mem")):
            _os.makedirs(_os.path.join(root, sub, d), exist_ok=True)
        # index：sym=a 的 01/03 日无 member 对应 → asof 取 01/02 的值
        pl.DataFrame({"sym": ["a", "a"], "date": ["2024-01-02", "2024-01-03"],
                      "price": [1.0, 2.0]}).write_parquet(
            _os.path.join(root, "index", "idx", "data.parquet"))
        pl.DataFrame({"sym": ["a"], "date": ["2024-01-02"], "x": [10.0]}).write_parquet(
            _os.path.join(root, "table", "mem", "data.parquet"))
        try:
            s = GraphService(root)
            s.table_add("mem")
            s.index_add("idx", symbol_col="sym", datetime_col="date")
            s.panel_add("ds1", "idx", ["mem:asof"])
            df, total = s.panel_get("ds1", count_total=True)
            assert total == 2
            assert df["x"].to_list() == [10.0, 10.0]  # 01/03 backward → 01/02 的 x
            s.close()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_panel_get_join_and_columns(self, svc):
        svc.table_add("m1")
        svc.table_add("m2")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1", "m2"])
        df, total = svc.panel_get("ds1", count_total=True)
        assert df.height == 2 and total == 2
        assert df.columns == ["sym", "date", "code", "price", "vol"]
        m = svc.panel_meta("ds1")
        src = {c["name"]: c["source_table"] for c in m["columns"]}
        assert src == {"sym": "index", "date": "index", "code": "index",
                       "price": "m1", "vol": "m2"}

    def test_panel_delete(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_delete("ds1")
        assert svc.store.get_node("panel:ds1") is None


class TestIndexGraph:
    def test_index_list_candidate(self, svc):
        """index list --candidate：indexs/ 下未登记为 index 但含 parquet 的目录"""
        for n in ("m1", "m2"):
            os.makedirs(os.path.join(svc.data_dir, "index", n), exist_ok=True)
            pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"]}).write_parquet(
                os.path.join(svc.data_dir, "index", n, "data.parquet"))
        svc.index_add("index")  # 登记 index（indexs/index）
        cands = svc.index_list(candidate=True)
        assert "m1" in cands and "m2" in cands
        assert "index" not in cands  # 已登记 index 的不作候选
        listed = svc.index_list()
        assert [i["name"] for i in listed] == ["index"]

    def test_index_add_all(self, svc):
        """index add --all：批量发现 index/ 下未登记的 parquet 目录（空目录跳过）"""
        # fixture 已含 index/index（未登记）；再造两个未登记候选 + 一个空目录
        for n in ("a1", "a2"):
            os.makedirs(os.path.join(svc.data_dir, "index", n), exist_ok=True)
            pl.DataFrame({"sym": ["x"], "date": ["2024-01-01"]}).write_parquet(
                os.path.join(svc.data_dir, "index", n, "data.parquet"))
        os.makedirs(os.path.join(svc.data_dir, "index", "empty"), exist_ok=True)

        reports = svc.index_add("", all=True)
        assert {r["name"] for r in reports} == {"index", "a1", "a2"}
        assert all(r["changed"] is True for r in reports)
        # 默认 symbol/datetime 列写入节点
        node = svc.store.get_node("index:a1")
        assert node["symbol_col"] == "sym"
        assert node["datetime_col"] == "date"
        # 已登记的不再重复登记（幂等）
        assert svc.index_add("", all=True) == []
        # 空目录不作候选
        assert svc.store.get_node("index:empty") is None


class TestFactorGraph:
    """factor：feature 公式 + sample 视图 + pipeline 算子链（graph 登记，scan 物化）。"""

    def _chain(self, svc, with_ready=True):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_check("fs1", "x2")
        svc.sample_add("sp1", "fs1")
        svc.feature_add("f1", "code * 2")
        if with_ready:
            # update 语义：上游依次就绪（panel → fieldset → sample → feature）
            for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                         ("sample", "sp1"), ("feature", "f1")]:
                getattr(svc, f"{t}_update")(n)

    def test_factor_add_meta_check_get_scan(self, svc):
        self._chain(svc, with_ready=False)  # 只建链，上游不 update（未就绪）
        fm = svc.factor_add("fac1", "f1", "sp1")
        assert fm["name"] == "fac1"
        assert fm["feature"] == "f1"
        assert fm["sample"] == "sp1"
        assert fm["keys"] == ["sym", "date"]
        m = svc.factor_meta("fac1")
        assert m["materialized"] is False
        assert {c["name"] for c in m["columns"]} >= {"sym", "date", "code", "price", "x2"}

        r = svc.factor_check("fac1")
        assert r["ok"] is True
        df, total = svc.factor_get("fac1", count_total=True)
        assert df.height == 2 and total == 2
        assert df.columns == ["sym", "date", "f1"]

        # 上游未就绪（链上有 valid=False）→ factor update 被传导拦截
        import pytest

        with pytest.raises(Exception):
            svc.factor_update("fac1")

        # 依次传导 update：panel → fieldset → sample → feature → factor(scan 别名)
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        svc.sample_update("sp1")
        svc.feature_update("f1")
        s1 = svc.factor_scan("fac1")
        assert s1["changed"] is True
        assert s1["version_after"] > s1["version_before"]
        s2 = svc.factor_scan("fac1")  # 幂等
        assert s2["changed"] is False
        assert svc.factor_meta("fac1")["curated"] is True
        assert (svc.data_dir / "factor" / "fac1" / "data.parquet").exists()

    def test_factor_add_requires_registered(self, svc):
        self._chain(svc)
        with pytest.raises(Exception):
            svc.factor_add("fac1", "nope", "sp1")
        with pytest.raises(Exception):
            svc.factor_add("fac1", "f1", "nope")

    def test_factor_list_set_delete(self, svc):
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_add("fac2", "f1", "sp1", pipeline="nothing()")
        assert [f["name"] for f in svc.factor_list()] == ["fac1", "fac2"]
        svc.factor_set("fac1", display_name="因子1")
        assert svc.factor_meta("fac1")["display_name"] == "因子1"
        svc.factor_delete("fac1")
        assert svc.store.get_node("factor:fac1") is None


class TestTesterGraph:
    """test：factor 关联 sample 视图 + 测试必需列；scan 物化，test_data 供 stat。"""

    def _chain(self, svc, with_test_cols=True, with_ready=True):
        if with_test_cols:
            # 覆盖 index/data.parquet 加入测试必需列（多文件 scan 不 union schema）
            pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                          "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0],
                          "code": [1, 2]}).write_parquet(
                os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_check("fs1", "x2")
        svc.sample_add("sp1", "fs1")
        svc.feature_add("f1", "code * 2")
        svc.factor_add("fac1", "f1", "sp1")
        if with_ready:
            for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                         ("sample", "sp1"), ("feature", "f1"),
                         ("factor", "fac1")]:
                getattr(svc, f"{t}_update")(n)

    def test_test_add_requires_columns(self, svc):
        self._chain(svc, with_test_cols=False)
        with pytest.raises(ValueError):
            svc.test_add("t1", "fac1")

    def test_test_add_get_check_scan_data_delete(self, svc):
        self._chain(svc)
        tm = svc.test_add("t1", "fac1")
        assert tm["name"] == "t1"
        assert tm["factor"] == "fac1"
        assert tm["sample"] == "sp1"
        assert tm["keys"] == ["sym", "date"]

        df, total = svc.test_get("t1", count_total=True)
        assert df.height == 2 and total == 2
        assert "factor_quantile" in df.columns
        assert "d1" in df.columns

        r = svc.test_check("t1")
        assert r["ok"] is True

        s1 = svc.test_scan("t1")
        assert s1["changed"] is True
        assert s1["rows"] == 2
        s2 = svc.test_scan("t1")  # 幂等
        assert s2["changed"] is False
        assert svc.test_meta("t1")["curated"] is True
        assert (svc.data_dir / "factor_test" / "t1" / "data.parquet").exists()

        d = svc.test_data("t1")
        assert d.height == 2

        svc.test_delete("t1")
        assert svc.store.get_node("tester:t1") is None
