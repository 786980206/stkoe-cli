"""GraphService 测试：table/index/panel 资产基于 graph 的登记/读取/事件。

验证：
- table add 建 graph 节点 + 物理指纹（graph.db 普通表），meta/list/get/scan 走 graph；
- 表数据变化（scan）→ 版本时间戳递增 + 下游（panel）置脏（notify_change）；
- index 独立主体（Index 节点 + symbol/datetime 列）；
- panel：建节点 + DEPENDS 边，get 实时 join；
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


def test_task_framework_index_handlers(tmp_path):
    """index 任务版（§9）：s:index add→meta→get 全链路"""
    from stkoe.task import TaskManager
    from stkoe.task.model import TERMINAL_STATES
    import time

    mgr = TaskManager(data_dir=tmp_path / "data")
    mgr.start()
    try:
        root = mgr.data_dir
        d = root / "index" / "index"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "code": [1, 2]}).write_parquet(d / "data.parquet")

        t_add = mgr.submit("index", "add", ["index"])
        _await_task(mgr, t_add, TERMINAL_STATES)
        assert _mgr_task_result(mgr, t_add)["name"] == "index"

        t_meta = mgr.submit("index", "meta", ["index"])
        _await_task(mgr, t_meta, TERMINAL_STATES)
        assert _mgr_task_result(mgr, t_meta)["symbol_col"] == "sym"

        t_get = mgr.submit("index", "get", ["index"])
        _await_task(mgr, t_get, TERMINAL_STATES)
        assert _mgr_task_result(mgr, t_get)["rows"] == 2

        t_col = mgr.submit("index", "col", ["index", "code", "--unit", "股"])
        _await_task(mgr, t_col, TERMINAL_STATES)
        assert _mgr_task_result(mgr, t_col)["columns"][0]["name"] == "sym"
    finally:
        mgr.stop()


def _await_task(mgr, task, terminal, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur is not None and cur.state in terminal:
            return cur
        time.sleep(0.02)
    raise TimeoutError(f"task not terminal: {mgr.get(task.task_id).state}")


def _mgr_task_result(mgr, task):
    """终态事件落库后取 result JSON（任务版断言专用 helper）。"""
    import json
    import time

    from stkoe.task.model import TERMINAL_STATES

    task_id = task.task_id if hasattr(task, "task_id") else task
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        evs = mgr.events.list_by_task(task_id)
        if evs and evs[-1].state in TERMINAL_STATES:
            return json.loads(evs[-1].data) if evs[-1].data else None
        time.sleep(0.01)
    evs = mgr.events.list_by_task(task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None


class TestTableGraph:
    def test_unregistered_raises_asset_not_found(self, svc):
        """§8 错误体系统一：未注册资产抛 AssetNotFoundError（不再抛 TableNotFoundError）"""
        from stkoe.graph.errors import AssetNotFoundError

        with pytest.raises(AssetNotFoundError):
            svc.panel_meta("nope")
        with pytest.raises(AssetNotFoundError):
            svc.fieldset_check("nope", "f1")
        with pytest.raises(AssetNotFoundError):
            svc.table_get("nope")

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
        from stkoe.table.errors import TableExistsError

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
        r = svc.table_update("m1")
        assert r["changed"] is False
        assert r["version_after"] == r["version_before"]

        # 追加文件：changed=True + 版本递增 + 下游 panel 置脏
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"],
                      "price": [3.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "more.parquet"))
        r2 = svc.table_update("m1")
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

    def test_scan_event_has_datetime_scope(self, svc):
        """P0-1：物理变化事件带 datetime 范围（footer min/max，不读数据页）"""
        svc.table_add("m1")
        pl.DataFrame({"sym": ["c"], "date": ["2024-02-01"], "price": [3.0]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "more.parquet"))
        svc.table_update("m1")
        node = svc.store.get_node("table:m1")
        ev = node["version_list"][str(node["version"])]
        assert ev["action"] == "upsert"
        assert ev["datetime_scope"], "事件应带 datetime 范围"
        assert any("2024-02-01" in str(v) for v in ev["datetime_scope"])
        assert ev["field_scope"] is None  # 物理变化影响文件全部列

    def test_scan_removed_emits_delete_and_new_partition_upsert(self, tmp_path):
        """P0-1：删分区文件 → delete 事件（分区路径提取）；新增分区 → upsert 事件"""
        import shutil

        base = os.path.join(os.environ.get("TEMP", "."), "gql_evt_hive")
        shutil.rmtree(base, ignore_errors=True)
        d = os.path.join(base, "table", "t1")
        os.makedirs(os.path.join(d, "date=2024-01-01"), exist_ok=True)
        os.makedirs(os.path.join(d, "date=2024-01-02"), exist_ok=True)
        pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"], "p": [1.0]}).write_parquet(
            os.path.join(d, "date=2024-01-01", "f.parquet"))
        pl.DataFrame({"sym": ["b"], "date": ["2024-01-02"], "p": [2.0]}).write_parquet(
            os.path.join(d, "date=2024-01-02", "f.parquet"))
        try:
            s = GraphService(base)
            s.table_add("t1")
            # 删 01-02 分区 + 新增 01-03 分区 → 一次 update 同时有 delete 与 upsert 事件
            os.remove(os.path.join(d, "date=2024-01-02", "f.parquet"))
            os.makedirs(os.path.join(d, "date=2024-01-03"), exist_ok=True)
            pl.DataFrame({"sym": ["c"], "date": ["2024-01-03"], "p": [3.0]}).write_parquet(
                os.path.join(d, "date=2024-01-03", "f.parquet"))
            s.table_update("t1")
            node = s.store.get_node("table:t1")
            vl = node["version_list"]
            versions = sorted(int(k) for k in vl)
            ev_new = vl[str(versions[-1])]
            ev_old = vl[str(versions[-2])]
            actions = {ev_new["action"], ev_old["action"]}
            assert actions == {"upsert", "delete"}, f"应同时记录两类事件: {actions}"
            for v in (ev_new, ev_old):
                if v["action"] == "delete":
                    assert "2024-01-02" in v["datetime_scope"]  # 分区路径提取
                else:
                    assert "2024-01-03" in v["datetime_scope"]  # 新分区 footer/分区值
            s.close()
        finally:
            shutil.rmtree(base, ignore_errors=True)


class TestIndexGraph:
    def test_index_independent(self, svc):
        r = svc.index_add("index", symbol_col="sym", datetime_col="date")
        assert r["name"] == "index"
        node = svc.store.get_node("index:index")
        assert node["type"] == "index"
        assert node["symbol_col"] == "sym"
        assert node["datetime_col"] == "date"
        # index 与 table 是不同 label；两者列登记为 Column 节点（列级血缘）
        svc.table_add("m1")
        assert {n["type"] for n in svc.store.list_nodes()} == {"index", "table", "column"}

    def test_index_add_requires_unique_keys(self, tmp_path):
        """V3.0 设计：index 的 (symbol, datetime) 组合必须唯一，重复拒绝登记"""
        import shutil

        base = os.path.join(os.environ.get("TEMP", "."), "gql_idx_unique")
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(os.path.join(base, "index", "dup"), exist_ok=True)
        pl.DataFrame({"sym": ["a", "a"], "date": ["2024-01-01", "2024-01-01"],
                      "code": [1, 2]}).write_parquet(
            os.path.join(base, "index", "dup", "data.parquet"))
        try:
            s = GraphService(base)
            with pytest.raises(ValueError, match="不唯一"):
                s.index_add("dup")
            assert s.store.get_node("index:dup") is None  # 未登记
            s.close()
        finally:
            shutil.rmtree(base, ignore_errors=True)

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
            s.panel_update("ds1")  # get 三态：未物化报错，先物化
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
        svc.panel_update("ds1")  # get 三态：先物化
        df, total = svc.panel_get("ds1", count_total=True)
        assert df.height == 2 and total == 2
        assert df.columns == ["sym", "date", "code", "price", "vol"]
        m = svc.panel_meta("ds1")
        src = {c["name"]: c["source_table"] for c in m["columns"]}
        assert src == {"sym": "index", "date": "index", "code": "index",
                       "price": "m1", "vol": "m2"}

    def test_panel_update_incremental_by_scope(self, svc, monkeypatch):
        """P2-3：源头新增日期 → panel update 增量（_panel_lazy 带 dt 区间过滤）"""
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_update("ds1")  # 首次全量（2 行）
        assert svc.panel_get("ds1").height == 2

        calls: list = []
        orig = GraphService._panel_lazy

        def spy(self, name, **kw):
            calls.append(kw.get("where"))
            return orig(self, name, **kw)

        monkeypatch.setattr(GraphService, "_panel_lazy", spy)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        assert any(c is not None for c in calls[-2:]), "增量应带 dt 区间过滤"
        df = svc.panel_get("ds1")
        assert df.height == 3
        assert sorted(df["date"].to_list()) == ["2024-01-01", "2024-01-01", "2024-01-02"]

    def test_panel_delete(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_delete("ds1")
        assert svc.store.get_node("panel:ds1") is None

    def test_panel_update_materializes(self, svc):
        """panel update：join 视图物化落盘 panel/<name>/ + get 读物化"""
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        assert svc.panel_meta("ds1")["materialized"] is False
        r = svc.panel_update("ds1")
        assert r["materialized"] is True
        # 物化按 index.materialize_partition（默认 yearly）时间桶落盘
        assert (svc.data_dir / "panel" / "ds1" / "part=2024").exists()
        m = svc.panel_meta("ds1")
        assert m["materialized"] is True and m["curated"] is True
        assert m["partition_by"] == ["part"] and m["partition_gran"] == "yearly"
        # 对外读取剔除内部桶列 part，列集合与实时视图一致
        df, total = svc.panel_get("ds1", count_total=True)
        assert df.height == 2 and total == 2
        assert "part" not in df.columns

    def test_partition_gran_follows_materialize_partition(self, svc):
        """物化时间桶粒度继承 index.materialize_partition（monthly/daily）"""
        svc.table_add("m1")
        svc.index_add("index", materialize_partition="monthly")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_update("ds1")
        assert (svc.data_dir / "panel" / "ds1" / "part=2024-01").exists()
        assert svc.panel_meta("ds1")["partition_gran"] == "monthly"

        os.makedirs(os.path.join(svc.data_dir, "index", "idx2"), exist_ok=True)
        pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"], "code": [1]}).write_parquet(
            os.path.join(svc.data_dir, "index", "idx2", "data.parquet"))
        svc.index_add("idx2", materialize_partition="daily")
        svc.panel_add("ds2", "idx2", ["m1"])
        svc.panel_update("ds2")
        assert (svc.data_dir / "panel" / "ds2" / "part=2024-01-01").exists()

    def test_incremental_cross_year_keeps_old_bucket(self, svc):
        """跨年增量：新日期开新桶（part=2025），旧桶（part=2024）不受影响"""
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_update("ds1")
        assert svc.panel_get("ds1").height == 2

        pl.DataFrame({"sym": ["c"], "date": ["2025-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")

        root = svc.data_dir / "panel" / "ds1"
        assert (root / "part=2024").exists() and (root / "part=2025").exists()
        df = svc.panel_get("ds1")
        assert df.height == 3
        assert sorted(df["date"].to_list()) == ["2024-01-01", "2024-01-01", "2025-01-02"]

    def test_panel_update_bumps_version_and_invalidates_curated(self, svc):
        """源头变化 → panel 置脏 + curated 失效 → update 铸版本恢复"""
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_update("ds1")
        assert svc.panel_meta("ds1")["curated"] is True
        v0 = svc.panel_meta("ds1")["version"]

        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        m = svc.panel_meta("ds1")
        assert m["valid"] is False  # 置脏
        assert m["curated"] is False  # 依赖 hash 变（index 版本推进）
        r = svc.panel_update("ds1")
        assert r["version"] != v0  # 铸版本（消费积累事件）
        assert svc.panel_meta("ds1")["curated"] is True


class TestFieldsetSampleGraph:
    """fieldset 衍生字段物化 + sample 铸版本。"""

    def _chain(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_check("fs1", "x2")

    def test_fieldset_update_materializes_derived_fields(self, svc):
        """fieldset update：衍生字段（keys + 已校验字段）落盘 + get 拼接/fields_only"""
        self._chain(svc)
        svc.panel_update("ds1")
        assert svc.fieldset_meta("fs1")["materialized"] is False
        r = svc.fieldset_update("fs1")
        assert r["materialized"] is True
        assert (svc.data_dir / "fieldset" / "fs1" / "part=2024").exists()
        m = svc.fieldset_meta("fs1")
        assert m["curated"] is True
        # fields_only 读物化字段（keys + x2）
        df, total = svc.fieldset_get("fs1", fields_only=True, count_total=True)
        assert df.columns == ["sym", "date", "x2"]
        assert total == 2
        assert sorted(df["x2"].to_list()) == [2.0, 4.0]
        # 全视图 = panel + x2
        df2, total2 = svc.fieldset_get("fs1", count_total=True)
        assert {"sym", "date", "code", "price", "x2"} <= set(df2.columns)
        assert total2 == 2

    def test_fieldset_curated_invalidated_on_upstream_change(self, svc):
        self._chain(svc)
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        assert svc.fieldset_meta("fs1")["curated"] is True
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        assert svc.fieldset_meta("fs1")["curated"] is False  # panel 版本变化 → 签名变
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        assert svc.fieldset_meta("fs1")["curated"] is True

    def test_sample_update_bumps_version_on_chain_change(self, svc):
        """源头变化 → 全链 update → sample 铸版本（积累事件入 version_list）"""
        self._chain(svc)
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        svc.sample_add("sp1", "fs1", "index")
        svc.sample_update("sp1")
        v0 = svc.store.get_node("sample:sp1")["version"]

        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        svc.sample_update("sp1")
        v1 = svc.store.get_node("sample:sp1")["version"]
        assert v1 != v0  # 铸版本
        # 版本事件已记录（sample 消费了上游事件）
        node = svc.store.get_node("sample:sp1")
        assert str(v1) in node["version_list"]

    def test_fieldset_add_field_invalidates_self(self, svc):
        """P2-4：fieldset 加字段（定义变化）→ 自身物化失效；check 写回 validated 不额外置脏"""
        self._chain(svc)
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        assert svc.fieldset_meta("fs1")["materialized"] is True
        svc.fieldset_add_field("fs1", "x3", "code * 3")
        m = svc.fieldset_meta("fs1")
        assert m["materialized"] is False  # 定义变化 → 自身物化失效
        assert m["valid"] is False
        # check 写回 validated（状态更新）→ 不额外置脏也不恢复 valid（等 update）
        svc.fieldset_check("fs1", "x3")
        m2 = svc.fieldset_meta("fs1")
        assert m2["materialized"] is False
        assert m2["valid"] is False
        # update 恢复物化
        svc.fieldset_update("fs1")
        m3 = svc.fieldset_meta("fs1")
        assert m3["materialized"] is True and m3["curated"] is True
        df, _ = svc.fieldset_get("fs1", fields_only=True, count_total=True)
        assert "x3" in df.columns

    def test_fieldset_update_incremental_by_scope(self, svc):
        """P2-3：源头新增日期 → fieldset update 增量（只重算区间字段，旧行保留）"""
        self._chain(svc)
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")  # 首次全量（2 行）
        df0, _ = svc.fieldset_get("fs1", fields_only=True, count_total=True)
        assert df0.height == 2

        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        df, total = svc.fieldset_get("fs1", fields_only=True, count_total=True)
        assert df.height == 3 and total == 3
        assert sorted(df["date"].to_list()) == ["2024-01-01", "2024-01-01", "2024-01-02"]
        assert sorted(df["x2"].to_list()) == [2.0, 4.0, 6.0]


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
        svc.sample_add("sp1", "fs1", "index")
        svc.feature_add("f1", "code * 2")
        if with_ready:
            # update 语义：上游依次就绪（panel → fieldset → sample → feature）
            for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                         ("sample", "sp1"), ("feature", "f1")]:
                getattr(svc, f"{t}_update")(n)

    def test_factor_add_meta_check_get_update(self, svc):
        self._chain(svc, with_ready=False)  # 只建链，上游不 update（未就绪）
        fm = svc.factor_add("fac1", "f1", "sp1")
        assert fm["name"] == "fac1"
        assert fm["feature"] == "f1"
        assert fm["sample"] == "sp1"
        assert fm["keys"] == ["sym", "date"]
        m = svc.factor_meta("fac1")
        assert m["materialized"] is False
        assert {c["name"] for c in m["columns"]} >= {"sym", "date", "code", "price", "x2"}
        # §10：factor meta columns 完整列元数据（panel 列继承 / fieldset 字段全键）
        by_name = {c["name"]: c for c in m["columns"]}
        assert set(by_name["code"]) >= {"name", "display_name", "description",
                                        "data_type", "unit", "formula", "tags"}
        assert by_name["x2"].get("formula") == "code * 2"  # fieldset 衍生字段公式
        sm = svc.sample_meta("sp1")
        assert sm["keys"] == ["sym", "date"] and sm["materialized"] is False
        assert {c["name"] for c in sm["columns"]} >= {"sym", "date", "code", "price", "x2"}
        assert next(c for c in sm["columns"] if c["name"] == "x2").get("validated") is True

        r = svc.factor_check("fac1")
        assert r["ok"] is True

        # 上游未就绪（链上有 valid=False）→ factor update 被传导拦截
        import pytest

        with pytest.raises(Exception):
            svc.factor_update("fac1")
        # get 三态：未物化 → 报错提示先 update
        with pytest.raises(ValueError, match="未物化"):
            svc.factor_get("fac1")

        # 依次传导 update：panel → fieldset → sample → feature → factor
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        svc.sample_update("sp1")
        svc.feature_update("f1")
        s1 = svc.factor_update("fac1")
        assert s1["changed"] is True
        assert s1["version_after"] >= s1["version_before"]  # 无事件首物化不空 bump
        s2 = svc.factor_update("fac1")  # 幂等
        assert s2["changed"] is False
        assert svc.factor_meta("fac1")["curated"] is True
        assert (svc.data_dir / "factor" / "fac1" / "part=2024").exists()
        # 物化后 get 读物化
        df, total = svc.factor_get("fac1", count_total=True)
        assert df.height == 2 and total == 2
        assert df.columns == ["sym", "date", "f1"]

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

    def test_factor_update_incremental_by_scope(self, svc, monkeypatch):
        """P0-2：源头新增日期 → factor update 增量重算（compute 带 dt_range，非全量）"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")  # 首次全量物化（2 行，date=01-01）
        assert svc.factor_get("fac1").height == 2

        calls: list = []
        orig = GraphService._factor_compute

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_factor_compute", spy)

        # 源头追加新日期文件 → upsert 事件 [01-02, 01-02] → 链置脏 → 依次 update
        # （dtype 与现有文件一致，避免多文件 scan 不 union schema）
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                     ("sample", "sp1"), ("feature", "f1")]:
            getattr(svc, f"{t}_update")(n)
        svc.factor_update("fac1")

        assert calls and calls[-1] == ("2024-01-02", "2024-01-02"), \
            f"增量应带 dt_range，实际 {calls}"
        df = svc.factor_get("fac1")
        assert df.height == 3
        assert sorted(df["date"].to_list()) == ["2024-01-01", "2024-01-01", "2024-01-02"]
        assert sorted(df["f1"].to_list()) == [2.0, 4.0, 6.0]
        # 边水位已对齐（供下次沿链增量判定）
        deps = svc.store.deps_of("factor:fac1")
        assert all(e.get("required_version", 0) > 0 for e in deps)

    def test_factor_update_resync_full(self, svc, monkeypatch):
        """P0-2：--resync 强制全量（compute 不带 dt_range）"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")
        calls: list = []
        orig = GraphService._factor_compute

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_factor_compute", spy)
        svc.factor_update("fac1", resync=True)
        assert calls == [None]  # 全量：无 dt_range


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
        svc.sample_add("sp1", "fs1", "index")
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

        r = svc.test_check("t1")
        assert r["ok"] is True
        # get 三态：未物化 → 报错提示先 update
        with pytest.raises(ValueError, match="未物化"):
            svc.test_get("t1")

        s1 = svc.test_update("t1")
        assert s1["changed"] is True
        assert s1["rows"] == 2
        s2 = svc.test_update("t1")  # 幂等
        assert s2["changed"] is False
        assert svc.test_meta("t1")["curated"] is True
        assert (svc.data_dir / "factor_test" / "t1" / "part=2024").exists()

        # 物化后 get/data 读物化
        df, total = svc.test_get("t1", count_total=True)
        assert df.height == 2 and total == 2
        assert "factor_quantile" in df.columns
        assert "d1" in df.columns
        d = svc.test_data("t1")
        assert d.height == 2

        svc.test_delete("t1")
        assert svc.store.get_node("tester:t1") is None

    def test_test_update_incremental_by_scope(self, svc, monkeypatch):
        """P0-2：源头新增日期 → factor/test 链增量重算（test_build 带 dt_range）"""
        self._chain(svc)
        svc.test_add("t1", "fac1")
        svc.test_update("t1")  # 首次全量（2 行）
        assert svc.test_data("t1").height == 2

        calls: list = []
        orig = GraphService._test_build

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_test_build", spy)

        # 源头追加新日期（含测试必需列；dtype 与现有文件一致）→ 链置脏 → 依次 update
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3],
                      "r": [0.03], "ic": ["G1"], "fv": [3.0]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"), ("sample", "sp1"),
                     ("feature", "f1"), ("factor", "fac1")]:
            getattr(svc, f"{t}_update")(n)
        svc.test_update("t1")

        assert calls and calls[-1] == ("2024-01-02", "2024-01-02"), \
            f"增量应带 dt_range，实际 {calls}"
        d = svc.test_data("t1")
        assert d.height == 3
        assert sorted(d["date"].to_list()) == ["2024-01-01", "2024-01-01", "2024-01-02"]


class TestColumnLineage:
    """列级血缘：DEPENDS 边 detail 的字段映射 → Column 节点 + DERIVES 边。

    链：index/m1 → panel:ds1 → fieldset:fs1(x2) → sample:sp1 → factor:fac1 → tester:tt1
    （index 含测试必需列 r/ic/fv；fieldset 字段 x2 = code * 2；feature f1 = code * 2）
    """

    def _chain(self, svc, with_test=True):
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
        svc.sample_add("sp1", "fs1", "index")
        svc.feature_add("f1", "code * 2")
        svc.factor_add("fac1", "f1", "sp1")
        if with_test:
            svc.test_add("tt1", "fac1")

    def test_source_columns_registered(self, svc):
        """源头（table/index）登记即建列节点（带元数据）。"""
        svc.table_add("m1")
        svc.index_add("index")
        st = svc.store
        idx = {c["name"]: c for c in st.columns_of("index:index")}
        assert set(idx) == {"sym", "date", "code"}
        assert idx["sym"]["data_type"]  # 源头列带类型
        assert idx["sym"]["asset_type"] == "index"
        assert {c["name"] for c in st.columns_of("table:m1")} == {"sym", "date", "price"}

    def test_panel_derives_from_index_and_members(self, svc):
        """panel 列 DERIVES → index 列 / 成员表列；DEPENDS 边 detail 携带映射。"""
        self._chain(svc, with_test=False)
        st = svc.store
        assert {c["name"] for c in st.columns_of("panel:ds1")} == \
            {"sym", "date", "code", "price", "r", "ic", "fv"}
        assert st.deps_of("column:panel:ds1.sym", rel_type="DERIVES")[0]["target"] \
            == "column:index:index.sym"
        assert st.deps_of("column:panel:ds1.price", rel_type="DERIVES")[0]["target"] \
            == "column:table:m1.price"
        # 同名去重：member 与 index 同名列不重复建映射
        e = st.get_edge("panel:ds1", "table:m1")
        assert e["detail"]["columns"] == {"price": "price"}
        assert "sym" not in e["detail"]["columns"]

    def test_fieldset_field_derives_to_panel_cols(self, svc):
        """字段列 DERIVES → 公式引用的 panel 列；set_field 改公式后重派发。"""
        self._chain(svc, with_test=False)
        st = svc.store
        cols = {c["name"]: c for c in st.columns_of("fieldset:fs1")}
        assert {"sym", "date", "x2"} <= set(cols)
        assert cols["sym"]["as_index"] is True
        assert cols["x2"]["formula"] == "code * 2"
        assert st.deps_of("column:fieldset:fs1.x2", rel_type="DERIVES")[0]["target"] \
            == "column:panel:ds1.code"
        assert st.deps_of("column:fieldset:fs1.sym", rel_type="DERIVES")[0]["target"] \
            == "column:panel:ds1.sym"
        # 改公式 → 清旧映射 + 按新公式重派发
        svc.fieldset_set_field("fs1", "x2", formula="price * 2")
        targets = {e["target"] for e in
                   st.deps_of("column:fieldset:fs1.x2", rel_type="DERIVES")}
        assert targets == {"column:panel:ds1.price"}

    def test_sample_factor_test_chain_derives(self, svc):
        """sample 视图透传 + index 键映射；factor_col → 公式引用列；test 跨依赖引用。"""
        self._chain(svc)
        st = svc.store
        # sample：视图列透传 fieldset（含已校验字段 x2）+ 键映射 index
        assert {c["name"] for c in st.columns_of("sample:sp1")} == \
            {"sym", "date", "code", "price", "r", "ic", "fv", "x2"}
        assert st.deps_of("column:sample:sp1.x2", rel_type="DERIVES")[0]["target"] \
            == "column:fieldset:fs1.x2"
        sym_srcs = {e["target"] for e in
                    st.deps_of("column:sample:sp1.sym", rel_type="DERIVES")}
        assert sym_srcs == {"column:fieldset:fs1.sym", "column:index:index.sym"}
        date_srcs = {e["target"] for e in
                     st.deps_of("column:sample:sp1.date", rel_type="DERIVES")}
        assert date_srcs == {"column:fieldset:fs1.date", "column:index:index.date"}
        # factor：keys 透传 sample + factor_col → feature 公式引用列
        assert {c["name"] for c in st.columns_of("factor:fac1")} == {"sym", "date", "f1"}
        assert st.deps_of("column:factor:fac1.f1", rel_type="DERIVES")[0]["target"] \
            == "column:sample:sp1.code"
        assert st.deps_of("column:factor:fac1.sym", rel_type="DERIVES")[0]["target"] \
            == "column:sample:sp1.sym"
        # test：factor 列（factor/factor_quantile/keys）+ 跨依赖 sample 列（returns/group/...）
        assert st.deps_of("column:tester:tt1.factor", rel_type="DERIVES")[0]["target"] \
            == "column:factor:fac1.f1"
        assert st.deps_of("column:tester:tt1.factor_quantile",
                          rel_type="DERIVES")[0]["target"] == "column:factor:fac1.f1"
        assert {e["target"] for e in
                st.deps_of("column:tester:tt1.returns", rel_type="DERIVES")} \
            == {"column:sample:sp1.r"}
        assert st.deps_of("column:tester:tt1.group", rel_type="DERIVES")[0]["target"] \
            == "column:sample:sp1.ic"
        assert st.deps_of("column:tester:tt1.marketcap",
                          rel_type="DERIVES")[0]["target"] == "column:sample:sp1.fv"
        assert st.deps_of("column:tester:tt1.d1", rel_type="DERIVES")[0]["target"] \
            == "column:sample:sp1.r"

    def test_delete_cascades_column_nodes(self, svc):
        """资产删除 → 其列节点级联删除（DERIVES 边随之清除）。"""
        self._chain(svc)
        st = svc.store
        svc.test_delete("tt1")
        assert st.columns_of("tester:tt1") == []
        svc.factor_delete("fac1")
        assert st.columns_of("factor:fac1") == []
        svc.sample_delete("sp1")
        assert st.columns_of("sample:sp1") == []
        svc.fieldset_delete("fs1")
        assert st.columns_of("fieldset:fs1") == []
        # 列删除后没有悬空 DERIVES 边（DETACH DELETE）
        assert st.deps_of("column:panel:ds1.price", rel_type="DERIVES")[0]["target"] \
            == "column:table:m1.price"

    def test_payload_with_columns_and_column_center(self, svc):
        """export：--columns 叠加列层；column_payload 以列为中心查上游来源/下游派生。"""
        from stkoe.graph.export import build_payload, column_payload

        self._chain(svc)
        store = svc.store
        p = build_payload(store, with_columns=True)
        ids = {n["data"]["id"] for n in p["elements"]["nodes"]}
        assert "column:fieldset:fs1.x2" in ids
        assert "column:table:m1.price" in ids
        assert "column" in p["graph"]["types"]
        assert any(e["data"]["id"].startswith("column:")
                   for e in p["elements"]["edges"])  # DERIVES 边
        # 不带 --columns：资产图口径不变
        p2 = build_payload(store)
        assert all(n["data"]["type"] != "column" for n in p2["elements"]["nodes"])

        cp = column_payload(store, "column:fieldset:fs1.x2")
        cids = {n["data"]["id"] for n in cp["elements"]["nodes"]}
        assert "column:panel:ds1.code" in cids   # 上游来源
        assert "column:sample:sp1.x2" in cids    # 下游派生
        assert "fieldset:fs1" in cids            # 所属资产上下文

    def test_dispatch_columns_and_column_lineage(self, svc):
        """Execute 通道：graph columns / lineage --columns / --column。"""
        import json

        from stkoe.grpc.dispatch import dispatch

        self._chain(svc)
        base = str(svc.data_dir)
        cols = json.loads(dispatch("graph", "columns",
                                   ["--node", "index:index"], data_dir=base)[0].data)
        assert {c["name"] for c in cols} == {"sym", "date", "r", "ic", "fv", "code"}
        stats = json.loads(dispatch("graph", "stats", [], data_dir=base)[0].data)
        assert stats["node_count"] == 8  # index/m1/panel/fieldset/sample/feature/factor/tester
        assert stats["column_count"] > 0 and stats["derives_count"] > 0
        p = json.loads(dispatch("graph", "lineage", ["--columns"],
                                data_dir=base)[0].data)
        assert "column" in p["graph"]["types"]
        cp = json.loads(dispatch("graph", "lineage",
                                 ["--column", "column:fieldset:fs1.x2"],
                                 data_dir=base)[0].data)
        assert cp["graph"]["center"] == "column:fieldset:fs1.x2"
        assert cp["elements"]["nodes"]
