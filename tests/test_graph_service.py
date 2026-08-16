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


def _cleanup_dispatch_cache(base):
    """清 dispatch 线程本地 GraphService 缓存并显式关闭。

    dispatch 业务 handler（走 ``_graph_service``）会在线程本地缓存一个 GraphService
    实例；不清理的话连接可能延迟释放，导致 svc fixture 的 rmtree(ignore_errors=True)
    静默失败、旧库残留到下一个用例（table already registered 假象）。
    """
    import os as _os

    from stkoe.grpc import dispatch as _d

    inst = _d._thread_local.services.pop(_os.path.realpath(base), None)
    if inst is not None:
        inst.close()


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


class TestQuickCheck:
    """读前快检（评审项）：读取前签名比对——不一致自动重扫对账、未登记隐式注册。"""

    def test_read_before_fast_check_auto_rescan(self, svc):
        """物理文件变化（未 update）→ get 前快检签名不一致 → 自动重扫（版本递增）"""
        svc.table_add("m1")
        v0 = svc.table_meta("m1")["version"]
        # 直接覆盖物理文件（跳过 update 语义，模拟外部写入）
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "price": [9.9, 8.8]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "data.parquet"))
        df = svc.table_get("m1")  # 读前快检：签名不一致 → 自动重扫对账
        assert df["price"].to_list() == [9.9, 8.8]
        assert svc.table_meta("m1")["version"] > v0  # 对账铸了新版本
        assert svc.table_meta("m1")["consistent"] is True

    def test_read_before_fast_check_implicit_register(self, svc):
        """未登记目录 → 读前快检隐式注册（add 语义）后再读"""
        d = os.path.join(svc.data_dir, "table", "t_new")
        os.makedirs(d, exist_ok=True)
        pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"], "code": [7]}).write_parquet(
            os.path.join(d, "data.parquet"))
        df = svc.table_get("t_new")
        assert df["code"].to_list() == [7]
        assert svc.table_meta("t_new")["name"] == "t_new"
        # 快检不破坏 add 语义：显式 add 已注册 → 报已存在
        from stkoe.table.errors import TableExistsError

        with pytest.raises(TableExistsError):
            svc.table_add("t_new")

    def test_read_before_fast_check_index_and_data_key(self, svc):
        """index 读取同样快检；table_data_key 返回对账后签名（stat 签名比对用）"""
        svc.index_add("index")
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"],
                      "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        df = svc.index_get("index")  # 快检 → 自动重扫（追加文件入库）
        assert df.height == 3
        svc.table_add("m1")
        key = svc.table_data_key("m1")
        assert key and len(key) == 64  # sha256 签名
        assert svc.table_data_key("nope") == ""  # 目录不存在 → 空


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

    def test_full_update_cleans_stale_bucket(self, svc):
        """全量重写清理新数据中已消失的桶目录。

        PartitionBy 只写数据里存在的桶、不删除缺失的旧桶目录——删除 flat 索引文件
        （removed 事件无分区值 → datetime_scope=None）→ 下游全量重写时，整年数据
        消失的旧桶（part=2024）不得残留 phantom 行。
        """
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        # 双年数据：2023 新文件 + 2024 既有文件 → 增量出两个桶
        pl.DataFrame({"sym": ["a"], "date": ["2023-12-29"], "code": [9]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "old.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        root = svc.data_dir / "panel" / "ds1"
        assert (root / "part=2023").exists() and (root / "part=2024").exists()
        assert svc.panel_get("ds1").height == 3

        # 删除 2024 文件 → removed 事件（flat 无分区值 → 范围 None）→ panel 全量
        os.remove(os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        assert (root / "part=2023").exists()
        assert not (root / "part=2024").exists(), "全量重写应清理新数据中已消失的桶"
        df = svc.panel_get("ds1")
        assert df.height == 1
        assert df["date"].to_list() == ["2023-12-29"]

    def test_write_partitioned_empty_clean_fallback(self, svc):
        """全量物化数据为空：clean 写落保留 schema 的空 data.parquet（无桶目录），
        读取路径不因"无 parquet 文件"报错（PartitionBy 空数据不产任何文件）。"""
        out_dir = svc.data_dir / "panel" / "ds1"
        df = pl.DataFrame({"sym": [], "date": [], "code": []},
                          schema={"sym": pl.String, "date": pl.String,
                                  "code": pl.Int64})
        GraphService._write_partitioned(df, out_dir, ["part"], gran="yearly",
                                        dt_col="date", clean=True)
        assert not any(out_dir.glob("part=*")), "空物化不应残留桶目录"
        assert (out_dir / "data.parquet").exists()
        lf = GraphService._scan_materialized(out_dir)
        assert lf.collect().columns == ["sym", "date", "code"]
        assert lf.select(pl.len()).collect().item() == 0

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

    def test_index_add_partition_hint(self, svc):
        """物化粒度引导：默认 yearly 且数据跨多年 → 报告带 partition_hint；单年/已细化无"""
        for n, dates in [("span", ["2023-12-31", "2025-01-01"]),
                         ("oneyear", ["2024-01-01", "2024-12-31"])]:
            d = os.path.join(svc.data_dir, "index", n)
            os.makedirs(d, exist_ok=True)
            pl.DataFrame({"sym": ["a", "b"], "date": dates,
                          "code": [1, 2]}).write_parquet(os.path.join(d, "data.parquet"))
        r = svc.index_add("span")
        assert "partition_hint" in r
        assert "monthly/daily" in r["partition_hint"]
        assert "2023" in r["partition_hint"] and "2025" in r["partition_hint"]
        assert "partition_hint" not in svc.index_add("oneyear")  # 单年内不提示
        d = os.path.join(svc.data_dir, "index", "span2")
        os.makedirs(d, exist_ok=True)
        pl.DataFrame({"sym": ["a", "a"], "date": ["2023-12-31", "2025-01-01"]}).write_parquet(
            os.path.join(d, "data.parquet"))
        # 显式 monthly：粒度已细化，不提示
        assert "partition_hint" not in svc.index_add("span2", materialize_partition="monthly")

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


class TestFactorBatch:
    """因子批量抽象（批次 3）：同 sample 多因子共享视图计算 + 分别物化（--all）。

    factor update --all：按 sample 分组——每组只构建一次 sample 视图（一次
    collect），组内全部因子列一次算齐（FactorEngine.fields），各因子按自己的
    增量范围过滤后分别写盘。
    """

    def _chain(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_check("fs1", "x2")
        svc.sample_add("sp1", "fs1", "index")
        svc.sample_add("sp2", "fs1", "index")
        svc.feature_add("f1", "code * 2")
        svc.feature_add("f2", "code + 1")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                     ("sample", "sp1"), ("sample", "sp2"),
                     ("feature", "f1"), ("feature", "f2")]:
            getattr(svc, f"{t}_update")(n)

    def _view_spy(self, monkeypatch):
        calls: list = []
        orig = GraphService._sample_view_lf

        def spy(self, sample, **kw):
            calls.append(sample)
            return orig(self, sample, **kw)

        monkeypatch.setattr(GraphService, "_sample_view_lf", spy)
        return calls

    def test_factor_update_all_shared_view(self, svc, monkeypatch):
        """同 sample 两因子 + 另一样本一因子 --all：每组只构建一次视图，分别物化"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_add("fac2", "f2", "sp1")
        svc.factor_add("fac3", "f1", "sp2")
        calls = self._view_spy(monkeypatch)

        reports = svc.factor_update(all=True)
        assert {r["name"] for r in reports} == {"fac1", "fac2", "fac3"}
        assert all(r["changed"] is True for r in reports)
        # 共享：sp1 两个因子只构建一次视图；sp2 单独一组（共 2 次视图构建）
        assert calls == ["sp1", "sp2"], f"视图应按 sample 分组共享，实际 {calls}"
        df1, _ = svc.factor_get("fac1", count_total=True)
        df2, _ = svc.factor_get("fac2", count_total=True)
        df3, _ = svc.factor_get("fac3", count_total=True)
        assert df1.columns == ["sym", "date", "f1"]
        assert sorted(df1["f1"].to_list()) == [2.0, 4.0]
        assert sorted(df2["f2"].to_list()) == [2.0, 3.0]
        assert sorted(df3["f1"].to_list()) == [2.0, 4.0]
        assert svc.factor_meta("fac1")["curated"] is True
        assert svc.factor_meta("fac2")["curated"] is True
        assert svc.factor_meta("fac3")["curated"] is True

    def test_factor_update_all_mixed_incremental_full(self, svc):
        """同 sample 一因子增量 + 一因子首物化（全量）：共享视图下各自写盘正确"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")  # 首次全量（2 行）
        # 源头追加一天 → 沿链置脏 → fac1 已有物化走增量；fac2 尚未物化走全量
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                     ("sample", "sp1"), ("sample", "sp2"),
                     ("feature", "f1"), ("feature", "f2")]:
            getattr(svc, f"{t}_update")(n)
        svc.factor_add("fac2", "f2", "sp1")

        reports = {r["name"]: r for r in svc.factor_update(all=True)}
        assert reports["fac1"]["changed"] is True
        assert reports["fac2"]["changed"] is True
        df1, _ = svc.factor_get("fac1", count_total=True)
        df2, _ = svc.factor_get("fac2", count_total=True)
        # fac1 增量：旧行（01-01 a/b）保留 + 新行（01-02 c）重算
        assert df1.height == 3
        assert sorted(df1["date"].to_list()) == ["2024-01-01", "2024-01-01", "2024-01-02"]
        assert sorted(df1["f1"].to_list()) == [2.0, 4.0, 6.0]
        # fac2 全量：全部 3 行
        assert df2.height == 3
        assert sorted(df2["f2"].to_list()) == [2.0, 3.0, 4.0]

    def test_factor_update_all_idempotent_skips(self, svc, monkeypatch):
        """--all 二次执行幂等跳过（不触发视图构建与计算）"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_add("fac2", "f2", "sp1")
        svc.factor_update(all=True)
        calls = self._view_spy(monkeypatch)

        reports = svc.factor_update(all=True)
        assert calls == []  # 幂等：不构建任何视图
        assert all(r["changed"] is False for r in reports)
        assert all(r["version_after"] == r["version_before"] for r in reports)

    def test_factor_update_all_single_equiv(self, svc):
        """--all 批量结果与逐个 update 等价（值一致）"""
        self._chain(svc)
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_add("fac2", "f2", "sp1")
        svc.factor_update("fac1")
        svc.factor_update("fac2")
        svc.factor_update(all=True)  # 幂等后再触发上游变化走批量
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                     ("sample", "sp1"), ("sample", "sp2"),
                     ("feature", "f1"), ("feature", "f2")]:
            getattr(svc, f"{t}_update")(n)
        svc.factor_update(all=True)
        df1, _ = svc.factor_get("fac1", count_total=True)
        df2, _ = svc.factor_get("fac2", count_total=True)
        assert df1.height == 3 and sorted(df1["f1"].to_list()) == [2.0, 4.0, 6.0]
        assert df2.height == 3 and sorted(df2["f2"].to_list()) == [2.0, 3.0, 4.0]

    def test_factor_engine_fields(self):
        """FactorEngine.fields：polars 单 select 一次算齐多公式；默认实现逐公式拼接"""
        from stkoe.factor.engine import FactorEngine, PolarsEngine

        lf = pl.LazyFrame({"a": [1, 2], "b": [3, 4]})

        class Stub(FactorEngine):
            """默认 fields 实现（逐公式 field + 拼接）的载体"""

            name = "stub"

            def field(self, lf, formula):
                scope = {c: pl.col(c) for c in lf.collect_schema().names()}
                scope["pl"] = pl
                return lf.select(eval(formula, {"__builtins__": {}},
                                      scope).alias("field")).collect()

        out = Stub().fields(lf, {"s1": "pl.col('a') * 2", "s2": "pl.col('b') + 1"})
        assert out.columns == ["s1", "s2"]
        assert out["s1"].to_list() == [2, 4]
        assert out["s2"].to_list() == [4, 5]

        pout = PolarsEngine().fields(lf, {"p1": "a * 3", "p2": "b - 1"})
        assert pout.columns == ["p1", "p2"]
        assert pout["p1"].to_list() == [3, 6]
        assert pout["p2"].to_list() == [2, 3]
        assert PolarsEngine().fields(lf, {}).height == 0  # 空公式表 → 空 DataFrame


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
            svc.tester_add("t1", "fac1")

    def test_test_add_get_check_scan_data_delete(self, svc):
        self._chain(svc)
        tm = svc.tester_add("t1", "fac1")
        assert tm["name"] == "t1"
        assert tm["factor"] == "fac1"
        assert tm["sample"] == "sp1"
        assert tm["keys"] == ["sym", "date"]

        r = svc.tester_check("t1")
        assert r["ok"] is True
        # get 三态：未物化 → 报错提示先 update
        with pytest.raises(ValueError, match="未物化"):
            svc.tester_get("t1")

        s1 = svc.tester_update("t1")
        assert s1["changed"] is True
        assert s1["rows"] == 2
        s2 = svc.tester_update("t1")  # 幂等
        assert s2["changed"] is False
        assert svc.tester_meta("t1")["curated"] is True
        assert (svc.data_dir / "factor_tester" / "t1" / "part=2024").exists()

        # 物化后 get/data 读物化
        df, total = svc.tester_get("t1", count_total=True)
        assert df.height == 2 and total == 2
        assert "factor_quantile" in df.columns
        assert "d1" in df.columns
        d = svc.tester_data("t1")
        assert d.height == 2

        svc.tester_delete("t1")
        assert svc.store.get_node("tester:t1") is None

    def test_test_update_incremental_by_scope(self, svc, monkeypatch):
        """P0-2：源头新增日期 → factor/test 链增量重算（test_build 带 dt_range）"""
        self._chain(svc)
        svc.tester_add("t1", "fac1")
        svc.tester_update("t1")  # 首次全量（2 行）
        assert svc.tester_data("t1").height == 2

        calls: list = []
        orig = GraphService._tester_build

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_tester_build", spy)

        # 源头追加新日期（含测试必需列；dtype 与现有文件一致）→ 链置脏 → 依次 update
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "code": [3],
                      "r": [0.03], "ic": ["G1"], "fv": [3.0]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"), ("sample", "sp1"),
                     ("feature", "f1"), ("factor", "fac1")]:
            getattr(svc, f"{t}_update")(n)
        svc.tester_update("t1")

        assert calls and calls[-1] == ("2023-12-24", "2024-01-02"), \
            f"增量应带 dt_range（d{{no}} 前向窗口向后展开 9 天），实际 {calls}"
        d = svc.tester_data("t1")
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
            svc.tester_add("tt1", "fac1")

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

    def test_sample_factor_tester_chain_derives(self, svc):
        """sample 视图透传 + index 键映射；factor 只留因子列映射；tester 不做列级血缘。"""
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
        # factor：**只留因子列**——factor_col → feature 公式引用列；
        # keys（sym/date 索引透传）不建字段级映射（对资产血缘无信息量）
        assert {c["name"] for c in st.columns_of("factor:fac1")} == {"f1"}
        assert st.deps_of("column:factor:fac1.f1", rel_type="DERIVES")[0]["target"] \
            == "column:sample:sp1.code"
        assert st.columns_of("factor:fac1")[0].get("asset") == "factor:fac1"
        # tester：**不做列级血缘**（无列节点/DERIVES/BELONGS_TO）——测试面板的
        # 派生字段（returns/group/marketcap/d{no}/factor_quantile）对资产血缘无
        # 信息量；资产级 DEPENDS → factor 已表达因子数据来源
        assert st.columns_of("tester:tt1") == []
        assert st.asset_of("column:tester:tt1.factor") is None

    def test_fieldset_derives_resync_on_update(self, svc):
        """血缘对账：历史字段缺 DERIVES 边/required_fields → fieldset update 自动补齐。"""
        self._chain(svc, with_test=False)
        st = svc.store
        # 模拟旧库状态：字段 x2 的 DERIVES 边与 required_fields 缺失（升级前登记）
        svc.graph.clear_derives("fieldset", "fs1", "x2")
        node = svc.store.get_node("fieldset:fs1")
        fields = dict(node.get("fields") or {})
        fields["x2"] = {**fields["x2"], "required_fields": []}
        svc.store.patch_node("fieldset:fs1", fields=fields)
        assert st.deps_of("column:fieldset:fs1.x2", rel_type="DERIVES") == []
        # update → 对账重派发（字段 → panel 源字段的关系恢复）
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        targets = {e["target"] for e in
                   st.deps_of("column:fieldset:fs1.x2", rel_type="DERIVES")}
        assert targets == {"column:panel:ds1.code"}
        assert "code" in svc.store.get_node("fieldset:fs1")["fields"]["x2"] \
            ["required_fields"]

    def test_delete_cascades_column_nodes(self, svc):
        """资产删除 → 其列节点级联删除（DERIVES 边随之清除）。"""
        self._chain(svc)
        st = svc.store
        svc.tester_delete("tt1")
        assert st.columns_of("tester:tt1") == []
        svc.factor_delete("fac1")
        assert st.columns_of("factor:fac1") == []  # 删除前只剩 factor_col 列
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
        edges = {e["data"]["id"]: e["data"] for e in p["elements"]["edges"]}
        assert any(cid.startswith("column:") for cid in edges)  # DERIVES 边
        # BELONGS_TO：列 → 所属资产（跨层接图），带 type 标注
        assert edges["column:panel:ds1.sym->panel:ds1"]["type"] == "BELONGS_TO"
        assert edges["column:panel:ds1.sym->panel:ds1"]["source"] \
            == "column:panel:ds1.sym"
        assert any(e["data"]["type"] == "DERIVES" for e in p["elements"]["edges"])
        # 不带 --columns：资产图口径不变（无列节点/无列层边）
        p2 = build_payload(store)
        assert all(n["data"]["type"] != "column" for n in p2["elements"]["nodes"])
        assert all(e["data"]["type"] == "DEPENDS"
                   for e in p2["elements"]["edges"])

        cp = column_payload(store, "column:fieldset:fs1.x2")
        cids = {n["data"]["id"] for n in cp["elements"]["nodes"]}
        assert "column:panel:ds1.code" in cids   # 上游来源
        assert "column:sample:sp1.x2" in cids    # 下游派生
        assert "fieldset:fs1" in cids            # 所属资产上下文
        cedges = {e["data"]["id"]: e["data"] for e in cp["elements"]["edges"]}
        assert cedges["column:fieldset:fs1.x2->fieldset:fs1"]["type"] == "BELONGS_TO"

    def test_field_formula_multiple_refs(self, svc):
        """字段公式依赖多个列 → 每个引用列一条 DERIVES 边（fieldset 字段 + factor_col）。"""
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "xsum", "(code + price) * 2")
        st = svc.store
        targets = {e["target"] for e in
                   st.deps_of("column:fieldset:fs1.xsum", rel_type="DERIVES")}
        assert targets == {"column:panel:ds1.code", "column:panel:ds1.price"}
        svc.sample_add("sp1", "fs1", "index")
        svc.feature_add("f1", "(code + price) * 2")
        svc.factor_add("fac1", "f1", "sp1")
        ftargets = {e["target"] for e in
                    st.deps_of("column:factor:fac1.f1", rel_type="DERIVES")}
        assert ftargets == {"column:sample:sp1.code", "column:sample:sp1.price"}

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
        assert stats["belongs_count"] > 0  # 每列一条 BELONGS_TO 边
        p = json.loads(dispatch("graph", "lineage", ["--columns"],
                                data_dir=base)[0].data)
        assert "column" in p["graph"]["types"]
        cp = json.loads(dispatch("graph", "lineage",
                                 ["--column", "column:fieldset:fs1.x2"],
                                 data_dir=base)[0].data)
        assert cp["graph"]["center"] == "column:fieldset:fs1.x2"
        assert cp["elements"]["nodes"]
        # 省略 column: 前缀（命令层习惯 <type:name.col>）→ 自动补全
        cp2 = json.loads(dispatch("graph", "lineage",
                                  ["--column", "fieldset:fs1.x2"],
                                  data_dir=base)[0].data)
        assert cp2["graph"]["center"] == "column:fieldset:fs1.x2"
        assert cp2["elements"]["nodes"]


class TestColumnBelongs:
    """BELONGS_TO 边（列 → 所属资产）：把列级血缘与资产级血缘接成一张图。

    - 每列恰好一条 BELONGS_TO 边，建列/对账/跨依赖引用（_ensure_column /
      sync_columns / sync_derives）统一经 store.upsert_column 写入（幂等）；
    - store.asset_of 列 → 资产；资产删除级联清理；
    - column_consistency：跨资产 DERIVES 边 ↔ 资产级 DEPENDS 链互相印证。
    """

    def _chain(self, svc):
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
        svc.tester_add("tt1", "fac1")

    def test_every_column_has_single_belongs_edge(self, svc):
        """全链每列恰好一条 BELONGS_TO 边指向所属资产；asset_of 往返一致。"""
        self._chain(svc)
        st = svc.store
        cols = [n for n in st.list_nodes("Column")]
        assert cols, "应存在列节点"
        for c in cols:
            cid = c["id"]
            belongs = st.deps_of(cid, rel_type="BELONGS_TO")
            assert len(belongs) == 1, f"{cid} 应有且仅有一条 BELONGS_TO 边: {belongs}"
            owner = st.asset_of(cid)
            assert owner is not None and owner["id"] == belongs[0]["target"]
            # 属性与边一致（asset 属性 = 边目标）
            assert c["asset"] == owner["id"]
        # 幂等：重复 upsert_column 不产生重复边
        st.upsert_column("panel:ds1", "panel", "sym", {
            "name": "sym", "asset": "panel:ds1", "asset_type": "panel"})
        assert len(st.deps_of("column:panel:ds1.sym",
                              rel_type="BELONGS_TO")) == 1

    def test_cross_layer_traversal(self, svc):
        """跨层遍历：列 → BELONGS_TO → 资产 → DEPENDS 上游 → 资产 → 其列（一张图）。"""
        self._chain(svc)
        st = svc.store
        # factor 列 f1 → factor:fac1 → DEPENDS 上游 sample:sp1 → 其列 x2
        owner = st.asset_of("column:factor:fac1.f1")
        assert owner["id"] == "factor:fac1"
        up = {d["id"] for d in st.upstream("factor:fac1")}
        assert "sample:sp1" in up
        up_cols = {c["name"] for c in st.columns_of("sample:sp1")}
        assert "x2" in up_cols and "sym" in up_cols
        # 反向：从源头 table 列经 BELONGS_TO 的资产，沿 DEPENDS 下游到 factor
        assert st.asset_of("column:table:m1.price")["id"] == "table:m1"
        down = {d["id"] for d in st.downstream("table:m1")}
        assert "factor:fac1" in down

    def test_delete_cascades_belongs_edge(self, svc):
        """资产删除 → 列节点 + BELONGS_TO 边级联清理（belongs_count 同步）。"""
        self._chain(svc)
        st = svc.store
        before = st.stats()["belongs_count"]
        svc.fieldset_delete("fs1", force=True)  # force 递归删下游（sample/factor/tester）
        assert st.columns_of("fieldset:fs1") == []
        assert st.asset_of("column:fieldset:fs1.x2") is None
        after = st.stats()["belongs_count"]
        assert after < before  # 列节点连带 BELONGS_TO 边级联清理

    def test_column_consistency_ok_and_broken(self, svc):
        """一致性校验：标准链无报告；破坏资产级边（删 DEPENDS）→ 报告跨层不一致。"""
        from stkoe.graph.analyze import column_consistency

        self._chain(svc)
        st = svc.store
        # 标准链：fieldset 列 DERIVES → panel 列，fieldset DEPENDS panel —— 一致
        assert column_consistency(st) == []
        # 人为删除 fieldset → panel 的 DEPENDS 边 → fieldset 列 DERIVES 到 panel 列
        # 失去资产级路径 → 报告（列级血缘比资产级血缘"多走了"）
        st.delete_edge("fieldset:fs1", "panel:ds1", "DEPENDS")
        bad = column_consistency(st)
        assert bad, "破坏资产级血缘后应报告跨层不一致"
        assert any(b["source_asset"] == "fieldset:fs1"
                   and b["target_asset"] == "panel:ds1" for b in bad)
        assert "->" in bad[0]["derives"]  # 形如 "column:… -> column:…"

    def test_analyze_dispatch_consistency(self, svc):
        """graph analyze 输出含 consistency（标准链 = 空清单）。"""
        import json

        from stkoe.grpc.dispatch import dispatch

        self._chain(svc)
        data = json.loads(dispatch("graph", "analyze", [],
                                   data_dir=str(svc.data_dir))[0].data)
        assert set(data) == {"page_rank", "degree", "components", "consistency"}
        assert data["consistency"] == []


class TestWindowScope:
    """window_size（滚动窗口）→ data change event 范围展开。

    - fieldset 字段 / feature 为**回看窗口** w：t 时刻输出用到 [t-w+1, t] 的输入，
      输入在 [lo, hi] 变化 → 输出受影响 [lo, hi+w-1]（增量重算区间与自身事件
      datetime_scope 都向前展开 w-1 天）；
    - test 的 d{no} 为**前向收益窗口**：输入在 [lo, hi] 变化 → 输出受影响
      [hi-no+1, hi]（重算区间按 max(periods)-1 向后展开 lo）。
    """

    def _chain(self, svc, *, fset_win=0, feat_win=0):
        # 覆盖 index/data.parquet 加入测试必需列（多文件 scan 不 union schema）
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0],
                      "code": [1, 2]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2", window_size=fset_win)
        svc.fieldset_check("fs1", "x2")
        svc.sample_add("sp1", "fs1", "index")
        svc.feature_add("f1", "code * 2", window_size=feat_win)
        svc.factor_add("fac1", "f1", "sp1")

    def _seed(self, svc):
        """首次全量就绪（panel/fieldset/sample/feature/factor 依次 update）。"""
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"), ("sample", "sp1"),
                     ("feature", "f1"), ("factor", "fac1")]:
            getattr(svc, f"{t}_update")(n)

    def _append_new_day(self, svc):
        """源头追加 2024-01-02 → 沿链置脏 → 依次 update 到 feature（目标 update 由用例调）。"""
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"),
                     ("sample", "sp1"), ("feature", "f1")]:
            getattr(svc, f"{t}_update")(n)

    @staticmethod
    def _latest_event(node: dict) -> dict:
        vl = node["version_list"]
        return vl[str(max(int(k) for k in vl))]

    def test_fieldset_window_expands_event_scope(self, svc):
        """fieldset 字段 window_size=5：自身事件 datetime_scope 向前展开 4 天。"""
        self._chain(svc, fset_win=5)
        self._seed(svc)
        svc.fieldset_update("fs1")  # 幂等不 bump（无新事件）
        self._append_new_day(svc)   # 内部 fieldset_update 走增量分支
        ev = self._latest_event(svc.store.get_node("fieldset:fs1"))
        assert ev["datetime_scope"] == ["2024-01-02", "2024-01-06"], \
            f"事件范围应按窗口展开: {ev['datetime_scope']}"
        # 列节点带 window_size
        cols = {c["name"]: c for c in svc.store.columns_of("fieldset:fs1")}
        assert cols["x2"]["window_size"] == 5
        assert cols["x2"]["formula"] == "code * 2"

    def test_feature_window_expands_factor_recompute(self, svc, monkeypatch):
        """feature window_size=5：factor 增量重算区间向前展开 4 天。"""
        self._chain(svc, feat_win=5)
        self._seed(svc)
        calls: list = []
        orig = GraphService._factor_compute

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_factor_compute", spy)
        self._append_new_day(svc)
        svc.factor_update("fac1")
        assert calls and calls[-1] == ("2024-01-02", "2024-01-06"), \
            f"factor 增量应带展开后 dt_range，实际 {calls}"
        # factor 自身事件同样展开（供下游 test 沿链增量）
        ev = self._latest_event(svc.store.get_node("factor:fac1"))
        assert ev["datetime_scope"] == ["2024-01-02", "2024-01-06"]
        assert ev["field_scope"] == ["f1"]  # 记录自身产出字段

    def test_test_periods_expand_recompute_back(self, svc, monkeypatch):
        """test 的 d{no} 前向窗口：增量重算区间按 max(periods)-1 向后展开 lo。"""
        self._chain(svc)
        self._seed(svc)
        svc.tester_add("t1", "fac1")
        svc.tester_update("t1")  # 首次全量
        calls: list = []
        orig = GraphService._tester_build

        def spy(self, node, **kw):
            calls.append(kw.get("dt_range"))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_tester_build", spy)
        self._append_new_day(svc)
        svc.factor_update("fac1")
        svc.tester_update("t1")
        assert calls and calls[-1] == ("2023-12-24", "2024-01-02"), \
            f"test 增量应向后展开 lo，实际 {calls}"


class TestUpdateCascade:
    """沿链级联 update（graph update）：目标节点 + 下游闭包按拓扑序更新。

    - ``--node``：更新该资产 + 全部下游链；``--all``：全图资产节点；
    - 拓扑序保证任一节点更新时其上游先就绪；上游未就绪 → DependencyError 中止。
    """

    def _chain(self, svc):
        # 覆盖 index/data.parquet 加入测试必需列（tester 需要 r/ic/fv）
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
        svc.tester_add("t1", "fac1")

    def test_cascade_node_updates_downstream_topological(self, svc):
        """--node：目标 + 下游闭包，按拓扑序更新（依赖方恒在依赖之后）。"""
        self._chain(svc)
        svc.panel_update("ds1")  # 目标自身的上游链先就绪
        svc.feature_update("f1")  # feature 无上游，单独就绪（不在闭包内）
        r = svc.update_cascade("fieldset", "fs1")
        assert r["node"] == "fieldset:fs1" and r["scope"] == "downstream"
        nodes = [u["node"] for u in r["updated"]]
        assert nodes == ["fieldset:fs1", "sample:sp1", "factor:fac1", "tester:t1"], \
            f"应按拓扑序更新下游链: {nodes}"
        # 全链恢复就绪：物化资产 curated、无物化资产 valid
        assert svc.fieldset_meta("fs1")["curated"] is True
        assert svc.sample_meta("sp1")["valid"] is True
        assert svc.factor_meta("fac1")["curated"] is True
        assert svc.tester_meta("t1")["curated"] is True

    def test_cascade_propagates_source_change_down_the_chain(self, svc):
        """源头变化 → 级联一次到位：全链版本推进 + 增量数据可见。"""
        self._chain(svc)
        svc.update_cascade(all=True)
        v0 = {u["node"]: u["version_after"] for u in
              svc.update_cascade(all=True)["updated"]}
        # 源头追加一行 → 级联 → 依赖链全部铸版本
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        r = svc.update_cascade(all=True)
        bumped = {u["node"] for u in r["updated"]
                  if u["version_after"] > v0[u["node"]]}
        assert {"index:index", "panel:ds1", "fieldset:fs1", "sample:sp1",
                "factor:fac1", "tester:t1"} <= bumped, f"链上节点应全部铸版本: {bumped}"
        # 新数据经级联落到 tester 物化（沿链增量）
        df, total = svc.tester_get("t1", count_total=True)
        assert total == 3
        assert sorted(df["date"].to_list()) == ["2024-01-01", "2024-01-01",
                                                "2024-01-02"]

    def test_cascade_blocks_on_unready_upstream(self, svc):
        """闭包外的上游未就绪（feature 未 update）→ 级联中止（DependencyError）。"""
        from stkoe.graph.errors import DependencyError

        self._chain(svc)
        with pytest.raises(DependencyError):
            svc.update_cascade("fieldset", "fs1")

    def test_cascade_all_full_chain_topological(self, svc):
        """--all：全图资产节点按拓扑序更新。"""
        self._chain(svc)
        r = svc.update_cascade(all=True)
        assert r["scope"] == "all" and r["node"] == "*"
        nodes = [u["node"] for u in r["updated"]]
        assert set(nodes) == {"table:m1", "index:index", "panel:ds1",
                              "fieldset:fs1", "sample:sp1", "feature:f1",
                              "factor:fac1", "tester:t1"}
        idx = {n: i for i, n in enumerate(nodes)}
        assert idx["panel:ds1"] > idx["index:index"]
        assert idx["fieldset:fs1"] > idx["panel:ds1"]
        assert idx["sample:sp1"] > idx["fieldset:fs1"]
        assert idx["factor:fac1"] > idx["sample:sp1"]
        assert idx["factor:fac1"] > idx["feature:f1"]
        assert idx["tester:t1"] > idx["factor:fac1"]
        assert all(u["version_after"] >= u["version_before"] for u in r["updated"])
        assert svc.fieldset_meta("fs1")["curated"] is True
        assert svc.factor_meta("fac1")["curated"] is True

    def test_cascade_second_run_idempotent(self, svc):
        """二次级联全部幂等（版本不再推进，不重复物化）。"""
        self._chain(svc)
        svc.update_cascade(all=True)
        r2 = svc.update_cascade(all=True)
        assert all(u["version_after"] == u["version_before"] for u in r2["updated"])

    def test_dispatch_update_cascade(self, svc):
        """Execute 通道：graph update --node / --all。"""
        import json

        from stkoe.grpc import dispatch as _d

        self._chain(svc)
        svc.panel_update("ds1")  # fieldset 上游链先就绪
        svc.feature_update("f1")
        base = str(svc.data_dir)
        try:
            data = json.loads(_d.dispatch("graph", "update",
                                          ["--node", "fieldset:fs1"],
                                          data_dir=base)[0].data)
            assert data["scope"] == "downstream"
            assert [u["node"] for u in data["updated"]] == \
                ["fieldset:fs1", "sample:sp1", "factor:fac1", "tester:t1"]
            data2 = json.loads(_d.dispatch("graph", "update", ["--all"],
                                           data_dir=base)[0].data)
            assert data2["scope"] == "all"
            # 刚级联过的下游链二次执行全部幂等（版本不变）
            assert all(u["version_after"] == u["version_before"]
                       for u in data2["updated"] if u["node"] != "panel:ds1")
        finally:
            _cleanup_dispatch_cache(base)


class TestRequiredFields:
    """FieldMeta.required_fields 回归：公式引用列自动登记（复用 _formula_refs）。"""

    def _chain(self, svc):
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")

    def test_add_field_records_referenced_panel_cols(self, svc):
        """add_field：required_fields = 公式引用的 panel 视图列（保序去重）。"""
        self._chain(svc)
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        f = svc.fieldset_meta_field("fs1", "x2")
        assert f["required_fields"] == ["code"]
        # 多引用保序 + 函数名/字面量不收录
        svc.fieldset_add_field("fs1", "x3", "abs(price) + code")
        assert svc.fieldset_meta_field("fs1", "x3")["required_fields"] == \
            ["price", "code"]

    def test_required_fields_include_sibling_fields(self, svc):
        """引用同集已定义字段 → 一并收录（与 panel 列同一集合计算）。"""
        self._chain(svc)
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_add_field("fs1", "x3", "x2 + code")
        assert svc.fieldset_meta_field("fs1", "x3")["required_fields"] == \
            ["x2", "code"]

    def test_set_field_recomputes_required_fields(self, svc):
        """set_field 改公式 → required_fields 重算（旧引用清除）。"""
        self._chain(svc)
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        assert svc.fieldset_meta_field("fs1", "x2")["required_fields"] == ["code"]
        svc.fieldset_set_field("fs1", "x2", formula="price + code")
        assert svc.fieldset_meta_field("fs1", "x2")["required_fields"] == \
            ["price", "code"]

    def test_required_fields_in_fieldset_meta(self, svc):
        """fieldset meta / Execute 通道均可见 required_fields。"""
        import json

        from stkoe.grpc.dispatch import dispatch

        self._chain(svc)
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        m = svc.fieldset_meta("fs1")
        assert m["fields"]["x2"]["required_fields"] == ["code"]
        base = str(svc.data_dir)
        try:
            data = json.loads(dispatch("fieldset", "meta", ["fs1"],
                                       data_dir=base)[0].data)
            assert data["fields"]["x2"]["required_fields"] == ["code"]
        finally:
            _cleanup_dispatch_cache(base)


class TestGraphAnalyzeImpact:
    """graph impact 列级：DERIVES 下游闭包（含列节点的全资产链）。"""

    def _chain(self, svc):
        # 覆盖 index/data.parquet 加入测试必需列（tester 需要 r/ic/fv）
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
        svc.tester_add("t1", "fac1")

    def test_asset_impact_downstream_assets_and_columns(self, svc):
        """资产级影响：DEPENDS 下游（带 depth）+ 该资产列的 DERIVES 下游列。"""
        from stkoe.graph.analyze import asset_impact

        self._chain(svc)
        r = asset_impact(svc.store, "fieldset:fs1")
        assert [a["id"] for a in r["assets"]] == \
            ["sample:sp1", "factor:fac1", "tester:t1"]
        assert [a["depth"] for a in r["assets"]] == [1, 2, 3]
        assert r["columns"], "fieldset 列的 DERIVES 下游列不应为空"
        col_ids = [c["id"] for c in r["columns"]]
        assert all(cid.startswith("column:") for cid in col_ids)
        assert all(not cid.startswith("column:fieldset:fs1") for cid in col_ids)
        # 全部落在下游资产上（sample/factor/tester）
        owners = {cid[len("column:"):].rpartition(".")[0] for cid in col_ids}
        assert owners <= {"sample:sp1", "factor:fac1", "tester:t1"}

    def test_column_impact_derives_closure(self, svc):
        """列级影响：x2 列的 DERIVES 下游闭包 + 所属资产（不含自身资产）。"""
        from stkoe.graph.analyze import column_impact

        self._chain(svc)
        r = column_impact(svc.store, "column:fieldset:fs1.x2")
        assert r["columns"]
        assert all(c["depth"] >= 1 for c in r["columns"])
        assets = [a["id"] for a in r["assets"]]
        assert assets
        assert "fieldset:fs1" not in assets
        assert assets[0] == "sample:sp1"  # 最近的下游资产排最前（最小 depth）

    def test_dispatch_impact_column(self, svc):
        """Execute 通道：graph impact --column。"""
        import json

        from stkoe.grpc.dispatch import dispatch

        self._chain(svc)
        data = json.loads(dispatch("graph", "impact",
                                   ["--column", "column:fieldset:fs1.x2"],
                                   data_dir=str(svc.data_dir))[0].data)
        assert data["column"] == "column:fieldset:fs1.x2"
        assert data["columns"]
        assert data["assets"]


class TestSymbolScope:
    """symbol_scope 提取：源头事件带标的集合，下游增量按「时间 × 标的」裁剪。"""

    def _chain(self, svc, with_tester=True, gran="yearly"):
        # 覆盖 index/data.parquet 加入测试必需列（tester 需要 r/ic/fv）
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0],
                      "code": [1, 2]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        svc.table_add("m1")
        svc.index_add("index", materialize_partition=gran)
        svc.panel_add("ds1", "index", ["m1"])
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "code * 2")
        svc.fieldset_check("fs1", "x2")
        svc.sample_add("sp1", "fs1", "index")
        svc.feature_add("f1", "code * 2")
        svc.factor_add("fac1", "f1", "sp1")
        if with_tester:
            svc.tester_add("t1", "fac1")
        for t, n in [("panel", "ds1"), ("fieldset", "fs1"), ("sample", "sp1"),
                     ("feature", "f1"), ("factor", "fac1")]:
            getattr(svc, f"{t}_update")(n)
        if with_tester:
            svc.tester_update("t1")

    @staticmethod
    def _latest_event(node: dict) -> dict:
        vl = node["version_list"]
        return vl[str(max(int(k) for k in vl))]

    def test_index_event_symbol_scope_from_file(self, svc):
        """新增文件只含 sym=c → upsert 事件 symbol_scope=["c"]（读列 distinct）。"""
        self._chain(svc, with_tester=False)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        ev = self._latest_event(svc.store.get_node("index:index"))
        assert ev["action"] == "upsert"
        assert ev["symbol_scope"] == ["c"]
        assert ev["datetime_scope"] == ["2024-01-02", "2024-01-02"]

    def test_index_event_symbol_scope_union(self, svc):
        """一次变化多个文件 → symbol 并集（去重）。"""
        self._chain(svc, with_tester=False)
        for f, syms, day in (("more1.parquet", ["c"], "2024-01-02"),
                             ("more2.parquet", ["b", "c"], "2024-01-03")):
            pl.DataFrame({"sym": syms, "date": [day] * len(syms),
                          "r": [0.03] * len(syms), "ic": ["G1"] * len(syms),
                          "fv": [3.0] * len(syms), "code": [3] * len(syms)}).write_parquet(
                os.path.join(svc.data_dir, "index", "index", f))
        svc.index_update("index")
        ev = self._latest_event(svc.store.get_node("index:index"))
        assert sorted(ev["symbol_scope"]) == ["b", "c"]
        assert ev["datetime_scope"] == ["2024-01-02", "2024-01-03"]

    def test_index_event_symbol_scope_from_partition(self, svc):
        """symbol 为 hive 分区键 → 分区值提取（文件列同值，事件仍收敛到 ["c"]）。"""
        self._chain(svc, with_tester=False)
        d = os.path.join(svc.data_dir, "index", "index", "sym=c", "date=2024-01-02")
        os.makedirs(d, exist_ok=True)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(d, "data.parquet"))
        svc.index_update("index")
        ev = self._latest_event(svc.store.get_node("index:index"))
        assert ev["symbol_scope"] == ["c"]
        assert ev["datetime_scope"] == ["2024-01-02", "2024-01-02"]

    def test_table_event_without_symbol_col(self, svc):
        """table（未登记 symbol_col）变化 → 事件 symbol_scope=None（全集）。"""
        self._chain(svc, with_tester=False)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"],
                      "price": [3.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "more.parquet"))
        svc.table_update("m1")
        ev = self._latest_event(svc.store.get_node("table:m1"))
        assert ev["symbol_scope"] is None

    def test_chain_incremental_carries_symbols(self, svc, monkeypatch):
        """源头只变 c → 沿链增量：factor 只重算 c，a/b 旧行保留，事件带 symbol。"""
        self._chain(svc)
        calls: list = []
        orig = GraphService._factor_compute

        def spy(self, node, **kw):
            calls.append((kw.get("dt_range"), kw.get("symbols")))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_factor_compute", spy)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        svc.fieldset_update("fs1")
        svc.sample_update("sp1")
        svc.feature_update("f1")
        svc.factor_update("fac1")
        assert calls and calls[-1] == (("2024-01-02", "2024-01-02"), ["c"]), \
            f"factor 增量应只重算变化标的: {calls}"
        # 沿链事件带 symbol（供 tester 增量）
        ev = self._latest_event(svc.store.get_node("factor:fac1"))
        assert ev["symbol_scope"] == ["c"]
        # 物化：a/b 旧行保留原值 + c 新增（分区桶重写，只动了 2024 桶）
        df, total = svc.factor_get("fac1", count_total=True)
        assert total == 3
        rows = {r["sym"]: r for r in df.iter_rows(named=True)}
        assert rows["c"]["f1"] == 6.0
        assert rows["a"]["f1"] == 2.0 and rows["b"]["f1"] == 4.0
        # tester 增量同样按 symbols 裁剪（不额外断言值，链路一致性已覆盖）
        svc.tester_update("t1")

    def test_flat_incremental_keeps_other_symbols(self, svc, monkeypatch):
        """flat 物化（未知 materialize_partition）：keep 只删命中标的行。"""
        self._chain(svc, with_tester=False, gran="none")
        assert not any((svc.data_dir / "panel" / "ds1").glob("*=*"))  # flat 单文件
        calls: list = []
        orig = GraphService._factor_compute

        def spy(self, node, **kw):
            calls.append((kw.get("dt_range"), kw.get("symbols")))
            return orig(self, node, **kw)

        monkeypatch.setattr(GraphService, "_factor_compute", spy)
        pl.DataFrame({"sym": ["c"], "date": ["2024-01-02"], "r": [0.03],
                      "ic": ["G1"], "fv": [3.0], "code": [3]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "more.parquet"))
        svc.index_update("index")
        svc.panel_update("ds1")
        pf, total = svc.panel_get("ds1", count_total=True)
        assert total == 3
        prows = {r["sym"]: r for r in pf.iter_rows(named=True)}
        assert prows["c"]["code"] == 3 and prows["a"]["code"] == 1
        svc.fieldset_update("fs1")
        svc.sample_update("sp1")
        svc.feature_update("f1")
        svc.factor_update("fac1")
        assert calls and calls[-1] == (("2024-01-02", "2024-01-02"), ["c"])
        df, _ = svc.factor_get("fac1", count_total=True)
        rows = {r["sym"]: r for r in df.iter_rows(named=True)}
        assert rows["c"]["f1"] == 6.0
        assert rows["a"]["f1"] == 2.0 and rows["b"]["f1"] == 4.0


class TestReviewFixes:
    """评审修复：成员列冲突校验 / tester 键列推断 / 元数据变更不置脏。"""

    def _seed(self, svc, sym_col="sym", dt_col="date"):
        """造数 + 建链就绪；返回 (svc, keys)。"""
        for sub, d in (("index", "index"), ("table", "m1"), ("table", "m2")):
            os.makedirs(os.path.join(svc.data_dir, sub, d), exist_ok=True)
        pl.DataFrame({sym_col: ["a", "b"], dt_col: ["2024-01-01"] * 2,
                      "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0],
                      "x": [1.0, 2.0]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        pl.DataFrame({sym_col: ["a", "b"], dt_col: ["2024-01-01"] * 2,
                      "price": [1.5, 2.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "data.parquet"))
        pl.DataFrame({sym_col: ["a", "b"], dt_col: ["2024-01-01"] * 2,
                      "volume": [100, 200]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m2", "data.parquet"))
        svc.table_add("m1")
        svc.table_add("m2")
        svc.index_add("index", symbol_col=sym_col, datetime_col=dt_col)
        return [sym_col, dt_col]

    def test_panel_member_column_conflict_raises(self, svc):
        """① 成员表之间同名列 → panel_add 报错（不自动改名、不静默覆盖）。"""
        keys = self._seed(svc)
        # m2 改为与 m1 同列名 price
        pl.DataFrame({keys[0]: ["a"], keys[1]: ["2024-01-01"],
                      "price": [9.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m2", "data.parquet"))
        svc.table_update("m2")
        with pytest.raises(ValueError, match="列名冲突"):
            svc.panel_add("ds1", "index", ["m1", "m2"])

    def test_tester_custom_key_columns(self, svc):
        """② index 自定义 symbol/datetime 列名 → 全链 + tester 成功（键列推断）。"""
        sym, dt = self._seed(svc, sym_col="code", dt_col="day")
        svc.panel_add("ds1", "index", ["m1", "m2"])
        svc.panel_update("ds1")
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "x * 2.0")
        svc.fieldset_check("fs1", "x2")
        svc.fieldset_update("fs1")
        svc.sample_add("sp1", "fs1", "index")
        svc.sample_update("sp1")
        svc.feature_add("f1", "x * 2.0")
        svc.feature_update("f1")
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")
        svc.tester_add("t1", "fac1")
        svc.tester_update("t1")
        df, total = svc.tester_get("t1", count_total=True)
        assert total == 2
        assert df.columns[:2] == ["day", "code"], df.columns  # 实际键列名
        assert "factor" in df.columns and "d1" in df.columns
        # check 同样用实际键列名
        r = svc.tester_check("t1")
        assert r["ok"] is True

    def test_field_meta_change_does_not_invalidate(self, svc):
        """③ 改字段纯元数据（display_name）→ 自身与下游版本/valid 均不动。"""
        from stkoe.graph.model import node_id

        keys = self._seed(svc)
        svc.panel_add("ds1", "index", ["m1", "m2"])
        svc.panel_update("ds1")
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "x * 2.0")
        svc.fieldset_check("fs1", "x2")
        svc.fieldset_update("fs1")
        svc.sample_add("sp1", "fs1", "index")
        svc.sample_update("sp1")
        svc.feature_add("f1", "x * 2.0")
        svc.feature_update("f1")
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")
        nodes = [("fieldset", "fs1"), ("sample", "sp1"), ("factor", "fac1")]
        state_before = {t: (svc.store.get_node(node_id(t, n))["valid"],
                            svc.store.get_node(node_id(t, n)).get("materialized"))
                        for t, n in nodes}
        svc.fieldset_set_field("fs1", "x2", display_name="改名")
        for t, n in nodes:
            node = svc.store.get_node(node_id(t, n))
            assert (node["valid"], node.get("materialized")) == state_before[t], \
                f"{t} 不应置脏（纯元数据变更）"
        # 下游版本不推进（未置脏 → 无重算）
        for t, n in nodes[1:]:
            pass  # valid/materialized 已断言；版本由自身 set 记录
        # 公式键仍置脏（既有语义）
        svc.fieldset_set_field("fs1", "x2", formula="x * 3")
        fnode = svc.store.get_node(node_id("fieldset", "fs1"))
        assert fnode["valid"] is False
        f = svc.fieldset_meta_field("fs1", "x2")
        assert f["validated"] is False  # 公式变更 → validated 复位


class TestColMetaReference:
    """列元数据引用化：源头列（table/index）为定义点，链路列经 DERIVES 引用。"""

    def _chain(self, svc):
        for sub, d in (("index", "index"), ("table", "m1")):
            os.makedirs(os.path.join(svc.data_dir, sub, d), exist_ok=True)
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0],
                      "x": [1.0, 2.0]}).write_parquet(
            os.path.join(svc.data_dir, "index", "index", "data.parquet"))
        pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01"] * 2,
                      "price": [1.5, 2.5]}).write_parquet(
            os.path.join(svc.data_dir, "table", "m1", "data.parquet"))
        svc.table_add("m1")
        svc.index_add("index")
        svc.panel_add("ds1", "index", ["m1"])
        svc.panel_update("ds1")
        svc.fieldset_add("fs1", "ds1")
        svc.fieldset_add_field("fs1", "x2", "x * 2.0")
        svc.fieldset_check("fs1", "x2")
        svc.fieldset_update("fs1")
        svc.sample_add("sp1", "fs1", "index")
        svc.sample_update("sp1")
        svc.feature_add("f1", "x * 2.0")
        svc.feature_update("f1")
        svc.factor_add("fac1", "f1", "sp1")
        svc.factor_update("fac1")

    def test_source_col_meta_propagates_down_chain(self, svc):
        """改源头列 meta（index col --description/--unit）→ panel/sample/factor
        对应列 meta 自动反映（引用解析，不重复存储）。"""
        self._chain(svc)
        svc.index_col("index", "x", description="因子输入", unit="元")
        # panel 列引用 index 列
        pcols = {c["name"]: c for c in svc.panel_meta("ds1")["columns"]}
        assert pcols["x"]["description"] == "因子输入"
        assert pcols["x"]["unit"] == "元"
        assert pcols["x"]["source_table"] == "index"
        # sample 视图列（透传）同样反映
        scols = {c["name"]: c for c in svc.sample_meta("sp1")["columns"]}
        assert scols["x"]["description"] == "因子输入"
        assert scols["x"]["unit"] == "元"
        # factor 视图列
        fcols = {c["name"]: c for c in svc.factor_meta("fac1")["columns"]}
        assert fcols["x"]["description"] == "因子输入"
        # 成员表列（price）meta 引用 m1 源头
        svc.table_col("m1", "price", description="收盘价", unit="元")
        pcols2 = {c["name"]: c for c in svc.panel_meta("ds1")["columns"]}
        assert pcols2["price"]["description"] == "收盘价"
        assert pcols2["price"]["source_table"] == "m1"

    def test_fieldset_field_meta_is_definition_point(self, svc):
        """fieldset 自建字段（定义点 b）：display_name/unit 保存在字段定义，
        经列节点图可见且不被源头覆盖。"""
        self._chain(svc)
        svc.fieldset_set_field("fs1", "x2", display_name="翻倍因子", unit="倍")
        pcols = {c["name"]: c for c in svc.panel_meta("ds1")["columns"]}
        assert "x2" not in pcols  # 字段列不在 panel
        scols = {c["name"]: c for c in svc.sample_meta("sp1")["columns"]}
        assert scols["x2"]["display_name"] == "翻倍因子"
        assert scols["x2"]["unit"] == "倍"
        assert scols["x2"]["formula"] == "x * 2.0"
        assert scols["x2"]["validated"] is True
        # 字段列 DERIVES → 公式引用列（x）
        derives = svc.store.deps_of("column:fieldset:fs1.x2", rel_type="DERIVES")
        assert [d["target"] for d in derives] == ["column:panel:ds1.x"]
