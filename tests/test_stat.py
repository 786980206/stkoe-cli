# -*- coding: utf-8 -*-
"""StatController 测试：scan 生成统计分组产物 / get 读文件 / meta/list/delete / 任务框架接入"""
import polars as pl
import pytest

from stkoe.stat import StatController, StatNotFoundError
from stkoe.stat.calc import ALL_COLS


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return StatController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")

def _write_idx(root, name, rows):
    """index 资产写 indexs/ 目录（独立于 tables/）"""
    d = root / "index" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _gsetup(root):
    """graph 语义造数：index/m1 表 → index 节点 → panel ds1（keys=sym,date）"""
    _write_idx(root, "index", pl.DataFrame({
        "sym": ["a", "b", "c"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "price": [1.0, 2.0, 3.0],
        "optime": ["2024-01-01 08:00:00"] * 3,  # 工具列
    }))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b", "d"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
        "name": ["AA", "BB", "DD"],
        "industry": ["金融", "科技", "金融"],
    }))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("m1")
    svc.index_add("index")
    svc.panel_add("ds1", "index", ["m1"])  # keys 由 index 推断
    svc.close()
    return root


def _graph_add(root, name):
    """注册表（graph 语义，供 storage 等用例使用）"""
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    try:
        return svc.table_add(name)
    finally:
        svc.close()


def _setup_sources(tmp_path):
    return _gsetup(tmp_path / "data")


def test_scan_generates_partition_files(ctl, tmp_path):
    """scan：生成 all.parquet + 按索引（keys）分组的 sym/date parquet"""
    root = _setup_sources(tmp_path)
    report = _scan(ctl, "panel", "ds1")
    assert report.target_type == "panel"
    assert report.partitions == ("all", "date", "sym")
    parts = {f.partition: f for f in report.files}
    assert set(parts) == {"all", "sym", "date"}
    for f in parts.values():
        assert (root / "stat" / f.rel_path).exists()

    out_dir = root / "stat" / "panel" / "ds1" / "coverage"
    assert (out_dir / "all.parquet").exists()
    assert (out_dir / "sym.parquet").exists()
    assert (out_dir / "date.parquet").exists()


def test_scan_all_stats_element(ctl, tmp_path):
    """all 分组：每字段一行，含 v1.0 统计要素列"""
    _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    df = _get(ctl, "panel", "ds1", partition_by="all")
    assert df.columns == ALL_COLS
    assert (df["group"] == "all").all()
    assert "price" in df["field"].to_list()
    assert "name" in df["field"].to_list()


def test_scan_sym_group_stats(ctl, tmp_path):
    """sym 分组：按 sym 取值各一组，首列列名为 sym"""
    _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    df = _get(ctl, "panel", "ds1", partition_by="sym")
    assert df.columns[0] == "sym"
    assert set(df["sym"].to_list()) == {"a", "b", "c"}


def test_get_all_partitions(ctl, tmp_path):
    """get 不带 partition_by：返回全部 ``{分区: DataFrame}``"""
    _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    out = _get(ctl, "panel", "ds1")
    assert isinstance(out, dict)
    assert set(out) == {"all", "sym", "date"}
    assert out["all"].columns == ALL_COLS


def test_get_missing_partition_errors(ctl, tmp_path):
    _setup_sources(tmp_path)
    with pytest.raises(StatNotFoundError):
        _get(ctl, "panel", "ds1", partition_by="all")


def test_meta_lists_files(ctl, tmp_path):
    _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    m = _meta(ctl, "panel", "ds1")
    assert m.target_type == "panel"
    assert m.partitions == ("all", "date", "sym")
    assert len(m.files) == 3


def test_list_and_delete(ctl, tmp_path):
    _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    assert [m.target_name for m in _list(ctl)] == ["ds1"]

    _delete(ctl, "panel", "ds1")
    assert _list(ctl) == []
    with pytest.raises(StatNotFoundError):
        _meta(ctl, "panel", "ds1")


# ---------- stat 进图 ----------

def _stat_svc(root):
    from stkoe.graph.service import GraphService

    return GraphService(data_dir=root)


def test_scan_registers_graph_node(ctl, tmp_path):
    """stat 进图：scan 后 Stat 节点 + DEPENDS 边 → 目标登记，graph 各通道可见"""
    root = _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    svc = _stat_svc(root)
    try:
        node = svc.store.get_node("stat:panel/ds1/coverage")
        assert node is not None
        assert node["type"] == "stat"
        assert node["kind"] == "coverage"
        assert node["target_type"] == "panel" and node["target_name"] == "ds1"
        assert node["partitions"] == ["all", "date", "sym"]
        assert len(node["files"]) == 3
        assert node["files"][0]["partition"] == "all"
        deps = svc.store.deps_of("stat:panel/ds1/coverage")
        assert [d["target"] for d in deps] == ["panel:ds1"]
        assert deps[0].get("role") == "target"
        # graph nodes 可见（node_summaries --type stat）；血缘：目标下游含 stat
        from stkoe.graph.export import node_summaries

        assert "panel/ds1/coverage" in \
            [n["name"] for n in node_summaries(svc.store, "stat")]
        assert "stat:panel/ds1/coverage" in \
            [d["id"] for d in svc.store.downstream("panel:ds1")]
    finally:
        svc.close()


def test_scan_rescan_updates_single_node(ctl, tmp_path):
    """重复 scan 幂等：更新同一 Stat 节点（不重复登记）"""
    root = _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    _scan(ctl, "panel", "ds1")
    svc = _stat_svc(root)
    try:
        assert len(svc.store.list_nodes("Stat")) == 1
    finally:
        svc.close()


def test_stat_delete_removes_graph_node(ctl, tmp_path):
    """stat delete 同步删图内节点（按 kind 精确 / 整目标）"""
    root = _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    _scan(ctl, "table", "m1")
    svc = _stat_svc(root)
    try:
        assert svc.store.get_node("stat:panel/ds1/coverage") is not None
        assert svc.store.get_node("stat:table/m1/coverage") is not None
    finally:
        svc.close()

    _delete(ctl, "panel", "ds1")  # 删整目标 → 该目标 stat 节点全清
    svc = _stat_svc(root)
    try:
        assert svc.store.get_node("stat:panel/ds1/coverage") is None
        assert svc.store.get_node("stat:table/m1/coverage") is not None  # 其他目标不动
    finally:
        svc.close()


def test_target_delete_cascades_stat_node(ctl, tmp_path):
    """目标资产删除 → Stat 节点级联清理（有统计引用时需 --force）"""
    root = _setup_sources(tmp_path)
    _scan(ctl, "panel", "ds1")
    svc = _stat_svc(root)
    try:
        assert svc.store.get_node("stat:panel/ds1/coverage") is not None
        with pytest.raises(Exception):
            svc.panel_delete("ds1")  # 统计节点是下游 → 非 force 被拦截
        svc.panel_delete("ds1", force=True)
        assert svc.store.get_node("panel:ds1") is None
        assert svc.store.get_node("stat:panel/ds1/coverage") is None
    finally:
        svc.close()


def test_table_target_scan(ctl, tmp_path):
    """table 目标：索引 = 非工具列（index 为独立资产，扫成员表 m1）"""
    _setup_sources(tmp_path)
    report = _scan(ctl, "table", "m1")
    parts = set(report.partitions)
    assert "all" in parts
    assert "sym" in parts and "date" in parts and "name" in parts


def _write_hive(root, name, parts, row_cnt=1):
    """写 hive 分区表：parts=[（key, value), ...] → tables/<name>/k1=v1/.../data.parquet"""
    tdir = root / "table" / name
    sizes = {}
    for i, (k, v) in enumerate(parts):
        d = tdir / f"{k}={v}"
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"part{i}.parquet"
        pl.DataFrame({"v": list(range(row_cnt))}).write_parquet(p)
        sizes[(k, v)] = sizes.get((k, v), 0) + p.stat().st_size
    return sizes


def test_storage_scan_hive_table(ctl, tmp_path):
    """storage（table）：按 hive 分区键/值聚合存储占用与文件数"""
    root = _setup_sources(tmp_path)
    parts = [("year", "2024"), ("year", "2024"), ("year", "2025")]
    sizes = _write_hive(root, "sales", parts)
    _graph_add(root, "sales")

    report = _scan(ctl, "table", "sales", kind="storage")
    assert report.kind == "storage"
    assert report.partitions == ("all", "year")

    all_df = _get(ctl, "table", "sales", kind="storage", partition_by="all")
    assert all_df.columns == ["partition_by", "partition_value", "storage_size", "file_no"]
    row = all_df.row(0)
    assert row[0] == "__all__" and row[1] == "__all__"
    assert row[2] == sum(sizes.values())
    assert row[3] == 3  # 3 个文件

    year_df = _get(ctl, "table", "sales", kind="storage", partition_by="year")
    yrows = {r["partition_value"]: (r["storage_size"], r["file_no"]) for r in year_df.iter_rows(named=True)}
    assert set(yrows) == {"2024", "2025"}
    assert yrows["2024"] == (sizes[("year", "2024")], 2)
    assert yrows["2025"] == (sizes[("year", "2025")], 1)


def test_storage_scan_flat_table(ctl, tmp_path):
    """storage（table 无分区）：只有 all 行，partition_by/partition_value=__all__"""
    _setup_sources(tmp_path)
    report = _scan(ctl, "table", "m1", kind="storage")
    assert report.partitions == ("all",)
    df = _get(ctl, "table", "m1", kind="storage", partition_by="all")
    assert df.height == 1
    row = df.row(0)
    assert row[0] == "__all__" and row[1] == "__all__"
    assert row[3] == 1  # m1 表单个 data.parquet


def test_storage_scan_get_all(ctl, tmp_path):
    """storage get 不带 partition_by：返回全部分区文件"""
    root = _setup_sources(tmp_path)
    _write_hive(root, "sales", [("year", "2024")])
    _graph_add(root, "sales")
    _scan(ctl, "table", "sales", kind="storage")
    out = _get(ctl, "table", "sales", kind="storage")
    assert isinstance(out, dict)
    assert set(out) == {"all", "year"}
    assert out["all"].columns == ["partition_by", "partition_value", "storage_size", "file_no"]


def test_task_framework_stat_handlers(mgr):
    """stat handlers 注册进任务框架：scan→get 全链路（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_scan = mgr.submit("stat", "scan", ["panel", "ds1"])
    _await(mgr, t_scan)
    scan_res = _mgr_result(mgr, t_scan)
    assert scan_res["partitions"] == ["all", "date", "sym"]

    t_get = mgr.submit("stat", "get", ["panel", "ds1", "--partition_by", "all"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["partition"] == "all"

    t_get_all = mgr.submit("stat", "get", ["panel", "ds1"])
    _await(mgr, t_get_all)
    all_res = _mgr_result(mgr, t_get_all)
    assert [p["partition"] for p in all_res["partitions"]] == ["all", "date", "sym"]


def test_scan_partition_subset(ctl, tmp_path):
    """stat scan --partition 只算指定分区；未知名报错（粗桶大表按需扫描）"""
    _setup_sources(tmp_path)
    r = _scan(ctl, "table", "m1", partitions=["all", "date"])
    assert list(r.partitions) == ["all", "date"]

    import pytest as _pytest

    with _pytest.raises(ValueError):
        _scan(ctl, "table", "m1", partitions=["nope"])


def test_scan_full_then_partial_is_idempotent(ctl, tmp_path):
    """先全量再按需重扫：只覆盖指定分区文件，其余保留"""
    _setup_sources(tmp_path)
    _scan(ctl, "table", "m1")
    r = _scan(ctl, "table", "m1", partitions=["all"])
    assert list(r.partitions) == ["all"]
    out = _get(ctl, "table", "m1")
    assert "all" in out and "date" in out and "sym" in out  # 其余分区文件保留
    assert set(out) == set(_partitions_of(ctl, "table", "m1"))


def _partitions_of(ctl, target_type, target_name):
    return ctl._partitions(target_type, target_name)


def test_task_scan_reports_index_progress(mgr):
    """s:stat scan 计算各索引 coverage 分组：ctx.update 逐分组上报进度（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_scan = mgr.submit("stat", "scan", ["panel", "ds1"])
    _await(mgr, t_scan)
    assert _mgr_result(mgr, t_scan)["partitions"] == ["all", "date", "sym"]

    evs = mgr.events.list_by_task(t_scan.task_id)
    prog = [(e.progress, e.message) for e in evs if e.message.startswith("panel/ds1:")]
    assert len(prog) == 3  # all + 每个索引各一条（分区并行，完成顺序不定）
    parts = {m.split(": ", 1)[1].split("（", 1)[0] for _, m in prog}
    assert parts == {"all", "date", "sym"}
    assert [p for p, _ in prog] == pytest.approx([1 / 3, 2 / 3, 1.0])
    assert evs[-1].state == "succeeded"
    assert evs[-1].progress == 1.0


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _scan(ctl, target_type, name, **kw):
    return _run(ctl.scan(target_type, name, **kw))


def _get(ctl, target_type, name, **kw):
    return _run(ctl.get(target_type, name, **kw))


def _meta(ctl, target_type, name, **kw):
    return _run(ctl.meta(target_type, name, **kw))


def _list(ctl):
    return _run(ctl.list())


def _delete(ctl, target_type, name, **kw):
    return _run(ctl.delete(target_type, name, **kw))


# ---------- 任务框架助手 ----------

def _await(mgr, task, timeout=5.0):
    import time

    from stkoe.task.model import TERMINAL_STATES

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur is not None and cur.state in TERMINAL_STATES:
            return cur
        time.sleep(0.02)
    raise TimeoutError(f"task not terminal: {mgr.get(task.task_id).state}")


def _mgr_result(mgr, task):
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