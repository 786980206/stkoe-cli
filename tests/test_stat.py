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


@pytest.fixture()
def tctl(tmp_path):
    """源表控制器：先建 index/member 表再测 stat 目标"""
    from stkoe.table import TableController

    return TableController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_sources(tmp_path, tctl):
    root = tmp_path / "data"
    _write(root, "index", pl.DataFrame({
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
    for t in ("index", "m1"):
        _run(tctl.add(t))
    from stkoe.dataset import DatasetController

    dc = DatasetController(data_dir=root)
    _run(dc.add("ds1", "index", "m1", keys=["sym", "date"]))
    return root


def test_scan_generates_partition_files(ctl, tmp_path, tctl):
    """scan：生成 all.parquet + 按索引（keys）分组的 sym/date parquet"""
    root = _setup_sources(tmp_path, tctl)
    report = _scan(ctl, "dataset", "ds1")
    assert report.target_type == "dataset"
    assert report.partitions == ("all", "date", "sym")
    parts = {f.partition: f for f in report.files}
    assert set(parts) == {"all", "sym", "date"}
    for f in parts.values():
        assert (root / "stats" / f.rel_path).exists()

    out_dir = root / "stats" / "dataset" / "ds1" / "coverage"
    assert (out_dir / "all.parquet").exists()
    assert (out_dir / "sym.parquet").exists()
    assert (out_dir / "date.parquet").exists()


def test_scan_all_stats_element(ctl, tmp_path, tctl):
    """all 分组：每字段一行，含 v1.0 统计要素列"""
    _setup_sources(tmp_path, tctl)
    _scan(ctl, "dataset", "ds1")
    df = _get(ctl, "dataset", "ds1", partition_by="all")
    assert df.columns == ALL_COLS
    assert (df["group"] == "all").all()
    assert "price" in df["field"].to_list()
    assert "name" in df["field"].to_list()


def test_scan_sym_group_stats(ctl, tmp_path, tctl):
    """sym 分组：按 sym 取值各一组，首列列名为 sym"""
    _setup_sources(tmp_path, tctl)
    _scan(ctl, "dataset", "ds1")
    df = _get(ctl, "dataset", "ds1", partition_by="sym")
    assert df.columns[0] == "sym"
    assert set(df["sym"].to_list()) == {"a", "b", "c"}


def test_get_all_partitions(ctl, tmp_path, tctl):
    """get 不带 partition_by：返回全部 ``{分区: DataFrame}``"""
    _setup_sources(tmp_path, tctl)
    _scan(ctl, "dataset", "ds1")
    out = _get(ctl, "dataset", "ds1")
    assert isinstance(out, dict)
    assert set(out) == {"all", "sym", "date"}
    assert out["all"].columns == ALL_COLS


def test_get_missing_partition_errors(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    with pytest.raises(StatNotFoundError):
        _get(ctl, "dataset", "ds1", partition_by="all")


def test_meta_lists_files(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _scan(ctl, "dataset", "ds1")
    m = _meta(ctl, "dataset", "ds1")
    assert m.target_type == "dataset"
    assert m.partitions == ("all", "date", "sym")
    assert len(m.files) == 3


def test_list_and_delete(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _scan(ctl, "dataset", "ds1")
    assert [m.target_name for m in _list(ctl)] == ["ds1"]

    _delete(ctl, "dataset", "ds1")
    assert _list(ctl) == []
    with pytest.raises(StatNotFoundError):
        _meta(ctl, "dataset", "ds1")


def test_table_target_scan(ctl, tmp_path, tctl):
    """table 目标：索引 = 非工具列"""
    _setup_sources(tmp_path, tctl)
    report = _scan(ctl, "table", "index")
    parts = set(report.partitions)
    assert "all" in parts
    assert "optime" not in parts  # 工具列剔除
    assert "sym" in parts and "date" in parts and "price" in parts


def test_task_framework_stat_handlers(mgr):
    """stat handlers 注册进任务框架：scan→get 全链路"""
    from stkoe.dataset import DatasetController
    from stkoe.table import TableController

    tctl = TableController(data_dir=mgr.data_dir)
    root = mgr.data_dir
    _write(root, "index", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "price": [1.0, 2.0], "optime": ["2024-01-01 08:00:00"] * 2}))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "name": ["AA", "BB"], "industry": ["金融", "科技"]}))
    for t in ("index", "m1"):
        _run(tctl.add(t))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds1", "index", "m1", keys=["sym", "date"]))

    t_scan = mgr.submit("stat", "scan", ["dataset", "ds1"])
    _await(mgr, t_scan)
    scan_res = _mgr_result(mgr, t_scan)
    assert scan_res["partitions"] == ["all", "date", "sym"]

    t_get = mgr.submit("stat", "get", ["dataset", "ds1", "--partition_by", "all"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["partition"] == "all"

    t_get_all = mgr.submit("stat", "get", ["dataset", "ds1"])
    _await(mgr, t_get_all)
    all_res = _mgr_result(mgr, t_get_all)
    assert [p["partition"] for p in all_res["partitions"]] == ["all", "date", "sym"]


def test_task_scan_reports_index_progress(mgr):
    """s:stat scan 计算各索引 coverage 分组：ctx.update 逐分组上报进度"""
    from stkoe.dataset import DatasetController
    from stkoe.table import TableController

    tctl = TableController(data_dir=mgr.data_dir)
    root = mgr.data_dir
    _write(root, "index", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "price": [1.0, 2.0], "optime": ["2024-01-01 08:00:00"] * 2}))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "name": ["AA", "BB"], "industry": ["金融", "科技"]}))
    for t in ("index", "m1"):
        _run(tctl.add(t))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds1", "index", "m1", keys=["sym", "date"]))

    t_scan = mgr.submit("stat", "scan", ["dataset", "ds1"])
    _await(mgr, t_scan)
    assert _mgr_result(mgr, t_scan)["partitions"] == ["all", "date", "sym"]

    evs = mgr.events.list_by_task(t_scan.task_id)
    prog = [(e.progress, e.message) for e in evs if e.message.startswith("dataset/ds1:")]
    assert len(prog) == 3  # all + 每个索引各一条（按 _partitions 计算顺序）
    assert prog[0] == (pytest.approx(1 / 3), "dataset/ds1: all（1/3）")
    assert prog[1] == (pytest.approx(2 / 3), "dataset/ds1: sym（2/3）")
    assert prog[2] == (pytest.approx(1.0), "dataset/ds1: date（3/3）")
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

    evs = mgr.events.list_by_task(task.task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None