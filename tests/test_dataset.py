# -*- coding: utf-8 -*-
"""DatasetController 测试：add/get/meta/list/scan/delete + left join 行数语义 + 物化 + 任务框架接入"""
import datetime

import polars as pl
import pytest

from stkoe.dataset import DatasetController, DatasetExistsError, DatasetNotFoundError


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return DatasetController(data_dir=tmp_path / "data")


@pytest.fixture()
def tctl(tmp_path):
    """源表控制器：先建 index/member 表再测 dataset"""
    from stkoe.table import TableController

    return TableController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")

def _write_idx(root, name, rows):
    """index 资产写 indexs/ 目录（独立于 tables/）"""
    d = root / "indexs" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_sources(tmp_path, tctl):
    """建 index 表（sym,date,price）+ 成员表（sym,date,name,industry）"""
    root = tmp_path / "data"
    _write(root, "index", pl.DataFrame({
        "sym": ["a", "b", "c"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "price": [1.0, 2.0, 3.0],
        "optime": ["2024-01-01 08:00:00"] * 3,  # 工具列，不参与 join 键
    }))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b", "d"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
        "name": ["AA", "BB", "DD"],
        "industry": ["金融", "科技", "金融"],
    }))
    for t in ("index", "m1"):
        _run(tctl.add(t, meta={"type": "index"} if t == "index" else None))
    return root


KEYS = ["sym", "date"]


def test_add_left_join_rows_eq_index(ctl, tmp_path, tctl):
    """left join 行数语义：dataset 行数 == index 表行数；成员缺失键行保留"""
    _setup_sources(tmp_path, tctl)
    dm = _add(ctl, "ds1", "index", "m1", keys=KEYS)
    assert dm.materialized is False  # add 只注册
    assert set(dm.keys) == set(KEYS)

    df = _get(ctl, "ds1")
    assert df.height == 3  # == index 行数
    assert "name" in df.columns
    assert "industry" in df.columns
    # 成员表多余的 sym=d 不参与；缺失时列值 null
    assert "DD" not in df["name"].to_list()


def test_add_inherits_source_col_meta(ctl, tmp_path, tctl):
    """dataset 列元数据继承源表列：display_name/description/unit/formula/tags"""
    _setup_sources(tmp_path, tctl)
    # 给源表列设置元数据
    _run(tctl.col("index", "sym", display_name="证券代码", description="股票标识",
                  tags="key, code"))
    _run(tctl.col("m1", "industry", display_name="行业", unit="类目",
                  formula="分类规则"))
    dm = _add(ctl, "ds1", "index", "m1", keys=KEYS)

    cols = {c.name: c for c in dm.columns}
    sym = cols["sym"]
    assert sym.as_index is True
    assert sym.display_name == "证券代码"
    assert sym.description == "股票标识"
    assert sym.tags == ("key", "code")

    ind = cols["industry"]
    assert ind.source_table == "m1"
    assert ind.display_name == "行业"
    assert ind.unit == "类目"
    assert ind.formula == "分类规则"

    # 成员列不因出现在 dataset 而丢失元数据；key 列 data_type 沿用源表
    assert cols["sym"].data_type == "String"
    assert cols["price"].display_name == "price"  # 未设置 → 默认名


def test_add_duplicate_exists(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    with pytest.raises(DatasetExistsError):
        _add(ctl, "ds1", "index", "m1", keys=KEYS)


def test_missing_member_key_errors(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    # 成员表缺 join 键 → 明确报错
    root = tmp_path / "data"
    _write(root, "m2", pl.DataFrame({"other": ["x"], "industry": ["金融"]}))
    _run(tctl.add("m2"))
    with pytest.raises(ValueError) as e:
        _add(ctl, "ds2", "index", "m2", keys=KEYS)
    assert "missing join keys" in str(e.value)


def test_index_table_must_be_type_index(ctl, tmp_path, tctl):
    """index_table 只允许 type='index' 的 table：非 index 类型拒绝创建"""
    _setup_sources(tmp_path, tctl)
    # 把 index 表 type 改为非 index → dataset add 拒绝
    _run(tctl.set("index", type=""))
    with pytest.raises(ValueError) as e:
        _add(ctl, "ds2", "index", "m1", keys=KEYS)
    assert "must be type 'index'" in str(e.value)

    # 另一张普通表（未设 type）也不能作 index
    _write(tmp_path / "data", "plain", pl.DataFrame({"k": ["a"], "x": [1.0]}))
    _run(tctl.add("plain"))
    with pytest.raises(ValueError) as e:
        _add(ctl, "ds3", "plain", keys=["k"])
    assert "must be type 'index'" in str(e.value)

    # 设回 type=index → 创建成功
    _run(tctl.set("index", type="index"))
    dm = _add(ctl, "ds4", "index", "m1", keys=KEYS)
    assert dm.name == "ds4"


def test_scan_propagates_source_col_meta(ctl, tmp_path, tctl):
    """源表 col 改动经 scan 覆盖 dataset 列说明（dataset 列不支持直接改）"""
    _setup_sources(tmp_path, tctl)
    _run(tctl.col("index", "sym", display_name="旧名"))
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    assert _meta(ctl, "ds1").columns[0].display_name == "旧名"

    # 源表改列说明 → dataset scan 后自动覆盖
    _run(tctl.col("index", "sym", display_name="证券代码", description="股票标识"))
    report = _scan(ctl, "ds1")
    assert report.changed is True

    m = _meta(ctl, "ds1")
    sym = {c.name: c for c in m.columns}["sym"]
    assert sym.display_name == "证券代码"
    assert sym.description == "股票标识"


def test_set_does_not_touch_columns(ctl, tmp_path, tctl):
    """dataset set 只改 dataset 级元数据，列说明不被修改"""
    _setup_sources(tmp_path, tctl)
    _run(tctl.col("index", "sym", display_name="证券代码"))
    _add(ctl, "ds1", "index", "m1", keys=KEYS)

    _set(ctl, "ds1", display_name="交易数据集", tags="a, b")
    m = _meta(ctl, "ds1")
    assert m.display_name == "交易数据集"
    sym = {c.name: c for c in m.columns}["sym"]
    assert sym.display_name == "证券代码"  # 列说明不受 dataset set 影响


def test_meta_and_list(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    _scan(ctl, "ds1")
    m = _meta(ctl, "ds1")
    assert m.name == "ds1"
    assert m.index_table == "index"
    assert m.tables == ("m1",)
    assert m.curated is True
    assert m.version == 1

    names = [d.name for d in _list(ctl)]
    assert names == ["ds1"]

    with pytest.raises(DatasetNotFoundError):
        _meta(ctl, "nope")


def test_set_updates_metadata(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    m = _set(ctl, "ds1", display_name="交易数据集", tags="a, b")
    assert m.display_name == "交易数据集"
    assert m.tags == ("a", "b")
    assert m.extra == {}


def test_scan_incremental_materialize(ctl, tmp_path, tctl):
    """scan：首次物化 → 源表追加数据（mtime 变化）→ 增量重物化，get 读到新数据"""
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    first = _scan(ctl, "ds1")
    assert first.materialized is True
    before = _meta(ctl, "ds1")
    assert before.version == 1

    # 给 index 表追加一行新 key
    from stkoe.table.controller import TableController

    tctl2 = TableController(data_dir=tmp_path / "data")
    root = tmp_path / "data"
    extra = pl.DataFrame({"sym": ["e"], "date": ["2024-01-04"], "price": [4.0]})
    extra.write_parquet(root / "tables" / "index" / "more.parquet")
    _run(tctl2.scan("index"))

    report = _scan(ctl, "ds1")
    assert report.changed is True
    assert report.version_after == before.version + 1

    df = _get(ctl, "ds1")
    assert df.height == 4
    assert "e" in df["sym"].to_list()


def test_add_registers_without_materialize(ctl, tmp_path, tctl):
    """add 只注册不物化；get 未物化时返回实时 join 视图（不隐式物化）"""
    _setup_sources(tmp_path, tctl)
    dm = _add(ctl, "ds1", "index", "m1", keys=KEYS)
    assert dm.materialized is False

    df = _get(ctl, "ds1")
    assert df.height == 3  # 实时 join 视图，行数 == index
    assert _meta(ctl, "ds1").materialized is False  # 读取不触发物化

    # count_total：limit 时 total=未分页总行数
    df, total = _get(ctl, "ds1", limit=2, count_total=True)
    assert df.height == 2 and total == 3
    # offset：跳过起始行；total 仍为过滤后全量
    df, total = _get(ctl, "ds1", offset=1, limit=1, count_total=True)
    assert df.height == 1 and total == 3
    assert df["sym"].to_list() == ["b"]

    # 显式 scan 才物化
    report = _scan(ctl, "ds1")
    assert report.materialized is True
    assert _meta(ctl, "ds1").materialized is True


def test_delete_removes_registration(ctl, tmp_path, tctl):
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    out = _delete(ctl, "ds1")
    assert out == {"deleted": "ds1"}
    with pytest.raises(DatasetNotFoundError):
        _meta(ctl, "ds1")


def test_table_delete_blocked_by_dataset_dep(tctl, ctl, tmp_path):
    """依赖保护：dataset 存在时删除成员 table 报错"""
    _setup_sources(tmp_path, tctl)
    _add(ctl, "ds1", "index", "m1", keys=KEYS)
    from stkoe.table.controller import DependencyError, TableController

    tc = TableController(data_dir=tmp_path / "data")
    with pytest.raises(DependencyError):
        _run(tc.delete("index"))


def test_task_framework_dataset_handlers(mgr):
    """dataset handlers 注册进任务框架：add→meta→get 全链路（转发 panel，graph 语义）"""
    from stkoe.graph.service import GraphService

    root = mgr.data_dir
    _write_idx(root, "index", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "price": [1.0, 2.0], "optime": ["2024-01-01 08:00:00"] * 2}))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "name": ["AA", "BB"], "industry": ["金融", "科技"]}))
    gsvc = GraphService(data_dir=root)
    gsvc.table_add("m1")
    gsvc.index_add("index")
    gsvc.close()

    t_add = mgr.submit("dataset", "add", ["ds1", "index", "m1"])  # keys 由 index 推断
    _await(mgr, t_add)
    add_res = _mgr_result(mgr, t_add)
    assert add_res["name"] == "ds1"
    assert add_res["keys"] == ["sym", "date"]  # panel 实时 join，无物化概念

    t_meta = mgr.submit("dataset", "meta", ["ds1"])
    _await(mgr, t_meta)
    assert _mgr_result(mgr, t_meta)["index"] == "index:index"

    t_list = mgr.submit("dataset", "list", [])
    _await(mgr, t_list)
    assert [d["name"] for d in _mgr_result(mgr, t_list)] == ["ds1"]

    t_get = mgr.submit("dataset", "get", ["ds1"])
    _await(mgr, t_get)
    assert _mgr_result(mgr, t_get)["rows"] == 2

    t_del = mgr.submit("dataset", "delete", ["ds1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "ds1"}


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name, index, *members, **kw):
    return _run(ctl.add(name, index, *members, **kw))


def _get(ctl, name, **kw):
    return _run(ctl.get(name, **kw))


def _meta(ctl, name):
    return _run(ctl.meta(name))


def _list(ctl):
    return _run(ctl.list())


def _set(ctl, name, **kw):
    return _run(ctl.set(name, **kw))


def _scan(ctl, name, **kw):
    return _run(ctl.scan(name, **kw))


def _delete(ctl, name, **kw):
    return _run(ctl.delete(name, **kw))


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