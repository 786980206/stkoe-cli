# -*- coding: utf-8 -*-
"""V2.0 死代码 TableController 回归测试（默认全量不收集；如需单独运行：
.venv/Scripts/python.exe -m pytest V2.0/tests/test_table.py -q）

V3.0 起 table 资产走 GraphService（src/stkoe/graph/service.py），
本文件保留对 src/stkoe/table/controller.py（死代码）的行为回归存档。
原始 V2.0 基线测试见 git f290378（V2.0 全量备份）。
"""
import polars as pl
import pytest

from stkoe.table import TableController
from stkoe.table.controller import TableExistsError, TableNotFoundError


@pytest.fixture()
def ctl(tmp_path):
    return TableController(data_dir=tmp_path / "data")


def _write_single(root, name, rows, columns=("sym", "price")):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pl.DataFrame):
        df = rows
    else:
        df = pl.DataFrame({c: rows[c] for c in columns})
    df.write_parquet(d / "data.parquet")
    return df


def test_add_single_then_meta(ctl, tmp_path):
    _write_single(tmp_path / "data", "demo", {
        "sym": ["a", "b"], "optime": ["2024-01-01", "2024-01-02"], "price": [1.0, 2.0]},
        columns=("sym", "optime", "price"))

    report = _add(ctl, "demo")
    assert report.name == "demo"
    assert report.version_before == 0
    assert report.version_after == 1
    assert report.implicit_registered is True  # add 经 _scan_impl 注册，视为隐式
    assert report.layout.value == "single"

    m = _meta(ctl, "demo")
    assert m.name == "demo"
    assert m.version == 1
    assert m.layout.value == "single"
    assert m.partition_count == 0
    assert len(m.files) == 1
    assert [c.name for c in m.columns] == ["sym", "optime", "price"]
    assert m.columns[1].is_tool is True  # optime 为默认工具字段


def test_add_all_batch_discovers(ctl, tmp_path):
    root = tmp_path / "data"
    for name in ("t1", "t2"):
        _write_single(root, name, {"sym": ["x"], "price": [1.0]})

    reports = _add_all(ctl)
    assert sorted(r.name for r in reports) == ["t1", "t2"]
    assert _names(ctl) == ["t1", "t2"]


def test_list_candidates(ctl, tmp_path):
    """table list --candidate：未登记但含 parquet 的目录（已登记/空目录排除）"""
    root = tmp_path / "data"
    _write_single(root, "reg", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "reg")
    _write_single(root, "cand", {"sym": ["b"], "price": [2.0]})
    (root / "table" / "empty").mkdir(parents=True)

    assert _run(ctl.list(candidate=True)) == ["cand"]
    # 默认仍输出已注册表
    assert _names(ctl) == ["reg"]


def test_add_errors(ctl, tmp_path):
    with pytest.raises(TableNotFoundError):
        _add(ctl, "missing")
    _write_single(tmp_path / "data", "demo", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "demo")
    with pytest.raises(TableExistsError):
        _add(ctl, "demo")


def test_add_with_meta(ctl, tmp_path):
    """table add 可携带元数据：标准字段 + tags 逗号分隔 + 任意键进 extra"""
    root = tmp_path / "data"
    _write_single(root, "demo", {"sym": ["a"], "price": [1.0]})
    report = _run(ctl.add("demo", meta={
        "display_name": "D表", "description": "说明", "source": "daily",
        "tags": "x,y", "custom": 1,
    }))
    assert report.name == "demo"
    m = _meta(ctl, "demo")
    assert m.display_name == "D表"
    assert m.description == "说明"
    assert m.source == "daily"
    assert m.tags == ("x", "y")
    assert m.extra == {"custom": 1}


def test_get_returns_data(ctl, tmp_path):
    src = pl.DataFrame({"sym": ["a", "b", "c"], "optime": ["2024-01-01"] * 3,
                        "price": [1.0, 2.0, 3.0]})
    _write_single(tmp_path / "data", "demo", src)
    _add(ctl, "demo")

    df = _get(ctl, "demo")
    assert df.height == 3
    assert df["price"].to_list() == [1.0, 2.0, 3.0]

    got = _get(ctl, "demo", where="price>=2")
    assert got.height == 2

    cols = _get(ctl, "demo", columns=["sym", "price"])
    assert cols.columns == ["sym", "price"]

    tool = _get(ctl, "demo", exclude_tool=True)
    assert "optime" not in tool.columns

    lim = _get(ctl, "demo", limit=2)
    assert lim.height == 2

    # count_total：limit 时 total=过滤后未分页总行数；无 limit 时 total==rows
    df, total = _get(ctl, "demo", limit=2, count_total=True)
    assert df.height == 2 and total == 3
    df, total = _get(ctl, "demo", where="price>=2", limit=1, count_total=True)
    assert df.height == 1 and total == 2
    df, total = _get(ctl, "demo", count_total=True)
    assert df.height == 3 and total == 3

    # offset：跳过起始行（可与 limit/where 组合）；total 仍为过滤后全量
    df, total = _get(ctl, "demo", offset=1, limit=2, count_total=True)
    assert df.height == 2 and total == 3
    assert df["price"].to_list() == [2.0, 3.0]
    df, total = _get(ctl, "demo", where="price>=2", offset=1, limit=1, count_total=True)
    assert df.height == 1 and total == 2
    assert df["price"].to_list() == [3.0]
    df, total = _get(ctl, "demo", offset=2, count_total=True)
    assert df.height == 1 and total == 3


def test_scan_refresh_updates_catalog(ctl, tmp_path):
    """table scan：显式扫描对账；无差异不 bump，新增文件 → changed 且版本递增"""
    root = tmp_path / "data"
    _write_single(root, "demo", {"sym": ["a", "b"], "price": [1.0, 2.0]})
    _add(ctl, "demo")
    assert _meta(ctl, "demo").version == 1

    r = _run(ctl.scan("demo"))
    assert r.changed is False
    assert r.version_after == 1  # 无差异不 bump

    pl.DataFrame({"sym": ["c"], "price": [3.0]}).write_parquet(
        root / "table" / "demo" / "more.parquet")
    r = _run(ctl.scan("demo"))
    assert r.changed is True
    assert r.version_after == 2
    assert {f.rel_path for f in _meta(ctl, "demo").files} == {"data.parquet", "more.parquet"}

    r = _run(ctl.scan("", all=True))
    assert {x.name for x in r} == {"demo"}  # --all 批量重扫


def test_get_reads_partitioned_hive(ctl, tmp_path):
    root = tmp_path / "data"
    d = root / "table" / "parted"
    (d / "date=2024-01-01").mkdir(parents=True)
    (d / "date=2024-01-02").mkdir(parents=True)
    pl.DataFrame({"sym": ["a"], "price": [1.0]}).write_parquet(d / "date=2024-01-01" / "f.parquet")
    pl.DataFrame({"sym": ["b"], "price": [2.0]}).write_parquet(d / "date=2024-01-02" / "f.parquet")

    report = _add(ctl, "parted")
    assert report.layout.value == "hive"
    assert report.partition_by == ("date",)
    assert report.partition_count == 2

    m = _meta(ctl, "parted")
    assert m.partition_count == 2
    assert m.partition_by == ("date",)

    df = _get(ctl, "parted")
    assert df.height == 2
    assert "date" in df.columns  # hive 分区列在读取时物化
    sub = _get(ctl, "parted", partition="date=2024-01-01")
    assert sub.height == 1
    assert sub["sym"].to_list() == ["a"]


def test_get_auto_sync_on_change(ctl, tmp_path):
    """读前快检：磁盘数据变更后自动重扫，get 返回新数据"""
    root = tmp_path / "data"
    d = root / "table" / "demo"
    d.mkdir(parents=True)
    pl.DataFrame({"sym": ["a"], "price": [1.0]}).write_parquet(d / "data.parquet")
    _add(ctl, "demo")
    before = _meta(ctl, "demo")
    assert before.version == 1

    # 磁盘追加数据（mtime 变化），直接 get 应自动同步
    pl.DataFrame({"sym": ["b"], "price": [2.0]}).write_parquet(d / "more.parquet")
    df = _get(ctl, "demo")
    assert df.height == 2
    after = _meta(ctl, "demo")
    assert after.version == 2


def test_add_and_set_type_metadata(ctl, tmp_path):
    """table --type：add/set 均可设置表类型（index 或其他值），属于标准元数据字段"""
    root = tmp_path / "data"
    _write_single(root, "idx", {"sym": ["a"], "price": [1.0]})
    report = _run(ctl.add("idx", meta={"type": "index"}))
    assert report.name == "idx"
    m = _meta(ctl, "idx")
    assert m.type == "index"

    # set 可改 type；未设置时默认空字符串
    m2 = _set(ctl, "idx", type="benchmark")
    assert m2.type == "benchmark"
    m3 = _set(ctl, "idx", type="")
    assert m3.type == ""

    # 普通表（不设 type）默认空
    _write_single(root, "plain", {"sym": ["b"], "price": [2.0]})
    _add(ctl, "plain")
    assert _meta(ctl, "plain").type == ""

    # 自定义值保留（不限定枚举）
    m4 = _set(ctl, "plain", type="feature")
    assert m4.type == "feature"


def test_set_updates_metadata(ctl, tmp_path):
    """table set：标准字段 + tags + 任意键进 extra，版本递增，返回更新后 meta"""
    _write_single(tmp_path / "data", "demo", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "demo")
    assert _meta(ctl, "demo").version == 1

    m = _set(ctl, "demo", display_name="Demo表", description="测试描述",
             source="local", tags="a, b, c", foo="bar")
    assert m.display_name == "Demo表"
    assert m.description == "测试描述"
    assert m.source == "local"
    assert m.tags == ("a", "b", "c")
    assert m.extra == {"foo": "bar"}
    assert m.version == 2

    # 再次 set 只更新传入字段，其余保留
    m2 = _set(ctl, "demo", display_name="改名")
    assert m2.display_name == "改名"
    assert m2.description == "测试描述"
    assert m2.extra == {"foo": "bar"}

    with pytest.raises(TableNotFoundError):
        _set(ctl, "nope", display_name="x")


def test_col_updates_column_metadata(ctl, tmp_path):
    """table col：更新列元数据（display_name/description/unit/formula/tags），版本递增"""
    _write_single(tmp_path / "data", "demo", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "demo")
    assert _meta(ctl, "demo").version == 1

    m = _col(ctl, "demo", "sym", display_name="代码", description="证券代码",
             unit="元", formula="x*2", tags="a, b, c")
    assert m.version == 2
    sym = next(c for c in m.columns if c.name == "sym")
    assert sym.display_name == "代码"
    assert sym.description == "证券代码"
    assert sym.unit == "元"
    assert sym.formula == "x*2"
    assert sym.tags == ("a", "b", "c")
    # 其余列不受影响
    price = next(c for c in m.columns if c.name == "price")
    assert price.display_name == "price"
    assert price.unit is None

    # 再次 col 只更新传入字段
    m2 = _col(ctl, "demo", "sym", display_name="改名")
    sym2 = next(c for c in m2.columns if c.name == "sym")
    assert sym2.display_name == "改名"
    assert sym2.description == "证券代码"

    with pytest.raises(TableNotFoundError):
        _col(ctl, "nope", "sym", display_name="x")
    with pytest.raises(TableNotFoundError):
        _col(ctl, "demo", "nope", display_name="x")
    with pytest.raises(ValueError):
        _col(ctl, "demo", "sym", bogus="x")


def test_delete_removes_registration_keeps_data(ctl, tmp_path):
    _write_single(tmp_path / "data", "demo", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "demo")

    out = _delete(ctl, "demo")
    assert out == {"deleted": "demo"}
    assert _names(ctl) == []
    with pytest.raises(TableNotFoundError):
        _meta(ctl, "demo")
    # 用户数据文件仍在
    assert (tmp_path / "data" / "table" / "demo" / "data.parquet").exists()

    with pytest.raises(TableNotFoundError):
        _delete(ctl, "demo")


def test_delete_requires_existing(ctl, tmp_path):
    with pytest.raises(TableNotFoundError):
        _delete(ctl, "nope")


def test_add_implicitly_registers_unregistered_dir(ctl, tmp_path):
    """sniff 语义：get 对未注册目录自动注册后再读"""
    _write_single(tmp_path / "data", "auto", {"sym": ["a"], "price": [1.0]})
    df = _get(ctl, "auto")
    assert df.height == 1
    assert "auto" in _names(ctl)


def test_re_register_after_delete(ctl, tmp_path):
    _write_single(tmp_path / "data", "demo", {"sym": ["a"], "price": [1.0]})
    _add(ctl, "demo")
    _delete(ctl, "demo")
    # 数据还在 → 可再次 add
    report = _add(ctl, "demo")
    assert report.version_after == 1
    assert _meta(ctl, "demo").version == 1


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name):
    return _run(ctl.add(name))


def _add_all(ctl):
    return _run(ctl.add("", all=True))


def _get(ctl, name, **kw):
    return _run(ctl.get(name, **kw))


def _meta(ctl, name):
    return _run(ctl.meta(name))


def _set(ctl, name, **kw):
    return _run(ctl.set(name, **kw))


def _col(ctl, name, column, **kw):
    return _run(ctl.col(name, column, **kw))


def _delete(ctl, name, **kw):
    return _run(ctl.delete(name, **kw))


def _names(ctl):
    return [m.name for m in _run(ctl.list())]
