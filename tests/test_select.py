"""select 裁剪测试：partition / 谓词 / 投影，正确性 = 裁剪结果 == 全量结果"""
import polars as pl
import pytest

import stkoe.data as data
from stkoe.data.table import TableNotFoundError

from conftest import make_df, write_hive, write_single


def _build(root):
    df = make_df([
        ("2020-01-01", "a", 0.01), ("2020-06-01", "b", 0.05),
        ("2021-01-01", "c", -0.02), ("2021-12-31", "d", 0.10),
        ("2022-03-15", "e", -0.05),
    ])
    write_hive(root, "t1", df, partition_by="year")
    data.table.scan("t1")
    return df


def test_select_partition(root):
    _build(root)
    lf = data.table.get_lazy("t1", partition="year=2020")
    got = lf.collect()
    assert sorted(got["year"].unique().to_list()) == [2020]
    assert got.height == 2
    # 前缀匹配
    assert data.table.get_lazy("t1", partition="year").collect().height == 5


def test_select_where_date(root):
    df = _build(root)
    got = data.table.get_lazy("t1", where="date>=2021-01-01").collect()
    assert got.height == df.filter(pl.col("date") >= pl.date(2021, 1, 1)).height


def test_select_where_range(root):
    _build(root)
    got = data.table.get_lazy("t1", where="2020-01-01<=date<=2021-12-31").collect()
    assert got.height == 4


def test_select_where_numeric(root):
    _build(root)
    got = data.table.get_lazy("t1", where="r>=0.05").collect()
    assert got.height == 2
    assert got["r"].min() >= 0.05


def test_select_columns(root):
    _build(root)
    got = data.table.get_lazy("t1", columns=["sym", "r"]).collect()
    assert got.columns == ["sym", "r"]


def test_select_pruning_equals_full(root):
    """裁剪路径结果与全量结果一致（含 partition + 谓词叠加）"""
    _build(root)
    lf = data.table.get_lazy("t1", partition=["year=2020", "year=2022"], where="r>=0.0")
    got = lf.collect()
    full = data.table.get("t1")
    expect = full.filter(
        (pl.col("year").is_in([2020, 2022])) & (pl.col("r") >= 0.0)
    )
    assert got.sort(["date", "sym"]).equals(expect.sort(["date", "sym"]))


def test_select_where_expr(root):
    _build(root)
    got = data.table.get_lazy("t1", where=pl.col("r") < 0).collect()
    assert got.height == 2


def test_select_empty_table(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    data.table.scan("t1")
    lf = data.table.get_lazy("t1")
    assert lf.collect().height == 0


def test_select_partition_no_match(root):
    _build(root)
    lf = data.table.get_lazy("t1", partition="year=1999")
    assert lf.collect().height == 0


def test_select_missing_table(root):
    with pytest.raises(TableNotFoundError):
        data.table.get_lazy("nope")


def test_get_limit(root):
    df = make_df([("2020-01-01", f"s{i}", i / 100) for i in range(10)])
    write_single(root, "t1", df)
    data.table.scan("t1")
    assert data.table.get("t1", limit=3).height == 3


def test_get_exclude_tool(root):
    df = pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01", "2020-01-02"], dtype=pl.Date),
        "sym": ["a", "b"],
        "r": [0.01, 0.02],
        "optime": ["2020-01-01", "2020-01-02"],
    })
    write_single(root, "t1", df)
    data.table.scan("t1")
    assert "optime" in data.table.get("t1").columns
    got = data.table.get("t1", exclude_tool=True)
    assert got.columns == ["date", "sym", "r"]
    # columns 显式指定时不受 exclude_tool 影响
    got2 = data.table.get_lazy("t1", columns=["sym", "optime"], exclude_tool=True).collect()
    assert got2.columns == ["sym", "optime"]

def test_select_filters_and_sort(root):
    """结构化过滤 + 排序 helper（nulls last）"""
    from stkoe.data.query import apply_sort, to_filters_expr

    _build(root)  # t1 5 行，r = 0.01/0.05/-0.02/0.10/-0.05, year HIVE
    filters = [
        {"field": "r", "op": "gte", "value": "0.0"},
        {"field": "sym", "op": "neq", "value": "e"},
    ]
    fexpr = to_filters_expr(filters)
    lf = data.table.get_lazy("t1")
    lf = lf.filter(fexpr)
    assert lf.collect().height == 3

    lf2 = apply_sort(data.table.get_lazy("t1"), [{"field": "r", "desc": True}, {"field": "sym"}])
    got = lf2.collect()
    assert got["r"].to_list() == [0.10, 0.05, 0.01, -0.02, -0.05]


def test_select_paging(root):
    """分页切片：slice 实现，DF 行序与全量一致"""
    _build(root)  # 5 行
    df = data.table.get_lazy("t1").slice(2, 2).collect()
    assert df.height == 2
    assert df["sym"].to_list() == ["c", "d"]
