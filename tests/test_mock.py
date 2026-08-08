"""mock 工具测试：参数化生成 + 写盘 + sniff 注册 + CLI"""
import polars as pl
import pytest

import stkoe.data as data
from stkoe.data import mock as mock_mod

from conftest import root  # noqa: F401


def test_tdcal(root):
    df = mock_mod.tdcal("2021-01-01", "2021-01-31")
    assert df.columns == ["date"]
    # 1 月自然日 31 天，去周末 = 21 天
    assert df.height == 21
    assert df["date"].dtype == pl.Date


def test_klday_shape_and_optime(root):
    df = mock_mod.klday(n_syms=10, start="2021-01-01", end="2021-01-31", seed=7)
    assert df.columns == ["date", "sym", "r", "ic", "fv", "sample", "optime"]
    assert df.height == 10 * 21
    assert df["sym"].n_unique() == 10
    assert df["sample"].min() == 0 and df["sample"].max() == 1
    assert df["optime"].cast(pl.Utf8).n_unique() == 1  # 工具字段为常量


def test_common_ic(root):
    df = mock_mod.common(n_syms=5, start="2021-01-01", end="2021-01-08", seed=1)
    assert df.columns == ["date", "sym", "ic"]
    # 每 sym 固定行业
    per_sym = df.group_by("sym").agg(pl.col("ic").n_unique().alias("n"))
    assert (per_sym["n"] == 1).all()


def test_feature(root):
    df = mock_mod.feature("zscore", n_syms=10, start="2021-01-01", end="2021-01-08", seed=3)
    assert df.columns == ["date", "sym", "zscore"]
    assert df.height == 10 * 6  # 2021-01-01~08 去周末 = 6 个交易日


def test_write_and_sniff(root):
    df = mock_mod.klday(n_syms=10, start="2021-01-01", end="2021-01-31", seed=9)
    report = mock_mod.write("mock_klday", df, partition_by="year")
    assert report.changed
    assert report.layout.value == "hive"
    m = data.describe("mock_klday")
    assert m.row_count == df.height
    assert m.partition_count == 1
    names = [c.name for c in m.columns]
    assert "optime" in names and any(c.is_tool for c in m.columns)


def test_write_single(root):
    report = mock_mod.write("mock_single", mock_mod.tdcal("2022-01-01", "2022-01-10"))
    assert report.layout.value == "single"


def test_write_demo(root):
    reports = mock_mod.write_demo(root, n_syms=20, start="2022-01-01", end="2022-03-31")
    names = {r.name for r in reports}
    assert names == {"mock_tdcal", "mock_common", "mock_klday", "mock_feature"}
    assert data.describe("mock_klday").layout.value == "hive"


def test_deterministic():
    a = mock_mod.klday(n_syms=10, start="2021-01-01", end="2021-01-08", seed=5)
    b = mock_mod.klday(n_syms=10, start="2021-01-01", end="2021-01-08", seed=5)
    assert a.equals(b)


def test_index(root):
    df = mock_mod.index(n_syms=10, start="2021-01-01", end="2021-01-08")
    assert df.columns == ["date", "sym"]
    assert df.height == 10 * 6  # 2021-01-01~08 去周末 = 6 个交易日
    assert df["sym"].n_unique() == 10


def test_cli_mock(root):
    from typer.testing import CliRunner
    from stkoe.data.cli import app

    r = CliRunner().invoke(app, ["mock", "gen", "mock_tdcal", "--kind", "tdcal"])
    assert r.exit_code == 0 and "mock_tdcal" in r.stdout
    assert "mock_tdcal" in {m.name for m in data.list()}
