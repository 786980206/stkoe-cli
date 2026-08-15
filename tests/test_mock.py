# -*- coding: utf-8 -*-
"""mock 接口测试：demo/gen 生成、写盘、table add 发现、任务版链路"""
import json

import polars as pl
import pytest

from stkoe.mock.gen import (INDUSTRIES, common, demo, demo_index, demo_m1,
                             feature, gen, index, klday, m1, tdcal, write)


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


# ---------- 生成器 ----------

def test_demo_index_shape():
    """演示 index 表：date×sym 面板，含 r/ic/fv/x，日期为字符串"""
    df = demo_index(n_syms=10, n_days=5)
    assert df.height == 50
    assert df.columns == ["date", "sym", "r", "ic", "fv", "x"]
    assert df["date"].dtype == pl.Utf8
    assert df["date"].to_list()[:10] == ["2024-01-01"] * 10
    assert df["sym"].n_unique() == 10


def test_demo_default_is_300x500():
    """默认演示规模：300 只 × 500 个交易日 = 15 万行"""
    df = demo_index()
    assert df.height == 300 * 500
    assert df["sym"].n_unique() == 300
    assert df["date"].n_unique() == 500
    assert df["date"].min() == "2024-01-01"


def test_demo_m1_shape():
    df = demo_m1(n_syms=10, n_days=5)
    assert df.height == 50
    assert df.columns == ["date", "sym", "name", "industry"]
    assert df["date"].dtype == pl.Utf8


def test_tdcal_weekdays_only():
    """交易日历只含周一~周五，日期升序"""
    df = tdcal("2024-01-01", "2024-01-07")
    assert df["date"].to_list() == ["2024-01-01", "2024-01-02", "2024-01-03",
                                    "2024-01-04", "2024-01-05"]


def test_index_has_example_columns():
    df = index(n_syms=6, seed=1)
    assert set(df.columns) == {"date", "sym", "r", "ic", "fv", "x"}
    assert df.height == 6 * len(df["date"].unique())
    assert df["date"].dtype == pl.Utf8


def test_m1_joinable_with_index():
    idx = index(n_syms=6, seed=1)
    sec = m1(n_syms=6, seed=1)
    assert set(idx.columns) & set(sec.columns) == {"date", "sym"}
    joined = idx.join(sec, on=["date", "sym"], how="inner")
    assert joined.height == idx.height


def test_klday_columns():
    df = klday(n_syms=4, seed=7)
    assert {"date", "sym", "r", "ic", "fv", "sample", "optime"} <= set(df.columns)
    assert df["sample"].dtype in (pl.Int8, pl.Int64)


def test_feature_named_column():
    df = feature("zscore", n_syms=4, seed=3)
    assert df.columns == ["date", "sym", "zscore"]


def test_common_uses_industries():
    df = common(n_syms=5, seed=1)
    assert df["ic"].to_list()
    assert all(v in INDUSTRIES for v in df["ic"].unique().to_list())


def test_generators_deterministic_with_seed():
    a = index(n_syms=8, seed=42)
    b = index(n_syms=8, seed=42)
    c = index(n_syms=8, seed=7)
    assert a.equals(b)
    assert not a.equals(c)


# ---------- 写盘 ----------

def test_write_creates_flat_parquet(tmp_path):
    df = demo_index(n_syms=4, n_days=3)
    rep = write(tmp_path / "data", "index", df, subdir="index")
    path = tmp_path / "data" / "index" / "index" / "data.parquet"
    assert path.exists()
    assert rep["name"] == "index"
    assert rep["rows"] == 12
    assert rep["columns"] == df.columns
    assert pl.read_parquet(path).equals(df)


def test_demo_writes_index_and_m1(tmp_path):
    reports = demo(tmp_path / "data", n_syms=10, n_days=5)
    assert [r["name"] for r in reports] == ["index", "m1"]
    assert reports[0]["rows"] == 50
    assert (tmp_path / "data" / "index" / "index" / "data.parquet").exists()
    assert (tmp_path / "data" / "table" / "m1" / "data.parquet").exists()


def test_gen_single_table(tmp_path):
    rep = gen("g1", "klday", data_dir=tmp_path / "data", seed=1)
    assert rep["name"] == "g1"
    assert (tmp_path / "data" / "table" / "g1" / "data.parquet").exists()
    assert pl.read_parquet(rep["path"]).height == 30


def test_gen_unknown_kind_error(tmp_path):
    with pytest.raises(ValueError):
        gen("x", "bogus", data_dir=tmp_path / "data")


# ---------- 发现语义：mock 只写盘，add 才登记 ----------

def test_add_discovers_mock_tables(tmp_path):
    """demo 写 indexs/index + tables/m1；index 走 index add、m1 走 table add 才登记"""
    from stkoe.graph.service import GraphService

    demo(tmp_path / "data", n_syms=10, n_days=5)
    svc = GraphService(data_dir=tmp_path / "data")
    svc.table_add("m1")
    svc.index_add("index")
    assert [t["name"] for t in svc.table_list()] == ["m1"]
    assert [i["name"] for i in svc.index_list()] == ["index"]
    df = svc.index_get("index")
    assert df.height == 50


# ---------- 任务版（s:mock demo / s:mock gen） ----------

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
    evs = mgr.events.list_by_task(task.task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None


def test_task_mock_demo(mgr):
    """任务版 s:mock demo：写 tables/index + tables/m1，终态 data 为写入清单"""
    task = mgr.submit("mock", "demo", [])
    done = _await(mgr, task)
    assert done.state == "succeeded"
    reports = _mgr_result(mgr, task)
    assert [r["name"] for r in reports] == ["index", "m1"]
    assert (mgr.data_dir / "index" / "index" / "data.parquet").exists()
    assert (mgr.data_dir / "table" / "m1" / "data.parquet").exists()


def test_task_mock_gen(mgr):
    task = mgr.submit("mock", "gen",
                      ["g1", "--kind", "feature", "--col", "zscore",
                       "--n-syms", "4"])
    done = _await(mgr, task)
    assert done.state == "succeeded"
    rep = _mgr_result(mgr, task)
    assert rep["name"] == "g1"
    assert rep["rows"] == 4 * 3
    assert (mgr.data_dir / "table" / "g1" / "data.parquet").exists()


def test_task_mock_gen_missing_name_fails(mgr):
    task = mgr.submit("mock", "gen", [])
    done = _await(mgr, task)
    assert done.state == "failed"
    assert "需要表名" in done.error
