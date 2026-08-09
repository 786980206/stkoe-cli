# -*- coding: utf-8 -*-
"""field 指标管理测试（catalog 注册语义）"""
import polars as pl
import pytest

import stkoe.data as data
from stkoe.data import field as F
from stkoe.data.field import FieldExistsError, FieldNotFoundError

from conftest import write_single


def _idx_df(n=2):
    return pl.DataFrame({
        "date": ["2020-01-01"] * n,
        "sym": [f"s{i}" for i in range(n)],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


def _mem_df(n=2):
    return pl.DataFrame({
        "date": ["2020-01-01"] * n,
        "sym": [f"s{i}" for i in range(n)],
        "val": [float(i + 1) for i in range(n)],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


def _make_ds(root, name="ds"):
    write_single(root, "idx", _idx_df())
    write_single(root, "mem", _mem_df())
    data.table.scan("idx")
    data.table.scan("mem")
    data.dataset.add(name, "idx", "mem", background=False)
    return name


def test_create_requires_dataset(root):
    from stkoe.data.dataset import DatasetNotFoundError
    with pytest.raises(DatasetNotFoundError):
        F.create("f1", "missing_ds")


def test_create_meta_and_list(root):
    _make_ds(root)
    F.create("f1", "ds", formula="val*2", description="测试指标")
    m = F.meta("f1")
    assert m.name == "f1"
    assert m.dataset == "ds"
    assert m.formula == "val*2"
    assert m.version >= 1

    with pytest.raises(FieldExistsError):
        F.create("f1", "ds")

    names = [x.name for x in F.list()]
    assert "f1" in names


def test_rename_and_del(root):
    _make_ds(root)
    F.create("f1", "ds")

    m = F.rename("f1", "f2")
    assert m.name == "f2"
    assert F.meta("f2").name == "f2"
    with pytest.raises(FieldNotFoundError):
        F.meta("f1")

    F.del_("f2")
    with pytest.raises(FieldNotFoundError):
        F.meta("f2")
    with pytest.raises(FieldNotFoundError):
        F.rename("f2", "f3")


def test_dep_registered(root):
    """field → dataset 依赖登记（血缘可见）"""
    _make_ds(root)
    F.create("f1", "ds")
    conn = data.catalog().conn
    rows = conn.execute(
        "SELECT dep_type, dep_name FROM stkoe_depends "
        "WHERE obj_type='field' AND obj_name='f1'").fetchall()
    assert ("dataset", "ds") in [(r["dep_type"], r["dep_name"]) for r in rows]