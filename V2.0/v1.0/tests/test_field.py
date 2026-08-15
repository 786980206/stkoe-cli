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

def test_update_set_and_execute(root):
    """field.update：formula/dataset 变更清空物化状态；test 执行返回前 N 行"""
    _make_ds(root)
    code1 = "def calc(data):\n    return data.with_columns((pl.col('val') * 2).alias('f1'))"
    F.create("f1", "ds", formula=code1)
    assert F.meta("f1").formula == code1

    code2 = "def calc(data):\n    return data.with_columns((pl.col('val') * 3).alias('f1'))"
    m = F.set("f1", formula=code2)
    assert m.formula == code2
    assert m.materialized is False

    out = F.test("f1", limit=1)
    assert out["name"] == "f1"
    assert out["dataset"] == "ds"
    assert "f1" in out["columns"]
    assert out["totalRows"] == 2
    assert {r["f1"] for r in out["rows"]} <= {3.0, 6.0}  # val*3；limit=1 取任意一行


def test_execute_requires_bound_dataset(root):
    _make_ds(root)
    F.create("f2", "ds", formula="def calc(data):\n    return data.with_columns((pl.col('val') * 2).alias('f2'))")
    with pytest.raises(ValueError):
        F.test("f_missing")
    F.del_("f2")


def test_materialize_writes_parquet_and_flag(root):
    """物化：结果须含与指标名同名的列；落盘 fields/<name>/data.parquet；meta 置已物化"""
    _make_ds(root)
    code = (
        "def calc(data):\n"
        "    return data.with_columns((pl.col('val') * 2).alias('f_cls'))\n"
    )
    F.create("f_cls", "ds", formula=code)
    m = F.meta("f_cls")
    assert m.materialized is False

    out = F.materialize("f_cls", ctl=None)
    assert out["rows"] == 2
    assert out["column"] == "f_cls"
    fpath = (data.get_root() / "fields" / "f_cls" / "data.parquet")
    assert fpath.exists()
    df = pl.read_parquet(fpath)
    assert df.columns == ["f_cls"]
    m = F.meta("f_cls")
    assert m.materialized is True


def test_materialize_column_mismatch(root):
    """物化要求结果含与指标名同列的列，否则报错"""
    _make_ds(root)
    F.create("f_bad", "ds", formula="def calc(data):\n    return data.with_columns((pl.col('val') * 2).alias('val'))")
    with pytest.raises(ValueError, match="同名"):
        F.materialize("f_bad")


def test_code_execute_without_registration(root):
    """test_code：未注册公式直接执行（测试-保存前预览）, 不写 catalog/磁盘"""
    _make_ds(root)
    code = "def calc(data):\n    return data.with_columns((pl.col('val') * 4).alias('v4'))"
    out = F.test_code("ds", code, limit=1)
    assert out["dataset"] == "ds"
    assert "v4" in out["columns"]
    assert out["totalRows"] == 2
    assert set(r["v4"] for r in out["rows"]) <= {4.0, 8.0}
    assert len(out["rows"]) == 1
    # 未注册任何 catalog 对象
    assert F.list() == []
    with pytest.raises(ValueError, match="未指定数据集"):
        F.test_code("", code)
    with pytest.raises(ValueError, match="代码为空"):
        F.test_code("ds", "")
