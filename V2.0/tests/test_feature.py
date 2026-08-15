# -*- coding: utf-8 -*-
"""V2.0 死代码 FeatureController 回归测试（默认全量不收集；如需单独运行：
.venv/Scripts/python.exe -m pytest V2.0/tests/test_feature.py -q）

V3.0 起 feature 资产走 GraphService（src/stkoe/graph/service.py），
本文件保留对 src/stkoe/feature/controller.py（死代码）的行为回归存档。
原始 V2.0 基线测试见 git f290378（V2.0 全量备份）。
"""
import polars as pl
import pytest

from stkoe.feature import FeatureController, FeatureNotFoundError
from stkoe.feature.engine import engine_names, get_engine


@pytest.fixture()
def ctl(tmp_path):
    return FeatureController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")

def _write_idx(root, name, rows):
    """index 资产写 index/ 目录（独立于 table/）"""
    d = root / "index" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_source(tmp_path):
    """源：idx 表 + dataset ds（keys=k）+ sample sp1（formula 过滤）"""
    root = tmp_path / "data"
    _write(root, "idx", pl.DataFrame({
        "k": ["a", "b", "c"],
        "x": [1.0, 2.0, 3.0],
        "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    }))
    from stkoe.table import TableController

    from stkoe.dataset import DatasetController

    from stkoe.sample import SampleController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx", meta={"type": "index"}))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds", "idx", keys=["k"]))
    sc = SampleController(data_dir=root)
    _run(sc.add("sp1", dataset="ds"))
    _run(sc.add("sp2", dataset="ds", formula="x>=2.0"))
    return root


def test_engine_registry_has_polars(ctl):
    assert "polars" in engine_names()
    assert get_engine("polars").name == "polars"


def test_add_requires_formula(ctl):
    with pytest.raises(ValueError):
        _add(ctl, "f1")


def test_add_meta_flow(ctl):
    ft = _add(ctl, "f1", formula="x*2", display_name="因子1", unit="元", tags="a,b")
    assert ft.formula == "x*2"
    assert ft.display_name == "因子1"
    assert ft.unit == "元"
    assert ft.tags == ("a", "b")
    assert ft.engine == "polars"
    assert ft.version == 1


def test_add_duplicate(ctl):
    _add(ctl, "f1", formula="x")
    from stkoe.feature import FeatureExistsError

    with pytest.raises(FeatureExistsError):
        _add(ctl, "f1", formula="x")


def test_set_updates(ctl):
    _add(ctl, "f1", formula="x*2")
    ft = _run(ctl.set("f1", formula="x*3", display_name="改名", custom="v"))
    assert ft.formula == "x*3"
    assert ft.display_name == "改名"
    assert ft.extra.get("custom") == "v"
    assert ft.version == 2


def test_meta_and_delete(ctl):
    _add(ctl, "f1", formula="x")
    assert _meta(ctl, "f1").name == "f1"
    with pytest.raises(FeatureNotFoundError):
        _meta(ctl, "nope")
    assert _run(ctl.delete("f1")) == {"deleted": "f1"}
    with pytest.raises(FeatureNotFoundError):
        _run(ctl.delete("f1"))


def test_list(ctl):
    _add(ctl, "f1", formula="x")
    _add(ctl, "f2", formula="y")
    assert [ft.name for ft in _run(ctl.list())] == ["f1", "f2"]


def test_test_valid_factor(ctl, tmp_path):
    """公式逐行求值且行数==样本行数 → valid（与 fieldset field 行为一致）"""
    _setup_source(tmp_path)
    _add(ctl, "f1", formula="x*2")
    res, df = _run(ctl.test("f1", "sp1"))
    assert res.ok is True
    assert res.valid is True
    assert res.rows == 3
    assert df is not None
    assert df["field"].to_list() == [2.0, 4.0, 6.0]


def test_test_respects_sample_filter(ctl, tmp_path):
    """test 作用在 sample 过滤后的视图上：sp2 仅 2 行"""
    _setup_source(tmp_path)
    _add(ctl, "f1", formula="x*2")
    res, df = _run(ctl.test("f1", "sp2"))
    assert res.valid is True
    assert res.rows == 2
    assert df["field"].to_list() == [4.0, 6.0]


def test_test_aggregate_invalid(ctl, tmp_path):
    """聚合公式（行数!=样本行数）→ valid=False"""
    _setup_source(tmp_path)
    _add(ctl, "f1", formula="pl.col('x').sum()")
    res, df = _run(ctl.test("f1", "sp1"))
    assert res.ok is True
    assert res.valid is False
    assert "需逐行计算" in res.message


def test_test_bad_formula(ctl, tmp_path):
    """公式执行失败 → ok=False，无结果数据"""
    _setup_source(tmp_path)
    _add(ctl, "f1", formula="nope+1")
    res, df = _run(ctl.test("f1", "sp1"))
    assert res.ok is False
    assert res.valid is False
    assert df is None


def test_test_unknown_sample(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "f1", formula="x")
    with pytest.raises(Exception):
        _run(ctl.test("f1", "nope"))


def test_delete_sample_not_blocked_by_feature(ctl, tmp_path):
    """feature 是纯定义、不依赖 sample：删除 sample 不受 feature 影响"""
    from stkoe.sample import SampleController

    root = _setup_source(tmp_path)
    _add(ctl, "f1", formula="x")
    sc = SampleController(data_dir=root)
    assert _run(sc.delete("sp1")) == {"deleted": "sp1"}
    assert _meta(ctl, "f1").name == "f1"


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name, **kw):
    return _run(ctl.add(name, **kw))


def _meta(ctl, name):
    return _run(ctl.meta(name))
