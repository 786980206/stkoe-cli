# -*- coding: utf-8 -*-
"""dbt manifest 元数据导入测试：table add/set --dbt-manifest 合并表/列描述"""
import json
from pathlib import Path

import polars as pl
import pytest

import stkoe.data as data
from stkoe.data import table
from stkoe.data.dbt import (
    DbtManifestError,
    DbtNodeNotFoundError,
    find_node,
    resolve_manifest,
)

from conftest import make_df, write_single


def _write_manifest(root: Path, nodes: dict) -> Path:
    """写 DBT 项目结构：<root>/dbt/target/manifest.json"""
    d = root / "dbt" / "target"
    d.mkdir(parents=True, exist_ok=True)
    manifest = d / "manifest.json"
    manifest.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return manifest


def _model_node(**kw) -> dict:
    base = {
        "resource_type": "model",
        "package_name": "stk",
        "name": "mdl_foo",
        "alias": "mdl_foo",
        "original_file_path": "models/mdl_foo.sql",
        "description": "dbt 模型描述",
        "tags": ["core", "daily"],
        "config": {"materialized": "table"},
        "columns": {
            "sym": {"name": "sym", "description": "股票代码", "tags": ["key"]},
            "close": {"name": "close", "description": "收盘价", "data_type": "decimal(12,2)"},
        },
    }
    base.update(kw)
    return base


def _registered(name, root) -> table.TableMeta:
    return table.meta(name)


def test_resolve_manifest_file_and_dir(tmp_path, monkeypatch):
    """文件直接定位；目录自动找 target/manifest.json；env 缺省"""
    p = _write_manifest(tmp_path, {})
    assert resolve_manifest(p) == p
    assert resolve_manifest(p.parent) == p          # dbt 项目根目录
    monkeypatch.setenv("STKOE_DBT_MANIFEST", str(p))
    assert resolve_manifest(None) == p
    monkeypatch.delenv("STKOE_DBT_MANIFEST")
    with pytest.raises(DbtManifestError):
        resolve_manifest(tmp_path / "nope")


def test_add_applies_dbt_meta(root, tmp_path):
    """add --dbt-manifest：表/列描述、extra.dbt 落 catalog，不 bump version"""
    write_single(root, "mdl_foo", make_df([("2024-01-01", "a", 1.0)]))
    m = _write_manifest(tmp_path, {"model.stk.mdl_foo": _model_node()})
    rep = table.add("mdl_foo", dbt_manifest=str(m))
    assert rep.changed

    meta = table.meta("mdl_foo")
    assert meta.description == "dbt 模型描述"
    assert meta.tags == ("core", "daily")
    by = {c.name: c for c in meta.columns}
    assert by["sym"].description == "股票代码"
    assert list(by["sym"].tags) == ["key"]
    # 物理表没有的列（close）跳过，不新增列
    assert "close" not in by
    assert meta.extra["dbt"]["package"] == "stk"
    assert meta.extra["dbt"]["materialized"] == "table"


def test_add_dir_manifest_resolution(root, tmp_path):
    """--dbt-manifest 传项目目录（非文件）同样生效"""
    write_single(root, "mdl_foo", make_df([("2024-01-01", "a", 1.0)]))
    _write_manifest(tmp_path, {"model.stk.mdl_foo": _model_node()})
    table.add("mdl_foo", dbt_manifest=str(tmp_path / "dbt"))
    assert table.meta("mdl_foo").description == "dbt 模型描述"


def test_set_merges_dbt_and_explicit_wins(root, tmp_path):
    """set --dbt-manifest 合并；--desc 显式覆盖 dbt；列说明保留"""
    write_single(root, "mdl_foo", make_df([("2024-01-01", "a", 1.0)]))
    m = _write_manifest(tmp_path, {"model.stk.mdl_foo": _model_node()})
    table.add("mdl_foo")
    v_before = table.meta("mdl_foo").version

    t = table.set("mdl_foo", dbt_manifest=str(m), description="显式描述", display_name="Foo")
    assert t.description == "显式描述"          # 显式参数优先
    assert t.display_name == "Foo"
    assert t.version == v_before                # 纯元数据不 bump
    assert t.extra["dbt"]["table"] == "mdl_foo"
    by = {c.name: c for c in t.columns}
    assert by["sym"].description == "股票代码"


def test_add_node_missing_raises(root, tmp_path):
    """manifest 里无同名模型 → 明确报错"""
    write_single(root, "other", make_df([("2024-01-01", "a", 1.0)]))
    m = _write_manifest(tmp_path, {"model.stk.mdl_foo": _model_node()})
    with pytest.raises(DbtNodeNotFoundError):
        table.add("other", dbt_manifest=str(m))


def test_add_missing_manifest_raises(root, tmp_path):
    write_single(root, "mdl_foo", make_df([("2024-01-01", "a", 1.0)]))
    with pytest.raises(DbtManifestError):
        table.add("mdl_foo", dbt_manifest=str(tmp_path / "no.json"))


def test_alias_matching(root, tmp_path):
    """表名匹配 node.alias（落库表名），而非模型 name"""
    write_single(root, "ods_daily", make_df([("2024-01-01", "a", 1.0)]))
    node = _model_node(name="daily_ingest", alias="ods_daily",
                       description="alias 匹配")
    m = _write_manifest(tmp_path, {"model.stk.daily_ingest": node})
    table.add("ods_daily", dbt_manifest=str(m))
    assert table.meta("ods_daily").description == "alias 匹配"
