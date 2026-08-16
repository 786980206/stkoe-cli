# -*- coding: utf-8 -*-
"""dbt manifest.json 元数据桥接测试：config 键 + table/index add 应用/覆盖语义"""
import os

import polars as pl
import pytest

from stkoe.dbt import asset_meta, column_meta, find_node
from stkoe.settings import load_config, save_config

MANIFEST = {
    "nodes": {
        "model.fin.m1": {
            "name": "m1", "resource_type": "model",
            "description": "成员表描述",
            "meta": {"display_name": "成员表", "tags": "a,b"},
            "columns": {
                "sym": {"name": "sym", "description": "证券代码",
                        "meta": {"unit": "无"}},
                "date": {"name": "date", "description": "交易日"},
                "price": {"name": "price", "description": "价格",
                          "meta": {"display_name": "收盘价", "unit": "元",
                                   "tags": "core"}},
            },
        },
        "model.fin.idx": {
            "name": "idx", "resource_type": "model", "description": "索引表描述",
            "columns": {
                "sym": {"name": "sym", "description": "代码", "meta": {"unit": "无"}},
                "date": {"name": "date", "description": "日期"},
                "x": {"name": "x", "description": "因子输入"},
            },
        },
    },
    "sources": {
        "source.fin.raw_t1": {"name": "raw_t1", "resource_type": "source",
                              "alias": "t1", "description": "源表"},
    },
}


@pytest.fixture()
def manifest_file(tmp_path):
    import json

    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(MANIFEST, ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture()
def cfg(tmp_path, monkeypatch):
    """STKOE_CONFIG 指向临时配置；save_config 写入 dbt-manifest-file"""
    p = tmp_path / "stkoe.json"
    monkeypatch.setenv("STKOE_CONFIG", str(p))
    return p


def _write_table(root, name, rows):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _write_idx(root, name, rows):
    d = root / "index" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _svc(root):
    from stkoe.graph.service import GraphService

    return GraphService(data_dir=root)


# ---------- 解析单元 ----------

def test_find_node_by_name_and_alias(manifest_file):
    assert find_node(manifest_file, "m1")["description"] == "成员表描述"
    assert find_node(manifest_file, "t1")["name"] == "raw_t1"  # alias 回退
    assert find_node(manifest_file, "nope") is None


def test_asset_and_column_meta(manifest_file):
    node = find_node(manifest_file, "m1")
    am = asset_meta(node)
    assert am["description"] == "成员表描述"
    assert am["display_name"] == "成员表"
    assert am["tags"] == ["a", "b"]
    cm = column_meta(node)
    assert cm["price"] == {"description": "价格", "display_name": "收盘价",
                           "unit": "元", "tags": ["core"]}
    assert cm["date"] == {"description": "交易日"}


# ---------- config 键 ----------

def test_config_roundtrip(cfg):
    save_config({"dbt-manifest-file": "./target/manifest.json"})
    c = load_config()
    assert c.dbt_manifest_file == "./target/manifest.json"
    assert c.to_dict()["dbt-manifest-file"] == "./target/manifest.json"


# ---------- table add 应用与覆盖 ----------

def test_table_add_applies_manifest(cfg, manifest_file, tmp_path):
    save_config({"dbt-manifest-file": str(manifest_file)})
    _write_table(tmp_path, "m1", pl.DataFrame({
        "sym": ["a"], "date": ["2024-01-01"], "price": [1.0]}))
    svc = _svc(tmp_path)
    svc.table_add("m1")
    m = svc.table_meta("m1")
    assert m["description"] == "成员表描述"
    assert m["display_name"] == "成员表"
    assert m["tags"] == ["a", "b"]
    cols = {c["name"]: c for c in m["columns"]}
    assert cols["price"]["description"] == "价格"
    assert cols["price"]["display_name"] == "收盘价"
    assert cols["price"]["unit"] == "元"
    assert cols["price"]["tags"] == ["core"]
    assert cols["date"]["description"] == "交易日"
    svc.close()


def test_table_add_param_overrides_manifest(cfg, manifest_file, tmp_path):
    save_config({"dbt-manifest-file": str(manifest_file)})
    _write_table(tmp_path, "m1", pl.DataFrame({
        "sym": ["a"], "date": ["2024-01-01"], "price": [1.0]}))
    svc = _svc(tmp_path)
    svc.table_add("m1", meta={"description": "参数描述"})
    m = svc.table_meta("m1")
    assert m["description"] == "参数描述"  # 参数覆盖 manifest
    assert m["display_name"] == "成员表"  # 未显式指定的 manifest 值保留
    assert m["columns"][2]["description"] == "价格"
    svc.close()


def test_table_add_all_applies_manifest(cfg, manifest_file, tmp_path):
    save_config({"dbt-manifest-file": str(manifest_file)})
    _write_table(tmp_path, "m1", pl.DataFrame({
        "sym": ["a"], "date": ["2024-01-01"], "price": [1.0]}))
    _write_table(tmp_path, "other", pl.DataFrame({"k": [1]}))  # 不在 manifest
    svc = _svc(tmp_path)
    out = svc.table_add("", all=True)
    assert {r["name"] for r in out} == {"m1", "other"}
    assert svc.table_meta("m1")["description"] == "成员表描述"
    assert svc.table_meta("other")["description"] == ""  # 无匹配 → 空
    svc.close()


# ---------- index add ----------

def test_index_add_applies_manifest(cfg, manifest_file, tmp_path):
    save_config({"dbt-manifest-file": str(manifest_file)})
    _write_idx(tmp_path, "idx", pl.DataFrame({
        "sym": ["a"], "date": ["2024-01-01"], "x": [1.0]}))
    svc = _svc(tmp_path)
    svc.index_add("idx")
    m = svc.index_meta("idx")
    assert m["description"] == "索引表描述"
    cols = {c["name"]: c for c in m["columns"]}
    assert cols["x"]["description"] == "因子输入"
    assert cols["sym"]["unit"] == "无"
    svc.close()


# ---------- 未配置 / 配置错误 ----------

def test_add_without_manifest_unaffected(tmp_path):
    _write_table(tmp_path, "m1", pl.DataFrame({"k": [1]}))
    svc = _svc(tmp_path)
    svc.table_add("m1")
    m = svc.table_meta("m1")
    assert m["description"] == "" and m["display_name"] == "m1"
    assert m["columns"][0]["description"] == ""
    svc.close()


def test_add_with_missing_manifest_file_errors(cfg, tmp_path):
    save_config({"dbt-manifest-file": str(tmp_path / "nope.json")})
    _write_table(tmp_path, "m1", pl.DataFrame({"k": [1]}))
    svc = _svc(tmp_path)
    with pytest.raises(ValueError, match="dbt manifest"):
        svc.table_add("m1")
    svc.close()
