"""config 模块测试：默认数据路径 + 工具字段配置"""
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

import stkoe.data as data

cfg = importlib.import_module("stkoe.data.settings")
from stkoe.data.settings import (  # noqa: E402
    StkoeConfig,
    load_config,
    save_config,
)

from conftest import make_df, write_single


def _write_cfg(monkeypatch, tmp_path, **kw) -> cfg.StkoeConfig:
    p = tmp_path / "stkoe.json"
    c = StkoeConfig(**kw)
    cfg.save_config(c, path=p)
    monkeypatch.setenv("STKOE_CONFIG", str(p))
    return c


def test_default_config(monkeypatch, tmp_path):
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "nope.json"))
    c = cfg.load_config()
    assert c.data_path is None
    assert c.ignore_cols == ("optime",)


def test_config_load_roundtrip(monkeypatch, tmp_path):
    c = _write_cfg(monkeypatch, tmp_path, data_path="/tmp/x", ignore_cols=["_etl", "_tag"])
    got = cfg.load_config()
    assert got.data_path == "/tmp/x"
    assert got.ignore_cols == ("_etl", "_tag")


def test_config_ignore_cols_override(monkeypatch, tmp_path):
    _write_cfg(monkeypatch, tmp_path, ignore_cols=["_sys", "_etl"])
    assert cfg.load_config().ignore_cols == ("_sys", "_etl")


def test_resolve_data_path_priority(monkeypatch, tmp_path):
    _write_cfg(monkeypatch, tmp_path, data_path="/cfg/path")
    # 配置文件 data_path 生效（未设环境变量）
    monkeypatch.delenv("STKOE_LOCAL_DATA", raising=False)
    assert str(cfg.resolve_data_path()) == str(Path("/cfg/path"))
    # 环境变量优先
    monkeypatch.setenv("STKOE_LOCAL_DATA", "/env/path")
    assert str(cfg.resolve_data_path()) == str(Path("/env/path"))


def test_set_config(monkeypatch, tmp_path):
    _write_cfg(monkeypatch, tmp_path)
    data.set_config(data_path="/new/root", ignore_cols=["_x", "_y"])
    c = cfg.load_config()
    assert c.data_path == "/new/root"
    assert c.ignore_cols == ("_x", "_y")
    # 未指定字段保持原值
    data.set_config(data_path="/other")
    assert cfg.load_config().data_path == "/other"
    assert cfg.load_config().ignore_cols == ("_x", "_y")


def test_tool_cols_marked(root):
    """meta 中工具字段被标记 is_tool；data_cols 剔除之"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)])).parent
    # 表带 optime 工具列
    df = pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01"], dtype=pl.Date),
        "sym": ["a"],
        "optime": ["2020-01-01 08:00:00"],
    })
    write_single(root, "t2", df)
    data.table.scan("t2")
    m = data.table.meta("t2")
    tool = [c for c in m.columns if c.is_tool]
    assert [c.name for c in tool] == ["optime"]
    assert "optime" not in data.data_cols(m.columns)


def test_select_exclude_tool(root):
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
    got2 = data.table.get("t1", columns=["sym", "optime"], exclude_tool=True)
    assert got2.columns == ["sym", "optime"]


def test_cli_config_set(monkeypatch, tmp_path):
    from typer.testing import CliRunner
    from stkoe.data.cli import app

    p = tmp_path / "stkoe.json"
    monkeypatch.setenv("STKOE_CONFIG", str(p))
    r = CliRunner().invoke(app, ["config", "set", "--data-path", "/cli/root", "--ignore-cols", "_a,_b"])
    assert r.exit_code == 0
    raw = json.loads(p.read_text("utf-8"))
    assert raw["data_path"] == "/cli/root"
    assert raw["ignore_cols"] == ["_a", "_b"]
