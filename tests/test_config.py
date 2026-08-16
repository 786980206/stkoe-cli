# -*- coding: utf-8 -*-
"""stkoe.json 配置：settings 加载/保存 + CLI config show/set"""
import json
from pathlib import Path

import pytest

from stkoe import settings
from stkoe.cli import main


@pytest.fixture()
def cfg_env(tmp_path, monkeypatch):
    """隔离的配置路径（STKOE_CONFIG → tmp 下 stkoe.json）"""
    p = tmp_path / "stkoe.json"
    monkeypatch.setenv("STKOE_CONFIG", str(p))
    return p


def test_load_defaults(cfg_env):
    cfg = settings.load_config()
    assert cfg.grpc_host == "127.0.0.1"
    assert cfg.grpc_port == 9569
    assert cfg.data_dir == "~/.stkoe"
    assert cfg.to_dict() == {
        "grpc-host": "127.0.0.1", "grpc-port": 9569, "data-dir": "~/.stkoe"}


def test_save_then_load_merge(cfg_env):
    settings.save_config({"grpc-host": "0.0.0.0"})
    settings.save_config({"grpc-port": "9000"})
    cfg = settings.load_config()
    assert cfg.grpc_host == "0.0.0.0"
    assert cfg.grpc_port == 9000  # 数字键映射为 int


def test_save_keeps_hyphen_key(cfg_env):
    p = settings.save_config({"grpc-host": "0.0.0.0"})
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "grpc-host" in data  # 键名保持连字符，不转下划线


def test_config_path_env_priority(tmp_path, monkeypatch):
    """STKOE_CONFIG 优先于本地/家目录"""
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "x.json"))
    assert settings.config_path() == tmp_path / "x.json"
    assert settings.save_path() == tmp_path / "x.json"


def test_config_path_home_fallback(tmp_path, monkeypatch):
    """无 env 且无本地配置时，回退 ~/.stkoe/stkoe.json"""
    monkeypatch.delenv("STKOE_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)  # 隔离 cwd，避免命中本地 stkoe.json
    assert settings.config_path() == Path.home() / ".stkoe" / "stkoe.json"
    assert settings.save_path() == tmp_path / "stkoe.json"  # 写入仍优先本地


def test_corrupt_config_raises(cfg_env):
    cfg_env.write_text("{bad json", encoding="utf-8")
    with pytest.raises(settings.ConfigError):
        settings.load_config()


def test_extra_keys_preserved(cfg_env):
    """自定义任意键保留在 extra，to_dict 原样带出"""
    settings.save_config({"foo-bar": "baz"})
    cfg = settings.load_config()
    assert cfg.extra == {"foo-bar": "baz"}
    assert cfg.to_dict()["foo-bar"] == "baz"


def test_cli_config_show_defaults(cfg_env, capsys):
    assert main(["config", "show"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["grpc-host"] == "127.0.0.1"
    assert out["config_file"] == str(cfg_env)


def test_cli_config_set_and_show(cfg_env, capsys):
    assert main(["config", "set", "--grpc-host", "0.0.0.0",
                 "--grpc-port", "9000"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["written"] == str(cfg_env)
    assert out["set"] == {"grpc-host": "0.0.0.0", "grpc-port": "9000"}

    assert main(["config", "show"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["grpc-host"] == "0.0.0.0"
    assert out["grpc-port"] == 9000


def test_cli_config_set_empty_returns_error(cfg_env, capsys):
    assert main(["config", "set"]) == 2
    assert "用法" in capsys.readouterr().out


def test_cli_serve_uses_config_host_port(cfg_env, capsys):
    """stkoe serve 缺省 host/port 取配置 grpc-host / grpc-port"""
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    settings.save_config({"grpc-host": "127.0.0.1", "grpc-port": str(port)})
    from stkoe.grpc.server import serve

    srv = serve()
    assert srv.host == "127.0.0.1"
    assert srv.port == port  # 来自配置 grpc-port
    srv.stop()


def test_cli_graph_commands(cfg_env, tmp_path, capsys, monkeypatch):
    """CLI graph 子命令（§9）：lineage/nodes/stats 与 Execute 同一分发"""
    import polars as pl

    data_dir = tmp_path / "data"
    (data_dir / "index" / "index").mkdir(parents=True)
    pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"], "code": [1]}).write_parquet(
        data_dir / "index" / "index" / "data.parquet")
    (data_dir / "table" / "m1").mkdir(parents=True)
    pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"], "price": [1.5]}).write_parquet(
        data_dir / "table" / "m1" / "data.parquet")
    settings.save_config({"data-dir": str(data_dir)})
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=data_dir)
    svc.table_add("m1")
    svc.index_add("index")
    svc.close()

    assert main(["graph", "stats"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["node_count"] == 2

    assert main(["graph", "nodes"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert {n["name"] for n in out} == {"index", "m1"}

    assert main(["graph", "lineage", "--node", "table:m1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["elements"]  # Cytoscape elements payload

    assert main(["graph", "lineage", "--node", "panel:xx"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["elements"]["nodes"] == [] and out["elements"]["edges"] == []  # 不存在 → 空图
