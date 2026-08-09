# -*- coding: utf-8 -*-
"""gRPC 服务测试：Execute 小结果 JSON + Select 表格 Arrow IPC"""
import json
import socket

import grpc
import polars as pl
import pytest

import stkoe.data as data
from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc
from stkoe.grpc.server import StkoeServer, server_port

from conftest import write_single


@pytest.fixture()
def srv(root):
    """每个测试独立 gRPC 服务（随机端口，避免 9569 冲突）"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = StkoeServer(port=port).start()
    yield srv
    srv.stop()


@pytest.fixture()
def client(srv):
    ch = grpc.insecure_channel(f"127.0.0.1:{srv.port}")
    stub = stkoe_pb2_grpc.StkoeServiceStub(ch)
    yield stub
    ch.close()


def _setup(root):
    df = pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"] * 4,
        "sym": [f"s{i}" for i in range(8)],
        "close": [10.0 + i for i in range(8)],
    })
    write_single(root, "t1", df)
    data.table.scan("t1")


def test_execute_meta_json(client, root):
    _setup(root)
    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["list"]))
    assert resp.code == 0
    items = json.loads(resp.json_out)
    assert "t1" in [x["name"] for x in items]

    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["meta", "t1"]))
    assert resp.code == 0
    meta = json.loads(resp.json_out)
    assert meta["name"] == "t1"
    assert any(c["name"] == "close" for c in meta["columns"])

    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="version"))
    assert json.loads(resp.json_out)["version"]


def test_execute_error(client):
    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["meta", "nope"]))
    assert resp.code == 2
    assert "nope" in resp.error


def test_select_missing(client):
    resp = client.Select(stkoe_pb2.SelectRequest(name="no_such_object"))
    assert resp.num_rows == 0
    assert resp.error != ""


def test_select_arrow_ipc(client, root):
    _setup(root)
    resp = client.Select(stkoe_pb2.SelectRequest(name="t1"))
    assert resp.num_rows == 8
    meta = json.loads(resp.schema_json)
    assert meta["name"] == "t1"
    assert {c["name"] for c in meta["columns"]} >= {"date", "sym", "close"}
    df = pl.read_ipc(resp.ipc)
    assert df.height == 8
    assert df["close"].max() == 17.0


def test_select_where_and_columns(client, root):
    _setup(root)
    resp = client.Select(stkoe_pb2.SelectRequest(
        name="t1", columns=["sym", "close"], where="close >= 14"))
    df = pl.read_ipc(resp.ipc)
    assert df.height == 4
    assert df["close"].min() >= 14


def test_server_port_config(root, tmp_path, monkeypatch):
    """server_port() 读配置 grpc_port（缺省 9569）"""
    assert server_port() == 9569
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "stkoe.json"))
    data.set_config(grpc_port=9876)
    assert server_port() == 9876


def test_port_conflict(srv):
    with pytest.raises(Exception):
        StkoeServer(port=srv.port).start()