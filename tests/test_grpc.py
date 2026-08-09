# -*- coding: utf-8 -*-
"""gRPC 服务测试：Execute 小结果 JSON + Select 表格 Arrow IPC"""
import json
import socket
from pathlib import Path

import grpc
import polars as pl
import pytest

import io

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


def test_server_host_config(root, tmp_path, monkeypatch):
    """server_host() 读配置 grpc_host（缺省 127.0.0.1；可改 0.0.0.0）"""
    from stkoe.grpc.server import server_host
    assert server_host() == "127.0.0.1"
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "stkoe.json"))
    data.set_config(grpc_host="0.0.0.0")
    assert server_host() == "0.0.0.0"
    # 与端口同写：config set 同时持久化 host+port
    data.set_config(grpc_host="0.0.0.0", grpc_port=9123)
    assert data.config().grpc_host == "0.0.0.0"
    assert data.config().grpc_port == 9123


def test_execute_config_show_has_host(client, root):
    """Execute config show 输出含 grpc_host（portal 桥可读）"""
    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="config", args=["show"]))
    assert resp.code == 0
    out = json.loads(resp.json_out)
    assert "grpc_host" in out
    assert out["grpc_host"] == "127.0.0.1"


def test_port_conflict(srv):
    with pytest.raises(Exception):
        StkoeServer(port=srv.port).start()

# ---------- Health / 新 Execute 动词 / Select 分页 / RunTask 流 ----------

def test_health(client):
    resp = client.Health(stkoe_pb2.HealthRequest())
    assert resp.status == "ok"
    assert resp.version


def test_execute_table_candidates(client, root):
    _setup(root)
    d = root / "tables" / "orphan_row"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"a": [1]}).write_parquet(d / "p0.parquet")
    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["candidates"]))
    assert resp.code == 0
    assert "orphan_row" in json.loads(resp.json_out)["tables"]


def test_execute_dataset_validate(client, root):
    df = pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "sym": ["s0", "s1"],
        "cv": [1.0, 2.0],
    })
    write_single(root, "i2", df)
    write_single(root, "m2", df)
    data.table.scan("i2")
    data.table.scan("m2")
    data.dataset.add("ds2", "i2", "m2", background=False)
    resp = client.Execute(stkoe_pb2.ExecuteRequest(
        cmd="dataset", args=["validate", "ds2", "--mode", "full"]))
    assert resp.code == 0
    out = json.loads(resp.json_out)
    assert out["valid"] is True
    assert {t["name"] for t in out["tables"]} == {"i2", "m2"}


def test_select_paging_filter_sort(client, root):
    _setup(root)  # 8 行：close=10..17
    # page=2 page_size=3, sym 降序 → 期望第 2 页 = s4..s2
    resp = client.Select(stkoe_pb2.SelectRequest(
        name="t1", type="table", page=2, page_size=3,
        sort=[stkoe_pb2.SortField(field="close", desc=True)]))
    assert not resp.error
    df = pl.read_ipc(source=io.BytesIO(resp.ipc))
    assert resp.num_rows == 3
    assert resp.total == 8
    assert df["close"].to_list() == [14.0, 13.0, 12.0]

    # filter: close>=15
    resp = client.Select(stkoe_pb2.SelectRequest(
        name="t1", type="table",
        filter=[stkoe_pb2.Filter(field="close", op="gte", value="15")],
        sort=[stkoe_pb2.SortField(field="close", desc=True)]))
    df = pl.read_ipc(source=io.BytesIO(resp.ipc))
    assert resp.total == 3
    assert df["close"].to_list() == [17.0, 16.0, 15.0]


def test_run_task_field_materialize_stream(client, root):
    _setup(root)
    data.dataset.add("ds", "t1", "t1", background=False)  # t1 同时作索引与成员
    code = "def calc(data):\n    return data.with_columns((pl.col('close') * 2).alias('f'))"
    data.field.create("f", "ds", formula=code)
    resp = client.RunTask(stkoe_pb2.TaskRequest(
        cmd="field", args=["materialize", "f"], task_id="task-1"))
    events = list(resp)
    types = [e.type for e in events]
    assert "result" in types and "done" in types
    res = json.loads([e for e in events if e.type == "result"][0].data)
    assert res["rows"] == 8 and res["column"] == "f"
    assert (data.get_root() / "fields" / "f" / "data.parquet").exists()


def test_run_task_error_stream(client, root):
    resp = client.RunTask(stkoe_pb2.TaskRequest(cmd="dataset", args=["scan", "missing"], task_id="x"))
    events = list(resp)
    assert events[-1].type == "error" and "missing" in events[-1].error


def test_run_task_dataset_materialize_payload(client, root):
    _setup(root)
    data.dataset.add("ds", "t1", "t1", background=False)
    resp = client.RunTask(stkoe_pb2.TaskRequest(
        cmd="dataset", args=["materialize", "ds"], task_id="task-ds"))
    events = list(resp)
    assert [e.type for e in events][-1] == "done"
    res = json.loads([e for e in events if e.type == "result"][0].data)
    # 物化结果契约：datasetId / rows / dataFile / elapsedMs
    assert res["datasetId"] == "ds"
    assert res["rows"] == 8
    assert res["dataFile"] and Path(res["dataFile"]).exists()
    assert res["elapsedMs"] >= 0
    assert "close" in res["columns"]


# ---------- Execute 同步契约：REPL/异步模式下 table/* 也同步完成 ----------

def test_execute_table_ops_sync_in_async_mode(client, root):
    """回归：删除表失败无感知 —— REPL 服务（set_default_async(True)）把
    table add/del 转成后台任务，Execute 立即返回成功，真实失败只进任务登记。

    修复后 Execute RPC 的 table add/del/set 全部强制同步（background=False）：
    成功/失败都当场返回，绝不落后台任务。"""
    from stkoe.data.task import set_default_async, is_default_async
    _setup(root)
    was = is_default_async()
    set_default_async(True)  # 模拟 REPL / STKOE_ASYNC 服务模式
    try:
        # 1) 目录不存在 → 同步失败（不落后台任务，不返回 task 句柄）
        resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["add", "ghost"]))
        assert resp.code != 0 and "not found" in resp.error
        assert "task" not in resp.error

        # 2) 正常删除 → 同步返回结果，catalog 立即可见已删
        r = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["del", "t1"]))
        assert r.code == 0 and json.loads(r.json_out) == {"deleted": "t1"}
        m = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["meta", "t1"]))
        assert m.code != 0 and "not registered" in m.error

        # 3) 被 dataset 引用 → 同步 DependencyError，t1 仍在
        data.table.scan("t1", background=False)  # 重新发现注册（数据文件未删）
        data.dataset.add("ds", "t1", "t1", background=False)
        d = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["del", "t1"]))
        assert d.code != 0 and "dependencies exist" in d.error
        m2 = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["meta", "t1"]))
        assert m2.code == 0
    finally:
        set_default_async(was)


def test_execute_table_add_report_jsonable(client, root):
    """回归：table add 的 Execute 结果含 TableScanReport，此前缺 to_dict 导致
    “Object of type TableScanReport is not JSON serializable”，前端报
    status: Unknown。修复后返回完整 JSON 报告。"""
    # 只写物理文件不登记 —— table add 就是“发现资产”语义
    write_single(root, "t2", pl.DataFrame({
        "date": ["2024-01-01"] * 2,
        "sym": ["a", "b"],
        "close": [1.0, 2.0],
    }))

    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["add", "t2"]))
    assert resp.code == 0, resp.error
    rep = json.loads(resp.json_out)
    assert rep["name"] == "t2"
    assert rep["layout"]
    assert rep["version_before"] >= 0 and rep["version_after"] >= rep["version_before"]
    assert "changed" in rep and "partition_by" in rep and "diffs" in rep and "triggered" in rep


def test_execute_del_error_returns_dependencies(client, root):
    """Execute 删除被引用对象时，error 带原因 + json_out 返回结构化依赖列表"""
    _setup(root)
    data.dataset.add("ds_dep", "t1", "t1", background=False, materialize=False)
    # table del 被 dataset 引用 → code=2 + dependencies 结构
    resp = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["del", "t1"]))
    assert resp.code == 2
    assert "dependencies exist" in resp.error and "dataset:ds_dep" in resp.error
    deps = json.loads(resp.json_out)["dependencies"]
    assert any(d["obj_type"] == "dataset" and d["obj_name"] == "ds_dep" for d in deps)
    assert all("obj_type" in d and "obj_name" in d for d in deps)
    # 未被删除：对象仍在
    m = client.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["meta", "t1"]))
    assert m.code == 0
    # dataset del 被 stat 引用 → 同样返回依赖
    data.stat.add("ds_dep", background=False)
    resp2 = client.Execute(stkoe_pb2.ExecuteRequest(cmd="dataset", args=["del", "ds_dep"]))
    assert resp2.code == 2
    deps2 = json.loads(resp2.json_out)["dependencies"]
    assert any(d["obj_type"] == "stat" and d["obj_name"] == "ds_dep" for d in deps2)
