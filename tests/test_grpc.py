# -*- coding: utf-8 -*-
"""gRPC 服务测试：Execute 流式（DataHeader/JsonData）+ SubmitTask/SubscribeTask + Health"""
import json
import socket

import grpc
import pytest

from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc
from stkoe.grpc.server import StkoeServer


@pytest.fixture()
def srv(tmp_path):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = StkoeServer(port=port, data_dir=str(tmp_path / "data")).start()
    yield srv
    srv.stop()


@pytest.fixture()
def client(srv):
    ch = grpc.insecure_channel(f"127.0.0.1:{srv.port}")
    stub = stkoe_pb2_grpc.StkoeServiceStub(ch)
    yield stub
    ch.close()


def _collect(responses):
    """收集 Execute 流：返回 (DataHeader, 数据消息列表)"""
    header, datas = None, []
    for r in responses:
        if r.WhichOneof("type") == "header":
            header = r.header
        else:
            datas.append(r)
    return header, datas


# ---------- Health ----------

def test_health(client):
    resp = client.Health(stkoe_pb2.HealthRequest())
    assert resp.status == "ok"
    assert resp.version


# ---------- Execute ----------

def test_execute_version_json(client):
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="version", action="")))
    assert header.code == 0
    assert len(datas) == 1
    assert datas[0].WhichOneof("type") == "json"
    assert datas[0].json.name == "version"
    assert "version" in datas[0].json.data


def test_execute_config_show_json(client):
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="config", action="show")))
    assert header.code == 0
    assert datas[0].WhichOneof("type") == "json"
    assert datas[0].json.name == "config"


def test_execute_config_set_then_show(client, tmp_path, monkeypatch):
    """Execute config set 任意字段 → stkoe.json；show 反映生效配置"""
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "stkoe.json"))

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="config", action="set",
        args=["--grpc-host", "0.0.0.0", "--grpc-port", "9000"])))
    assert header.code == 0
    assert json.loads(datas[0].json.data)["set"] == {
        "grpc-host": "0.0.0.0", "grpc-port": "9000"}

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="config", action="show")))
    assert header.code == 0
    out = json.loads(datas[0].json.data)
    assert out["config_file"] == str(tmp_path / "stkoe.json")
    assert out["grpc-host"] == "0.0.0.0"
    assert out["grpc-port"] == 9000


def test_execute_unknown_command_error_header(client):
    """未注册命令：首条即错误 DataHeader，且无数据消息"""
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="table", action="list")))
    assert header.code != 0
    assert "不支持的命令" in header.message
    assert datas == []


# ---------- SubmitTask / SubscribeTask ----------

def test_submit_and_subscribe_replay(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="version", action="get"))
    assert resp.header.code == 0
    assert resp.task_id

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=resp.task_id, replay=True)))
    assert responses[0].header.code == 0
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events
    assert events[0].seq == 1
    assert events[-1].state == "succeeded"
    assert events[-1].progress == 1.0
    assert "version" in events[-1].data
    assert all(e.state in ("pending", "running", "succeeded",
                           "failed", "cancelled") for e in events)


def test_subscribe_missing_task_error_header(client):
    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id="no_such_task", replay=True)))
    assert responses[0].WhichOneof("type") == "header"
    assert responses[0].header.code != 0
    assert "not found" in responses[0].header.message


def test_submit_task_error_event(client):
    """提交未注册命令的任务：事件流以 failed 状态结束，含错误消息"""
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="no_such_source", action="bogus"))
    assert resp.header.code == 0
    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=resp.task_id, replay=True)))
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events[-1].state == "failed"
    assert "不支持的命令" in events[-1].message


# ---------- TaskControl ----------

def test_task_control_cancel(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    assert resp.header.code == 0
    task_id = resp.task_id

    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=task_id, action="cancel"))
    assert ctrl.header.code == 0
    assert ctrl.task_id == task_id

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=task_id, replay=True)))
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events[-1].state == "cancelled"


def test_task_control_pause_resume(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    task_id = resp.task_id

    # 等任务进入 running 后暂停
    import time as _time
    deadline = _time.monotonic() + 5
    events = []
    while _time.monotonic() < deadline:
        r = client.TaskControl(stkoe_pb2.TaskControlRequest(
            task_id=task_id, action="pause"))
        if r.header.code == 0:
            break
        _time.sleep(0.02)
    else:
        assert False, "任务未能暂停"

    assert client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=task_id, action="resume")).header.code == 0

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=task_id, replay=True)))
    states = [r.event.state for r in responses if r.WhichOneof("type") == "event"]
    assert "paused" in states
    assert states[-1] == "succeeded"


def test_task_control_missing_task_error(client):
    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id="no_such_task", action="cancel"))
    assert ctrl.header.code != 0
    assert "not found" in ctrl.header.message


def test_task_control_unknown_action(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=resp.task_id, action="explode"))
    assert ctrl.header.code != 0
    assert "不支持的任务操作" in ctrl.header.message


def test_port_conflict(srv):
    with pytest.raises(Exception):
        StkoeServer(port=srv.port).start()
