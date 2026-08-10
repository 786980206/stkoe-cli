"""stkoe gRPC 服务器：实现 stkoe.proto 的 StkoeService

- Execute：服务端流式。首条恒为 DataHeader（0=成功 / 非 0=业务错误），
  成功后按需跟随 JsonData（小结果 JSON）或 ArrowTable（表格数据 Arrow IPC）
- SubmitTask：提交后台任务，立即返回 task_id
- SubscribeTask：按 task_id 订阅事件流（seq/progress/message/data/state）
- Health：存活探活 + 版本

默认绑定 127.0.0.1:9569（本地服务）；``serve(host, port)`` 可显式指定。
"""
from __future__ import annotations

import socket
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Iterator

import grpc

from . import stkoe_pb2, stkoe_pb2_grpc
from .dispatch import CommandError, dispatch
from ..logutil import LOG
from ..settings import DEFAULT_HOST, DEFAULT_PORT
from ..task import TaskManager


class StkoeServerError(RuntimeError):
    pass


def _header(code: int, message: str = "") -> stkoe_pb2.DataHeader:
    return stkoe_pb2.DataHeader(code=code, message=message)


def _peer(context) -> str:
    try:
        return context.peer()
    except Exception:
        return "unknown"


def _cost_ms(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 1)


def _execute_stream(source: str, action: str, args: list[str],
                    data_dir=None) -> Iterator[stkoe_pb2.ExecuteResponse]:
    """Execute 流式实现：先出 DataHeader，成功再出数据消息（JsonData/ArrowTable）"""
    try:
        results = dispatch(source, action, args, data_dir=data_dir)
    except CommandError as e:
        yield stkoe_pb2.ExecuteResponse(header=_header(e.code, e.message))
        return
    except Exception as e:
        yield stkoe_pb2.ExecuteResponse(header=_header(2, str(e)))
        return

    yield stkoe_pb2.ExecuteResponse(header=_header(0, "ok"))
    for r in results:
        if r.kind == "table":
            yield stkoe_pb2.ExecuteResponse(table=stkoe_pb2.ArrowTable(
                name=r.name, data=r.data, meta=r.meta))
        else:
            yield stkoe_pb2.ExecuteResponse(json=stkoe_pb2.JsonData(
                name=r.name, data=r.data))


class _StkoeServicer(stkoe_pb2_grpc.StkoeServiceServicer):
    def __init__(self, tasks: TaskManager, data_dir=None):
        self._tasks = tasks
        self._data_dir = data_dir

    def Execute(self, request, context) -> Iterator[stkoe_pb2.ExecuteResponse]:
        args = list(request.args)
        LOG.info("接收请求 Execute: source=%s action=%s args=%s peer=%s",
                 request.source, request.action, args, _peer(context))
        start = time.monotonic()
        code: int | str = "?"
        try:
            for resp in _execute_stream(request.source, request.action, args,
                                        self._data_dir):
                if resp.WhichOneof("type") == "header":
                    code = resp.header.code
                yield resp
        finally:
            LOG.info("完成 Execute: source=%s action=%s code=%s 耗时=%.1fms",
                     request.source, request.action, code, _cost_ms(start))

    def SubmitTask(self, request, context) -> stkoe_pb2.SubmitTaskResponse:
        args = list(request.args)
        LOG.info("接收请求 SubmitTask: source=%s action=%s args=%s peer=%s",
                 request.source, request.action, args, _peer(context))
        start = time.monotonic()
        try:
            task = self._tasks.submit(request.source, request.action, args)
            LOG.info("完成 SubmitTask: task_id=%s 耗时=%.1fms",
                     task.task_id, _cost_ms(start))
            return stkoe_pb2.SubmitTaskResponse(
                header=_header(0, "ok"), task_id=task.task_id)
        except Exception as e:
            LOG.warning("失败 SubmitTask: %s", e)
            return stkoe_pb2.SubmitTaskResponse(header=_header(2, str(e)))

    def SubscribeTask(self, request, context):
        LOG.info("接收请求 SubscribeTask: task_id=%s replay=%s peer=%s",
                 request.task_id, request.replay, _peer(context))
        start = time.monotonic()
        events = 0
        try:
            for resp in self._tasks.subscribe(request.task_id, request.replay):
                if resp.WhichOneof("type") == "event":
                    events += 1
                yield resp
        finally:
            LOG.info("完成 SubscribeTask: task_id=%s events=%d 耗时=%.1fms",
                     request.task_id, events, _cost_ms(start))

    def TaskControl(self, request, context) -> stkoe_pb2.TaskControlResponse:
        LOG.info("接收请求 TaskControl: task_id=%s action=%s peer=%s",
                 request.task_id, request.action, _peer(context))
        start = time.monotonic()
        ok, message = self._tasks.control(request.task_id, request.action)
        LOG.info("完成 TaskControl: task_id=%s action=%s ok=%s 耗时=%.1fms",
                 request.task_id, request.action, ok, _cost_ms(start))
        return stkoe_pb2.TaskControlResponse(
            header=_header(0, "ok") if ok else _header(2, message),
            task_id=request.task_id)

    def Health(self, request, context) -> stkoe_pb2.HealthResponse:
        LOG.info("接收请求 Health: peer=%s", _peer(context))
        from importlib.metadata import version as pkg_version

        try:
            version = pkg_version("stkoe")
        except Exception:
            version = "unknown"
        return stkoe_pb2.HealthResponse(status="ok", version=version)


@dataclass
class StkoeServer:
    """进程内 gRPC 服务（单端口）"""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    max_workers: int = 4
    data_dir: str | None = None
    _tasks: TaskManager | None = field(default=None, init=False, repr=False)
    _server: grpc.Server = field(default=None, init=False, repr=False)

    @property
    def tasks(self) -> TaskManager:
        if self._tasks is None:
            self._tasks = TaskManager(data_dir=self.data_dir)
        return self._tasks

    def start(self) -> "StkoeServer":
        """启动（非阻塞）；端口占用抛 StkoeServerError；port=0 时自动分配"""
        if self._server is not None:
            return self
        # 端口预检：grpcio 的 add_insecure_port 在端口冲突时可能静默返回同端口
        if self.port != 0:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.bind((self.host, self.port))
            except OSError as e:
                raise StkoeServerError(
                    f"gRPC 端口 {self.port} 绑定失败（可能被占用/权限不足）") from e
            finally:
                probe.close()
        self.tasks.start()
        server = grpc.server(ThreadPoolExecutor(max_workers=self.max_workers))
        stkoe_pb2_grpc.add_StkoeServiceServicer_to_server(
            _StkoeServicer(self.tasks, self.data_dir), server)
        bound = server.add_insecure_port(f"{self.host}:{self.port}")
        if bound == 0 or (self.port != 0 and bound != self.port):
            server.stop(0)
            raise StkoeServerError(
                f"gRPC 端口 {self.port} 绑定失败（可能被占用/权限不足）")
        if self.port == 0:
            self.port = bound  # 自动分配：回填实际端口
        server.start()
        self._server = server
        LOG.info("gRPC 服务已启动: %s:%d (max_workers=%d)",
                 self.host, self.port, self.max_workers)
        return self

    def stop(self, grace: float | None = 0.5) -> None:
        """停止（幂等）"""
        srv, self._server = self._server, None
        if srv is not None:
            srv.stop(grace)
        if self._tasks is not None:
            self._tasks.stop()
        if srv is not None:
            LOG.info("gRPC 服务已停止: %s:%d", self.host, self.port)

    def wait(self) -> None:
        """阻塞直到服务结束（前台 server 命令）"""
        if self._server is not None:
            self._server.wait_for_termination()


def serve(host: str | None = None, port: int | None = None) -> StkoeServer:
    """启动服务并返回：host/port/data_dir 缺省取 stkoe.json 配置"""
    from ..logutil import setup_logging
    from ..settings import load_config

    setup_logging()
    cfg = load_config()
    return StkoeServer(
        host=host or cfg.grpc_host,
        port=port or cfg.grpc_port,
        data_dir=cfg.data_dir,
    ).start()
