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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Iterator

import grpc

from . import stkoe_pb2, stkoe_pb2_grpc
from .dispatch import CommandError, dispatch
from ..settings import DEFAULT_HOST, DEFAULT_PORT
from ..task import TaskManager


class StkoeServerError(RuntimeError):
    pass


def _header(code: int, message: str = "") -> stkoe_pb2.DataHeader:
    return stkoe_pb2.DataHeader(code=code, message=message)


def _execute_stream(source: str, action: str,
                    args: list[str]) -> Iterator[stkoe_pb2.ExecuteResponse]:
    """Execute 流式实现：先出 DataHeader，成功再出数据消息（JsonData/ArrowTable）"""
    try:
        results = dispatch(source, action, args)
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
    def __init__(self, tasks: TaskManager):
        self._tasks = tasks

    def Execute(self, request, context) -> Iterator[stkoe_pb2.ExecuteResponse]:
        return _execute_stream(request.source, request.action, list(request.args))

    def SubmitTask(self, request, context) -> stkoe_pb2.SubmitTaskResponse:
        try:
            task = self._tasks.submit(request.source, request.action,
                                      list(request.args))
            return stkoe_pb2.SubmitTaskResponse(
                header=_header(0, "ok"), task_id=task.task_id)
        except Exception as e:
            return stkoe_pb2.SubmitTaskResponse(header=_header(2, str(e)))

    def SubscribeTask(self, request, context):
        yield from self._tasks.subscribe(request.task_id, request.replay)

    def TaskControl(self, request, context) -> stkoe_pb2.TaskControlResponse:
        ok, message = self._tasks.control(request.task_id, request.action)
        return stkoe_pb2.TaskControlResponse(
            header=_header(0, "ok") if ok else _header(2, message),
            task_id=request.task_id)

    def Health(self, request, context) -> stkoe_pb2.HealthResponse:
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
        stkoe_pb2_grpc.add_StkoeServiceServicer_to_server(_StkoeServicer(self.tasks), server)
        bound = server.add_insecure_port(f"{self.host}:{self.port}")
        if bound == 0 or (self.port != 0 and bound != self.port):
            server.stop(0)
            raise StkoeServerError(
                f"gRPC 端口 {self.port} 绑定失败（可能被占用/权限不足）")
        if self.port == 0:
            self.port = bound  # 自动分配：回填实际端口
        server.start()
        self._server = server
        return self

    def stop(self, grace: float | None = 0.5) -> None:
        """停止（幂等）"""
        srv, self._server = self._server, None
        if srv is not None:
            srv.stop(grace)
        if self._tasks is not None:
            self._tasks.stop()

    def wait(self) -> None:
        """阻塞直到服务结束（前台 server 命令）"""
        if self._server is not None:
            self._server.wait_for_termination()


def serve(host: str | None = None, port: int | None = None) -> StkoeServer:
    """启动服务并返回：host/port/data_dir 缺省取 stkoe.json 配置"""
    from ..settings import load_config

    cfg = load_config()
    return StkoeServer(
        host=host or cfg.grpc_host,
        port=port or cfg.grpc_port,
        data_dir=cfg.data_dir,
    ).start()
