"""stkoe gRPC 服务：数据管理框架远程访问

- 默认端口 9569，可通过 `config set --grpc-port` 修改
- REPL 启动时同步后台启动（进程内单例），退出时停止；也可 `stkoe server` 独立前台运行
- 约定：小数据量（元数据/列表/状态）走 JSON（``json_out``）；表格数据（select 查询）
  走 Arrow IPC 字节（``ipc``），polars/pyarrow 可直接读取
- 只绑定 127.0.0.1（本地服务）
"""
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import grpc
import polars as pl

from . import stkoe_pb2, stkoe_pb2_grpc


class StkoeServerError(RuntimeError):
    pass


def _jsonable(obj):
    """元数据对象 → JSON 可序列化（dataclass 自带 to_dict）"""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj


def _execute(cmd: str, args: list[str]) -> dict:
    """Execute 路由：元数据/列表/状态等小结果 → JSON"""
    from ..data import dataset, field, stat, table, task_list
    from ..data.settings import config_path as cfg_path
    from ..data.settings import load_config
    from importlib.metadata import version as _pkg_version

    c = load_config()
    if cmd == "config" and (not args or args[0] == "show"):
        return {"config_file": str(cfg_path()), "data_path": c.data_path,
                "grpc_port": c.grpc_port, "ignore_cols": list(c.ignore_cols)}
    if cmd == "version":
        return {"version": _pkg_version("stkoe")}
    if cmd == "task" and (not args or args[0] == "list"):
        return [_jsonable(t) for t in task_list()]
    if cmd == "table":
        if args and args[0] == "list":
            return [_jsonable(t) for t in table.list()]
        if len(args) >= 2 and args[0] in ("meta", "get"):
            return _jsonable(table.meta(args[1]))
    if cmd == "dataset":
        if args and args[0] == "list":
            return [_jsonable(d) for d in dataset.list()]
        if len(args) >= 2 and args[0] in ("meta", "describe"):
            return _jsonable(dataset.meta(args[1]))
    if cmd == "stat":
        if args and args[0] == "list":
            return [_jsonable(s) for s in stat.list()]
        if len(args) >= 2 and args[0] == "meta":
            return _jsonable(stat.meta(args[1]))
    if cmd == "field":
        if args and args[0] == "list":
            return [_jsonable(f) for f in field.list()]
        if len(args) >= 2 and args[0] in ("meta", "describe"):
            return _jsonable(field.meta(args[1]))
    raise ValueError(f"不支持的命令: {cmd} {' '.join(args)}")


def _select(name: str, type_: str, columns: list[str] | None,
            where: str | None, partition: str | None,
            include_tool: bool) -> pl.DataFrame:
    """Select 路由：复用数据层 get 读路径（读前快检 + 文件级裁剪）"""
    from ..data import dataset, stat, table
    if type_ == "stat":
        return stat.get(name)
    if type_ == "table":
        return table.get(name, columns=columns, where=where,
                         partition=partition, exclude_tool=not include_tool)
    if type_ == "dataset":
        return dataset.get(name, columns=columns, where=where, partition=partition)
    try:
        return dataset.get(name, columns=columns, where=where, partition=partition)
    except dataset.DatasetNotFoundError:
        return table.get(name, columns=columns, where=where,
                         partition=partition, exclude_tool=not include_tool)


def _schema_json(df: pl.DataFrame, name: str, partition: str) -> str:
    return json.dumps({
        "name": name, "num_rows": len(df), "partition": partition,
        "columns": [{"name": c, "dtype": str(df.schema[c])} for c in df.columns],
    })


class _StkoeServicer(stkoe_pb2_grpc.StkoeServiceServicer):
    def Execute(self, request, context):
        try:
            out = _execute(request.cmd, list(request.args))
        except Exception as e:
            return stkoe_pb2.ExecuteResponse(code=2, error=str(e))
        return stkoe_pb2.ExecuteResponse(
            code=0, json_out=json.dumps(out, ensure_ascii=False))

    def Select(self, request, context):
        try:
            df = _select(request.name, request.type, list(request.columns) or None,
                         request.where or None, request.partition or None,
                         request.include_tool)
            buf = io.BytesIO()
            df.write_ipc(buf, compression=None)
            return stkoe_pb2.SelectResponse(
                ipc=buf.getvalue(), schema_json=_schema_json(df, request.name, request.partition),
                num_rows=len(df))
        except Exception as e:
            return stkoe_pb2.SelectResponse(error=str(e))


@dataclass
class StkoeServer:
    """进程内 gRPC 服务（单端口；REPL 生命周期共享）"""

    port: int = 9569
    host: str = "127.0.0.1"
    max_workers: int = 4
    _server: grpc.Server = field(default=None, init=False, repr=False)

    def start(self) -> "StkoeServer":
        """启动（非阻塞）；端口占用抛 StkoeServerError"""
        if self._server is not None:
            return self
        server = grpc.server(ThreadPoolExecutor(max_workers=self.max_workers))
        stkoe_pb2_grpc.add_StkoeServiceServicer_to_server(_StkoeServicer(), server)
        bound = server.add_insecure_port(f"{self.host}:{self.port}")
        if bound != self.port:
            server.stop(0)
            raise StkoeServerError(f"gRPC 端口 {self.port} 绑定失败（可能被占用/权限不足）")
        server.start()
        self._server = server
        return self

    def stop(self, grace: float | None = 0.5) -> None:
        """停止（幂等）"""
        srv, self._server = self._server, None
        if srv is not None:
            srv.stop(grace)

    def wait(self) -> None:
        """阻塞直到服务结束（前台 server 命令）"""
        if self._server is not None:
            self._server.wait_for_termination()


def server_port() -> int:
    """生效端口：配置 grpc_port（缺省 9569）"""
    from ..data.settings import load_config
    return load_config().grpc_port


def serve(port: int | None = None) -> StkoeServer:
    """启动服务并返回（端口缺省取配置）；供 REPL 同步启动与 `stkoe server` 使用"""
    return StkoeServer(port=port if port is not None else server_port()).start()