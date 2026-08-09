"""stkoe gRPC 服务：数据管理框架远程访问

- 默认端口 9569，可通过 `config set --grpc-port` 修改；也支持 `--port` 显式覆盖
- REPL 启动时同步后台启动（进程内单例），退出时停止；也可 `stkoe server` 独立前台运行
- 约定：
  - 小数据量（元数据/列表/状态）走 ``json_out``（Execute）
  - 表格数据（select 查询）走 Arrow IPC 字节（``ipc``），polars/pyarrow 可直接读取；
    支持分页（page/page_size）+ 结构化过滤（filter）+ 排序（sort），响应带 ``total``
  - 长任务（物化/公式执行/统计）走 ``RunTask`` 服务端流式（log/progress/result 事件）
  - 存活探针走 ``Health``
- 只绑定 127.0.0.1（本地服务）
"""
import io
import json
import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

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


# ============================================================================
# Execute 路由（小结果 JSON）
# ============================================================================

_JSON_SKIP = {"__proto__", "constructor"}


def _dumps(obj) -> str:
    """结果序列化：pl 行集里的 date/datetime/Decimal 转可 JSON 形态。"""
    import datetime as _dt
    import decimal as _dec

    def conv(o):
        if isinstance(o, (_dt.datetime, _dt.date, _dt.time)):
            return o.isoformat()
        if isinstance(o, _dec.Decimal):
            return float(o)
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    return json.dumps(obj, ensure_ascii=False, default=conv)


def _df_to_records(df: pl.DataFrame) -> list[dict]:
    """pl.DataFrame → 记录列表（stat 结果走 JSON）"""
    return df.to_dicts()


def _execute(cmd: str, args: list[str]) -> dict:
    """元数据/列表/状态等小结果 → JSON；表格数据走 Select/RunTask"""
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
    if cmd == "task" and args and args[0] == "list":
        return [_jsonable(t) for t in task_list()]

    if cmd == "table":
        sub = args[0] if args else ""
        if sub == "list":
            return [_jsonable(m) for m in table.list()]
        if sub == "candidates":
            return {"tables": table.candidates()}
        if sub in ("meta", "get") and len(args) >= 2:
            return _jsonable(table.meta(args[1]))
        if sub == "add" and len(args) >= 2:
            _, kv = _parse_kv(args[1:])
            return _jsonable(table.add(args[1], dbt_manifest=kv.get("dbt_manifest"),
                                       background=False))
        if sub == "set" and len(args) >= 2:
            return _jsonable(_table_set(table, args[1], args[2:]))
        if sub == "del" and len(args) >= 2:
            table.del_(args[1], background=False)
            return {"deleted": args[1]}
        if sub == "scan" and len(args) >= 2:
            report = table.scan(args[1], background=False)
            return _jsonable(report)

    if cmd == "dataset":
        sub = args[0] if args else ""
        if sub == "list":
            return [_jsonable(d) for d in dataset.list()]
        if sub in ("meta", "describe") and len(args) >= 2:
            return _jsonable(dataset.meta(args[1]))
        if sub == "add" and len(args) >= 3:
            return _jsonable(_dataset_add(dataset, args))
        if sub == "set" and len(args) >= 2:
            return _jsonable(_dataset_set(dataset, args[1], args[2:]))
        if sub == "del" and len(args) >= 2:
            dataset.del_(args[1], background=False)
            return {"deleted": args[1]}
        if sub == "scan" and len(args) >= 2:
            return _jsonable(dataset.scan(args[1], background=False))
        if sub == "validate" and len(args) >= 2:
            mode = "full"
            if "--mode" in args:
                i = args.index("--mode")
                if len(args) > i + 1:
                    mode = args[i + 1]
            return dataset.validate(args[1], mode=mode)

    if cmd == "stat":
        sub = args[0] if args else ""
        if sub == "list":
            return [_jsonable(s) for s in stat.list()]
        if sub == "meta" and len(args) >= 2:
            return _jsonable(stat.meta(args[1]))
        if sub == "get" and len(args) >= 2:
            return _stat_get(stat, args[1], args[2:])

    if cmd == "field":
        sub = args[0] if args else ""
        if sub == "list":
            return [_jsonable(f) for f in field.list()]
        if sub in ("meta", "describe") and len(args) >= 2:
            return _jsonable(field.meta(args[1]))
        if sub == "create" and len(args) >= 3:
            return _jsonable(_field_create(field, args))
        if sub == "set" and len(args) >= 2:
            return _jsonable(_field_set(field, args[1], args[2:]))
        if sub == "del" and len(args) >= 2:
            field.del_(args[1])
            return {"deleted": args[1]}
        if sub == "rename" and len(args) >= 3:
            return _jsonable(field.rename(args[1], args[2]))
    raise ValueError(f"不支持的命令: {cmd} {' '.join(args)}")


def _parse_kv(args: list[str]) -> tuple[str, dict]:
    """位置参数 + ``key=value``/``--key value``/``--key=value`` 混用解析；
    返回剩余位置参数与 KV 字典（键统一下划线）。"""
    pos, kv = [], {}
    name = None
    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--"):
            key = a[2:].replace("-", "_")
            if "=" in key:
                k, v = key.split("=", 1)
                kv[k] = v
                i += 1
                continue
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                kv[key] = args[i + 1]
                i += 2
                continue
            kv[key] = True
        elif "=" in a:
            k, v = a.split("=", 1)
            kv[k.replace("-", "_")] = v
        else:
            name = a
        i += 1
    return name, kv


def _table_set(table, name: str, args: list[str]):
    _, kv = _parse_kv(args)
    return table.set(name,
                     display_name=kv.get("display_name"),
                     description=kv.get("description"),
                     tags=kv["tags"].split(",") if kv.get("tags") else None,
                     source=kv.get("source"),
                     dbt_manifest=kv.get("dbt_manifest"),
                     background=False)


def _dataset_add(dataset, args: list[str]) -> dict:
    name, index_table = args[1], args[2]
    tables = args[3:]
    keys = None
    if "--keys" in tables:
        i = tables.index("--keys")
        keys = tables[i + 1].split(",")
        tables = tables[:i]
    meta_extra = {}
    for a in ("display_name", "description", "category", "tags"):
        mark = f"--{a}"
        if mark in tables:
            i = tables.index(mark)
            meta_extra[a] = tables[i + 1]
            tables = tables[:i]
    if meta_extra.get("tags"):
        meta_extra["tags"] = meta_extra["tags"].split(",")
    if not tables:
        raise ValueError("dataset add 需要至少一张成员表: " + " ".join(args))
    return dataset.add(name, index_table, *tables, keys=keys, background=False,
                       **meta_extra)


def _dataset_set(dataset, name: str, args: list[str]):
    _, kv = _parse_kv(args)
    return dataset.set(name,
                       display_name=kv.get("display_name"),
                       description=kv.get("description"),
                       tags=kv["tags"].split(",") if kv.get("tags") else None,
                       category=kv.get("category"),
                       background=False)


def _field_create(field, args: list[str]) -> dict:
    name, dataset_name, formula = args[1], args[2], None
    kv = {}
    for a in args[3:]:
        if "=" in a:
            k, v = a.split("=", 1)
            kv[k.replace("-", "_")] = v
    formula = kv.get("formula")
    return field.create(name, dataset_name, formula=formula,
                        display_name=kv.get("display_name"),
                        description=kv.get("description"),
                        tags=(kv["tags"].split(",") if kv.get("tags") else None))


def _field_set(field, name: str, args: list[str]):
    _, kv = _parse_kv(args)
    return field.set(name,
                     formula=kv.get("formula"),
                     dataset=kv.get("dataset"),
                     display_name=kv.get("display_name"),
                     description=kv.get("description"),
                     tags=kv["tags"].split(",") if kv.get("tags") else None)


def _stat_get(stat, name: str, args: list[str]):
    """stat get → JSON 行集（支持 --group/--all/--refresh）"""
    group = None
    all_ = False
    refresh = False
    if "--all" in args:
        all_ = True
    if "--refresh" in args:
        refresh = True
    if "--group" in args:
        group = args[args.index("--group") + 1]
    df = stat.get(name, group_col=group, all_=all_, refresh=refresh)
    if isinstance(df, dict):
        return {"groups": {g: _df_to_records(v) for g, v in df.items()}}
    return {"groups": {"all": _df_to_records(df)}}


# ============================================================================
# Select 路由（表格数据 → Arrow IPC + 分页/过滤/排序）
# ============================================================================

def _filters_from_message(filters) -> list[dict] | None:
    if not filters:
        return None
    out = []
    for f in filters:
        out.append({"field": f.field, "op": f.op, "value": f.value})
    return out or None


def _sorts_from_message(sorts) -> list[dict] | None:
    if not sorts:
        return None
    return [{"field": s.field, "desc": s.desc} for s in sorts]


def _select(name: str, type_: str, columns: list[str] | None,
            where: str | None, partition: str | None,
            include_tool: bool, page: int, page_size: int,
            filters=None, sorts=None) -> tuple[pl.DataFrame, int]:
    """Select 路由：复用数据层 get 读路径（读前快检 + 文件级裁剪）。

    返回 (本页 DataFrame, 过滤后总行数)。
    """
    from ..data import dataset, stat, table
    from ..data.query import apply_sort, to_filters_expr

    total: int | None = None
    if type == "stat":
        df = stat.get(name)
        return df, len(df)
    if type == "table":
        lf = table.get_lazy(name, columns=columns, where=where,
                            partition=partition, exclude_tool=not include_tool)
    elif type == "dataset":
        lf = dataset.get_lazy(name, columns=columns, where=where, partition=partition)
    else:
        try:
            lf = dataset.get_lazy(name, columns=columns, where=where, partition=partition)
        except dataset.DatasetNotFoundError:
            lf = table.get_lazy(name, columns=columns, where=where,
                                partition=partition, exclude_tool=not include_tool)

    fexpr = to_filters_expr(filters)
    if fexpr is not None:
        lf = lf.filter(fexpr)

    if page and page > 0:
        try:
            total = int(lf.select(pl.len()).collect()[0, 0])
        except Exception:
            total = -1
        lf = apply_sort(lf, sorts)
        lf = lf.slice((page - 1) * page_size, page_size)
        df = lf.collect()
    else:
        lf = apply_sort(lf, sorts)
        df = lf.collect()
        total = len(df)
    return df, total


def _schema_json(df: pl.DataFrame, name: str, partition: str) -> str:
    return json.dumps({
        "name": name, "num_rows": len(df), "partition": partition,
        "columns": [{"name": c, "dtype": str(df.schema[c])} for c in df.columns],
    }, ensure_ascii=False)


# ============================================================================
# RunTask 流式（长任务：物化/公式执行/统计）
# ============================================================================

class _StreamControl:
    """注入流式事件的 TaskControl 代理：数据层长任务以 ctl 执行，事件实时推送。

    仅转发 log/progress/check（check 不阻塞——服务端任务不参与协作暂停）。
    """

    def __init__(self, task_id: str, kind: str, obj_ref: str, emit):
        self.task_id = task_id
        self.kind = kind
        self.obj_ref = obj_ref
        self._emit = emit

    def info(self, msg: str) -> None:
        self._emit({"type": "log", "level": "info", "message": msg})

    def warning(self, msg: str) -> None:
        self._emit({"type": "log", "level": "warn", "message": msg})

    def error(self, msg: str) -> None:
        self._emit({"type": "log", "level": "error", "message": msg})

    def progress(self, value: float, msg: str | None = None) -> None:
        self._emit({"type": "progress", "done": value, "total": 1, "message": msg or ""})

    def stage(self, msg: str) -> None:
        self._emit({"type": "progress", "done": 0, "total": 0, "message": msg})

    def check(self) -> None:
        return


def _run_task_body(cmd: str, args: list[str], ctl) -> dict:
    """RunTask 命令分发：dataset/field/stat 的长任务，返回结果 JSON 可序列化对象"""
    from ..data import dataset, field, stat

    if cmd == "dataset":
        sub = args[0] if args else ""
        name = args[1] if len(args) > 1 else ""
        if sub in ("scan", "materialize") and name:
            ds = dataset.describe(name)
            t0 = time.time()
            report = dataset.scan_impl(ds, ctl=ctl)
            # 物化结果契约（DatasetMaterializeResult）：报告字段对前端
            # 无意义，直接返回产物形态；scan 与 materialize 同路径同结果。
            return dataset.materialized_payload(
                name, elapsed_ms=int((time.time() - t0) * 1000))
        if sub == "add" and len(args) >= 3:
            return _jsonable(dataset.add(args[1], args[2], *args[3:],
                                         materialize=True))
        raise ValueError(f"不支持的长任务: dataset {' '.join(args)}")

    if cmd == "field":
        sub = args[0] if args else ""
        name = args[1] if len(args) > 1 else ""
        if sub == "test" and name:
            return field.test(name, ctl=ctl)
        if sub == "test-code" and len(args) >= 3:
            # 未注册公式调试（测试-保存前预览）：dataset, code
            return field.test_code(args[1], args[2], ctl=ctl)
        if sub == "materialize" and name:
            return field.materialize(name, ctl=ctl)
        if sub == "create" and len(args) >= 3:
            return _field_create(field, args)
        raise ValueError(f"不支持的长任务 field {' '.join(args)}")

    if cmd == "stat":
        sub = args[0] if args else ""
        name = args[1] if len(args) > 1 else ""
        if sub == "add" and name:
            ctl.stage(f"stat 计算 {name}")
            m = stat.add(name)
            return _jsonable(m)
        if sub == "get" and name:
            ctl.info(f"stat 读取/重算 {name} …")
            return _stat_get(stat, name, args[2:])
        raise ValueError(f"不支持的长任务 stat {' '.join(args)}")

    raise ValueError(f"不支持的长任务: {cmd}")


def _task_event(ev: dict) -> stkoe_pb2.TaskEvent:
    ty = ev.get("type", "log")
    if ty == "log":
        return stkoe_pb2.TaskEvent(type="log", level=ev.get("level", "info"),
                                   message=ev.get("message", ""))
    if ty == "progress":
        return stkoe_pb2.TaskEvent(type="progress", done=int(ev.get("done", 0)),
                                   total=int(ev.get("total", 0)),
                                   message=ev.get("message", ""))
    if ty == "result":
        return stkoe_pb2.TaskEvent(type="result", data=ev.get("data", ""))
    if ty == "error":
        return stkoe_pb2.TaskEvent(type="error", error=ev.get("error", ""))
    return stkoe_pb2.TaskEvent(type="done")


def _task_stream(cmd: str, args: list[str], task_id: str) -> Iterator[stkoe_pb2.TaskEvent]:
    """RunTask 服务端流式实现：独立线程执行，事件经队列推送"""
    q: "queue.Queue[dict | None]" = queue.Queue()

    def emit(ev: dict) -> None:
        q.put(ev)

    def run() -> None:
        try:
            out = _run_task_body(cmd, args if args else [], _StreamControl(task_id, cmd, "", emit))
            if out is not None:
                emit({"type": "result", "data": _dumps(out)})
            emit({"type": "done"})
        except Exception as e:
            emit({"type": "error", "error": str(e)})
        finally:
            q.put(None)

    threading.Thread(target=run, daemon=True).start()
    while True:
        ev = q.get()
        if ev is None:
            break
        yield _task_event(ev)


class _StkoeServicer(stkoe_pb2_grpc.StkoeServiceServicer):
    def Execute(self, request, context):
        try:
            out = _execute(request.cmd, list(request.args))
        except Exception as e:
            return stkoe_pb2.ExecuteResponse(code=2, error=str(e))
        return stkoe_pb2.ExecuteResponse(
            code=0, json_out=_dumps(out))

    def Select(self, request, context):
        try:
            df, total = _select(
                request.name, request.type, list(request.columns) or None,
                request.where or None, request.partition or None,
                request.include_tool, request.page, request.page_size or 50,
                _filters_from_message(request.filter),
                _sorts_from_message(request.sort))
            buf = io.BytesIO()
            df.write_ipc(buf, compression=None)
            return stkoe_pb2.SelectResponse(
                ipc=buf.getvalue(),
                schema_json=_schema_json(df, request.name, request.partition),
                num_rows=len(df), total=int(total))
        except Exception as e:
            return stkoe_pb2.SelectResponse(error=str(e))

    def RunTask(self, request, context):
        return _task_stream(request.cmd, list(request.args), request.task_id)

    def Health(self, request, context):
        from importlib.metadata import version as _pkg_version
        try:
            return stkoe_pb2.HealthResponse(status="ok",
                                            version=_pkg_version("stkoe"))
        except Exception:
            return stkoe_pb2.HealthResponse(status="ok", version="unknown")


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
        # 端口预检：grpcio 的 add_insecure_port 在端口冲突时可能静默返回同端口
        # （不报错、start 也不抛），故先用原始 socket 探测，占用立即抛错。
        if self.port != 0:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.bind((self.host, self.port))
            except OSError as e:
                raise StkoeServerError(
                    f"gRPC 端口 {self.port} 绑定失败（可能被占用/权限不足）") from e
            finally:
                probe.close()
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


def serve_reload(port: int | None = None) -> None:
    """带代码监听重载的前台服务：改任一 stkoe 源码文件自动重启。"""
    import subprocess
    import sys
    import time
    from pathlib import Path

    pkg_dir = Path(_stkoe_root())
    port_arg = port if port is not None else server_port()
    print(f"[server] watch mode: {pkg_dir}  (port {port_arg})", flush=True)
    while True:
        proc = subprocess.Popen(
            [sys.executable, "-m", "stkoe", "server", "run", "--port", str(port_arg)],
        )
        changed = _watch_pkg(pkg_dir)
        if proc.poll() is None:
            print("[server] 检测到代码变更，重启服务…", flush=True)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        else:
            print(f"[server] 服务进程退出（{proc.returncode}），2 秒后重启…", flush=True)
            time.sleep(2)


def _stkoe_root() -> Path:
    import importlib.util
    spec = importlib.util.find_spec("stkoe")
    if spec is not None and spec.origin:
        return Path(spec.origin).resolve().parent
    return Path(__file__).resolve().parent.parent


def _watch_pkg(root: Path) -> bool:
    """轮询 stkoe 包下 *.py 的 mtime；有变更返回 True"""
    import time

    def snapshot() -> dict:
        out = {}
        for p in root.rglob("*.py"):
            try:
                out[str(p)] = p.stat().st_mtime_ns
            except OSError:
                pass
        return out

    prev = snapshot()
    while True:
        time.sleep(0.7)
        cur = snapshot()
        if cur != prev:
            return True