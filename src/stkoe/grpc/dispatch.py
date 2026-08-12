"""命令分发：把 Execute / SubmitTask 的 ``(source, action, args)`` 路由到对应处理器

协议约定请求为 ``stkoe <source> <action> <args...>`` 位置参数形态：
- source：table / dataset / stat / field / config / task / mock / version
- action：add / get / del / set / list / meta / ... 等子命令动词
- args：action 之后的位置参数

处理器通过 ``@handler(source, action)`` 装饰器注册，签名 ``fn(args, data_dir=None) -> list[Result]``；
``Result`` 携带 name + kind（json/table），由 gRPC 层分别序列化为
``JsonData`` / ``ArrowTable``。后续数据层模块（table/dataset/...）逐步注册处理器即可。

table 家族处理器为 Execute 同步路径：直接调用 TableController（内部 asyncio.run 收敛），
与 SubmitTask 的任务版（task/handlers.py 的 TaskHandler）行为对齐。
"""
from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass
from typing import Callable

from ..args import parse_flags
from ..jsonutil import dumps_str


class CommandError(Exception):
    """业务命令错误（非传输层错误）：写入 DataHeader.code（非 0）+ message"""

    def __init__(self, message: str, code: int = 2):
        super().__init__(message)
        self.message = message
        self.code = code


@dataclass(frozen=True)
class Result:
    """Execute 流中一条数据消息：kind=json → JsonData；kind=table → ArrowTable"""

    name: str
    kind: str = "json"
    data: str | bytes = ""
    meta: str = ""

    @classmethod
    def json(cls, name: str, obj) -> "Result":
        """JSON 结果（小数据：元数据/列表/状态）"""
        return cls(name=name, kind="json", data=dumps_str(obj))

    @classmethod
    def table(cls, name: str, ipc: bytes, meta: str = "") -> "Result":
        """表格结果（Arrow IPC 字节 + 元信息 JSON）"""
        return cls(name=name, kind="table", data=ipc, meta=meta)


_handlers: dict[tuple[str, str], Callable[[list[str]], list[Result]]] = {}


def handler(source: str, action: str):
    """注册 ``(source, action)`` 命令处理器；签名：``fn(args: list[str], data_dir=None) -> list[Result]``"""

    def deco(fn: Callable[[list[str], object], list[Result]]):
        _handlers[(source, action)] = fn
        return fn

    return deco


def dispatch(source: str, action: str, args: list[str], *,
             data_dir=None) -> list[Result]:
    """路由命令；未注册的 (source, action) 抛 CommandError（DataHeader.code != 0）

    ``data_dir`` 为服务数据目录（None 时业务模块按配置/默认目录回退）。
    """
    fn = _handlers.get((source, action))
    if fn is None:
        tail = " ".join(args)
        raise CommandError(f"不支持的命令: {source} {action}" + (f" {tail}" if tail else ""))
    return fn(list(args), data_dir=data_dir)


# ---------------------------------------------------------------------------
# 内置处理器（不依赖数据层的最小实现；数据模块后续逐步补充）
# ---------------------------------------------------------------------------

@handler("version", "")
@handler("version", "get")
def _version(args: list[str], data_dir=None) -> list[Result]:
    from importlib.metadata import version as pkg_version

    try:
        ver = pkg_version("stkoe")
    except Exception:
        ver = "unknown"
    return [Result.json("version", {"version": ver})]


@handler("config", "show")
@handler("config", "")
def _config_show(args: list[str], data_dir=None) -> list[Result]:
    from ..settings import config_path, load_config

    cfg = load_config()
    return [Result.json("config", {
        "config_file": str(config_path()),
        **cfg.to_dict(),
    })]


@handler("config", "set")
def _config_set(args: list[str], data_dir=None) -> list[Result]:
    from ..settings import save_config

    kv = parse_flags(args)
    if not kv:
        raise CommandError("config set 需要至少一个 --key value")
    path = save_config(kv)
    return [Result.json("config", {"written": str(path), "set": kv})]


# ---------------------------------------------------------------------------
# task 同步处理器（Execute 路径；task 为任务框架元操作，仅此一处注册）
# ---------------------------------------------------------------------------

@handler("task", "list")
@handler("task", "")
def _task_list(args: list[str], data_dir=None) -> list[Result]:
    """任务列表：按创建时间倒序；``--state <state>`` 按状态过滤"""
    from pathlib import Path

    from ..settings import load_config
    from ..task.store import TaskStore, _DB

    flags = parse_flags(args)
    d = Path(data_dir).expanduser() if data_dir else Path(load_config().data_dir).expanduser()
    tasks = TaskStore(_DB(d / "tasks.db")).list(state=flags.get("state"))
    return [Result.json("tasks", [t.to_dict() for t in tasks])]


# ---------------------------------------------------------------------------
# table 同步处理器（Execute 路径；SubmitTask 后台任务版在 task/handlers.py）
# ---------------------------------------------------------------------------

def _positional(args: list[str]) -> list[str]:
    out = []
    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--"):
            key = a[2:]
            if "=" not in key and i + 1 < len(args) and not args[i + 1].startswith("--"):
                i += 1
        else:
            out.append(a)
        i += 1
    return out


def _controller(data_dir=None):
    from ..table.controller import TableController

    return TableController(data_dir=data_dir)


def _arrow_meta(name: str, df, total: int, col_metas) -> str:
    """ArrowTable.meta JSON：rows/total + 返回列的完整列元数据（display_name/unit/formula 等）"""
    known = {c.name: c.to_dict() for c in col_metas}
    cols = []
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        col = known.get(cn)
        if col is None:
            col = {"name": cn, "data_type": dt}
        cols.append(col)
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


@handler("table", "add")
def _table_add(args: list[str], data_dir=None) -> list[Result]:
    flags = parse_flags(args)
    pos = _positional(args)
    ctl = _controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.add("", all=True))
        return [Result.json("tables", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("table add 需要表名（或 --all）")
    report = asyncio.run(ctl.add(pos[0]))
    return [Result.json("table", report.to_dict())]


@handler("table", "get")
def _table_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table get 需要表名")
    flags = parse_flags(args)
    ctl = _controller(data_dir)
    df, total = asyncio.run(ctl.get(
        pos[0],
        columns=flags.get("columns").split(",") if flags.get("columns") else None,
        where=flags.get("where"),
        partition=flags.get("partition"),
        exclude_tool=bool(flags.get("exclude-tool")),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    ))
    meta = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, meta.columns))]


@handler("table", "delete")
@handler("table", "del")
def _table_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table delete 需要表名")
    flags = parse_flags(args)
    ctl = _controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("table", out)]


@handler("table", "scan")
def _table_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    ctl = _controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.scan("", all=True, resync=bool(flags.get("resync"))))
        return [Result.json("tables", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("table scan 需要表名（或 --all）")
    report = asyncio.run(ctl.scan(pos[0]))
    return [Result.json("table", report.to_dict())]


@handler("table", "list")
@handler("table", "")
def _table_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _controller(data_dir)
    flags = parse_flags(args)
    if flags.get("candidate"):
        cands = asyncio.run(ctl.list(candidate=True))
        return [Result.json("candidates", cands)]
    metas = asyncio.run(ctl.list())
    return [Result.json("tables", [m.to_dict() for m in metas])]


@handler("table", "meta")
def _table_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table meta 需要表名")
    ctl = _controller(data_dir)
    meta = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("table", meta.to_dict())]


@handler("table", "col")
def _table_col(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("table col 需要表名和列名")
    flags = parse_flags(args)
    if not flags:
        raise CommandError("table col 需要至少一个 --key value")
    ctl = _controller(data_dir)
    meta = asyncio.run(ctl.col(pos[0], pos[1], **flags))
    return [Result.json("table", meta.to_dict())]


@handler("table", "set")
def _table_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("table set 需要表名")
    if not flags:
        raise CommandError("table set 需要至少一个 --key value")
    ctl = _controller(data_dir)
    meta = asyncio.run(ctl.set(pos[0], **flags))
    return [Result.json("table", meta.to_dict())]


# ---------------------------------------------------------------------------
# stat 同步处理器（Execute 路径；SubmitTask 后台任务版在 stat/handlers.py）
# ---------------------------------------------------------------------------

def _stat_controller(data_dir=None):
    from ..stat.controller import StatController

    return StatController(data_dir=data_dir)


@handler("stat", "scan")
def _stat_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("stat scan 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    report = asyncio.run(ctl.scan(pos[0], pos[1], kind=flags.get("kind") or "coverage"))
    return [Result.json("stat", report.to_dict())]


@handler("stat", "get")
def _stat_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("stat get 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    partition_by = flags.get("partition_by") or flags.get("partition-by")
    out = asyncio.run(ctl.get(pos[0], pos[1],
                              kind=flags.get("kind") or "coverage",
                              partition_by=partition_by))
    if isinstance(out, dict):
        results: list[Result] = []
        for partition, df in out.items():
            buf = io.BytesIO()
            df.write_ipc_stream(buf)
            results.append(Result.json(f"stat/{partition}",
                                       {"partition": partition, "rows": df.height,
                                        "columns": df.columns}))
            results.append(Result.table(f"stat/{partition}", buf.getvalue()))
        return results
    buf = io.BytesIO()
    out.write_ipc_stream(buf)
    return [
        Result.json(pos[1], {"partition": partition_by, "rows": out.height,
                             "columns": out.columns}),
        Result.table(pos[1], buf.getvalue()),
    ]


@handler("stat", "meta")
def _stat_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("stat meta 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    meta = asyncio.run(ctl.meta(pos[0], pos[1], kind=flags.get("kind") or "coverage"))
    return [Result.json("stat", meta.to_dict())]


@handler("stat", "list")
def _stat_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _stat_controller(data_dir)
    metas = asyncio.run(ctl.list())
    return [Result.json("stats", [m.to_dict() for m in metas])]


@handler("stat", "delete")
@handler("stat", "del")
def _stat_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("stat delete 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], pos[1], kind=flags.get("kind")))
    return [Result.json("stat", out)]


# ---------------------------------------------------------------------------
# dataset 同步处理器（Execute 路径；SubmitTask 后台任务版在 dataset/handlers.py）
# ---------------------------------------------------------------------------

def _dataset_controller(data_dir=None):
    from ..dataset.controller import DatasetController

    return DatasetController(data_dir=data_dir)


@handler("dataset", "add")
def _dataset_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 3:
        raise CommandError("dataset add 需要 dataset 名、index 表与至少一个成员表")
    flags = parse_flags(args)
    ctl = _dataset_controller(data_dir)
    keys = None
    if flags.get("keys"):
        keys = [k.strip() for k in flags["keys"].split(",") if k.strip()]
    dm = asyncio.run(ctl.add(pos[0], pos[1], *pos[2:], keys=keys,
                             materialize=bool(flags.get("materialize"))))
    return [Result.json("dataset", dm.to_dict())]


@handler("dataset", "get")
def _dataset_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("dataset get 需要 dataset 名")
    flags = parse_flags(args)
    ctl = _dataset_controller(data_dir)
    columns = flags.get("columns") or None
    df, total = asyncio.run(ctl.get(
        pos[0],
        columns=columns.split(",") if columns else None,
        where=flags.get("where"),
        partition=flags.get("partition"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    ))
    dm = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, dm.columns))]


@handler("dataset", "delete")
@handler("dataset", "del")
def _dataset_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("dataset delete 需要 dataset 名")
    flags = parse_flags(args)
    ctl = _dataset_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("dataset", out)]


@handler("dataset", "list")
@handler("dataset", "")
def _dataset_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _dataset_controller(data_dir)
    dms = asyncio.run(ctl.list())
    return [Result.json("datasets", [dm.to_dict() for dm in dms])]


@handler("dataset", "meta")
def _dataset_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("dataset meta 需要 dataset 名")
    ctl = _dataset_controller(data_dir)
    dm = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("dataset", dm.to_dict())]


@handler("dataset", "set")
def _dataset_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("dataset set 需要 dataset 名")
    if not flags:
        raise CommandError("dataset set 需要至少一个 --key value")
    ctl = _dataset_controller(data_dir)
    dm = asyncio.run(ctl.set(pos[0], **flags))
    return [Result.json("dataset", dm.to_dict())]


@handler("dataset", "scan")
def _dataset_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    ctl = _dataset_controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.scan(all=True, resync=bool(flags.get("resync"))))
        return [Result.json("datasets", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("dataset scan 需要 dataset 名（或 --all）")
    report = asyncio.run(ctl.scan(pos[0], resync=bool(flags.get("resync"))))
    return [Result.json("dataset", report.to_dict())]
