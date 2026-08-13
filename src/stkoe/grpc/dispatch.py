"""命令分发：把 Execute / SubmitTask 的 ``(source, action, args)`` 路由到对应处理器

协议约定请求为 ``stkoe <source> <action> <args...>`` 位置参数形态：
- source：table / dataset / stat / fieldset / sample / feature / factor / test / config / task / mock / version
- action：add / get / del / set / list / meta / check / test / scan / ... 等子命令动词
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
    meta = {k: v for k, v in flags.items() if k != "all"}
    report = asyncio.run(ctl.add(pos[0], meta=meta or None))
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
    flags = parse_flags(args)
    if not pos:
        raise CommandError("stat scan 需要 target 类型和名字（如 dataset <name>）")
    from ..factor_test.tester import TESTER_KINDS

    kind = flags.get("kind") or "coverage"
    if len(pos) == 1 and kind in TESTER_KINDS:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat scan 需要 target 类型和名字（如 dataset <name>，"
                           "或 test <name> --kind <tester>）")
    ctl = _stat_controller(data_dir)
    report = asyncio.run(ctl.scan(target_type, target_name, kind=kind))
    return [Result.json("stat", report.to_dict())]


@handler("stat", "get")
def _stat_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("stat get 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    kind = flags.get("kind") or "coverage"
    partition_by = flags.get("partition_by") or flags.get("partition-by")
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat get 需要 target 类型和名字（如 dataset <name>）")
    out = asyncio.run(ctl.get(target_type, target_name,
                              kind=kind, partition_by=partition_by))
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
        Result.json(target_name, {"partition": partition_by, "rows": out.height,
                                  "columns": out.columns}),
        Result.table(target_name, buf.getvalue()),
    ]


@handler("stat", "meta")
def _stat_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("stat meta 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    kind = flags.get("kind") or "coverage"
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat meta 需要 target 类型和名字（如 dataset <name>）")
    meta = asyncio.run(ctl.meta(target_type, target_name, kind=kind))
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
    if not pos:
        raise CommandError("stat delete 需要 target 类型和名字（如 dataset <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat delete 需要 target 类型和名字（如 dataset <name>）")
    out = asyncio.run(ctl.delete(target_type, target_name, kind=flags.get("kind")))
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


# ---------------------------------------------------------------------------
# fieldset 同步处理器（Execute 路径；SubmitTask 后台任务版在 fieldset/handlers.py）
# ---------------------------------------------------------------------------

def _fieldset_controller(data_dir=None):
    from ..fieldset.controller import FieldsetController

    return FieldsetController(data_dir=data_dir)


def _fieldset_arrow_meta(name: str, df, total: int, fm) -> str:
    """ArrowTable.meta JSON：rows/total + keys 列元数据（源 dataset）+ 指标列"""
    cols = []
    pkeys = set(fm.keys)
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        meta = {"name": cn, "data_type": dt}
        if cn in pkeys:
            src = next((c for c in fm.columns if c.name == cn), None)
            if src is not None:
                meta = {**src.to_dict(), "data_type": dt}
        else:
            f = next((fp for fp in fm.fields if fp.name == cn), None)
            if f is not None:
                meta = {**f.to_dict(), "data_type": dt}
        cols.append(meta)
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


@handler("fieldset", "add")
def _fieldset_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset add 需要指标集名")
    flags = parse_flags(args)
    ctl = _fieldset_controller(data_dir)
    if len(pos) == 1:
        fm = asyncio.run(ctl.add(pos[0], dataset=flags.get("dataset"),
                                 engine=flags.get("engine") or "polars", **{
                                     k: v for k, v in flags.items()
                                     if k not in ("dataset", "engine")}))
        return [Result.json("fieldset", fm.to_dict())]
    fm = asyncio.run(ctl.add_field(pos[0], pos[1], **flags))
    return [Result.json("fieldset", fm.to_dict())]


@handler("fieldset", "get")
def _fieldset_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset get 需要指标集名")
    flags = parse_flags(args)
    ctl = _fieldset_controller(data_dir)
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
    fm = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_fieldset_arrow_meta(pos[0], df, total, fm))]


@handler("fieldset", "meta")
def _fieldset_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset meta 需要指标集名")
    ctl = _fieldset_controller(data_dir)
    if len(pos) == 1:
        fm = asyncio.run(ctl.meta(pos[0]))
        return [Result.json("fieldset", fm.to_dict())]
    field = asyncio.run(ctl.field_meta(pos[0], pos[1]))
    return [Result.json("field", field.to_dict())]


@handler("fieldset", "list")
@handler("fieldset", "")
def _fieldset_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _fieldset_controller(data_dir)
    fms = asyncio.run(ctl.list())
    return [Result.json("fieldsets", [fm.to_dict() for fm in fms])]


@handler("fieldset", "set")
def _fieldset_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("fieldset set 需要指标集名")
    if not flags:
        raise CommandError("fieldset set 需要至少一个 --key value")
    ctl = _fieldset_controller(data_dir)
    if len(pos) == 1:
        fm = asyncio.run(ctl.set(pos[0], **flags))
        return [Result.json("fieldset", fm.to_dict())]
    fm = asyncio.run(ctl.set_field(pos[0], pos[1], **flags))
    return [Result.json("fieldset", fm.to_dict())]


@handler("fieldset", "del")
@handler("fieldset", "delete")
def _fieldset_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset delete 需要指标集名")
    flags = parse_flags(args)
    ctl = _fieldset_controller(data_dir)
    if len(pos) > 1:
        fm = asyncio.run(ctl.delete_field(pos[0], pos[1]))
        return [Result.json("fieldset", fm.to_dict())]
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("fieldset", out)]


@handler("fieldset", "check")
def _fieldset_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset check 需要指标集名")
    flags = parse_flags(args)
    ctl = _fieldset_controller(data_dir)
    field = pos[1] if len(pos) > 1 else None
    results = asyncio.run(ctl.check(pos[0], field, all_fields=bool(flags.get("all"))))
    return [Result.json("fieldset", [r.to_dict() for r in results])]


@handler("fieldset", "test")
def _fieldset_test(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset test 需要指标集名")
    flags = parse_flags(args)
    if not flags.get("formula"):
        raise CommandError("fieldset test 需要 --formula <表达式>")
    ctl = _fieldset_controller(data_dir)
    try:
        df = asyncio.run(ctl.test(pos[0], flags["formula"]))
    except Exception as e:
        return [Result.json("fieldset", {"ok": False, "error": str(e)})]
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.json("fieldset", {"ok": True, "rows": df.height,
                                     "columns": df.columns}),
            Result.table(f"test/{pos[0]}", buf.getvalue())]


@handler("fieldset", "scan")
def _fieldset_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    ctl = _fieldset_controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.scan(all=True, resync=bool(flags.get("resync"))))
        return [Result.json("fieldsets", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("fieldset scan 需要指标集名（或 --all）")
    report = asyncio.run(ctl.scan(pos[0], resync=bool(flags.get("resync"))))
    return [Result.json("fieldset", report.to_dict())]


# ---------------------------------------------------------------------------
# sample 同步处理器（Execute 路径；SubmitTask 后台任务版在 sample/handlers.py）
# ---------------------------------------------------------------------------

def _sample_controller(data_dir=None):
    from ..sample.controller import SampleController

    return SampleController(data_dir=data_dir)


def _sample_arrow_meta(name: str, df, total: int, sm) -> str:
    """ArrowTable.meta JSON：rows/total + 返回列的完整列元数据（源 dataset + fieldset 衍生列）"""
    known = {c.name: c.to_dict() for c in sm.columns}
    cols = []
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        col = known.get(cn)
        if col is None:
            col = {"name": cn, "data_type": dt}
        else:
            col = {**col, "data_type": dt}
        cols.append(col)
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


@handler("sample", "add")
def _sample_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample add 需要样本池名")
    flags = parse_flags(args)
    ctl = _sample_controller(data_dir)
    sm = asyncio.run(ctl.add(pos[0], dataset=flags.get("dataset"),
                             engine=flags.get("engine") or "polars",
                             formula=flags.get("formula") or "", **{
                                 k: v for k, v in flags.items()
                                 if k not in ("dataset", "engine", "formula")}))
    return [Result.json("sample", sm.to_dict())]


@handler("sample", "get")
def _sample_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample get 需要样本池名")
    flags = parse_flags(args)
    ctl = _sample_controller(data_dir)
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
    sm = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_sample_arrow_meta(pos[0], df, total, sm))]


@handler("sample", "meta")
def _sample_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample meta 需要样本池名")
    ctl = _sample_controller(data_dir)
    sm = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("sample", sm.to_dict())]


@handler("sample", "list")
@handler("sample", "")
def _sample_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _sample_controller(data_dir)
    sms = asyncio.run(ctl.list())
    return [Result.json("samples", [sm.to_dict() for sm in sms])]


@handler("sample", "set")
def _sample_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("sample set 需要样本池名")
    if not flags:
        raise CommandError("sample set 需要至少一个 --key value")
    ctl = _sample_controller(data_dir)
    sm = asyncio.run(ctl.set(pos[0], **flags))
    return [Result.json("sample", sm.to_dict())]


@handler("sample", "check")
def _sample_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample check 需要样本池名")
    ctl = _sample_controller(data_dir)
    res = asyncio.run(ctl.check(pos[0]))
    return [Result.json("sample", res.to_dict())]


@handler("sample", "del")
@handler("sample", "delete")
def _sample_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample delete 需要样本池名")
    flags = parse_flags(args)
    ctl = _sample_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("sample", out)]


# ---------------------------------------------------------------------------
# feature 同步处理器（Execute 路径；SubmitTask 后台任务版在 feature/handlers.py）
# ---------------------------------------------------------------------------

def _feature_controller(data_dir=None):
    from ..feature.controller import FeatureController

    return FeatureController(data_dir=data_dir)


@handler("feature", "add")
def _feature_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature add 需要因子名")
    flags = parse_flags(args)
    ctl = _feature_controller(data_dir)
    ft = asyncio.run(ctl.add(pos[0], engine=flags.get("engine") or "polars",
                             formula=flags.get("formula") or "", **{
                                 k: v for k, v in flags.items()
                                 if k not in ("engine", "formula")}))
    return [Result.json("feature", ft.to_dict())]


@handler("feature", "set")
def _feature_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("feature set 需要因子名")
    if not flags:
        raise CommandError("feature set 需要至少一个 --key value")
    ctl = _feature_controller(data_dir)
    ft = asyncio.run(ctl.set(pos[0], **flags))
    return [Result.json("feature", ft.to_dict())]


@handler("feature", "del")
@handler("feature", "delete")
def _feature_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature delete 需要因子名")
    flags = parse_flags(args)
    ctl = _feature_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("feature", out)]


@handler("feature", "meta")
def _feature_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature meta 需要因子名")
    ctl = _feature_controller(data_dir)
    ft = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("feature", ft.to_dict())]


@handler("feature", "list")
@handler("feature", "")
def _feature_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _feature_controller(data_dir)
    fts = asyncio.run(ctl.list())
    return [Result.json("features", [ft.to_dict() for ft in fts])]


@handler("feature", "test")
def _feature_test(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature test 需要因子名")
    flags = parse_flags(args)
    if not flags.get("sample"):
        raise CommandError("feature test 需要 --sample <样本池名>")
    ctl = _feature_controller(data_dir)
    res, df = asyncio.run(ctl.test(pos[0], flags["sample"]))
    if df is not None and df.height:
        buf = io.BytesIO()
        df.write_ipc_stream(buf)
        return [Result.json("feature", res.to_dict()),
                Result.table(f"test/{pos[0]}", buf.getvalue())]
    return [Result.json("feature", res.to_dict())]


# ---------------------------------------------------------------------------
# factor 同步处理器（Execute 路径；SubmitTask 后台任务版在 factor/handlers.py）
# ---------------------------------------------------------------------------

def _factor_controller(data_dir=None):
    from ..factor.controller import FactorController

    return FactorController(data_dir=data_dir)


def _factor_arrow_meta(name: str, df, total: int, fm) -> str:
    """ArrowTable.meta JSON：rows/total + factor 列元数据（索引列 + 因子列说明）"""
    keys = set(fm.keys)
    cols = []
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        if cn in keys:
            cols.append({"name": cn, "data_type": dt, "as_index": True})
        elif fm.field is not None and cn == fm.factor_col:
            cols.append({**fm.field.to_dict(), "data_type": dt})
        else:
            cols.append({"name": cn, "data_type": dt})
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


@handler("factor", "add")
def _factor_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor add 需要因子名")
    flags = parse_flags(args)
    ctl = _factor_controller(data_dir)
    fm = asyncio.run(ctl.add(pos[0], **flags))
    return [Result.json("factor", fm.to_dict())]


@handler("factor", "get")
def _factor_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor get 需要因子名")
    flags = parse_flags(args)
    ctl = _factor_controller(data_dir)
    df, total = asyncio.run(ctl.get(
        pos[0],
        where=flags.get("where"),
        partition=flags.get("partition"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    ))
    fm = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_factor_arrow_meta(pos[0], df, total, fm))]


@handler("factor", "set")
def _factor_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("factor set 需要因子名")
    if not flags:
        raise CommandError("factor set 需要至少一个 --key value")
    ctl = _factor_controller(data_dir)
    fm = asyncio.run(ctl.set(pos[0], **flags))
    return [Result.json("factor", fm.to_dict())]


@handler("factor", "del")
@handler("factor", "delete")
def _factor_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor delete 需要因子名")
    flags = parse_flags(args)
    ctl = _factor_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("factor", out)]


@handler("factor", "meta")
def _factor_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor meta 需要因子名")
    ctl = _factor_controller(data_dir)
    fm = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("factor", fm.to_dict())]


@handler("factor", "list")
@handler("factor", "")
def _factor_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _factor_controller(data_dir)
    fms = asyncio.run(ctl.list())
    return [Result.json("factors", [fm.to_dict() for fm in fms])]


@handler("factor", "check")
def _factor_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor check 需要因子名")
    ctl = _factor_controller(data_dir)
    res = asyncio.run(ctl.check(pos[0]))
    return [Result.json("factor", res.to_dict())]


@handler("factor", "scan")
def _factor_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    ctl = _factor_controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.scan(all=True, resync=bool(flags.get("resync"))))
        return [Result.json("factors", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("factor scan 需要因子名（或 --all）")
    report = asyncio.run(ctl.scan(pos[0], resync=bool(flags.get("resync"))))
    return [Result.json("factor", report.to_dict())]


# ---------------------------------------------------------------------------
# test 同步处理器（Execute 路径；SubmitTask 后台任务版在 factor_test/handlers.py）
# ---------------------------------------------------------------------------

def _test_controller(data_dir=None):
    from ..factor_test.controller import FactorTestController

    return FactorTestController(data_dir=data_dir)


def _test_arrow_meta(name: str, df, total: int, tm) -> str:
    """ArrowTable.meta JSON：rows/total + 测试数据集列元数据"""
    known = {c.name: c.to_dict() for c in tm.columns}
    cols = []
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        col = known.get(cn)
        if col is None:
            col = {"name": cn, "data_type": dt}
        else:
            col = {**col, "data_type": dt}
        cols.append(col)
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


@handler("test", "add")
def _test_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test add 需要测试集名")
    flags = parse_flags(args)
    if not flags.get("factor"):
        raise CommandError("test add 需要 --factor <因子名>")
    ctl = _test_controller(data_dir)
    from ..factor_test.spec import FactorTesterSpec

    spec = FactorTesterSpec(
        by_group=bool(flags.get("by_group")),
        quantiles=int(flags["quantiles"]) if flags.get("quantiles") else 5,
        periods=tuple(int(p) for p in (flags.get("periods") or "1,5,10").split(",")
                      if p.strip()),
        date_range=tuple(str(x) for x in
                         (flags.get("date_range") or "2023-01-01,2026-01-01").split(",")),
        rolling_window=int(flags["rolling_window"]) if flags.get("rolling_window") else 252,
    )
    tm = asyncio.run(ctl.add(
        pos[0], factor=flags["factor"],
        returns=flags.get("returns") or "r",
        groupby=flags.get("groupby") or "ic",
        marketcap=flags.get("marketcap") or "fv",
        spec=spec, **{k: v for k, v in flags.items()
                      if k not in ("factor", "returns", "groupby", "marketcap",
                                   "by_group", "quantiles", "periods",
                                   "date_range", "rolling_window")}))
    return [Result.json("test", tm.to_dict())]


@handler("test", "get")
def _test_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test get 需要测试集名")
    flags = parse_flags(args)
    ctl = _test_controller(data_dir)
    df, total = asyncio.run(ctl.get(
        pos[0],
        where=flags.get("where"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    ))
    tm = asyncio.run(ctl.meta(pos[0]))
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_test_arrow_meta(pos[0], df, total, tm))]


@handler("test", "meta")
def _test_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test meta 需要测试集名")
    ctl = _test_controller(data_dir)
    tm = asyncio.run(ctl.meta(pos[0]))
    return [Result.json("test", tm.to_dict())]


@handler("test", "list")
@handler("test", "")
def _test_list(args: list[str], data_dir=None) -> list[Result]:
    ctl = _test_controller(data_dir)
    tms = asyncio.run(ctl.list())
    return [Result.json("tests", [tm.to_dict() for tm in tms])]


@handler("test", "set")
def _test_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("test set 需要测试集名")
    if not flags:
        raise CommandError("test set 需要至少一个 --key value")
    ctl = _test_controller(data_dir)
    kw = dict(flags)
    if "spec" in kw and isinstance(kw["spec"], str):
        kw["spec"] = {"periods": [int(p) for p in kw["spec"].split(",")]}
    tm = asyncio.run(ctl.set(pos[0], **kw))
    return [Result.json("test", tm.to_dict())]


@handler("test", "del")
@handler("test", "delete")
def _test_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test delete 需要测试集名")
    flags = parse_flags(args)
    ctl = _test_controller(data_dir)
    out = asyncio.run(ctl.delete(pos[0], force=bool(flags.get("force"))))
    return [Result.json("test", out)]


@handler("test", "check")
def _test_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test check 需要测试集名")
    ctl = _test_controller(data_dir)
    res = asyncio.run(ctl.check(pos[0]))
    return [Result.json("test", res.to_dict())]


@handler("test", "scan")
def _test_scan(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    ctl = _test_controller(data_dir)
    if flags.get("all"):
        reports = asyncio.run(ctl.scan(all=True, resync=bool(flags.get("resync"))))
        return [Result.json("tests", [r.to_dict() for r in reports])]
    if not pos:
        raise CommandError("test scan 需要测试集名（或 --all）")
    report = asyncio.run(ctl.scan(pos[0], resync=bool(flags.get("resync"))))
    return [Result.json("test", report.to_dict())]


# ---------------------------------------------------------------------------
# mock 同步处理器（Execute 路径；SubmitTask 后台任务版在 mock/handlers.py）
# ---------------------------------------------------------------------------

@handler("mock", "demo")
def _mock_demo(args: list[str], data_dir=None) -> list[Result]:
    """生成 example.md 演示源表 index + m1 到 tables/（默认 300 只 × 500 日）"""
    from ..mock.gen import demo

    flags = parse_flags(args)
    reports = demo(data_dir=data_dir,
                   n_syms=int(flags["n-syms"]) if flags.get("n-syms") else 300,
                   n_days=int(flags["n-days"]) if flags.get("n-days") else 500)
    return [Result.json("mock", reports)]


@handler("mock", "gen")
def _mock_gen(args: list[str], data_dir=None) -> list[Result]:
    """参数化生成单张表：mock gen <name> --kind <kind> [--n-syms/--n-days/--start/--end/--seed/--col]"""
    from ..mock.gen import gen as _mock_gen_

    pos = _positional(args)
    if not pos:
        raise CommandError("mock gen 需要表名（如 mock gen mytable --kind index）")
    flags = parse_flags(args)
    report = _mock_gen_(
        pos[0], flags.get("kind") or "index", data_dir=data_dir,
        n_syms=int(flags["n-syms"]) if flags.get("n-syms") else 10,
        start=flags.get("start") or "2024-01-01",
        end=flags.get("end") or "2024-01-03",
        n_days=int(flags["n-days"]) if flags.get("n-days") else None,
        seed=int(flags["seed"]) if flags.get("seed") else None,
        col=flags.get("col"),
    )
    return [Result.json("mock", report)]
