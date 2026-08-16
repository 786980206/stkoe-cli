"""命令分发：把 Execute / SubmitTask 的 ``(source, action, args)`` 路由到对应处理器

协议约定请求为 ``stkoe <source> <action> <args...>`` 位置参数形态：
- source：table / index / panel / fieldset / sample / feature /
  factor / test / stat / config / task / mock / graph / version
- action：add / get / del / set / list / meta / check / test / update / ... 等子命令动词
- args：action 之后的位置参数

处理器通过 ``@handler(source, action)`` 装饰器注册，签名 ``fn(args, data_dir=None) -> list[Result]``；
``Result`` 携带 name + kind（json/table），由 gRPC 层分别序列化为
``JsonData`` / ``ArrowTable``。

V3.0：table/index/panel/fieldset/sample/feature/factor/test 全部走 ``GraphService``
（登记/依赖/版本进 graph，物理数据走 graph.db 指纹 + polars）；SubmitTask 任务版
（task/handlers.py 的 TaskHandler）行为对齐（同走 GraphService）。
"""
from __future__ import annotations

import asyncio
import io
import threading
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


def _arrow_meta(name: str, df, total: int, col_metas) -> str:
    """ArrowTable.meta JSON：rows/total + 返回列的完整列元数据（display_name/unit/formula 等）"""
    known = {}
    for c in col_metas:
        d = c.to_dict() if hasattr(c, "to_dict") else dict(c)
        known[d.get("name")] = d
    cols = []
    for cn, dt in zip(df.columns, (str(t) for t in df.dtypes)):
        col = known.get(cn)
        if col is None:
            col = {"name": cn, "data_type": dt}
        cols.append(col)
    return dumps_str({"name": name, "rows": df.height, "total": total,
                      "columns": cols})


_thread_local = threading.local()


def _graph_service(data_dir=None):
    """V3.0 GraphService（登记/依赖/版本走 graph；物理指纹 graph.db 普通表）。

    按**线程本地**缓存（key = data_dir 真实路径）：gRPC worker / CLI 主线程内
    顺序复用同一服务——连接数有界（线程数 × 目录数）、不再每次 Execute 泄漏一个
    SQLite 连接；跨线程仍各自独立连接（SQLite 文件锁 + WAL/busy_timeout 兜底）。
    """
    from ..graph.service import GraphService
    from ..settings import load_config

    import os

    base = os.path.realpath(os.path.expanduser(data_dir or load_config().data_dir))
    cache = getattr(_thread_local, "services", None)
    if cache is None:
        cache = _thread_local.services = {}
    svc = cache.get(base)
    if svc is None:
        svc = GraphService(data_dir=base)
        cache[base] = svc
    return svc


@handler("table", "add")
def _table_add(args: list[str], data_dir=None) -> list[Result]:
    flags = parse_flags(args)
    pos = _positional(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        reports = svc.table_add("", all=True)
        return [Result.json("tables", reports)]
    if not pos:
        raise CommandError("table add 需要表名（或 --all）")
    meta = {k: v for k, v in flags.items() if k != "all"}
    return [Result.json("table", svc.table_add(pos[0], meta=meta or None))]


@handler("table", "get")
def _table_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table get 需要表名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.table_get(
        pos[0],
        columns=flags.get("columns").split(",") if flags.get("columns") else None,
        where=flags.get("where"),
        partition=flags.get("partition"),
        exclude_tool=bool(flags.get("exclude-tool")),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    meta = svc.table_meta(pos[0])
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, meta["columns"]))]


@handler("table", "delete")
@handler("table", "del")
def _table_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table delete 需要表名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("table", svc.table_delete(pos[0], force=bool(flags.get("force"))))]


@handler("table", "update")
def _table_update(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        return [Result.json("tables", svc.table_update("", all=True))]
    if not pos:
        raise CommandError("table update 需要表名（或 --all）")
    return [Result.json("table", svc.table_update(pos[0]))]


@handler("table", "list")
@handler("table", "")
def _table_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    flags = parse_flags(args)
    if flags.get("candidate"):
        return [Result.json("candidates", svc.table_list(candidate=True))]
    return [Result.json("tables", svc.table_list())]


@handler("table", "meta")
def _table_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("table meta 需要表名")
    svc = _graph_service(data_dir)
    return [Result.json("table", svc.table_meta(pos[0]))]


@handler("table", "col")
def _table_col(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("table col 需要表名和列名")
    flags = parse_flags(args)
    if not flags:
        raise CommandError("table col 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("table", svc.table_col(pos[0], pos[1], **flags))]


@handler("table", "set")
def _table_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("table set 需要表名")
    if not flags:
        raise CommandError("table set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("table", svc.table_set(pos[0], **flags))]


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
        raise CommandError("stat scan 需要 target 类型和名字（如 panel <name>）")
    from ..factor_test.tester import TESTER_KINDS

    kind = flags.get("kind") or "coverage"
    if len(pos) == 1 and kind in TESTER_KINDS:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat scan 需要 target 类型和名字（如 panel <name>，"
                           "或 test <name> --kind <tester>）")
    ctl = _stat_controller(data_dir)
    report = asyncio.run(ctl.scan(target_type, target_name, kind=kind))
    return [Result.json("stat", report.to_dict())]


@handler("stat", "get")
def _stat_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("stat get 需要 target 类型和名字（如 panel <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    kind = flags.get("kind") or "coverage"
    partition_by = flags.get("partition_by") or flags.get("partition-by")
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat get 需要 target 类型和名字（如 panel <name>）")
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
        raise CommandError("stat meta 需要 target 类型和名字（如 panel <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    kind = flags.get("kind") or "coverage"
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat meta 需要 target 类型和名字（如 panel <name>）")
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
        raise CommandError("stat delete 需要 target 类型和名字（如 panel <name>）")
    flags = parse_flags(args)
    ctl = _stat_controller(data_dir)
    if len(pos) == 1:
        target_type, target_name = "test", pos[0]
    elif len(pos) >= 2:
        target_type, target_name = pos[0], pos[1]
    else:
        raise CommandError("stat delete 需要 target 类型和名字（如 panel <name>）")
    out = asyncio.run(ctl.delete(target_type, target_name, kind=flags.get("kind")))
    return [Result.json("stat", out)]


# ---------------------------------------------------------------------------
# panel 同步处理器（登记/依赖走 graph，get 实时 join）
# ---------------------------------------------------------------------------

@handler("panel", "add")
def _panel_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 3:
        raise CommandError("panel add 需要 panel 名、index 节点与至少一个成员表")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    # keys 由 index 推断（symbol_col + datetime_col），不再接受显式 --keys
    dm = svc.panel_add(pos[0], pos[1], pos[2:], **flags)
    return [Result.json("panel", dm)]


@handler("panel", "get")
def _panel_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("panel get 需要 panel 名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    columns = flags.get("columns") or None
    df, total = svc.panel_get(
        pos[0],
        columns=columns.split(",") if columns else None,
        where=flags.get("where"),
        partition=flags.get("partition"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    dm = svc.panel_meta(pos[0])
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, dm["columns"]))]


@handler("panel", "delete")
@handler("panel", "del")
def _panel_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("panel delete 需要 panel 名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("panel", svc.panel_delete(pos[0], force=bool(flags.get("force"))))]


@handler("panel", "list")
@handler("panel", "")
def _panel_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("panels", svc.panel_list())]


@handler("panel", "meta")
def _panel_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("panel meta 需要 panel 名")
    svc = _graph_service(data_dir)
    return [Result.json("panel", svc.panel_meta(pos[0]))]


@handler("panel", "set")
def _panel_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("panel set 需要 panel 名")
    if not flags:
        raise CommandError("panel set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("panel", svc.panel_set(pos[0], **flags))]


@handler("panel", "update")
def _panel_update(args: list[str], data_dir=None) -> list[Result]:
    """panel 更新：上游（index/成员表）就绪后标记有效（无物化）。"""
    pos = _positional(args)
    if not pos:
        raise CommandError("panel update 需要 panel 名")
    svc = _graph_service(data_dir)
    return [Result.json("panel", svc.panel_update(pos[0]))]


# ---------------------------------------------------------------------------
# index 同步处理器（V3.0 独立主体：Index 节点 + 物理 parquet，symbol/datetime 列）
# ---------------------------------------------------------------------------

@handler("index", "add")
def _index_add(args: list[str], data_dir=None) -> list[Result]:
    flags = parse_flags(args)
    pos = _positional(args)
    svc = _graph_service(data_dir)
    meta = {k: v for k, v in flags.items()
            if k not in ("all", "symbol-col", "datetime-col", "materialize-partition")}
    if flags.get("all"):
        reports = svc.index_add(
            "", all=True,
            symbol_col=flags.get("symbol-col") or "sym",
            datetime_col=flags.get("datetime-col") or "date",
            materialize_partition=flags.get("materialize-partition") or "yearly",
            meta=meta or None)
        return [Result.json("indexes", reports)]
    if not pos:
        raise CommandError("index add 需要 index 名（或 --all）")
    return [Result.json("index", svc.index_add(
        pos[0],
        symbol_col=flags.get("symbol-col") or "sym",
        datetime_col=flags.get("datetime-col") or "date",
        materialize_partition=flags.get("materialize-partition") or "yearly",
        meta=meta or None))]


@handler("index", "get")
def _index_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("index get 需要 index 名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.index_get(
        pos[0],
        columns=flags.get("columns").split(",") if flags.get("columns") else None,
        where=flags.get("where"),
        partition=flags.get("partition"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    meta = svc.index_meta(pos[0])
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, meta["columns"]))]


@handler("index", "delete")
@handler("index", "del")
def _index_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("index delete 需要 index 名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("index", svc.index_delete(pos[0], force=bool(flags.get("force"))))]


@handler("index", "update")
def _index_update(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        return [Result.json("indexes", svc.index_update("", all=True))]
    if not pos:
        raise CommandError("index update 需要 index 名（或 --all）")
    return [Result.json("index", svc.index_update(pos[0]))]


@handler("index", "list")
@handler("index", "")
def _index_list(args: list[str], data_dir=None) -> list[Result]:
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("candidate"):
        return [Result.json("candidates", svc.index_list(candidate=True))]
    return [Result.json("indexes", svc.index_list())]


@handler("index", "meta")
def _index_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("index meta 需要 index 名")
    svc = _graph_service(data_dir)
    return [Result.json("index", svc.index_meta(pos[0]))]


@handler("index", "col")
def _index_col(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 2:
        raise CommandError("index col 需要 index 名和列名")
    flags = parse_flags(args)
    if not flags:
        raise CommandError("index col 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("index", svc.index_col(pos[0], pos[1], **flags))]


@handler("index", "set")
def _index_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("index set 需要 index 名")
    if not flags:
        raise CommandError("index set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("index", svc.index_set(pos[0], **flags))]


# ---------------------------------------------------------------------------
# fieldset 同步处理器（graph 登记；check/update 用 panel 视图 + 公式引擎）
# ---------------------------------------------------------------------------

@handler("fieldset", "add")
def _fieldset_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset add 需要指标集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if len(pos) == 1:
        if not flags.get("panel"):
            raise CommandError("fieldset add 需要 --panel <panel 名>")
        fm = svc.fieldset_add(pos[0], flags["panel"],
                              engine=flags.get("engine") or "polars", **{
                                  k: v for k, v in flags.items()
                                  if k not in ("panel", "engine")})
        return [Result.json("fieldset", fm)]
    fm = svc.fieldset_add_field(pos[0], pos[1], flags.get("formula") or "", **{
        k: v for k, v in flags.items() if k != "formula"})
    return [Result.json("fieldset", fm)]


@handler("fieldset", "get")
def _fieldset_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset get 需要指标集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.fieldset_get(
        pos[0],
        columns=flags.get("columns").split(",") if flags.get("columns") else None,
        where=flags.get("where"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
        fields_only=bool(flags.get("fields-only")),
    )
    fm = svc.fieldset_meta(pos[0])
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, fm.get("columns") or []))]


@handler("fieldset", "meta")
def _fieldset_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset meta 需要指标集名")
    svc = _graph_service(data_dir)
    if len(pos) == 1:
        return [Result.json("fieldset", svc.fieldset_meta(pos[0]))]
    return [Result.json("field", svc.fieldset_meta_field(pos[0], pos[1]))]


@handler("fieldset", "list")
@handler("fieldset", "")
def _fieldset_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("fieldsets", svc.fieldset_list())]


@handler("fieldset", "set")
def _fieldset_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("fieldset set 需要指标集名")
    if not flags:
        raise CommandError("fieldset set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    if len(pos) == 1:
        return [Result.json("fieldset", svc.fieldset_set(pos[0], **flags))]
    return [Result.json("fieldset", svc.fieldset_set_field(pos[0], pos[1], **flags))]


@handler("fieldset", "del")
@handler("fieldset", "delete")
def _fieldset_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset delete 需要指标集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if len(pos) > 1:
        return [Result.json("fieldset", svc.fieldset_delete_field(pos[0], pos[1]))]
    return [Result.json("fieldset", svc.fieldset_delete(pos[0], force=bool(flags.get("force"))))]


@handler("fieldset", "check")
def _fieldset_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset check 需要指标集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all") or len(pos) < 2:
        fm = svc.fieldset_meta(pos[0])
        results = [svc.fieldset_check(pos[0], f) for f in (fm.get("fields") or {})]
        return [Result.json("fieldset", results)]
    return [Result.json("fieldset", [svc.fieldset_check(pos[0], pos[1])])]


@handler("fieldset", "test")
def _fieldset_test(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("fieldset test 需要指标集名")
    flags = parse_flags(args)
    if not flags.get("formula"):
        raise CommandError("fieldset test 需要 --formula <表达式>")
    svc = _graph_service(data_dir)
    try:
        res, df = svc.fieldset_test(pos[0], flags["formula"])
    except Exception as e:
        return [Result.json("fieldset", {"ok": False, "error": str(e)})]
    buf = io.BytesIO()
    if df is not None and df.height:
        df.write_ipc_stream(buf)
        return [Result.json("fieldset", res), Result.table(f"test/{pos[0]}", buf.getvalue())]
    return [Result.json("fieldset", res)]


@handler("fieldset", "update")
def _fieldset_update(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        return [Result.json("fieldsets",
                            [svc.fieldset_update(n["name"]) for n in svc.graph.list("fieldset")])]
    if not pos:
        raise CommandError("fieldset update 需要指标集名（或 --all）")
    return [Result.json("fieldset", svc.fieldset_update(pos[0]))]


# ---------------------------------------------------------------------------
# sample 同步处理器（graph 登记；依赖 fieldset，get/check 实时过滤）
# ---------------------------------------------------------------------------

@handler("sample", "add")
def _sample_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if len(pos) < 3:
        raise CommandError("sample add 需要 <样本池名> <fieldset 名> <index 名>")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    sm = svc.sample_add(pos[0], pos[1], pos[2], **{
        k: v for k, v in flags.items() if k not in ("fieldset", "index")})
    return [Result.json("sample", sm)]


@handler("sample", "get")
def _sample_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample get 需要样本池名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.sample_get(
        pos[0],
        columns=flags.get("columns").split(",") if flags.get("columns") else None,
        where=flags.get("where"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    sm = svc.sample_meta(pos[0])
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, sm.get("columns") or []))]


@handler("sample", "meta")
def _sample_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample meta 需要样本池名")
    svc = _graph_service(data_dir)
    return [Result.json("sample", svc.sample_meta(pos[0]))]


@handler("sample", "list")
@handler("sample", "")
def _sample_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("samples", svc.sample_list())]


@handler("sample", "set")
def _sample_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("sample set 需要样本池名")
    if not flags:
        raise CommandError("sample set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("sample", svc.sample_set(pos[0], **flags))]


@handler("sample", "update")
def _sample_update(args: list[str], data_dir=None) -> list[Result]:
    """sample 更新：上游（fieldset 链）就绪后标记有效（无物化）。"""
    pos = _positional(args)
    if not pos:
        raise CommandError("sample update 需要样本池名")
    svc = _graph_service(data_dir)
    return [Result.json("sample", svc.sample_update(pos[0]))]


@handler("sample", "check")
def _sample_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample check 需要样本池名")
    svc = _graph_service(data_dir)
    return [Result.json("sample", svc.sample_check(pos[0]))]


@handler("sample", "del")
@handler("sample", "delete")
def _sample_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("sample delete 需要样本池名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("sample", svc.sample_delete(pos[0], force=bool(flags.get("force"))))]


# ---------------------------------------------------------------------------
# feature 同步处理器（graph 登记，纯定义；test 在 sample 视图上求值）
# ---------------------------------------------------------------------------

@handler("feature", "add")
def _feature_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature add 需要因子名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    ft = svc.feature_add(pos[0], flags.get("formula") or "",
                         engine=flags.get("engine") or "polars", **{
                             k: v for k, v in flags.items()
                             if k not in ("engine", "formula")})
    return [Result.json("feature", ft)]


@handler("feature", "set")
def _feature_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("feature set 需要因子名")
    if not flags:
        raise CommandError("feature set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("feature", svc.feature_set(pos[0], **flags))]


@handler("feature", "update")
def _feature_update(args: list[str], data_dir=None) -> list[Result]:
    """feature 更新：纯定义资产，标记有效（无物化）。"""
    pos = _positional(args)
    if not pos:
        raise CommandError("feature update 需要因子名")
    svc = _graph_service(data_dir)
    return [Result.json("feature", svc.feature_update(pos[0]))]


@handler("feature", "del")
@handler("feature", "delete")
def _feature_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature delete 需要因子名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("feature", svc.feature_delete(pos[0], force=bool(flags.get("force"))))]


@handler("feature", "meta")
def _feature_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature meta 需要因子名")
    svc = _graph_service(data_dir)
    return [Result.json("feature", svc.feature_meta(pos[0]))]


@handler("feature", "list")
@handler("feature", "")
def _feature_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("features", svc.feature_list())]


@handler("feature", "test")
def _feature_test(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("feature test 需要因子名")
    flags = parse_flags(args)
    if not flags.get("sample"):
        raise CommandError("feature test 需要 --sample <样本池名>")
    svc = _graph_service(data_dir)
    res, df = svc.feature_test(pos[0], flags["sample"])
    if df is not None and df.height:
        buf = io.BytesIO()
        df.write_ipc_stream(buf)
        return [Result.json("feature", res),
                Result.table(f"test/{pos[0]}", buf.getvalue())]
    return [Result.json("feature", res)]


# ---------------------------------------------------------------------------
# factor 同步处理器（graph 登记；get/check 实时计算，scan 物化落盘）
# ---------------------------------------------------------------------------

@handler("factor", "add")
def _factor_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor add 需要因子名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    fm = svc.factor_add(pos[0], flags.get("feature") or "",
                        flags.get("sample") or "",
                        engine=flags.get("engine") or "polars",
                        pipeline=flags.get("pipeline") or "nothing()",
                        factor_col=flags.get("factor_col"), **{
                            k: v for k, v in flags.items()
                            if k not in ("feature", "sample", "engine",
                                         "pipeline", "factor_col")})
    return [Result.json("factor", fm)]


@handler("factor", "get")
def _factor_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor get 需要因子名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.factor_get(
        pos[0],
        where=flags.get("where"),
        partition=flags.get("partition"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    fm = svc.factor_meta(pos[0])
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, fm.get("columns") or []))]


@handler("factor", "set")
def _factor_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("factor set 需要因子名")
    if not flags:
        raise CommandError("factor set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    return [Result.json("factor", svc.factor_set(pos[0], **flags))]


@handler("factor", "del")
@handler("factor", "delete")
def _factor_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor delete 需要因子名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("factor", svc.factor_delete(pos[0], force=bool(flags.get("force"))))]


@handler("factor", "meta")
def _factor_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor meta 需要因子名")
    svc = _graph_service(data_dir)
    return [Result.json("factor", svc.factor_meta(pos[0]))]


@handler("factor", "list")
@handler("factor", "")
def _factor_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("factors", svc.factor_list())]


@handler("factor", "check")
def _factor_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("factor check 需要因子名")
    svc = _graph_service(data_dir)
    return [Result.json("factor", svc.factor_check(pos[0]))]


@handler("factor", "update")
def _factor_update(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        reports = svc.factor_update(all=True, resync=bool(flags.get("resync")))
        return [Result.json("factors", reports)]
    if not pos:
        raise CommandError("factor update 需要因子名（或 --all）")
    report = svc.factor_update(pos[0], resync=bool(flags.get("resync")))
    return [Result.json("factor", report)]


# ---------------------------------------------------------------------------
# test 同步处理器（graph 登记；get/check 实时构造，scan 物化落盘）
# ---------------------------------------------------------------------------

@handler("test", "add")
def _test_add(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test add 需要测试集名")
    flags = parse_flags(args)
    if not flags.get("factor"):
        raise CommandError("test add 需要 --factor <因子名>")
    svc = _graph_service(data_dir)
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
    tm = svc.test_add(
        pos[0], flags["factor"],
        returns=flags.get("returns") or "r",
        groupby=flags.get("groupby") or "ic",
        marketcap=flags.get("marketcap") or "fv",
        factor_col=flags.get("factor_col"),
        spec=spec.to_dict(), **{k: v for k, v in flags.items()
                                if k not in ("factor", "returns", "groupby",
                                             "marketcap", "factor_col",
                                             "by_group", "quantiles", "periods",
                                             "date_range", "rolling_window")})
    return [Result.json("test", tm)]


@handler("test", "get")
def _test_get(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test get 需要测试集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    df, total = svc.test_get(
        pos[0],
        where=flags.get("where"),
        limit=int(flags["limit"]) if flags.get("limit") else None,
        offset=int(flags["offset"]) if flags.get("offset") else None,
        count_total=True,
    )
    tm = svc.test_meta(pos[0])
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return [Result.table(pos[0], buf.getvalue(),
                         meta=_arrow_meta(pos[0], df, total, tm.get("columns") or []))]


@handler("test", "meta")
def _test_meta(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test meta 需要测试集名")
    svc = _graph_service(data_dir)
    return [Result.json("test", svc.test_meta(pos[0]))]


@handler("test", "list")
@handler("test", "")
def _test_list(args: list[str], data_dir=None) -> list[Result]:
    svc = _graph_service(data_dir)
    return [Result.json("tests", svc.test_list())]


@handler("test", "set")
def _test_set(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    if not pos:
        raise CommandError("test set 需要测试集名")
    if not flags:
        raise CommandError("test set 需要至少一个 --key value")
    svc = _graph_service(data_dir)
    kw = dict(flags)
    if "spec" in kw and isinstance(kw["spec"], str):
        kw["spec"] = {"periods": [int(p) for p in kw["spec"].split(",")]}
    return [Result.json("test", svc.test_set(pos[0], **kw))]


@handler("test", "del")
@handler("test", "delete")
def _test_delete(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test delete 需要测试集名")
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    return [Result.json("test", svc.test_delete(pos[0], force=bool(flags.get("force"))))]


@handler("test", "check")
def _test_check(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    if not pos:
        raise CommandError("test check 需要测试集名")
    svc = _graph_service(data_dir)
    return [Result.json("test", svc.test_check(pos[0]))]


@handler("test", "update")
def _test_update(args: list[str], data_dir=None) -> list[Result]:
    pos = _positional(args)
    flags = parse_flags(args)
    svc = _graph_service(data_dir)
    if flags.get("all"):
        reports = svc.test_update(all=True, resync=bool(flags.get("resync")))
        return [Result.json("tests", reports)]
    if not pos:
        raise CommandError("test update 需要测试集名（或 --all）")
    report = svc.test_update(pos[0], resync=bool(flags.get("resync")))
    return [Result.json("test", report)]


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

# ---------------------------------------------------------------------------
# graph 血缘图处理器（V3.0 graphqlite 图数据，经 Execute 通道返回 JSON）
# ---------------------------------------------------------------------------

def _graph_store(data_dir):
    """按 data_dir 打开资产图库（§13：统一走 GraphService.open_graph_store——
    catalog.db/graph.db 命名回退只此一处；库不存在返回 None，命令返回空图）"""
    from ..graph.service import GraphService

    return GraphService.open_graph_store(data_dir)


@handler("graph", "lineage")
def _graph_lineage(args: list[str], data_dir=None) -> list[Result]:
    """graph lineage [--node <type:name>] [--depth N]：血缘图 Cytoscape elements payload。

    缺 --node 返回全图；带 --node 返回该节点上下游子图（--depth 限制深度）。
    """
    from ..graph.export import build_payload

    flags = parse_flags(args)
    node = flags.get("node")
    depth = int(flags["depth"]) if flags.get("depth") else None
    if depth is not None and depth < 1:
        raise CommandError("--depth 需为正整数")
    store = _graph_store(data_dir)
    if store is None:
        return [Result.json("graph", {
            "graph": {"exported_at": "", "center": node, "node_count": 0,
                      "edge_count": 0, "types": []},
            "elements": {"nodes": [], "edges": []},
        })]
    try:
        payload = build_payload(store, center=node, depth=depth, with_meta=True)
    finally:
        store.close()
    return [Result.json("graph", payload)]


@handler("graph", "nodes")
def _graph_nodes(args: list[str], data_dir=None) -> list[Result]:
    """graph nodes [--type <t>]：节点摘要列表（中心节点选择器用）。"""
    from ..graph.export import node_summaries

    flags = parse_flags(args)
    store = _graph_store(data_dir)
    if store is None:
        return [Result.json("graph", [])]
    try:
        data = node_summaries(store, flags.get("type"))
    finally:
        store.close()
    return [Result.json("graph", data)]


@handler("graph", "stats")
def _graph_stats(args: list[str], data_dir=None) -> list[Result]:
    """graph stats：图节点/边统计。"""
    store = _graph_store(data_dir)
    if store is None:
        return [Result.json("graph", {"node_count": 0, "edge_count": 0})]
    try:
        data = store.stats()
    finally:
        store.close()
    return [Result.json("graph", data)]
