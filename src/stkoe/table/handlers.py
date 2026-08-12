"""table TaskHandler：把 TableController 接进任务框架（source="table"）

每个动作一个 Handler，解析位置参数 + ``--flag`` 后调用 TableController 的
async 方法，结果以 JSON（小结果）或 Arrow IPC（get 的表格数据）返回。
"""
from __future__ import annotations

import io

import polars as pl

from ..args import parse_flags
from ..jsonutil import dumps_str
from ..task.model import TaskResult
from ..task.registry import TaskHandler


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


def _controller(ctx):
    from .controller import TableController

    return TableController(data_dir=ctx.data_dir)


class TableAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        ctl = _controller(ctx)
        if flags.get("all"):
            reports = await ctl.add("", all=True)
            return TaskResult(data=dumps_str([r.to_dict() for r in reports]))
        if not pos:
            raise ValueError("table add 需要表名（或 --all）")
        report = await ctl.add(pos[0])
        return TaskResult(data=dumps_str(report.to_dict()))


class TableGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table get 需要表名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        columns = flags.get("columns") or None
        df, total = await ctl.get(
            pos[0],
            columns=columns.split(",") if columns else None,
            where=flags.get("where"),
            partition=flags.get("partition"),
            exclude_tool=bool(flags.get("exclude-tool")),
            limit=int(flags["limit"]) if flags.get("limit") else None,
            count_total=True,
        )
        buf = io.BytesIO()
        df.write_ipc_stream(buf)
        ref = ctx.put_result("data.arrow", buf.getvalue())
        return TaskResult(
            data=dumps_str({"name": pos[0], "rows": df.height, "total": total,
                            "columns": df.columns, "result_ref": ref}),
            result_ref=ref,
        )


class TableDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table delete 需要表名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class TableListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        if flags.get("candidate"):
            cands = await ctl.list(candidate=True)
            return TaskResult(data=dumps_str(cands))
        metas = await ctl.list()
        return TaskResult(data=dumps_str([m.to_dict() for m in metas]))


class TableMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table meta 需要表名")
        ctl = _controller(ctx)
        meta = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(meta.to_dict()))


class TableSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table set 需要表名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("table set 需要至少一个 --key value")
        ctl = _controller(ctx)
        meta = await ctl.set(pos[0], **flags)
        return TaskResult(data=dumps_str(meta.to_dict()))


class TableColHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 2:
            raise ValueError("table col 需要表名和列名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("table col 需要至少一个 --key value")
        ctl = _controller(ctx)
        meta = await ctl.col(pos[0], pos[1], **flags)
        return TaskResult(data=dumps_str(meta.to_dict()))


def register(registry) -> None:
    registry.register("table", "add", TableAddHandler())
    registry.register("table", "get", TableGetHandler())
    registry.register("table", "delete", TableDeleteHandler())
    registry.register("table", "del", TableDeleteHandler())
    registry.register("table", "list", TableListHandler())
    registry.register("table", "", TableListHandler())
    registry.register("table", "meta", TableMetaHandler())
    registry.register("table", "set", TableSetHandler())
    registry.register("table", "col", TableColHandler())
