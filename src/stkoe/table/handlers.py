"""table TaskHandler：把 GraphService 的 table 资产接进任务框架（source="table"）"""
from __future__ import annotations

import asyncio
import io

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


def _service(ctx):
    from ..graph.service import GraphService

    return GraphService(data_dir=ctx.data_dir)


class TableAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        svc = _service(ctx)
        if flags.get("all"):
            reports = await asyncio.to_thread(svc.table_add, "", all=True)
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("table add 需要表名（或 --all）")
        meta = {k: v for k, v in flags.items() if k != "all"}
        report = await asyncio.to_thread(svc.table_add, pos[0], meta=meta or None)
        return TaskResult(data=dumps_str(report))


class TableGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table get 需要表名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        columns = flags.get("columns") or None
        df, total = await asyncio.to_thread(
            svc.table_get, pos[0],
            columns=columns.split(",") if columns else None,
            where=flags.get("where"),
            partition=flags.get("partition"),
            exclude_tool=bool(flags.get("exclude-tool")),
            limit=int(flags["limit"]) if flags.get("limit") else None,
            offset=int(flags["offset"]) if flags.get("offset") else None,
            count_total=True,
        )
        buf = io.BytesIO()
        if df.height:
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
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.table_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class TableUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        svc = _service(ctx)
        if flags.get("all"):
            reports = await asyncio.to_thread(svc.table_update, "", all=True)
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("table update 需要表名（或 --all）")
        report = await asyncio.to_thread(svc.table_update, pos[0])
        return TaskResult(data=dumps_str(report))


class TableListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if flags.get("candidate"):
            cands = await asyncio.to_thread(svc.table_list, candidate=True)
            return TaskResult(data=dumps_str(cands))
        metas = await asyncio.to_thread(svc.table_list)
        return TaskResult(data=dumps_str(metas))


class TableMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table meta 需要表名")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.table_meta, pos[0])
        return TaskResult(data=dumps_str(meta))


class TableSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("table set 需要表名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("table set 需要至少一个 --key value")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.table_set, pos[0], **flags)
        return TaskResult(data=dumps_str(meta))


class TableColHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 2:
            raise ValueError("table col 需要表名和列名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("table col 需要至少一个 --key value")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.table_col, pos[0], pos[1], **flags)
        return TaskResult(data=dumps_str(meta))


def register(registry) -> None:
    registry.register("table", "add", TableAddHandler())
    registry.register("table", "get", TableGetHandler())
    registry.register("table", "update", TableUpdateHandler())
    registry.register("table", "delete", TableDeleteHandler())
    registry.register("table", "del", TableDeleteHandler())
    registry.register("table", "list", TableListHandler())
    registry.register("table", "", TableListHandler())
    registry.register("table", "meta", TableMetaHandler())
    registry.register("table", "set", TableSetHandler())
    registry.register("table", "col", TableColHandler())
