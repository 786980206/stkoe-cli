"""index TaskHandler：把 GraphService 的 index 资产接进任务框架（source="index"）

行为与 Execute 的 ``e:index ...`` 一致：add 支持 ``--symbol-col/--datetime-col/
--materialize-partition`` 与 ``--all`` 批量发现；get 返回 ArrowTable（IPC）。
"""
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


class IndexAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        svc = _service(ctx)
        meta = {k: v for k, v in flags.items()
                if k not in ("all", "symbol-col", "datetime-col", "materialize-partition")}
        if flags.get("all"):
            reports = await asyncio.to_thread(
                svc.index_add, "", all=True,
                symbol_col=flags.get("symbol-col") or "sym",
                datetime_col=flags.get("datetime-col") or "date",
                materialize_partition=flags.get("materialize-partition") or "yearly",
                meta=meta or None)
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("index add 需要 index 名（或 --all）")
        report = await asyncio.to_thread(
            svc.index_add, pos[0],
            symbol_col=flags.get("symbol-col") or "sym",
            datetime_col=flags.get("datetime-col") or "date",
            materialize_partition=flags.get("materialize-partition") or "yearly",
            meta=meta or None)
        return TaskResult(data=dumps_str(report))


class IndexGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("index get 需要 index 名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        df, total = await asyncio.to_thread(
            svc.index_get, pos[0],
            columns=flags.get("columns").split(",") if flags.get("columns") else None,
            where=flags.get("where"),
            partition=flags.get("partition"),
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


class IndexDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("index delete 需要 index 名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.index_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class IndexUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        svc = _service(ctx)
        if flags.get("all"):
            reports = await asyncio.to_thread(svc.index_update, "", all=True)
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("index update 需要 index 名（或 --all）")
        report = await asyncio.to_thread(svc.index_update, pos[0])
        return TaskResult(data=dumps_str(report))


class IndexListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if flags.get("candidate"):
            cands = await asyncio.to_thread(svc.index_list, candidate=True)
            return TaskResult(data=dumps_str(cands))
        metas = await asyncio.to_thread(svc.index_list)
        return TaskResult(data=dumps_str(metas))


class IndexMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("index meta 需要 index 名")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.index_meta, pos[0])
        return TaskResult(data=dumps_str(meta))


class IndexSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        flags = parse_flags(ctx.args)
        if not pos:
            raise ValueError("index set 需要 index 名")
        if not flags:
            raise ValueError("index set 需要至少一个 --key value")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.index_set, pos[0], **flags)
        return TaskResult(data=dumps_str(meta))


class IndexColHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 2:
            raise ValueError("index col 需要 index 名和列名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("index col 需要至少一个 --key value")
        svc = _service(ctx)
        meta = await asyncio.to_thread(svc.index_col, pos[0], pos[1], **flags)
        return TaskResult(data=dumps_str(meta))


def register(registry) -> None:
    registry.register("index", "add", IndexAddHandler())
    registry.register("index", "get", IndexGetHandler())
    registry.register("index", "update", IndexUpdateHandler())
    registry.register("index", "delete", IndexDeleteHandler())
    registry.register("index", "del", IndexDeleteHandler())
    registry.register("index", "list", IndexListHandler())
    registry.register("index", "", IndexListHandler())
    registry.register("index", "meta", IndexMetaHandler())
    registry.register("index", "set", IndexSetHandler())
    registry.register("index", "col", IndexColHandler())
