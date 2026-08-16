"""factor TaskHandler：把 GraphService 的 factor 资产接进任务框架（source="factor"）"""
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


class FactorAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor add 需要因子名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        fm = await asyncio.to_thread(
            svc.factor_add, pos[0], flags.get("feature") or "",
            flags.get("sample") or "",
            engine=flags.get("engine") or "polars",
            pipeline=flags.get("pipeline") or "nothing()",
            factor_col=flags.get("factor_col"), **{
                k: v for k, v in flags.items()
                if k not in ("feature", "sample", "engine",
                             "pipeline", "factor_col")})
        return TaskResult(data=dumps_str(fm))


class FactorGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor get 需要因子名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        df, total = await asyncio.to_thread(
            svc.factor_get, pos[0],
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


class FactorMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor meta 需要因子名")
        svc = _service(ctx)
        fm = await asyncio.to_thread(svc.factor_meta, pos[0])
        return TaskResult(data=dumps_str(fm))


class FactorListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        fms = await asyncio.to_thread(svc.factor_list)
        return TaskResult(data=dumps_str(fms))


class FactorSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor set 需要因子名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("factor set 需要至少一个 --key value")
        svc = _service(ctx)
        fm = await asyncio.to_thread(svc.factor_set, pos[0], **flags)
        return TaskResult(data=dumps_str(fm))


class FactorCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor check 需要因子名")
        svc = _service(ctx)
        res = await asyncio.to_thread(svc.factor_check, pos[0])
        return TaskResult(data=dumps_str(res))


class FactorUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if flags.get("all"):
            reports = await asyncio.to_thread(
                svc.factor_update, all=True, resync=bool(flags.get("resync")))
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("factor update 需要因子名（或 --all）")
        report = await asyncio.to_thread(
            svc.factor_update, pos[0], resync=bool(flags.get("resync")))
        return TaskResult(data=dumps_str(report))


class FactorDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor delete 需要因子名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.factor_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("factor", "add", FactorAddHandler())
    registry.register("factor", "get", FactorGetHandler())
    registry.register("factor", "meta", FactorMetaHandler())
    registry.register("factor", "list", FactorListHandler())
    registry.register("factor", "", FactorListHandler())
    registry.register("factor", "set", FactorSetHandler())
    registry.register("factor", "check", FactorCheckHandler())
    registry.register("factor", "update", FactorUpdateHandler())
    registry.register("factor", "delete", FactorDeleteHandler())
    registry.register("factor", "del", FactorDeleteHandler())
