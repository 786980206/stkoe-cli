"""factor TaskHandler：把 FactorController 接进任务框架（source="factor"）"""
from __future__ import annotations

import asyncio
import io

import polars as pl

from ..args import parse_flags
from ..jsonutil import dumps_str
from ..task.progress import worker_on_progress
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
    from .controller import FactorController

    return FactorController(data_dir=ctx.data_dir)


class FactorAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor add 需要因子名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        fm = await ctl.add(pos[0], **flags)
        return TaskResult(data=dumps_str(fm.to_dict()))


class FactorGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor get 需要因子名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        df, total = await ctl.get(
            pos[0],
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
        ctl = _controller(ctx)
        fm = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(fm.to_dict()))


class FactorListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        fms = await ctl.list()
        return TaskResult(data=dumps_str([fm.to_dict() for fm in fms]))


class FactorSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor set 需要因子名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("factor set 需要至少一个 --key value")
        ctl = _controller(ctx)
        fm = await ctl.set(pos[0], **flags)
        return TaskResult(data=dumps_str(fm.to_dict()))


class FactorCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor check 需要因子名")
        ctl = _controller(ctx)
        res = await ctl.check(pos[0])
        return TaskResult(data=dumps_str(res.to_dict()))


class FactorScanHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        loop = asyncio.get_running_loop()
        on_progress = worker_on_progress(ctx, loop)

        if flags.get("all"):
            reports = await ctl.scan(all=True, resync=bool(flags.get("resync")),
                                     on_progress=on_progress)
            return TaskResult(data=dumps_str([r.to_dict() for r in reports]))
        if not pos:
            raise ValueError("factor scan 需要因子名（或 --all）")
        report = await ctl.scan(pos[0], resync=bool(flags.get("resync")),
                                on_progress=on_progress)
        return TaskResult(data=dumps_str(report.to_dict()))


class FactorDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("factor delete 需要因子名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("factor", "add", FactorAddHandler())
    registry.register("factor", "get", FactorGetHandler())
    registry.register("factor", "meta", FactorMetaHandler())
    registry.register("factor", "list", FactorListHandler())
    registry.register("factor", "", FactorListHandler())
    registry.register("factor", "set", FactorSetHandler())
    registry.register("factor", "check", FactorCheckHandler())
    registry.register("factor", "scan", FactorScanHandler())
    registry.register("factor", "delete", FactorDeleteHandler())
    registry.register("factor", "del", FactorDeleteHandler())