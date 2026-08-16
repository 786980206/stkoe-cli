"""sample TaskHandler：把 GraphService 的 sample 资产接进任务框架（source="sample"）"""
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


class SampleAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 3:
            raise ValueError("sample add 需要 <样本池名> <fieldset 名> <index 名>")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        sm = await asyncio.to_thread(
            svc.sample_add, pos[0], pos[1], pos[2], **{
                k: v for k, v in flags.items()
                if k not in ("fieldset", "index")})
        return TaskResult(data=dumps_str(sm))


class SampleGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample get 需要样本池名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        columns = flags.get("columns") or None
        df, total = await asyncio.to_thread(
            svc.sample_get, pos[0],
            columns=columns.split(",") if columns else None,
            where=flags.get("where"),
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


class SampleMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample meta 需要样本池名")
        svc = _service(ctx)
        sm = await asyncio.to_thread(svc.sample_meta, pos[0])
        return TaskResult(data=dumps_str(sm))


class SampleListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        sms = await asyncio.to_thread(svc.sample_list)
        return TaskResult(data=dumps_str(sms))


class SampleSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample set 需要样本池名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("sample set 需要至少一个 --key value")
        svc = _service(ctx)
        sm = await asyncio.to_thread(svc.sample_set, pos[0], **flags)
        return TaskResult(data=dumps_str(sm))


class SampleCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample check 需要样本池名")
        svc = _service(ctx)
        res = await asyncio.to_thread(svc.sample_check, pos[0])
        return TaskResult(data=dumps_str(res))


class SampleUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample update 需要样本池名")
        svc = _service(ctx)
        out = await asyncio.to_thread(svc.sample_update, pos[0])
        return TaskResult(data=dumps_str(out))


class SampleDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample delete 需要样本池名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.sample_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("sample", "add", SampleAddHandler())
    registry.register("sample", "get", SampleGetHandler())
    registry.register("sample", "meta", SampleMetaHandler())
    registry.register("sample", "list", SampleListHandler())
    registry.register("sample", "", SampleListHandler())
    registry.register("sample", "set", SampleSetHandler())
    registry.register("sample", "update", SampleUpdateHandler())
    registry.register("sample", "check", SampleCheckHandler())
    registry.register("sample", "delete", SampleDeleteHandler())
    registry.register("sample", "del", SampleDeleteHandler())
