"""sample TaskHandler：把 SampleController 接进任务框架（source="sample"）

每个动作一个 Handler，解析位置参数 + ``--flag`` 后调用 SampleController 的
async 方法，结果以 JSON 返回。
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
    from .controller import SampleController

    return SampleController(data_dir=ctx.data_dir)


class SampleAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample add 需要样本池名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        sm = await ctl.add(pos[0], dataset=flags.get("dataset"),
                           engine=flags.get("engine") or "polars",
                           formula=flags.get("formula") or "", **{
                               k: v for k, v in flags.items()
                               if k not in ("dataset", "engine", "formula")})
        return TaskResult(data=dumps_str(sm.to_dict()))


class SampleGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample get 需要样本池名")
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
        ctl = _controller(ctx)
        sm = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(sm.to_dict()))


class SampleListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        sms = await ctl.list()
        return TaskResult(data=dumps_str([sm.to_dict() for sm in sms]))


class SampleSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample set 需要样本池名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("sample set 需要至少一个 --key value")
        ctl = _controller(ctx)
        sm = await ctl.set(pos[0], **flags)
        return TaskResult(data=dumps_str(sm.to_dict()))


class SampleCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample check 需要样本池名")
        ctl = _controller(ctx)
        res = await ctl.check(pos[0])
        return TaskResult(data=dumps_str(res.to_dict()))


class SampleDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("sample delete 需要样本池名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("sample", "add", SampleAddHandler())
    registry.register("sample", "get", SampleGetHandler())
    registry.register("sample", "meta", SampleMetaHandler())
    registry.register("sample", "list", SampleListHandler())
    registry.register("sample", "", SampleListHandler())
    registry.register("sample", "set", SampleSetHandler())
    registry.register("sample", "check", SampleCheckHandler())
    registry.register("sample", "delete", SampleDeleteHandler())
    registry.register("sample", "del", SampleDeleteHandler())