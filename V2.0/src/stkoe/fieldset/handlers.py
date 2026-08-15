"""fieldset TaskHandler：把 FieldsetController 接进任务框架（source="fieldset"）"""
from __future__ import annotations

import asyncio
import io

import polars as pl

from ..args import parse_flags
from ..jsonutil import dumps_str
from ..task.model import TaskResult
from ..task.progress import worker_on_progress
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
    from .controller import FieldsetController

    return FieldsetController(data_dir=ctx.data_dir)


class FieldsetAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset add 需要指标集名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        if len(pos) == 1:
            fm = await ctl.add(pos[0], dataset=flags.get("dataset"),
                               engine=flags.get("engine") or "polars", **{
                                   k: v for k, v in flags.items()
                                   if k not in ("dataset", "engine")})
            return TaskResult(data=dumps_str(fm.to_dict()))
        field = pos[1]
        fm = await ctl.add_field(pos[0], field, **{
            k: v for k, v in flags.items()})
        return TaskResult(data=dumps_str(fm.to_dict()))


class FieldsetGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset get 需要指标集名")
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
            fields_only=bool(flags.get("fields-only")),
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


class FieldsetMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset meta 需要指标集名")
        ctl = _controller(ctx)
        if len(pos) == 1:
            fm = await ctl.meta(pos[0])
            return TaskResult(data=dumps_str(fm.to_dict()))
        field = await ctl.field_meta(pos[0], pos[1])
        return TaskResult(data=dumps_str(field.to_dict()))


class FieldsetListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        fms = await ctl.list()
        return TaskResult(data=dumps_str([fm.to_dict() for fm in fms]))


class FieldsetSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset set 需要指标集名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("fieldset set 需要至少一个 --key value")
        ctl = _controller(ctx)
        if len(pos) == 1:
            fm = await ctl.set(pos[0], **flags)
            return TaskResult(data=dumps_str(fm.to_dict()))
        fm = await ctl.set_field(pos[0], pos[1], **flags)
        return TaskResult(data=dumps_str(fm.to_dict()))


class FieldsetCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset check 需要指标集名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        loop = asyncio.get_running_loop()
        field = pos[1] if len(pos) > 1 else None
        results = await ctl.check(pos[0], field, all_fields=bool(flags.get("all")),
                                  on_progress=worker_on_progress(ctx, loop))
        return TaskResult(data=dumps_str([r.to_dict() for r in results]))


class FieldsetScanHandler(TaskHandler):
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
            raise ValueError("fieldset scan 需要指标集名（或 --all）")
        report = await ctl.scan(pos[0], resync=bool(flags.get("resync")),
                                on_progress=on_progress)
        return TaskResult(data=dumps_str(report.to_dict()))


class FieldsetDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset delete 需要指标集名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        if len(pos) > 1:
            fm = await ctl.delete_field(pos[0], pos[1])
            return TaskResult(data=dumps_str(fm.to_dict()))
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class FieldsetTestHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset test 需要指标集名")
        flags = parse_flags(ctx.args)
        if not flags.get("formula"):
            raise ValueError("fieldset test 需要 --formula <表达式>")
        ctl = _controller(ctx)
        df = await ctl.test(pos[0], flags["formula"])
        buf = io.BytesIO()
        if df.height:
            df.write_ipc_stream(buf)
        ref = ctx.put_result("test.arrow", buf.getvalue())
        return TaskResult(
            data=dumps_str({"name": pos[0], "ok": True, "rows": df.height,
                            "columns": df.columns, "result_ref": ref}),
            result_ref=ref,
        )


def register(registry) -> None:
    registry.register("fieldset", "add", FieldsetAddHandler())
    registry.register("fieldset", "get", FieldsetGetHandler())
    registry.register("fieldset", "meta", FieldsetMetaHandler())
    registry.register("fieldset", "list", FieldsetListHandler())
    registry.register("fieldset", "", FieldsetListHandler())
    registry.register("fieldset", "set", FieldsetSetHandler())
    registry.register("fieldset", "scan", FieldsetScanHandler())
    registry.register("fieldset", "check", FieldsetCheckHandler())
    registry.register("fieldset", "test", FieldsetTestHandler())
    registry.register("fieldset", "delete", FieldsetDeleteHandler())
    registry.register("fieldset", "del", FieldsetDeleteHandler())