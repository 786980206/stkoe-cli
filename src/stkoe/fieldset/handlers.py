"""fieldset TaskHandler：把 GraphService 的 fieldset 资产接进任务框架（source="fieldset"）"""
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


class FieldsetAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset add 需要指标集名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if len(pos) == 1:
            if not flags.get("dataset"):
                raise ValueError("fieldset add 需要 --dataset <panel 名>")
            fm = await asyncio.to_thread(
                svc.fieldset_add, pos[0], flags["dataset"],
                engine=flags.get("engine") or "polars", **{
                    k: v for k, v in flags.items()
                    if k not in ("dataset", "engine")})
            return TaskResult(data=dumps_str(fm))
        fm = await asyncio.to_thread(
            svc.fieldset_add_field, pos[0], pos[1],
            flags.get("formula") or "", **{
                k: v for k, v in flags.items() if k != "formula"})
        return TaskResult(data=dumps_str(fm))


class FieldsetGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset get 需要指标集名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        columns = flags.get("columns") or None
        df, total = await asyncio.to_thread(
            svc.fieldset_get, pos[0],
            columns=columns.split(",") if columns else None,
            where=flags.get("where"),
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
        svc = _service(ctx)
        if len(pos) == 1:
            fm = await asyncio.to_thread(svc.fieldset_meta, pos[0])
            return TaskResult(data=dumps_str(fm))
        field = await asyncio.to_thread(svc.fieldset_meta_field, pos[0], pos[1])
        return TaskResult(data=dumps_str(field))


class FieldsetListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        fms = await asyncio.to_thread(svc.fieldset_list)
        return TaskResult(data=dumps_str(fms))


class FieldsetSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset set 需要指标集名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("fieldset set 需要至少一个 --key value")
        svc = _service(ctx)
        if len(pos) == 1:
            fm = await asyncio.to_thread(svc.fieldset_set, pos[0], **flags)
            return TaskResult(data=dumps_str(fm))
        fm = await asyncio.to_thread(svc.fieldset_set_field, pos[0], pos[1], **flags)
        return TaskResult(data=dumps_str(fm))


class FieldsetCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset check 需要指标集名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if flags.get("all") or len(pos) < 2:
            fm = await asyncio.to_thread(svc.fieldset_meta, pos[0])
            results = []
            for f in (fm.get("fields") or {}):
                r = await asyncio.to_thread(svc.fieldset_check, pos[0], f)
                results.append(r)
            return TaskResult(data=dumps_str(results))
        r = await asyncio.to_thread(svc.fieldset_check, pos[0], pos[1])
        return TaskResult(data=dumps_str([r]))


class FieldsetScanHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if flags.get("all"):
            metas = await asyncio.to_thread(svc.fieldset_list)
            reports = []
            for m in metas:
                r = await asyncio.to_thread(svc.fieldset_scan, m["name"])
                reports.append(r)
            return TaskResult(data=dumps_str(reports))
        if not pos:
            raise ValueError("fieldset scan 需要指标集名（或 --all）")
        report = await asyncio.to_thread(svc.fieldset_scan, pos[0])
        return TaskResult(data=dumps_str(report))


class FieldsetDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset delete 需要指标集名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        if len(pos) > 1:
            fm = await asyncio.to_thread(svc.fieldset_delete_field, pos[0], pos[1])
            return TaskResult(data=dumps_str(fm))
        out = await asyncio.to_thread(
            svc.fieldset_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class FieldsetTestHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("fieldset test 需要指标集名")
        flags = parse_flags(ctx.args)
        if not flags.get("formula"):
            raise ValueError("fieldset test 需要 --formula <表达式>")
        svc = _service(ctx)
        res, df = await asyncio.to_thread(svc.fieldset_test, pos[0], flags["formula"])
        data = dict(res)
        if df is not None and df.height:
            buf = io.BytesIO()
            df.write_ipc_stream(buf)
            data["result_ref"] = ctx.put_result("test.arrow", buf.getvalue())
        return TaskResult(data=dumps_str(data))


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
