"""dataset TaskHandler：转发到 GraphService 的 panel 资产（source="dataset" 旧别名）

V3.0 起 dataset 概念改名为 panel；本模块保持旧 source 可用（行为与 Execute 的
``e:dataset ...`` 一致，返回 name 用 "panel"）。
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


class DatasetAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 3:
            raise ValueError("dataset add 需要 dataset 名、index 节点与至少一个成员表")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        keys = None
        if flags.get("keys"):
            keys = [k.strip() for k in flags["keys"].split(",") if k.strip()]
        dm = await asyncio.to_thread(svc.panel_add, pos[0], pos[1], pos[2:],
                                     keys=keys, **{
                                         k: v for k, v in flags.items()
                                         if k not in ("keys", "materialize")})
        return TaskResult(data=dumps_str(dm))


class DatasetGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset get 需要 dataset 名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        columns = flags.get("columns") or None
        df, total = await asyncio.to_thread(
            svc.panel_get, pos[0],
            columns=columns.split(",") if columns else None,
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


class DatasetMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset meta 需要 dataset 名")
        svc = _service(ctx)
        dm = await asyncio.to_thread(svc.panel_meta, pos[0])
        return TaskResult(data=dumps_str(dm))


class DatasetListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        dms = await asyncio.to_thread(svc.panel_list)
        return TaskResult(data=dumps_str(dms))


class DatasetSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset set 需要 dataset 名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("dataset set 需要至少一个 --key value")
        svc = _service(ctx)
        dm = await asyncio.to_thread(svc.panel_set, pos[0], **flags)
        return TaskResult(data=dumps_str(dm))


class DatasetDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset delete 需要 dataset 名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.panel_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("dataset", "add", DatasetAddHandler())
    registry.register("dataset", "get", DatasetGetHandler())
    registry.register("dataset", "meta", DatasetMetaHandler())
    registry.register("dataset", "list", DatasetListHandler())
    registry.register("dataset", "", DatasetListHandler())
    registry.register("dataset", "set", DatasetSetHandler())
    registry.register("dataset", "delete", DatasetDeleteHandler())
    registry.register("dataset", "del", DatasetDeleteHandler())
