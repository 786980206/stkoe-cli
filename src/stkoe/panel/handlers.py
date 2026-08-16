"""panel TaskHandler：把 GraphService 的 panel 资产接进任务框架（source="panel"）

行为与 Execute 的 ``e:panel ...`` 一致。
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


class PanelAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 3:
            raise ValueError("panel add 需要 panel 名、index 节点与至少一个成员表")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        # keys 由 index 推断（symbol_col + datetime_col），不再接受显式 --keys
        dm = await asyncio.to_thread(
            svc.panel_add, pos[0], pos[1], pos[2:], **{
                k: v for k, v in flags.items()
                if k not in ("keys", "materialize")})
        return TaskResult(data=dumps_str(dm))


class PanelGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("panel get 需要 panel 名")
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


class PanelMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("panel meta 需要 panel 名")
        svc = _service(ctx)
        dm = await asyncio.to_thread(svc.panel_meta, pos[0])
        return TaskResult(data=dumps_str(dm))


class PanelListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        dms = await asyncio.to_thread(svc.panel_list)
        return TaskResult(data=dumps_str(dms))


class PanelSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("panel set 需要 panel 名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("panel set 需要至少一个 --key value")
        svc = _service(ctx)
        dm = await asyncio.to_thread(svc.panel_set, pos[0], **flags)
        return TaskResult(data=dumps_str(dm))


class PanelUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("panel update 需要 panel 名")
        svc = _service(ctx)
        out = await asyncio.to_thread(svc.panel_update, pos[0])
        return TaskResult(data=dumps_str(out))


class PanelDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("panel delete 需要 panel 名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.panel_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("panel", "add", PanelAddHandler())
    registry.register("panel", "get", PanelGetHandler())
    registry.register("panel", "meta", PanelMetaHandler())
    registry.register("panel", "list", PanelListHandler())
    registry.register("panel", "", PanelListHandler())
    registry.register("panel", "set", PanelSetHandler())
    registry.register("panel", "update", PanelUpdateHandler())
    registry.register("panel", "delete", PanelDeleteHandler())
    registry.register("panel", "del", PanelDeleteHandler())
