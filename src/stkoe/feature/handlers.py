"""feature TaskHandler：把 GraphService 的 feature 资产接进任务框架（source="feature"）"""
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


class FeatureAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature add 需要因子名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        ft = await asyncio.to_thread(
            svc.feature_add, pos[0], flags.get("formula") or "",
            engine=flags.get("engine") or "polars", **{
                k: v for k, v in flags.items()
                if k not in ("engine", "formula")})
        return TaskResult(data=dumps_str(ft))


class FeatureSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature set 需要因子名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("feature set 需要至少一个 --key value")
        svc = _service(ctx)
        ft = await asyncio.to_thread(svc.feature_set, pos[0], **flags)
        return TaskResult(data=dumps_str(ft))


class FeatureDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature delete 需要因子名")
        flags = parse_flags(ctx.args)
        svc = _service(ctx)
        out = await asyncio.to_thread(
            svc.feature_delete, pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class FeatureMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature meta 需要因子名")
        svc = _service(ctx)
        ft = await asyncio.to_thread(svc.feature_meta, pos[0])
        return TaskResult(data=dumps_str(ft))


class FeatureListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        svc = _service(ctx)
        fts = await asyncio.to_thread(svc.feature_list)
        return TaskResult(data=dumps_str(fts))


class FeatureTestHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature test 需要因子名")
        flags = parse_flags(ctx.args)
        sample = flags.get("sample")
        if not sample:
            raise ValueError("feature test 需要 --sample <样本池名>")
        svc = _service(ctx)
        res, df = await asyncio.to_thread(svc.feature_test, pos[0], sample)
        buf = io.BytesIO()
        if df is not None and df.height:
            df.write_ipc_stream(buf)
        data = dict(res)
        if df is not None:
            data["result_ref"] = ctx.put_result("test.arrow", buf.getvalue())
        return TaskResult(data=dumps_str(data))


class FeatureUpdateHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature update 需要因子名")
        svc = _service(ctx)
        out = await asyncio.to_thread(svc.feature_update, pos[0])
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("feature", "add", FeatureAddHandler())
    registry.register("feature", "set", FeatureSetHandler())
    registry.register("feature", "meta", FeatureMetaHandler())
    registry.register("feature", "list", FeatureListHandler())
    registry.register("feature", "", FeatureListHandler())
    registry.register("feature", "update", FeatureUpdateHandler())
    registry.register("feature", "test", FeatureTestHandler())
    registry.register("feature", "delete", FeatureDeleteHandler())
    registry.register("feature", "del", FeatureDeleteHandler())
