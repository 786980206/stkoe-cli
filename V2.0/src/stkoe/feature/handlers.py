"""feature TaskHandler：把 FeatureController 接进任务框架（source="feature"）

每个动作一个 Handler，解析位置参数 + ``--flag`` 后调用 FeatureController 的
async 方法，结果以 JSON 返回。
"""
from __future__ import annotations

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


def _controller(ctx):
    from .controller import FeatureController

    return FeatureController(data_dir=ctx.data_dir)


class FeatureAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature add 需要因子名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        ft = await ctl.add(pos[0], engine=flags.get("engine") or "polars",
                           formula=flags.get("formula") or "", **{
                               k: v for k, v in flags.items()
                               if k not in ("engine", "formula")})
        return TaskResult(data=dumps_str(ft.to_dict()))


class FeatureSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature set 需要因子名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("feature set 需要至少一个 --key value")
        ctl = _controller(ctx)
        ft = await ctl.set(pos[0], **flags)
        return TaskResult(data=dumps_str(ft.to_dict()))


class FeatureDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature delete 需要因子名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


class FeatureMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature meta 需要因子名")
        ctl = _controller(ctx)
        ft = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(ft.to_dict()))


class FeatureListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        fts = await ctl.list()
        return TaskResult(data=dumps_str([ft.to_dict() for ft in fts]))


class FeatureTestHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("feature test 需要因子名")
        flags = parse_flags(ctx.args)
        sample = flags.get("sample")
        if not sample:
            raise ValueError("feature test 需要 --sample <样本池名>")
        ctl = _controller(ctx)
        res, df = await ctl.test(pos[0], sample)
        buf = io.BytesIO()
        if df is not None and df.height:
            df.write_ipc_stream(buf)
        data = res.to_dict()
        if df is not None:
            data["result_ref"] = ctx.put_result("test.arrow", buf.getvalue())
        return TaskResult(data=dumps_str(data))


def register(registry) -> None:
    registry.register("feature", "add", FeatureAddHandler())
    registry.register("feature", "set", FeatureSetHandler())
    registry.register("feature", "meta", FeatureMetaHandler())
    registry.register("feature", "list", FeatureListHandler())
    registry.register("feature", "", FeatureListHandler())
    registry.register("feature", "test", FeatureTestHandler())
    registry.register("feature", "delete", FeatureDeleteHandler())
    registry.register("feature", "del", FeatureDeleteHandler())