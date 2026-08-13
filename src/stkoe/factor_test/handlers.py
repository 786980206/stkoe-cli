"""factor_test TaskHandler：把 FactorTestController 接进任务框架（source="test"）"""
from __future__ import annotations

import asyncio
import io

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
    from .controller import FactorTestController

    return FactorTestController(data_dir=ctx.data_dir)


def _spec_flags(flags: dict):
    from .spec import FactorTesterSpec

    return FactorTesterSpec(
        by_group=bool(flags.get("by_group")),
        quantiles=int(flags["quantiles"]) if flags.get("quantiles") else 5,
        periods=tuple(int(p) for p in (flags.get("periods") or "1,5,10").split(",")
                      if p.strip()),
        date_range=tuple(str(x) for x in
                         (flags.get("date_range") or "2023-01-01,2026-01-01").split(",")),
        rolling_window=int(flags["rolling_window"]) if flags.get("rolling_window") else 252,
    )


class TestAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test add 需要测试集名")
        flags = parse_flags(ctx.args)
        if not flags.get("factor"):
            raise ValueError("test add 需要 --factor <因子名>")
        ctl = _controller(ctx)
        spec = _spec_flags(flags)
        tm = await ctl.add(
            pos[0], factor=flags["factor"],
            returns=flags.get("returns") or "r",
            groupby=flags.get("groupby") or "ic",
            marketcap=flags.get("marketcap") or "fv",
            spec=spec, **{k: v for k, v in flags.items()
                          if k not in ("factor", "returns", "groupby", "marketcap",
                                       "by_group", "quantiles", "periods",
                                       "date_range", "rolling_window")})
        return TaskResult(data=dumps_str(tm.to_dict()))


class TestGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test get 需要测试集名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        df, total = await ctl.get(
            pos[0],
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


class TestMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test meta 需要测试集名")
        ctl = _controller(ctx)
        tm = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(tm.to_dict()))


class TestListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        tms = await ctl.list()
        return TaskResult(data=dumps_str([tm.to_dict() for tm in tms]))


class TestSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test set 需要测试集名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("test set 需要至少一个 --key value")
        ctl = _controller(ctx)
        kw = dict(flags)
        if "spec" in kw and isinstance(kw["spec"], str):
            kw["spec"] = {"periods": [int(p) for p in kw["spec"].split(",")]}
        tm = await ctl.set(pos[0], **kw)
        return TaskResult(data=dumps_str(tm.to_dict()))


class TestCheckHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test check 需要测试集名")
        ctl = _controller(ctx)
        res = await ctl.check(pos[0])
        return TaskResult(data=dumps_str(res.to_dict()))


class TestScanHandler(TaskHandler):
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
            raise ValueError("test scan 需要测试集名（或 --all）")
        report = await ctl.scan(pos[0], resync=bool(flags.get("resync")),
                                on_progress=on_progress)
        return TaskResult(data=dumps_str(report.to_dict()))


class TestDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("test delete 需要测试集名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("test", "add", TestAddHandler())
    registry.register("test", "get", TestGetHandler())
    registry.register("test", "meta", TestMetaHandler())
    registry.register("test", "list", TestListHandler())
    registry.register("test", "", TestListHandler())
    registry.register("test", "set", TestSetHandler())
    registry.register("test", "check", TestCheckHandler())
    registry.register("test", "scan", TestScanHandler())
    registry.register("test", "delete", TestDeleteHandler())
    registry.register("test", "del", TestDeleteHandler())