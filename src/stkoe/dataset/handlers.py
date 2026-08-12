"""dataset TaskHandler：把 DatasetController 接进任务框架（source="dataset"）

每个动作一个 Handler，解析位置参数 + ``--flag`` 后调用 DatasetController 的
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
    from .controller import DatasetController

    return DatasetController(data_dir=ctx.data_dir)


class DatasetAddHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if len(pos) < 3:
            raise ValueError("dataset add 需要 dataset 名、index 表与至少一个成员表")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        keys = None
        if flags.get("keys"):
            keys = [k.strip() for k in flags["keys"].split(",") if k.strip()]
        dm = await ctl.add(pos[0], pos[1], *pos[2:], keys=keys,
                           materialize=bool(flags.get("materialize")))
        return TaskResult(data=dumps_str(dm.to_dict()))


class DatasetGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset get 需要 dataset 名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        columns = flags.get("columns") or None
        df, total = await ctl.get(
            pos[0],
            columns=columns.split(",") if columns else None,
            where=flags.get("where"),
            partition=flags.get("partition"),
            limit=int(flags["limit"]) if flags.get("limit") else None,
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
        ctl = _controller(ctx)
        dm = await ctl.meta(pos[0])
        return TaskResult(data=dumps_str(dm.to_dict()))


class DatasetListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        dms = await ctl.list()
        return TaskResult(data=dumps_str([dm.to_dict() for dm in dms]))


class DatasetSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset set 需要 dataset 名")
        flags = parse_flags(ctx.args)
        if not flags:
            raise ValueError("dataset set 需要至少一个 --key value")
        ctl = _controller(ctx)
        dm = await ctl.set(pos[0], **flags)
        return TaskResult(data=dumps_str(dm.to_dict()))


class DatasetScanHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        if flags.get("all"):
            reports = await ctl.scan(all=True, resync=bool(flags.get("resync")))
            return TaskResult(data=dumps_str([r.to_dict() for r in reports]))
        if not pos:
            raise ValueError("dataset scan 需要 dataset 名（或 --all）")
        report = await ctl.scan(pos[0], resync=bool(flags.get("resync")))
        return TaskResult(data=dumps_str(report.to_dict()))


class DatasetDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("dataset delete 需要 dataset 名")
        flags = parse_flags(ctx.args)
        ctl = _controller(ctx)
        out = await ctl.delete(pos[0], force=bool(flags.get("force")))
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("dataset", "add", DatasetAddHandler())
    registry.register("dataset", "get", DatasetGetHandler())
    registry.register("dataset", "meta", DatasetMetaHandler())
    registry.register("dataset", "list", DatasetListHandler())
    registry.register("dataset", "", DatasetListHandler())
    registry.register("dataset", "set", DatasetSetHandler())
    registry.register("dataset", "scan", DatasetScanHandler())
    registry.register("dataset", "delete", DatasetDeleteHandler())
    registry.register("dataset", "del", DatasetDeleteHandler())