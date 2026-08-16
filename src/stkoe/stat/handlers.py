"""stat TaskHandler：把 StatController 接进任务框架（source="stat"）"""
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
    from .controller import StatController

    return StatController(data_dir=ctx.data_dir)


def _target(ctx, *, tester_only: bool = False) -> tuple[str, str]:
    """解析 stat target；单位置参数 → test 目标（与 Execute 对齐）"""
    pos = _positional(ctx.args)
    if len(pos) >= 2:
        return pos[0], pos[1]
    if len(pos) == 1:
        kind = (parse_flags(ctx.args).get("kind") or "coverage")
        from ..factor_test.tester import TESTER_KINDS

        if not tester_only or kind in TESTER_KINDS:
            return "test", pos[0]
    raise ValueError("stat 命令需要 target 类型和名字（如 panel <name>，"
                     "或 test <name> --kind <tester>）")


def _write_ipc(df: pl.DataFrame, ctx, name: str) -> str:
    buf = io.BytesIO()
    if df.height:
        df.write_ipc_stream(buf)
    return ctx.put_result(name, buf.getvalue())


class StatScanHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        flags = parse_flags(ctx.args)
        kind = flags.get("kind") or "coverage"
        target_type, target_name = _target(ctx, tester_only=True)
        ctl = _controller(ctx)
        loop = asyncio.get_running_loop()
        report = await ctl.scan(target_type, target_name, kind=kind,
                                on_progress=worker_on_progress(ctx, loop))
        return TaskResult(data=dumps_str(report.to_dict()))


class StatGetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        target_type, target_name = _target(ctx)
        flags = parse_flags(ctx.args)
        kind = flags.get("kind") or "coverage"
        partition_by = flags.get("partition_by") or flags.get("partition-by")
        ctl = _controller(ctx)
        out = await ctl.get(target_type, target_name, kind=kind, partition_by=partition_by)
        if isinstance(out, pl.DataFrame):
            ref = _write_ipc(out, ctx, "stat.arrow")
            return TaskResult(
                data=dumps_str({"target": f"{target_type}:{target_name}", "kind": kind,
                                "partition": partition_by, "rows": out.height,
                                "columns": out.columns, "result_ref": ref}),
                result_ref=ref,
            )
        parts = []
        for partition, df in out.items():
            ref = _write_ipc(df, ctx, f"stat_{partition}.arrow")
            parts.append({"partition": partition, "rows": df.height,
                          "columns": df.columns, "result_ref": ref})
        return TaskResult(data=dumps_str({"target": f"{target_type}:{target_name}",
                                          "kind": kind, "partitions": parts}))


class StatMetaHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        target_type, target_name = _target(ctx)
        flags = parse_flags(ctx.args)
        kind = flags.get("kind") or "coverage"
        ctl = _controller(ctx)
        meta = await ctl.meta(target_type, target_name, kind=kind)
        return TaskResult(data=dumps_str(meta.to_dict()))


class StatListHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        ctl = _controller(ctx)
        metas = await ctl.list()
        return TaskResult(data=dumps_str([m.to_dict() for m in metas]))


class StatDeleteHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        target_type, target_name = _target(ctx)
        flags = parse_flags(ctx.args)
        kind = flags.get("kind")
        ctl = _controller(ctx)
        out = await ctl.delete(target_type, target_name, kind=kind)
        return TaskResult(data=dumps_str(out))


def register(registry) -> None:
    registry.register("stat", "scan", StatScanHandler())
    registry.register("stat", "get", StatGetHandler())
    registry.register("stat", "meta", StatMetaHandler())
    registry.register("stat", "list", StatListHandler())
    registry.register("stat", "delete", StatDeleteHandler())
    registry.register("stat", "del", StatDeleteHandler())