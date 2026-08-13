"""mock TaskHandler：把 mock 造数接进任务框架（source="mock"）

与 Execute（dispatch.py 的 ``@handler("mock", ...)``）行为对齐：
- ``demo``：生成 example.md 演示源表 index + m1 到 tables/
- ``gen``：参数化生成单张表（``--kind <kind> [--n-syms/--start/--end/--seed/--col]``）

``mock``（空 action）仍是内置示例任务（task/handlers.py 的 MockProgressHandler），
作为任务框架联调样例（进度/日志/取消/暂停）。
"""
from __future__ import annotations

import asyncio

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


class MockDemoHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        from .gen import demo

        flags = parse_flags(ctx.args)
        reports = await asyncio.to_thread(
            demo, ctx.data_dir,
            n_syms=int(flags["n-syms"]) if flags.get("n-syms") else 300,
            n_days=int(flags["n-days"]) if flags.get("n-days") else 500)
        return TaskResult(data=dumps_str(reports))


class MockGenHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        from .gen import gen as _gen

        flags = parse_flags(ctx.args)
        pos = _positional(ctx.args)
        if not pos:
            raise ValueError("mock gen 需要表名（如 mock gen mytable --kind index）")
        report = await asyncio.to_thread(
            _gen, pos[0], flags.get("kind") or "index",
            data_dir=ctx.data_dir,
            n_syms=int(flags["n-syms"]) if flags.get("n-syms") else 10,
            start=flags.get("start") or "2024-01-01",
            end=flags.get("end") or "2024-01-03",
            n_days=int(flags["n-days"]) if flags.get("n-days") else None,
            seed=int(flags["seed"]) if flags.get("seed") else None,
            col=flags.get("col"),
        )
        return TaskResult(data=dumps_str(report))


def register(registry) -> None:
    registry.register("mock", "demo", MockDemoHandler())
    registry.register("mock", "gen", MockGenHandler())
