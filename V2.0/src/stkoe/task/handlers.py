"""内置 TaskHandler：version / config / mock（示例 Handler）

演示：progress + message + state 事件、task.log 日志、取消检查、ResultStore 引用。
"""
from __future__ import annotations

import asyncio

from ..args import parse_flags
from ..jsonutil import dumps_str
from .model import TaskCancelled, TaskResult
from .registry import TaskHandler


class VersionHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        from importlib.metadata import version as pkg_version

        try:
            ver = pkg_version("stkoe")
        except Exception:
            ver = "unknown"
        return TaskResult(data=dumps_str({"version": ver}))


class ConfigShowHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        from ..settings import config_path, load_config

        cfg = load_config()
        return TaskResult(data=dumps_str({
            "config_file": str(config_path()), **cfg.to_dict(),
        }))


class ConfigSetHandler(TaskHandler):
    async def run(self, ctx) -> TaskResult:
        from ..settings import save_config

        kv = parse_flags(ctx.args)
        if not kv:
            raise ValueError("config set 需要至少一个 --key value")
        path = save_config(kv)
        return TaskResult(data=dumps_str({"written": str(path), "set": kv}))


class MockProgressHandler(TaskHandler):
    """示例任务：分 5 步推进进度 + 写日志 + 落盘结果；支持取消与暂停"""

    async def run(self, ctx) -> TaskResult:
        steps = 5
        for i in range(1, steps + 1):
            if ctx.is_cancelled():
                raise TaskCancelled()
            await ctx.wait_if_paused()  # 暂停检查点
            await ctx.update(progress=i / steps, message=f"mock 步骤 {i}/{steps}")
            await ctx.log(f"步骤 {i}/{steps} 处理中")
            await asyncio.sleep(0.01)
        payload = dumps_str({"steps": steps}).encode("utf-8")
        ref = ctx.put_result("mock_result", payload)
        return TaskResult(data=dumps_str({"steps": steps}), result_ref=ref)


def register(registry) -> None:
    registry.register("version", "", VersionHandler())
    registry.register("config", "", ConfigShowHandler())
    registry.register("config", "set", ConfigSetHandler())
    registry.register("mock", "", MockProgressHandler())
