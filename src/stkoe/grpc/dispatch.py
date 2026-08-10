"""命令分发：把 Execute / SubmitTask 的 ``(source, action, args)`` 路由到对应处理器

协议约定请求为 ``stkoe <source> <action> <args...>`` 位置参数形态：
- source：table / dataset / stat / field / config / task / mock / version
- action：add / get / del / set / list / meta / ... 等子命令动词
- args：action 之后的位置参数

处理器通过 ``@handler(source, action)`` 装饰器注册，签名 ``fn(args) -> list[Result]``；
``Result`` 携带 name + kind（json/table），由 gRPC 层分别序列化为
``JsonData`` / ``ArrowTable``。后续数据层模块（table/dataset/...）逐步注册处理器即可。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..args import parse_flags
from ..jsonutil import dumps_str


class CommandError(Exception):
    """业务命令错误（非传输层错误）：写入 DataHeader.code（非 0）+ message"""

    def __init__(self, message: str, code: int = 2):
        super().__init__(message)
        self.message = message
        self.code = code


@dataclass(frozen=True)
class Result:
    """Execute 流中一条数据消息：kind=json → JsonData；kind=table → ArrowTable"""

    name: str
    kind: str = "json"
    data: str | bytes = ""
    meta: str = ""

    @classmethod
    def json(cls, name: str, obj) -> "Result":
        """JSON 结果（小数据：元数据/列表/状态）"""
        return cls(name=name, kind="json", data=dumps_str(obj))

    @classmethod
    def table(cls, name: str, ipc: bytes, meta: str = "") -> "Result":
        """表格结果（Arrow IPC 字节 + 元信息 JSON）"""
        return cls(name=name, kind="table", data=ipc, meta=meta)


_handlers: dict[tuple[str, str], Callable[[list[str]], list[Result]]] = {}


def handler(source: str, action: str):
    """注册 ``(source, action)`` 命令处理器；签名：``fn(args: list[str]) -> list[Result]``"""

    def deco(fn: Callable[[list[str]], list[Result]]):
        _handlers[(source, action)] = fn
        return fn

    return deco


def dispatch(source: str, action: str, args: list[str]) -> list[Result]:
    """路由命令；未注册的 (source, action) 抛 CommandError（DataHeader.code != 0）"""
    fn = _handlers.get((source, action))
    if fn is None:
        tail = " ".join(args)
        raise CommandError(f"不支持的命令: {source} {action}" + (f" {tail}" if tail else ""))
    return fn(list(args))


# ---------------------------------------------------------------------------
# 内置处理器（不依赖数据层的最小实现；数据模块后续逐步补充）
# ---------------------------------------------------------------------------

@handler("version", "")
@handler("version", "get")
def _version(args: list[str]) -> list[Result]:
    from importlib.metadata import version as pkg_version

    try:
        ver = pkg_version("stkoe")
    except Exception:
        ver = "unknown"
    return [Result.json("version", {"version": ver})]


@handler("config", "show")
@handler("config", "")
def _config_show(args: list[str]) -> list[Result]:
    from ..settings import config_path, load_config

    cfg = load_config()
    return [Result.json("config", {
        "config_file": str(config_path()),
        **cfg.to_dict(),
    })]


@handler("config", "set")
def _config_set(args: list[str]) -> list[Result]:
    from ..settings import save_config

    kv = parse_flags(args)
    if not kv:
        raise CommandError("config set 需要至少一个 --key value")
    path = save_config(kv)
    return [Result.json("config", {"written": str(path), "set": kv})]
