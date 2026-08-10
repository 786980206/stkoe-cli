"""JSON 工具：统一 orjson 序列化

选项：
- OPT_SERIALIZE_DATACLASS：dataclass 实例直接序列化
- OPT_SERIALIZE_NUMPY：numpy 标量 / 数组（数据层结果）
- OPT_NON_STR_KEYS：非字符串键

orjson 原生序列化 datetime/date/time 为 ISO 字符串。
"""
from __future__ import annotations

import orjson

_OPTIONS = (
    orjson.OPT_SERIALIZE_DATACLASS
    | orjson.OPT_SERIALIZE_NUMPY
    | orjson.OPT_NON_STR_KEYS
)


def dumps(obj, *, indent: bool = False) -> bytes:
    """orjson.dumps → bytes；indent=True 时缩进 2（配置文件等）"""
    opts = _OPTIONS | (orjson.OPT_INDENT_2 if indent else 0)
    return orjson.dumps(obj, option=opts)


def dumps_str(obj, *, indent: bool = False) -> str:
    """orjson.dumps → UTF-8 字符串"""
    return dumps(obj, indent=indent).decode("utf-8")


def loads(data: bytes | str | memoryview) -> object:
    """orjson.loads：bytes / str 均可"""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return orjson.loads(data)
