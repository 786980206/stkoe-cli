"""orjson 统一 JSON 通道（标准 JSON，与 serde_json 兼容）"""
import orjson


def dumps(obj) -> str:
    return orjson.dumps(obj).decode("utf-8")


def loads(s: str):
    return orjson.loads(s)
