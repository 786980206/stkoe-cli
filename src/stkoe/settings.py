"""stkoe.json 配置：查找 / 加载 / 保存

约定：
- 查找优先级：``STKOE_CONFIG`` 环境变量 > ``./stkoe.json``（若存在）> ``~/.stkoe.json``
- 写入位置：``STKOE_CONFIG``（若设置）> ``./stkoe.json``
- 文件内键名保持用户输入形态（含连字符，如 ``grpc-host``），不做任何转换
- 内存对象为 ``StkoeConfig`` dataclass：已知键映射为类型化字段，任意自定义键进 ``extra``
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from .jsonutil import dumps_str, loads

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9569
DEFAULT_DATA_DIR = "~/.stkoe"


class ConfigError(RuntimeError):
    pass


@dataclass
class StkoeConfig:
    """stkoe.json 配置的类型化视图

    已知键映射到字段（``grpc-host`` → ``grpc_host``、``grpc-port`` → ``grpc_port``、
    ``data-dir`` → ``data_dir``），其余任意自定义键保留在 ``extra``（原样）。
    """

    grpc_host: str = DEFAULT_HOST
    grpc_port: int = DEFAULT_PORT
    data_dir: str = DEFAULT_DATA_DIR
    extra: dict[str, object] = field(default_factory=dict)

    #: JSON 键名（其余任意键进 extra）
    _KNOWN = ("grpc-host", "grpc-port", "data-dir")

    def to_dict(self) -> dict:
        """输出用字典：键名回到连字符形态 + 任意自定义键"""
        out = {
            "grpc-host": self.grpc_host,
            "grpc-port": self.grpc_port,
            "data-dir": self.data_dir,
        }
        out.update(self.extra)
        return out


def config_path() -> Path:
    """读取用配置路径（查找优先级：env > 本地 > home）"""
    env = os.environ.get("STKOE_CONFIG")
    if env:
        return Path(env)
    local = Path.cwd() / "stkoe.json"
    if local.exists():
        return local
    return Path.home() / ".stkoe.json"


def save_path() -> Path:
    """写入用配置路径（env > 本地 stkoe.json）"""
    env = os.environ.get("STKOE_CONFIG")
    if env:
        return Path(env)
    return Path.cwd() / "stkoe.json"


def _read_dict(path: Path) -> dict:
    """读配置文件为原始字典（不存在返回 {}；解析/格式错误抛 ConfigError）"""
    if not path.exists():
        return {}
    try:
        data = loads(path.read_bytes())
    except (OSError, ValueError) as e:
        raise ConfigError(f"配置文件解析失败 {path}: {e}") from e
    if not isinstance(data, dict):
        raise ConfigError(f"配置文件格式错误（应为 JSON 对象）: {path}")
    return data


def load_config() -> StkoeConfig:
    """加载生效配置：默认值 + 文件覆盖，映射为 StkoeConfig dataclass"""
    cfg = StkoeConfig()
    data = _read_dict(config_path())
    if "grpc-host" in data:
        cfg.grpc_host = str(data["grpc-host"])
    if "grpc-port" in data:
        cfg.grpc_port = int(data["grpc-port"])
    if "data-dir" in data:
        cfg.data_dir = str(data["data-dir"])
    cfg.extra = {k: v for k, v in data.items() if k not in cfg._KNOWN}
    return cfg


def save_config(kv: dict) -> Path:
    """合并写入配置项（保留文件中未涉及的键）；返回写入路径"""
    p = save_path()
    existing = _read_dict(p)
    existing.update(kv)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes((dumps_str(existing, indent=True) + "\n").encode("utf-8"))
    return p
