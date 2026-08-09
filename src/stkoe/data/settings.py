"""stkoe 配置：数据根目录 + 工具字段（处理数据时忽略的列）

配置文件：`stkoe.json`（JSON，便于用户直接编辑）。查找顺序：
1. 环境变量 `STKOE_CONFIG` 指定的文件
2. 当前目录 `./stkoe.json`
3. 用户目录 `~/.stkoe.json`

数据根目录优先级：`STKOE_LOCAL_DATA` 环境变量 > 配置文件 `data_path` > 默认值。
`stkoe config` 命令查看/修改。
"""
import json
import os
from dataclasses import dataclass
from pathlib import Path

CONFIG_ENV = "STKOE_CONFIG"
CONFIG_NAME = "stkoe.json"
DEFAULT_IGNORE_COLS = ("optime",)
DEFAULT_GRPC_HOST = "127.0.0.1"
DEFAULT_GRPC_PORT = 9569


@dataclass(frozen=True)
class StkoeConfig:
    """框架配置

    - ``data_path``：默认数据根目录（未显式 configure / 未设环境变量时使用）
    - ``ignore_cols``：工具字段（如 ``optime``），处理数据时忽略；支持多个
    - ``grpc_host``：gRPC 服务绑定地址（REPL 启动时同步监听；缺省 127.0.0.1）
    - ``grpc_port``：gRPC 服务端口（REPL 启动时同步监听；缺省 9569）
    """

    data_path: str | None = None
    ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS
    grpc_host: str = DEFAULT_GRPC_HOST
    grpc_port: int = DEFAULT_GRPC_PORT

    def to_dict(self) -> dict:
        d: dict = {"data_path": self.data_path}
        if self.ignore_cols != DEFAULT_IGNORE_COLS:
            d["ignore_cols"] = list(self.ignore_cols)
        if self.grpc_host != DEFAULT_GRPC_HOST:
            d["grpc_host"] = self.grpc_host
        if self.grpc_port != DEFAULT_GRPC_PORT:
            d["grpc_port"] = self.grpc_port
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "StkoeConfig":
        return cls(
            data_path=d.get("data_path"),
            ignore_cols=tuple(d.get("ignore_cols") or DEFAULT_IGNORE_COLS),
            grpc_host=d.get("grpc_host") or DEFAULT_GRPC_HOST,
            grpc_port=int(d.get("grpc_port") or DEFAULT_GRPC_PORT),
        )


def _candidates() -> list[Path]:
    paths: list[Path] = []
    env = os.getenv(CONFIG_ENV)
    if env:
        paths.append(Path(env))
    paths.append(Path.cwd() / CONFIG_NAME)
    paths.append(Path.home() / f".{CONFIG_NAME}")
    return paths


def config_path() -> Path:
    """当前生效的配置文件路径

    读取：STKOE_CONFIG > ./stkoe.json > ~/.stkoe.json（首个存在者）。
    写入：STKOE_CONFIG 指定则用之；否则首个已存在者；否则默认 ./stkoe.json。
    """
    env = os.getenv(CONFIG_ENV)
    if env:
        return Path(env)
    for p in _candidates():
        if p.is_file():
            return p
    return Path.cwd() / CONFIG_NAME


def load_config() -> StkoeConfig:
    p = config_path()
    if p.is_file():
        try:
            return StkoeConfig.from_dict(json.loads(p.read_text("utf-8")))
        except (ValueError, OSError):
            pass
    return StkoeConfig()


def ignore_cols() -> tuple[str, ...]:
    """当前生效的工具字段（处理数据时忽略，支持多个）"""
    return load_config().ignore_cols


def save_config(cfg: StkoeConfig, path: Path | None = None) -> Path:
    p = path or config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg.to_dict(), ensure_ascii=False, indent=2), "utf-8")
    return p


def resolve_data_path() -> Path:
    """数据根目录：STKOE_LOCAL_DATA 环境变量 > 配置 data_path > 默认 ~/.stkoe"""
    env = os.getenv("STKOE_LOCAL_DATA")
    if env:
        return Path(env)
    cfg = load_config()
    if cfg.data_path:
        return Path(cfg.data_path)
    return Path.home() / ".stkoe"
