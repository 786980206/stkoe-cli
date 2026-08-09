"""stkoe.data：数据管理框架（table → dataset → field 统一入口）"""
from pathlib import Path

from loguru import logger

from .catalog.db import Catalog
from .settings import (
    StkoeConfig,
    config_path,
    load_config,
    resolve_data_path,
    save_config,
)

STKOE_LOCAL_DATA: Path = resolve_data_path()

_catalog: Catalog | None = None


def configure(root: str | Path):
    """切换数据根目录（测试/部署用）；重设后旧 catalog 失效"""
    global STKOE_LOCAL_DATA, _catalog
    if _catalog is not None:
        _catalog.close()
        _catalog = None
    STKOE_LOCAL_DATA = Path(root)


def get_root() -> Path:
    return STKOE_LOCAL_DATA


def config() -> StkoeConfig:
    """当前生效配置（读取配置文件）"""
    return load_config()


def set_config(data_path: str | None = None, ignore_cols=None) -> StkoeConfig:
    """写入配置并落盘 stkoe.json；返回新配置"""
    cur = load_config()
    cfg = StkoeConfig(
        data_path=data_path if data_path is not None else cur.data_path,
        ignore_cols=tuple(ignore_cols) if ignore_cols is not None else cur.ignore_cols,
    )
    save_config(cfg)
    return cfg


def ignore_cols() -> tuple[str, ...]:
    """工具字段（处理数据时忽略，如 optime；支持多个）"""
    return load_config().ignore_cols


def data_cols(columns) -> list[str]:
    """从列清单（列名或 ColumnMeta）中剔除工具字段，返回业务列名"""
    names = [getattr(c, "name", c) for c in columns]
    tool = set(ignore_cols())
    return [c for c in names if c not in tool]


def catalog() -> Catalog:
    """全局 SQLite 目录（惰性初始化）"""
    global _catalog
    if _catalog is None:
        _catalog = Catalog(STKOE_LOCAL_DATA / "catalog.db")
    return _catalog


def init():
    """初始化本地数据处理目录"""
    for sub in ("tables", "datasets", "stats", "fields"):
        (STKOE_LOCAL_DATA / sub).mkdir(parents=True, exist_ok=True)
    catalog()


from . import table  # noqa: E402
from .table import (  # noqa: E402
    add as table_add,
    col,
    data_key,
    del_ as table_del,
    get,
    get_lazy,
    list as table_list,
    meta,
    rename,
    scan,
    scan_all,
    set as table_set,
    field_graph,
)
from .task import (  # noqa: E402
    defer,
    is_default_async,
    run_task,
    set_default_async,
    task_clean,
    task_list,
    task_log,
    task_meta,
    task_pause,
    task_resume,
    task_stop,
    task_stop_all,
)
from . import dataset  # noqa: E402
from . import stat  # noqa: E402
from . import field  # noqa: E402

# 旧版轻量别名（只读观察者时代：sniff = 扫描同步；select = 读）
sniff = scan
sniff_all = scan_all

__all__ = [
    "STKOE_LOCAL_DATA",
    "configure",
    "get_root",
    "config",
    "set_config",
    "config_path",
    "ignore_cols",
    "data_cols",
    "catalog",
    "init",
    "logger",
    "table_add",
    "col",
    "data_key",
    "table_del",
    "get",
    "get_lazy",
    "table_list",
    "meta",
    "rename",
    "scan",
    "scan_all",
    "table_set",
    "field_graph",
    "defer",
    "is_default_async",
    "run_task",
    "set_default_async",
    "task_list",
    "task_log",
    "task_meta",
    "task_pause",
    "task_resume",
    "task_stop",
    "task_stop_all",
    "task_clean",
    "dataset",
    "stat",
    "field",
    "sniff",
    "sniff_all",
]
