"""table 模块：原始表资产元数据管理与读取（登记/版本走 GraphService）

- 错误类型与常量见 ``table/errors.py``（GraphService / stat 复用）
- 物理工具（指纹/布局/footer/差异）见 ``table/util.py``、``table/query.py``
"""
from .errors import (
    DEFAULT_IGNORE_COLS,
    DependencyError,
    TableExistsError,
    TableNotFoundError,
)

__all__ = [
    "TableNotFoundError",
    "TableExistsError",
    "DependencyError",
    "DEFAULT_IGNORE_COLS",
]
