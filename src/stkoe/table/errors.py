"""table 模块共享错误类型与表约定常量（GraphService / stat 复用）。

原 V2.0 ``TableController``（SQLite catalog 登记层）已废弃，业务统一走
``graph/service.py`` 的 GraphService；本文件只保留仍被当前代码引用的
错误类型与常量。
"""
from __future__ import annotations

#: 工具字段名（处理数据时忽略，标记为 is_tool）
DEFAULT_IGNORE_COLS = ("optime",)


class TableNotFoundError(FileNotFoundError):
    pass


class TableExistsError(ValueError):
    pass


class DependencyError(ValueError):
    """删除/重命名被依赖方时存在下游引用；``dependents`` 为结构化依赖列表"""

    def __init__(self, dependents: list, action: str = "delete"):
        self.dependents = [dict(d) for d in dependents]
        self.action = action
        msg = "dependencies exist: " + ", ".join(
            f"{d['obj_type']}:{d['obj_name']}" for d in self.dependents)
        super().__init__(msg + f" (use --force to {action})")


__all__ = ["DEFAULT_IGNORE_COLS", "TableNotFoundError", "TableExistsError",
           "DependencyError"]
