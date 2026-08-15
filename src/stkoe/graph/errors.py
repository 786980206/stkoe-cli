"""图资产异常类型。"""


class GraphError(Exception):
    """图操作通用错误基类。"""


class AssetNotFoundError(GraphError, FileNotFoundError):
    """资产节点不存在。"""


class AssetExistsError(GraphError, ValueError):
    """资产节点已存在。"""


class EdgeNotFoundError(GraphError, FileNotFoundError):
    """依赖边不存在。"""


class DependencyError(GraphError, ValueError):
    """存在下游依赖，禁止删除（force 可绕过）。"""


class CycleError(GraphError, ValueError):
    """血缘图出现环（应为 DAG）。"""


class StorageNotWiredError(GraphError, NotImplementedError):
    """物理数据存储钩子未接入（本阶段仅图账本）。"""


__all__ = [
    "GraphError",
    "AssetNotFoundError",
    "AssetExistsError",
    "EdgeNotFoundError",
    "DependencyError",
    "CycleError",
    "StorageNotWiredError",
]
