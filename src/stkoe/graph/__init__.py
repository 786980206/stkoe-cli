"""graph：V3.0 资产血缘图（graphqlite 存储）。

图模型：节点（label = 资产类型，id = "<type>:<name>"）+ DEPENDS 边
（依赖方 → 被依赖方，带 required_version 消费水位）。详见仓库根 README.md §2 图设计。
"""
from .controller import DEFINITION_KEYS, GraphController, NullStorage
from .errors import (
    AssetExistsError,
    AssetNotFoundError,
    CycleError,
    DependencyError,
    EdgeNotFoundError,
    GraphError,
    StorageNotWiredError,
)
from .events import accumulate, event_from_kwargs, merge_events
from .handlers import (
    HANDLERS,
    FactorHandler,
    FeatureHandler,
    FieldsetHandler,
    GraphHandler,
    IndexHandler,
    ModelHandler,
    PanelHandler,
    SampleHandler,
    StatHandler,
    TableHandler,
    TesterHandler,
)
from .model import (
    ASSET_TYPES,
    AssetMeta,
    ColumnMeta,
    DataChangeEvent,
    DependencyEdge,
    FieldMeta,
    node_id,
)
from .store import GraphStore

__all__ = [
    "GraphStore", "GraphController", "NullStorage", "DEFINITION_KEYS",
    "AssetMeta", "AssetNode", "ColumnMeta", "FieldMeta", "DataChangeEvent",
    "DependencyEdge", "ASSET_TYPES", "node_id",
    "merge_events", "accumulate", "event_from_kwargs",
    "GraphError", "AssetNotFoundError", "AssetExistsError",
    "DependencyError", "EdgeNotFoundError", "CycleError", "StorageNotWiredError",
    "TableHandler", "IndexHandler", "PanelHandler", "FieldsetHandler",
    "SampleHandler", "FeatureHandler", "FactorHandler", "TesterHandler",
    "ModelHandler", "StatHandler", "GraphHandler", "HANDLERS",
]
