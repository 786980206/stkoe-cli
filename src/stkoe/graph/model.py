"""图节点/边/事件数据模型（对应 V3.0 初始设计的节点定义）。

节点在 graphqlite 中以 ``label = 资产类型``、``id = "<type>:<name>"`` 存储；
通用属性 + 类型专属属性平铺在节点属性上（``AssetMeta.data`` 承载类型专属部分）。
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields as _fields
from typing import Any

# 资产类型（与 GraphHandler.list 的 asset_type 对齐）
ASSET_TYPES: tuple[str, ...] = (
    "table", "index", "panel", "fieldset", "sample",
    "feature", "factor", "tester", "model", "stat",
)

# 类型标签 → 用户类型名
LABEL_TO_TYPE = {t.capitalize(): t for t in ASSET_TYPES}
TYPE_TO_LABEL = {t: t.capitalize() for t in ASSET_TYPES}


def node_id(asset_type: str, name: str) -> str:
    """图节点唯一 id：``<type>:<name>``。"""
    return f"{asset_type}:{name}"


def split_node_id(nid: str) -> tuple[str, str]:
    """把节点 id 拆回 (type, name)。"""
    t, _, n = nid.partition(":")
    return t, n


def column_node_id(asset_id: str, column: str) -> str:
    """列节点唯一 id：``column:<资产 id>.<列名>``（如 ``column:table:m1.price``）。

    ``column:`` 前缀保证与资产节点 id（``<type>:<name>``）永不冲突。
    """
    return f"column:{asset_id}.{column}"


def split_column_node_id(cid: str) -> tuple[str, str]:
    """把列节点 id 拆回 (资产 id, 列名)。"""
    rest = cid[len("column:"):]
    asset, _, col = rest.rpartition(".")
    return asset, col


@dataclass(frozen=True)
class DataChangeEvent:
    """一次物理数据变化：对「时间范围 × 标的 × 字段」的一批 upsert / delete。

    scope 为 None 表示作用于全集（所有字段 / 所有标的 / 所有时间）。
    """

    action: str = "upsert"  # upsert | delete
    field_scope: list[str] | None = None
    symbol_scope: list[str] | None = None
    datetime_scope: list[Any] | None = None

    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "field_scope": self.field_scope,
            "symbol_scope": self.symbol_scope,
            "datetime_scope": self.datetime_scope,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DataChangeEvent":
        return cls(
            action=d.get("action", "upsert"),
            field_scope=d.get("field_scope"),
            symbol_scope=d.get("symbol_scope"),
            datetime_scope=d.get("datetime_scope"),
        )

    def is_empty(self) -> bool:
        """没有任何 scope 约束 = 全量事件（最重）。"""
        return (
            self.field_scope is None
            and self.symbol_scope is None
            and self.datetime_scope is None
        )


@dataclass(frozen=True)
class ColumnMeta:
    """列元数据（原始表字段 / fieldset 衍生字段 / 因子字段共用）。"""

    name: str
    display_name: str = ""
    description: str = ""
    data_type: str | None = None
    unit: str | None = None
    formula: str | None = None
    tags: tuple[str, ...] = ()
    as_index: bool = False
    is_tool: bool = False
    source_table: str | None = None
    source_field: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "data_type": self.data_type,
            "unit": self.unit,
            "formula": self.formula,
            "tags": list(self.tags),
            "as_index": self.as_index,
            "is_tool": self.is_tool,
            "source_table": self.source_table,
            "source_field": self.source_field,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnMeta":
        names = {f.name for f in _fields(cls)}
        kw = {k: v for k, v in d.items() if k in names}
        if "tags" in kw and not isinstance(kw["tags"], tuple):
            kw["tags"] = tuple(kw["tags"] or ())
        return cls(**kw)

    def patch(self, **kw) -> "ColumnMeta":
        """返回浅拷贝修改版（frozen dataclass 更新元数据字段用）。"""
        return ColumnMeta.from_dict({**self.to_dict(), **kw})


@dataclass(frozen=True)
class FieldMeta:
    """fieldset 衍生指标 / feature 命名公式元数据。"""

    name: str
    formula: str = ""
    display_name: str = ""
    description: str = ""
    unit: str | None = None
    tags: tuple[str, ...] = ()
    validated: bool = False
    engine: str = "polars"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "formula": self.formula,
            "display_name": self.display_name,
            "description": self.description,
            "unit": self.unit,
            "tags": list(self.tags),
            "validated": self.validated,
            "engine": self.engine,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FieldMeta":
        names = {f.name for f in _fields(cls)}
        kw = {k: v for k, v in d.items() if k in names}
        if "tags" in kw and not isinstance(kw["tags"], tuple):
            kw["tags"] = tuple(kw["tags"] or ())
        return cls(**kw)

    def patch(self, **kw) -> "FieldMeta":
        return FieldMeta.from_dict({**self.to_dict(), **kw})


# 通用节点属性键（其余键归入 data / extra）
_COMMON_KEYS = frozenset({
    "name", "display_name", "description", "tags", "source",
    "version", "version_list", "materialized", "valid",
    "create_time", "update_time", "extra",
})


@dataclass(frozen=True)
class AssetMeta:
    """资产节点通用元数据（图节点的 Python 视图）。"""

    type: str  # 资产类型（label 小写）
    name: str
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    source: str = "local"
    version: int = 0  # 当前版本（高精度时间戳，见 version.py）
    version_list: dict = field(default_factory=dict)  # {version: DataChangeEvent dict}
    materialized: bool = False
    valid: bool = False
    create_time: str = ""
    update_time: str = ""
    extra: dict = field(default_factory=dict)
    data: dict = field(default_factory=dict)  # 类型专属属性

    @property
    def node_id(self) -> str:
        return node_id(self.type, self.name)

    def to_dict(self) -> dict:
        """输出扁平 dict（API 形态）：通用键 + 类型专属 data 合并。"""
        return {
            "type": self.type,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "source": self.source,
            "version": self.version,
            "version_list": self.version_list,
            "materialized": self.materialized,
            "valid": self.valid,
            "create_time": self.create_time,
            "update_time": self.update_time,
            "extra": self.extra,
            **self.data,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AssetMeta":
        return cls(
            type=d.get("type", ""),
            name=d.get("name", ""),
            display_name=d.get("display_name", ""),
            description=d.get("description", ""),
            tags=tuple(d.get("tags") or ()),
            source=d.get("source", "local"),
            version=int(d.get("version", 0)),
            version_list=dict(d.get("version_list") or {}),
            materialized=bool(d.get("materialized", False)),
            valid=bool(d.get("valid", False)),
            create_time=d.get("create_time", ""),
            update_time=d.get("update_time", ""),
            extra=dict(d.get("extra") or {}),
            data={k: v for k, v in d.items() if k not in _COMMON_KEYS},
        )


@dataclass(frozen=True)
class DependencyEdge:
    """依赖边（依赖方 → 被依赖方）。

    ``source`` = 依赖方节点 id；``target`` = 被依赖方节点 id；
    ``required_version`` = 依赖方已消费的被依赖方版本（消费水位线）。
    """

    source: str  # 依赖方 node_id
    target: str  # 被依赖方 node_id
    required_version: int = 0
    detail: dict = field(default_factory=dict)
    create_time: str = ""

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "required_version": self.required_version,
            "detail": self.detail,
            "create_time": self.create_time,
        }


# 初始设计的 AssetNode 即通用资产节点视图（本实现以 AssetMeta 承载）
AssetNode = AssetMeta


__all__ = [
    "ASSET_TYPES", "LABEL_TO_TYPE", "TYPE_TO_LABEL",
    "node_id", "split_node_id",
    "column_node_id", "split_column_node_id",
    "DataChangeEvent", "ColumnMeta", "FieldMeta",
    "AssetMeta", "AssetNode", "DependencyEdge",
]
