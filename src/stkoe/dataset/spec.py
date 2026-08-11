"""dataset 公共数据类型：DatasetMeta / DatasetScanReport"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..table.spec import ColumnMeta


@dataclass(frozen=True)
class DatasetMeta:
    """dataset 逻辑数据集元数据（``dataset meta`` 输出）"""

    name: str
    version: int
    index_table: str
    tables: tuple[str, ...]
    keys: tuple[str, ...]
    columns: tuple[ColumnMeta, ...]
    partition_by: tuple[str, ...] = ()
    partition_gran: str = ""  # ''（flat）/year/month/date/identity（镜像 index）
    materialized: bool = False
    materialized_at: str | None = None
    curated: bool = False            # 物化数据与当前源一致（读前自物化用）
    pending_partitions: tuple[str, ...] = ()
    validation: dict | None = None
    extra: dict = field(default_factory=dict)
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "index_table": self.index_table,
            "tables": list(self.tables),
            "keys": list(self.keys),
            "columns": [c.to_dict() for c in self.columns],
            "partition_by": list(self.partition_by),
            "partition_gran": self.partition_gran,
            "materialized": self.materialized,
            "materialized_at": self.materialized_at,
            "curated": self.curated,
            "pending_partitions": list(self.pending_partitions),
            "validation": self.validation,
            "extra": self.extra,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class DatasetScanReport:
    """``dataset scan``（重物化）结果"""

    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    incremental: bool
    partition_by: tuple[str, ...]
    rebuilt_partitions: tuple[str, ...] = ()
    triggered: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "materialized": self.materialized,
            "changed": self.changed,
            "incremental": self.incremental,
            "partition_by": list(self.partition_by),
            "rebuilt_partitions": list(self.rebuilt_partitions),
            "triggered": list(self.triggered),
        }


__all__ = ["DatasetMeta", "DatasetScanReport"]