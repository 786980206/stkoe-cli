"""fieldset 公共数据类型：FieldMeta / FieldsetMeta / FieldsetScanReport / FieldsetCheckResult"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..table.spec import ColumnMeta


@dataclass(frozen=True)
class FieldMeta:
    """一个衍生指标（字段）：基于源 dataset 列 + 公式计算，check 通过后 validated=True"""

    name: str
    formula: str = ""
    display_name: str = ""
    description: str = ""
    unit: str | None = None
    tags: tuple[str, ...] = ()
    validated: bool = False  # 是否通过 check（看公式与源数据可逐行计算）

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "formula": self.formula,
            "display_name": self.display_name,
            "description": self.description,
            "unit": self.unit,
            "tags": list(self.tags),
            "validated": self.validated,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FieldMeta":
        kw = dict(d)
        kw["tags"] = tuple(d.get("tags") or ())
        return cls(**kw)


@dataclass(frozen=True)
class FieldsetMeta:
    """衍生指标集元数据（``fieldset meta`` 输出）：基于一个源 dataset + 若干衍生指标"""

    name: str
    version: int
    dataset: str                      # 源 dataset（数据来源）
    engine: str = "polars"            # 公式计算引擎（插件注册名）
    keys: tuple[str, ...] = ()        # 继承源 dataset 主键（物化输出保留）
    fields: tuple[FieldMeta, ...] = ()
    partition_by: tuple[str, ...] = ()
    partition_gran: str = ""          # ''（flat）/year/month/date/identity
    materialized: bool = False
    materialized_at: str | None = None
    curated: bool = False             # 物化数据与当前源+当前字段公式一致
    columns: tuple[ColumnMeta, ...] = ()
    extra: dict = field(default_factory=dict)
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    source: str = "local"
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "dataset": self.dataset,
            "engine": self.engine,
            "keys": list(self.keys),
            "fields": [f.to_dict() for f in self.fields],
            "partition_by": list(self.partition_by),
            "partition_gran": self.partition_gran,
            "materialized": self.materialized,
            "materialized_at": self.materialized_at,
            "curated": self.curated,
            "columns": [c.to_dict() for c in self.columns],
            "extra": self.extra,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "source": self.source,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class FieldsetScanReport:
    """``fieldset scan``（物化衍生指标集）结果"""

    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    fields_count: int          # 本次物化的已校验指标数
    partition_by: tuple[str, ...] = ()
    rebuilt_partitions: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "materialized": self.materialized,
            "changed": self.changed,
            "fields_count": self.fields_count,
            "partition_by": list(self.partition_by),
            "rebuilt_partitions": list(self.rebuilt_partitions),
        }


@dataclass(frozen=True)
class FieldsetCheckResult:
    """单个指标 check（验证）结果"""

    fieldset: str
    field: str
    ok: bool
    message: str = ""

    def to_dict(self) -> dict:
        return {"fieldset": self.fieldset, "field": self.field,
                "ok": self.ok, "message": self.message}


__all__ = ["FieldMeta", "FieldsetMeta", "FieldsetScanReport", "FieldsetCheckResult"]