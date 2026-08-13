"""factor 公共数据类型：FactorMeta / FactorScanReport / FactorCheckResult"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field

from ..table.spec import ColumnMeta


@dataclass(frozen=True)
class FieldMeta:
    """因子列说明：最终因子的因子列元数据（列名 = factor_col）"""

    name: str
    formula: str = ""
    display_name: str = ""
    description: str = ""
    unit: str | None = None
    tags: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "formula": self.formula,
            "display_name": self.display_name,
            "description": self.description,
            "unit": self.unit,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FieldMeta":
        kw = dict(d)
        kw["tags"] = tuple(d.get("tags") or ())
        return cls(**kw)


@dataclass(frozen=True)
class FactorMeta:
    """最终因子元数据（``factor meta`` 输出）

    factor = 在 ``sample``（样本池视图）上经 ``feature``（命名公式）算出因子列，
    再经 ``pipeline``（算子链）变换后的产物；结构恒为「样本索引列 + 一列因子列」。
    """

    name: str
    version: int
    feature: str                   # 因子列公式定义（feature 名）
    sample: str                    # 源样本池（数据来源）
    pipeline: str = ""             # 算子链（如 nothing()|standardlize()|...）
    engine: str = "polars"         # 公式/算子引擎（插件注册名，当前仅 polars）
    factor_col: str = ""           # 输出因子列名（默认 = feature 名）
    keys: tuple[str, ...] = ()     # 样本索引列（画样 sample 主键）
    partition_by: tuple[str, ...] = ()
    partition_gran: str = ""       # ''（flat）/year/month/date/identity（镜像 dataset）
    materialized: bool = False
    materialized_at: str | None = None
    curated: bool = False          # 物化数据与当前 feature 公式+pipeline+源数据一致
    columns: tuple[ColumnMeta, ...] = ()  # 源 sample 视图列（dataset_with_fieldset）
    field: FieldMeta | None = None        # 因子列说明
    extra: dict = dc_field(default_factory=dict)
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
            "feature": self.feature,
            "sample": self.sample,
            "pipeline": self.pipeline,
            "engine": self.engine,
            "factor_col": self.factor_col,
            "keys": list(self.keys),
            "partition_by": list(self.partition_by),
            "partition_gran": self.partition_gran,
            "materialized": self.materialized,
            "materialized_at": self.materialized_at,
            "curated": self.curated,
            "columns": [c.to_dict() for c in self.columns],
            "field": self.field.to_dict() if self.field is not None else None,
            "extra": self.extra,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "source": self.source,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class FactorScanReport:
    """``factor scan``（物化最终因子）结果"""

    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    partition_by: tuple[str, ...] = ()
    rebuilt_partitions: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "materialized": self.materialized,
            "changed": self.changed,
            "partition_by": list(self.partition_by),
            "rebuilt_partitions": list(self.rebuilt_partitions),
        }


@dataclass(frozen=True)
class FactorCheckResult:
    """``factor check``（因子有效性校验）结果：计算成功、含全部索引列、因子列单列"""

    factor: str
    ok: bool
    rows: int = 0
    columns: tuple[str, ...] = ()
    message: str = ""

    def to_dict(self) -> dict:
        return {"factor": self.factor, "ok": self.ok, "rows": self.rows,
                "columns": list(self.columns), "message": self.message}


__all__ = ["FieldMeta", "FactorMeta", "FactorScanReport", "FactorCheckResult"]