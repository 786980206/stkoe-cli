"""sample 公共数据类型：SampleMeta / SampleCheckResult"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..table.spec import ColumnMeta


@dataclass(frozen=True)
class SampleMeta:
    """样本池元数据（``sample meta`` 输出）：基于一个 dataset 的过滤产物定义

    样本池没有物化概念：``get``/``check`` 时动态构造
    ``dataset_with_fieldset``（源 dataset 全列 + 其 fieldset 已校验衍生字段），
    再施加 ``engine`` 的 filter ``formula``。
    """

    name: str
    version: int
    dataset: str                   # 源 dataset（数据来源）
    engine: str = "polars"         # 过滤表达式引擎（插件注册名，当前仅 polars）
    formula: str = ""              # 过滤表达式（空 = 返回整个 dataset_with_fieldset）
    keys: tuple[str, ...] = ()     # 继承源 dataset 主键（check 校验列集合的依据）
    columns: tuple[ColumnMeta, ...] = ()  # dataset_with_fieldset 列（源列 + fieldset 衍生指标列）
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    source: str = "local"
    extra: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "dataset": self.dataset,
            "engine": self.engine,
            "formula": self.formula,
            "keys": list(self.keys),
            "columns": [c.to_dict() for c in self.columns],
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "source": self.source,
            "extra": self.extra,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class SampleCheckResult:
    """``sample check``（样本池有效性校验）结果

    校验规则：dataset_with_fieldset 施加过滤后，结果集包含全部源索引列且行数 > 0。
    """

    sample: str
    ok: bool
    rows: int = 0
    columns: tuple[str, ...] = ()
    message: str = ""

    def to_dict(self) -> dict:
        return {"sample": self.sample, "ok": self.ok, "rows": self.rows,
                "columns": list(self.columns), "message": self.message}


__all__ = ["SampleMeta", "SampleCheckResult"]