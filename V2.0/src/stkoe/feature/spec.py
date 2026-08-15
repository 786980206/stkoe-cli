"""feature 公共数据类型：FeatureMeta / FeatureTestResult"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FeatureMeta:
    """因子定义元数据（``feature meta`` 输出）：基于公式 + engine 的纯定义，无物化

    feature 是"在 dataset 上创建 field"的独立化形态：不绑定具体数据源，
    公式在指定 sample（dataset_with_fieldset + filter）视图上逐行计算出因子列。
    """

    name: str
    version: int
    engine: str = "polars"         # 公式计算引擎（插件注册名，当前仅 polars）
    formula: str = ""              # 因子公式（列作用域 polars 表达式）
    display_name: str = ""
    description: str = ""
    unit: str | None = None
    tags: tuple[str, ...] = ()
    source: str = "local"
    extra: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "engine": self.engine,
            "formula": self.formula,
            "display_name": self.display_name,
            "description": self.description,
            "unit": self.unit,
            "tags": list(self.tags),
            "source": self.source,
            "extra": self.extra,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class FeatureTestResult:
    """``feature test`` 结果：在指定 sample 上对公式逐行求值 + 合法性校验

    合法因子 = 公式执行成功（ok）且结果行数 == 样本行数（valid），即逐行计算。
    """

    feature: str
    sample: str
    ok: bool                # 公式是否执行成功
    valid: bool             # 结果行数 == 样本行数（可作逐行因子）
    rows: int = 0
    columns: tuple[str, ...] = ()
    message: str = ""

    def to_dict(self) -> dict:
        return {"feature": self.feature, "sample": self.sample,
                "ok": self.ok, "valid": self.valid, "rows": self.rows,
                "columns": list(self.columns), "message": self.message}


__all__ = ["FeatureMeta", "FeatureTestResult"]