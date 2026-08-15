"""factor_test 公共数据类型：FactorTesterSpec / FactorTestMeta / FactorTestScanReport / FactorTestCheckResult"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field

from ..stat.spec import StatFile, StatMeta, StatScanReport
from ..table.spec import ColumnMeta


@dataclass(frozen=True)
class FactorTesterSpec:
    """因子测试器配置（对话 v1.0 ``FactorTesterSpec``）"""

    by_group: bool = False            # 分位是否在 group（行业）内部计算
    quantiles: int = 5                # 分位数量
    periods: tuple[int, ...] = (1, 5, 10)   # 前向收益周期
    date_range: tuple[str, str] = ("2023-01-01", "2026-01-01")  # 测试窗口 [start, end]
    rolling_window: int = 252         # 核心指标滚动窗口（如滚动平均 AC）

    def to_dict(self) -> dict:
        return {
            "by_group": self.by_group,
            "quantiles": self.quantiles,
            "periods": list(self.periods),
            "date_range": list(self.date_range),
            "rolling_window": self.rolling_window,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FactorTesterSpec":
        return cls(
            by_group=bool(d.get("by_group", False)),
            quantiles=int(d.get("quantiles", 5)),
            periods=tuple(int(p) for p in (d.get("periods") or (1, 5, 10))),
            date_range=tuple(str(x) for x in (d.get("date_range") or ("2023-01-01", "2026-01-01"))),
            rolling_window=int(d.get("rolling_window", 252)),
        )


@dataclass(frozen=True)
class FactorTestMeta:
    """因子测试数据集元数据（``test meta`` 输出）

    test = 在 factor 关联的 sample 视图上，结合测试必需列（returns/groupby/marketcap）
    生成的一份因子测试数据集（date/sym/sample/returns/group/marketcap/factor/d{no}/
    factor_quantile）；注册于 catalog（type='factor_test'）。
    """

    name: str
    version: int
    factor: str            # 被测因子（factor 名）
    sample: str            # factor 关联的样本池（数据来源）
    returns: str = "r"     # sample 视图中的收益列名
    groupby: str = "ic"    # sample 视图中的分组（行业）列名
    marketcap: str = "fv"  # sample 视图中的市值列名
    spec: FactorTesterSpec = dc_field(default_factory=FactorTesterSpec)
    factor_col: str = ""   # 测试使用的因子列名（默认取 factor 的因子列）
    keys: tuple[str, ...] = ()
    materialized: bool = False
    materialized_at: str | None = None
    curated: bool = False          # 物化数据与当前 factor/测试列/配置一致
    columns: tuple[ColumnMeta, ...] = ()  # 测试数据集列说明
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
            "factor": self.factor,
            "sample": self.sample,
            "returns": self.returns,
            "groupby": self.groupby,
            "marketcap": self.marketcap,
            "spec": self.spec.to_dict(),
            "factor_col": self.factor_col,
            "keys": list(self.keys),
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
class FactorTestScanReport:
    """``test scan``（物化测试数据集）结果"""

    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    rows: int = 0
    quantiles: int = 5
    periods: tuple[int, ...] = (1, 5, 10)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "materialized": self.materialized,
            "changed": self.changed,
            "rows": self.rows,
            "quantiles": self.quantiles,
            "periods": list(self.periods),
        }


@dataclass(frozen=True)
class FactorTestCheckResult:
    """``test check``（测试数据集有效性校验）结果"""

    test: str
    ok: bool
    rows: int = 0
    columns: tuple[str, ...] = ()
    message: str = ""

    def to_dict(self) -> dict:
        return {"test": self.test, "ok": self.ok, "rows": self.rows,
                "columns": list(self.columns), "message": self.message}


__all__ = ["FactorTesterSpec", "FactorTestMeta", "FactorTestScanReport",
           "FactorTestCheckResult"]