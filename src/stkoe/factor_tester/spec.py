"""factor_tester 公共数据类型：FactorTesterSpec（测试器配置）

V2.0 的 FactorTestMeta/ScanReport/CheckResult 已随 FactorTestController 废弃
（业务统一走 graph/service.py 的 GraphService，`test meta` 输出为 dict 形态）。
"""
from __future__ import annotations

from dataclasses import dataclass


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


__all__ = ["FactorTesterSpec"]
