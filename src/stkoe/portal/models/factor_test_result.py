"""因子回测模型 - 根据 result_id 加载回测结果数据"""

from __future__ import annotations

import orjson
import polars as pl
from typing import TYPE_CHECKING
from ..config import RESULTS_DIR
from ...factor.core.tester import FactorTester

if TYPE_CHECKING:
    from ...factor.testers.returns import BucketReturnsTestResults, FactorReturnsTestResults
    from ...factor.testers.ic import ICTestResults
    from ...factor.testers.coverage import CoverageTestResults
    from ...factor.testers.stability import BucketTurnoverTestResults, AutoCorrelationTestResults
    from ...factor.testers.model import CSRegModelTestResults


_RESULT_MAP = {
    "bucket_returns": "BucketReturnsTestResults",
    "factor_returns": "FactorReturnsTestResults",
    "ic_test": "ICTestResults",
    "coverage_test": "CoverageTestResults",
    "bucket_turnover": "BucketTurnoverTestResults",
    "auto_correlation": "AutoCorrelationTestResults",
    "cs_reg_model": "CSRegModelTestResults",
}


class FactorTestResultModel:
    """因子回测模型，按 result_id 加载对应回测结果目录下的数据文件"""

    def __init__(self, result_id: str) -> None:
        self.result_id: str = result_id
        self.result_dir = RESULTS_DIR / result_id
        loaded = FactorTester.load_results(self.result_dir)

        self.bucket_returns: BucketReturnsTestResults | None = loaded.get("BucketReturnsTestResults")
        self.factor_returns: FactorReturnsTestResults | None = loaded.get("FactorReturnsTestResults")
        self.ic_test: ICTestResults | None = loaded.get("ICTestResults")
        self.coverage_test: CoverageTestResults | None = loaded.get("CoverageTestResults")
        self.bucket_turnover: BucketTurnoverTestResults | None = loaded.get("BucketTurnoverTestResults")
        self.auto_correlation: AutoCorrelationTestResults | None = loaded.get("AutoCorrelationTestResults")
        self.cs_reg_model: CSRegModelTestResults | None = loaded.get("CSRegModelTestResults")

    def get_bucket_returns_years(self) -> list[int]:
        years = self.bucket_returns.rtn_date["date"].dt.year() if self.bucket_returns is not None else pl.Series(dtype=pl.Int32)
        return sorted(years.unique().to_list())

    def get_bucket_returns_spec_json(self) -> bytes:
        return orjson.dumps(self.bucket_returns.spec.__dict__)

    def get_spec_json(self, result_type: str) -> bytes:
        cls_name = _RESULT_MAP.get(result_type)
        if cls_name is None:
            return orjson.dumps({})
        result = getattr(self, result_type, None)
        if result is not None:
            return orjson.dumps(result.spec.__dict__)
        return orjson.dumps({})
