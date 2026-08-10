import polars as pl
from ..factor.core.tester import FactorTester, FactorTesterSpec
from ..factor.core.feature import Feature as Factor
from ..factor.testers.model import CSRegModelTest
from ..factor.testers.stability import AutoCorrelationTest
from ..factor.testers.coverage import CoverageTest


def get_test_stat_by_factor(factor: Factor):
    """根据因子获取因子测试结果"""
    from wsdata import WSData
    tester = FactorTester(factor, FactorTesterSpec()).perpare_data(
        sf_base=WSData.query("from sf_base where date>='2020-01-01'").pl(),
        groupby=pl.col("/inc/sw2021"),
    )
    test_stat = pl.concat([
        CSRegModelTest(tester).calc_core_index(),
        AutoCorrelationTest(tester).calc_core_idx().drop("date"),
        CoverageTest(tester).calc_core_idx().drop("date"),
    ], how="horizontal")
    return test_stat