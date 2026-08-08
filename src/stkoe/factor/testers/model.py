"""
模型测试, 通过指定模型, 验证增加因子后的模型解释效果有没有提升
"""
from ..core import FactorTester, FactorTesterSpec
from dataclasses import dataclass
import polars as pl
from ...data import WSData, c, NumeralTickFormatter, hv
import polars_ds as plds

@dataclass(frozen=True, slots=True)
class CSRegModelTestResults:
    spec: FactorTesterSpec
    stat_date: pl.DataFrame

    def calc_core_index(self):
        return self.stat_date.select("date","ΔR2(m1,252)","ΔR2(m2,252)","Prop(|t|>2,252)")


def CSRegModelTest(tester: FactorTester) -> CSRegModelTestResults:
    """截面回归模型测试, 返回模型`t`值与`R2`
    - m0: `Rex ~ 1 + factor`
    - m1: `Rex ~ 1 + factor + beta`
    - m2: `Rex ~ 1 + factor + beta + size`
    """
    data = tester.factor_data.join(
        # 市场超额收益率和无风险利率
        WSData.query("select date,rmx,rf from df_inday").pl(), on="date"
    ).join(
        # 市场因子
        WSData.query("select date,sym,beta1 as beta from df_beta").pl(), on=["date","sym"]
    ).select(
        "date",
        "sym",
        "sample",
        # 个股超额
        (c.returns - c.rf).alias("rex"),
        # 市场因子
        "beta",
        c.marketcap.log().alias("size"),
        # 个股因子暴露
        "factor"
    ).filter(
        # 样本筛选
        c.sample==1
    )
    # m0: Rex ~ 1 + factor
    m0 = data.group_by("date").agg(
        plds.lin_reg_report(c.factor, target=c.rex, add_bias=True, null_policy="raise").struct.unnest()
    ).sort("date").select(
        "date", c.t.list.get(0).alias("t(m0)"), c.r2.list.get(0).alias("R2(m0)")
    )
    # m1: Rex ~ 1 + factor + beta
    m1 = data.group_by("date").agg(
        plds.lin_reg_report(c.factor, c.beta, target=c.rex, add_bias=True, null_policy="raise").struct.unnest()
    ).sort("date").select(
        c.t.list.get(0).alias("t(m1)"), c.r2.list.get(0).alias("R2(m1)")
    )
    # m2: Rex ~ 1 + factor + beta + size
    m2 = data.group_by("date").agg(
        plds.lin_reg_report(c.factor, c.beta, c.size, target=c.rex, add_bias=True, null_policy="raise").struct.unnest()
    ).sort("date").select(
        c.t.list.get(0).alias("t(m2)"), c.r2.list.get(0).alias("R2(m2)")
    )
    stat_date = pl.concat([m0, m1, m2], how="horizontal").with_columns(
        (c["R2(m1)"] - c["R2(m0)"]).rolling_mean(tester.spec.rolling_window,min_samples=30).alias(f"ΔR2(m1,{tester.spec.rolling_window})"),
        (c["R2(m2)"] - c["R2(m0)"]).rolling_mean(tester.spec.rolling_window,min_samples=30).alias(f"ΔR2(m2,{tester.spec.rolling_window})"),
        (c["t(m2)"].abs() > 2).rolling_mean(tester.spec.rolling_window,min_samples=30).alias(f"Prop(|t|>2,{tester.spec.rolling_window})")
    )
    return CSRegModelTestResults(
        spec=tester.spec,
        stat_date=stat_date
    )