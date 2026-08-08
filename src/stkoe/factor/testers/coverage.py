import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv

def plot_fac_cvg_date(data:pl.DataFrame, title:str="因子覆盖率") -> hv.core.ViewableElement:
    data = data.rename({"SF2S": "因子样本覆盖率","F2T":"因子标的覆盖率","S2T":"样本标的覆盖率","X2S":"因子样本剔除率"})
    g = data.hvplot(
        x="date",
        y=["因子样本覆盖率", "因子标的覆盖率","样本标的覆盖率","因子样本剔除率"],
        group_label="覆盖率",
        title=title,
        yformatter=NumeralTickFormatter(format="0.00%"),
        # ylim=(0, 1),
        # legend=False,
        hover="vline",
        hover_cols="all",   
        hover_tooltips=[
            ("日期", "@date{%Y-%m-%d}"),
            ("指标", "@覆盖率"),
            ("数值", "@value{0.00%}"),            
        ],
    ).opts(legend_position="top_left",legend_opts={'title': None})
    return g

@dataclass(frozen=True, slots=True)
class CoverageTestResults:
    spec: FactorTesterSpec
    cvg_date: pl.DataFrame

    def calc_core_idx(self) -> pl.DataFrame:
        """核心指标: 参与因子有效验证的关键指标"""
        return self.cvg_date.select("date", c.SF2S.rolling_mean(self.spec.rolling_window, min_samples=30).alias(f"SF2S({self.spec.rolling_window})"))
    
    def plot_cvg_date(self, tilte:str="因子覆盖率") -> hv.core.ViewableElement:
        return plot_fac_cvg_date(self.cvg_date, title=tilte)

def CoverageTest(tester: FactorTester) -> CoverageTestResults:
    """覆盖率测试
    - SFNo: 截面有效样本数, 截面样本中因子值不为空得标的数;
    - FNo: 截面有效因子数据, 截面因子值不为空得标的数;
    - SNo: 截面样本数, 通过定义在`FactorTesterSpec.sample_range`中得条件确定得范围, `FactorTesterSpec.sample_range is None`时为`sf_base.sample>0`;
    - XNo: 截面剔除样本数, 由于因子值为空导致从`SNo`中剔除得样本数;
    - TNo: 截面全部标的数;

    其他说明:
    - `SFNo + XNo = SNo`
    - SF2S: 因子样本覆盖率, 截面有效样本数占总样本数比值, 最重要得覆盖率指标, 值越大越好;
    - X2S: 因子样本剔除率, 截面剔除样本数占总样本数比值, `X2S = 1 - SF2S`, 值越小越好;
    - F2T: 因子数据覆盖率, 截面有效因子数据占总标的数比值, 次一等的覆盖率指标, 值越大越好;
    - S2T: 样本标的覆盖率, 截面样本数占总标的数比值, 用于衡量样本筛选条件是否合理;
    """
    cvg_date = tester.factor_data.group_by("date").agg(
        (c.sample==1).sum().alias("SFNo"),
        c.factor.is_not_null().sum().alias("FNo"),
        (c.sample!=0).sum().alias("SNo"),
        (c.sample==-1).sum().alias("XNo"),
        pl.len().alias("TNo"),
    ).with_columns(
        (c.SFNo / c.SNo).alias("SF2S"),
        (c.XNo / c.SNo).alias("X2S"),
        (c.FNo / c.TNo).alias("F2T"),
        (c.SNo / c.TNo).alias("S2T"),
    )
    return CoverageTestResults(
        spec=tester.spec,
        cvg_date=cvg_date.sort("date"),
    )



