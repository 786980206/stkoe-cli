import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv


def plot_fac_tr_date(data:pl.DataFrame, dno:int=10, title:str="因子分组换手率(d{dno})") -> hv.core.ViewableElement:
    """因子分层换手情况"""
    qnos = data["factor_quantile"].unique().to_list()
    data = data.pivot(values = f"TR(d{dno})", index=["date"], on="factor_quantile").select(
        "date",
        *[pl.col(f"{qno}").alias(f"TR(d{dno},q{qno})") for qno in qnos]
    )
    g = data.hvplot(
        y=[f"TR(d{dno},q{qno})" for qno in qnos],
        x="date",
        group_label="分组",
        yformatter=NumeralTickFormatter(format="0%"),
        hover_tooltips=[
            ("分组", "@分组"),
            ("日期", "@date{%Y-%m-%d}"),
            ("换手", "@value{0.00%}")
        ],
    ).opts(legend_position="top_left",title=title.format(dno=dno))
    return g

def plot_fac_ac_date(data:pl.DataFrame, dno:int = 10, ma1=22, ma2=252, title:str="因子自相关系数(d{dno})") -> hv.core.ViewableElement:
    """因子自相关系数情况"""
    data = data.with_columns(
        pl.col(f"AC(d{dno})").rolling_mean(ma1).alias(f"ma1"),
        pl.col(f"AC(d{dno})").rolling_mean(ma2).alias(f"ma2")
    )
    g = data.hvplot(
        y=f"AC(d{dno})",
        x="date",
        label=f"AC(d{dno})",
        yformatter=NumeralTickFormatter(format="0%"),
        color='gray',
        alpha=0.3,
        hover="vline",
        hover_cols="all",
        hover_tooltips=[
            ("日期", "@date{%Y-%m-%d}"),
            (f"AC(d{dno})", f"@AC(d{dno}){{0.00%}}"),
            (f"MA({ma1})", f"@ma1{{0.00%}}"),
            (f"MA({ma2})", f"@ma2{{0.00%}}"),
        ],
    )
    ma1 = data.hvplot.line(x='date', y="ma1", label=f"MA({ma1})",hover=False)
    ma2 = data.hvplot.line(x='date', y="ma2", label=f"MA({ma2})",hover=False)
    p70 = hv.HLine(0.7).opts(color="green", line_width=2, line_dash="dashed")
    g = ( g * ma1 * ma2 * p70 ).opts(legend_position="bottom_left",title=title.format(dno=dno))
    return g

@dataclass(frozen=True, slots=True)
class BucketTurnoverTestResults:
    spec: FactorTesterSpec
    tr_date: pl.DataFrame

    def plot_tr_date(self, title:str="因子分组换手率(d{dno})") -> hv.core.ViewableElement:
        return hv.Layout([plot_fac_tr_date(self.tr_date, title=title, dno=no) for no in self.spec.periods] )


def BucketTurnoverTest(tester: FactorTester) -> BucketTurnoverTestResults:
    """分层换手测试"""
    tr_date = (
        # 先进行时序变化
        tester.factor_data.with_columns( 
            (ts_shift(c.factor_quantile,-no)==c.factor_quantile).alias(f"TR(d{no})")
            for no in tester.spec.periods
        )
        # 过滤截面样本
        .filter( c.sample==1 )
        # 按日期分组
        .group_by(["date", "factor_quantile"]).agg(
            ( 1 - pl.sum(f"TR(d{no})") / pl.len() ).alias(f"TR(d{no})")
            for no in tester.spec.periods
        ).sort("date", "factor_quantile").with_columns(
            pl.col(f"TR(d{no})").shift(no).over("factor_quantile")
            for no in tester.spec.periods  
        )
    )
    return BucketTurnoverTestResults(
        spec=tester.spec,
        tr_date=tr_date.sort("date","factor_quantile")
    )
  

@dataclass(frozen=True, slots=True)
class AutoCorrelationTestResults:
    spec: FactorTesterSpec
    ac_date: pl.DataFrame
    rank_ac_date: pl.DataFrame

    def calc_core_idx(self) -> pl.DataFrame:
        """核心指标: 参与因子有效验证的关键指标"""
        return self.rank_ac_date.select(
            "date",
            *(c[f"AC(d{no})"].rolling_mean(self.spec.rolling_window,min_samples=30).alias(f"AC(d{no},{self.spec.rolling_window})") for no in self.spec.periods)   
        )

    def plot_ac_date(self, title:str="因子原始自相关系数(d{dno})") -> hv.core.ViewableElement:
        return hv.Layout([plot_fac_ac_date(self.ac_date, dno=dno, title=title) for dno in self.spec.periods])
    
    def plot_rank_ac_date(self, title:str="因子Rank自相关系数(d{dno})") -> hv.core.ViewableElement:
        return hv.Layout([plot_fac_ac_date(self.rank_ac_date, dno=dno, title=title) for dno in self.spec.periods])    


def AutoCorrelationTest(tester: FactorTester) -> AutoCorrelationTestResults:
    """自相关测试"""
    # 先进行时序变化
    data = tester.factor_data.with_columns( 
        ts_shift(c.factor,-no).alias(f"factor(d{no})")
        for no in tester.spec.periods
    # 过滤截面样本
    ).filter( c.sample==1 )
    # 自相关系数
    ac_date = data.group_by("date").agg(
        pl.corr(c.factor, pl.col(f"factor(d{no})"), method="pearson").alias(f"AC(d{no})")
        for no in tester.spec.periods
    ).sort("date").with_columns(
        pl.col(f"AC(d{no})").rolling_mean(no,min_samples=1).shift(no)
        for no in tester.spec.periods  
    )
    # Rank 自相关系数
    rank_ac_date = data.group_by("date").agg(
        pl.corr(c.factor, pl.col(f"factor(d{no})"), method="spearman").alias(f"AC(d{no})")
        for no in tester.spec.periods
    ).sort("date").with_columns(
        pl.col(f"AC(d{no})").rolling_mean(no,min_samples=1).shift(no)
        for no in tester.spec.periods  
    )
    return AutoCorrelationTestResults(
        spec=tester.spec,
        ac_date=ac_date.sort("date"),
        rank_ac_date=rank_ac_date.sort("date")
    )
