import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv
import polars_ds as plds

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
class AlphaBetaTestResults:
    spec: FactorTesterSpec
    cvg_date: pl.DataFrame

    def calc_core_idx(self) -> pl.DataFrame:
        """核心指标: 参与因子有效验证的关键指标"""
        return self.cvg_date.select("date", *[c.SF2S.rolling_mean(dno, min_samples=1).alias(f"coverage_sf2s_ma{dno}") for dno in self.spec.periods])

    def plot_cvg_date(self, tilte:str="因子覆盖率") -> hv.core.ViewableElement:
        return plot_fac_cvg_date(self.cvg_date, title=tilte)

def AlphaBetaTest(tester: FactorTester) -> AlphaBetaTestResults:
    """AlphaBeta测试"""

    # 因子回报
    if "因子日度收益序列" not in self.perf.keys(): self.factor_returns()
    fac_ret = self.perf["因子日度收益序列"]
    # 市场回报: 简单平均?
    mkt_ret = tester.factor_data.group_by("date").agg([pl.col(f"d{no}").mean().alias(f"ME(d{no})") for no in tester.spec.periods])
    # 合并回归
    ret = mkt_ret.join(fac_ret,on="date")
    # 滚动 alpha
    alphabeta_date = ret.sort("date").with_columns(
        *[ pl.col(f"FR(d{no})").least_squares.rolling_ols(pl.col(f"ME(d{no})"),add_intercept=True, window_size=252, mode="coefficients").alias(f"coeffs(d{no})").struct.rename_fields([f"beta(d{no},1Y)",f"alpha(d{no},1Y)"]).struct.unnest() for no in tester.spec.periods ], 
        *[ pl.col(f"FR(d{no})").least_squares.rolling_ols(pl.col(f"ME(d{no})"),add_intercept=True, window_size=1 , mode="coefficients").alias(f"coeffs(d{no})").struct.rename_fields([f"beta(d{no},1M)",f"alpha(d{no},1M)"]).struct.unnest() for no in tester.spec.periods ]
    ).select(
        "date",
        *[f"alpha(d{no},1Y)" for no in tester.spec.periods],
        *[f"beta(d{no},1Y)" for no in tester.spec.periods],
        *[(pl.col(f"alpha(d{no},1Y)") + 1).pow( 252 / no ).alias(f"Ann. alpha(d{no},1Y)") - 1 for no in tester.spec.periods],
        *[f"alpha(d{no},1M)" for no in tester.spec.periods],
        *[f"beta(d{no},1M)" for no in tester.spec.periods],    
        *[(pl.col(f"alpha(d{no},1M)") + 1).pow( 252 / no ).alias(f"Ann. alpha(d{no},1M)") - 1 for no in tester.spec.periods]
    )
    # 数据保存
    self.perf["Alpha/Beta统计"] = alphabeta_stat
    self.perf["Alpha/Beta序列"] = alphabeta_date
    return AlphaBetaTestResults(
        spec=tester.spec,
        cvg_date=cvg_date.sort("date"),
    )



def calc_fac_alpha(self):
    # 因子回报
    if "因子日度收益序列" not in self.perf.keys(): self.factor_returns()
    fac_ret = self.perf["因子日度收益序列"]
    # 市场回报
    mkt_ret = self.factor_data.group_by("date").agg([pl.col(f"d{no}").mean().alias(f"ME(d{no})") for no in self.perf["periods"]])
    # 合并回归
    ret = mkt_ret.join(fac_ret,on="date")
    # 计算整体
    alphabeta_stat = ret.select([
        plds.lin_reg(pl.col(f"ME(d{no})"),target=pl.col(f"FR(d{no})"), add_bias=True).alias(f"d{no}") for no in self.perf["periods"]
    ]).unpivot(variable_name="periods", value_name="coeffs").with_columns([
        pl.col("coeffs").list.to_struct(fields=["beta","alpha"]).struct.unnest(),
        pl.Series(self.perf["periods"]).alias("dno")
    ]).select([
        "periods",
        "alpha",
        "beta",
        (pl.col("alpha") + 1).pow( 252 / pl.col("dno")).alias("Ann. alpha") - 1
    ])
    # 滚动 alpha
    alphabeta_date = ret.sort("date").with_columns(
        *[ pl.col(f"FR(d{no})").least_squares.rolling_ols(pl.col(f"ME(d{no})"),add_intercept=True, window_size=252, mode="coefficients").alias(f"coeffs(d{no})").struct.rename_fields([f"beta(d{no},1Y)",f"alpha(d{no},1Y)"]).struct.unnest() for no in self.perf["periods"] ], 
        *[ pl.col(f"FR(d{no})").least_squares.rolling_ols(pl.col(f"ME(d{no})"),add_intercept=True, window_size=22 , mode="coefficients").alias(f"coeffs(d{no})").struct.rename_fields([f"beta(d{no},1M)",f"alpha(d{no},1M)"]).struct.unnest() for no in self.perf["periods"] ]
    ).select(
        "date",
        *[f"alpha(d{no},1Y)" for no in self.perf["periods"]],
        *[f"beta(d{no},1Y)" for no in self.perf["periods"]],
        *[(pl.col(f"alpha(d{no},1Y)") + 1).pow( 252 / no ).alias(f"Ann. alpha(d{no},1Y)") - 1 for no in self.perf["periods"]],
        *[f"alpha(d{no},1M)" for no in self.perf["periods"]],
        *[f"beta(d{no},1M)" for no in self.perf["periods"]],    
        *[(pl.col(f"alpha(d{no},1M)") + 1).pow( 252 / no ).alias(f"Ann. alpha(d{no},1M)") - 1 for no in self.perf["periods"]]
    )
    # 数据保存
    self.perf["Alpha/Beta统计"] = alphabeta_stat
    self.perf["Alpha/Beta序列"] = alphabeta_date