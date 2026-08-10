import polars as pl
from dataclasses import dataclass
from ..core import FactorTester, FactorTesterSpec
from ..core.funcs import *
from .base import NumeralTickFormatter, hv
import polars_ds as plds

@dataclass(frozen=True, slots=True)
class PortfolioTestResults:
    spec: FactorTesterSpec
    cvg_date: pl.DataFrame


def PortfolioTest(tester: FactorTester) -> PortfolioTestResults:
    """
    
    """
    # 筛选截面样本
    factor_data = tester.factor_data.filter( c.sample==1 )
    # 提起计算
    if not calc_expr is None: factor_data = factor_data.with_columns( calc_expr )
    # 计算权重
    factor_data = factor_data.with_columns( 
        # 判断分组调整
        weight.over(pl.col("date") if not group_adjust else [pl.col("date"),pl.col("group")]).alias("weight")
    ).with_columns(
        # 权重归一
        ( pl.col("weight") / pl.col("weight").abs().sum() ).over("date").alias("weight")
    )
    if return_weight: return factor_data
    # 计算因子收益
    fac_rets = factor_data.select(["date", "sym"] + [(pl.col("weight") * pl.col(f"d{no}")).alias(f"d{no}") for no in tester.spec.periods])
    # 单日收益
    fac_rets = fac_rets.group_by("date").agg([pl.col(f"d{no}").sum().alias(f"FR(d{no})") for no in tester.spec.periods])
    # 累计收益
    fac_cret = fac_rets.sort("date").with_columns([
        pl.all().exclude("date") + 1,
        *[pl.row_index().mod(no).alias(f"i{no}") for no in tester.spec.periods]
    ]).with_columns([
        pl.col(f"FR(d{no})").cum_prod().over(f"i{no}") for no in tester.spec.periods
    ])
    fac_cret = pl.concat([fac_cret[["date"]]] + [
        fac_cret.pivot(values = f"FR(d{no})", index=["date"], on=f"i{no}").fill_null(strategy="forward").select(pl.concat_list(pl.all().exclude("date")).list.mean().alias(f"FR(d{no})")) - 1
        for no in tester.spec.periods
    ], how="horizontal").sort("date")    
    pass