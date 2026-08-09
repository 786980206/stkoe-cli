from dataclasses import dataclass
from ...core import FeatureBuilder, FeatureSpec, Pipeline
from ...core.funcs import c, pl, pls

@dataclass(frozen=True)
class FacHBetaBuilder(FeatureBuilder):
    window_size: int = 504
    half_life: int = 252

    @property
    def required_fields(self) -> list[str]:
        return ["date","sym","rm","rf","re"]

    def calc(self, data: pl.DataFrame) -> pl.DataFrame:
        ret = data.sort("date","sym").with_columns(
            # 市场超额
            (c.rm - c.rf).rolling_mean(window_size=4,min_samples=4).over("sym").alias("Rmx(t)"),
            # 个股超额
            (c.re - c.rf).rolling_mean(window_size=4,min_samples=4).over("sym").alias("Rex(t)"),
            # 半衰期权重: date升序排列, 每间隔 self.spec.builder_params["half_life"] 天, 权重翻倍
            pl.lit(2).pow(pl.row_index() / self.half_life).over("sym").alias("weight")
        ).with_columns(
            # 滚动加权回归
            c["Rex(t)"].least_squares.rolling_ols(
                c["Rmx(t)"],
                add_intercept=True,
                sample_weights=c.weight,
                window_size=self.window_size,
                mode="coefficients",
                min_periods=self.window_size,
            ).over("sym").struct.unnest()
        ).select(
            # 返回计算结果
            "date",
            "sym",
            c["Rmx(t)"].alias("feature")
        )
        return ret


fac_hbeta = FeatureSpec(
    name="fac_hbeta", 
    description="基于过去504个交易日的股票超额收益与估计市值加权超额收益的时间序列回归斜率系数计算，采用252天半衰期；收益按4天窗口聚合以减少非同步性和自相关影响。",
    builder=FacHBetaBuilder(
        window_size=504,
        half_life=252,
    ),
    pipeline=Pipeline([])
)

    
if __name__ == '__main__':
    from wsdata import WSData
    data = WSData.query("""
        select 
            base.date,
            base.sym,
            inday.rm,
            inday.rf,
            base.r as re
        from df_base base
        left join df_inday inday on base.date=inday.date
        where base.date >= '2020-01-01'
    """).pl()
    fac_hbeta.build(data=data)
    