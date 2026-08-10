
import polars as pl
from ....factor.core.feature import FeatureSpec as FactorSpec, Pipeline
from ....factor.core.builder import FeatureBuilder as FactorBuilder


class LogMarketCapBuilder(FactorBuilder):

    @property
    def required_fields(self) -> list[str]:
        return ["tv"]

    def calc(self, data: pl.DataFrame) -> pl.DataFrame:
        return (
            data
            .select(
                "date",
                "sym",
                pl.col("tv")
                .clip(lower_bound=1e-12)
                .log()
                .alias(self.spec.name)
            )
        )


fac_log_market_cap = FactorSpec(
    name="log_market_cap", description="市值因子", direction=-1,
    builder=LogMarketCapBuilder(), pipeline=Pipeline([]),
)

if __name__ == '__main__':
    from wsdata import WSData
    factor = fac_log_market_cap.build(WSData.query("from sf_base").pl())
    
