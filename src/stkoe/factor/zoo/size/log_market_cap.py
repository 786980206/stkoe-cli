
import polars as pl
from ....factor.core.feature import Feature as Factor, FeatureSpec as FactorSpec
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


factor = Factor(
    spec = FactorSpec(name="log_market_cap", description="市值因子",direction=-1),
    builder = LogMarketCapBuilder,
)

if __name__ == '__main__':
    from ....data import WSData
    factor = factor.build(WSData.query("from sf_base").pl())
    
