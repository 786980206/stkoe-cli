
from ...core import FeatureBuilder, FeatureSpec, Pipeline
from ....data import c, pl, pls

class FacBTOPBuilder(FeatureBuilder):

    @property
    def required_fields(self) -> list[str]:
        return ["date","sym","pb"]

    def calc(self, data: pl.DataFrame) -> pl.DataFrame:
        ret = data.select(
            "date",
            "sym",
            (1 / c.pb).alias("feature")
        )
        return ret
    
fac_btop = FeatureSpec(
    name="fac_btop", 
    description="最近报告的普通股账面价值除以当前市值。",
    builder=FacBTOPBuilder(),
    pipeline=Pipeline([])
)    