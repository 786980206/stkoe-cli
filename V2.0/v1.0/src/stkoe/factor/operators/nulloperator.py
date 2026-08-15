from ..core import Operator, Feature
from ...data.plugins.wsdata import get_common_data
from dataclasses import dataclass
from typing import Any, Literal
import polars as pl
from ..core.funcs import c

@dataclass(frozen=True)    
class NAKeep(Operator):
    """不做任何处理"""
    def pipeline(self, feature:Feature) -> Feature:
        return feature
    
@dataclass(frozen=True)    
class TimeSeriesFill(Operator):
    """时序填充"""
    method:Literal["ffill"] = "ffill"

    def pipeline(self, feature:Feature) -> Feature:
        data = feature.data.with_columns(
            c.feature.forward_fill().over("sym")
        )
        return Feature(data, feature.name)

@dataclass(frozen=True)    
class CrossSectionFill(Operator):
    """截面填充"""
    method: Literal[
        "mean",
        "median",
        "mode",
        "mean_by_ic",
        "median_by_ic",
        "mode_by_ic",
    ] = "mean"

    def pipeline(self, feature:Feature) -> Feature:
        if self.method == "mean":
            data = feature.data.with_columns(
               c.feature.fill_null( c.feature.mean().over("date") )
            )
        elif self.method == "median":
            data = feature.data.with_columns(
                c.feature.fill_null( c.feature.median().over("date") )
            )
        elif self.method == "mode":
            data = feature.data.with_columns(
                c.feature.fill_null(c.feature.filter(c.feature.is_not_null()).mode().mean().over("date") )
            )
        elif self.method == "mean_by_ic":
            data = feature.data.join(get_common_data(), on=["date","sym"]).with_columns(
                c.feature.fill_null( c.feature.mean().over("date","ic") )
            ).select("date","sym","feature")
        elif self.method == "median_by_ic":
            data = feature.data.join(get_common_data(), on=["date","sym"]).with_columns(
                c.feature.fill_null( c.feature.median().over("date","ic") )
            ).select("date","sym","feature")
        elif self.method == "mode_by_ic":
            data = feature.data.join(get_common_data(), on=["date","sym"]).with_columns(
                c.feature.fill_null(c.feature.filter(c.feature.is_not_null()).mode().mean().over("date", "ic") )
            ).select("date","sym","feature")

        return Feature(data, feature.name)

@dataclass(frozen=True)    
class ConstantFill(Operator):
    """常数填充"""
    value: float

    def pipeline(self, feature:Feature) -> Feature:
        data = feature.data.with_columns(
            c.feature.fill_null( self.value )
        )
        return Feature(data, feature.name)
