from dataclasses import dataclass
from abc import ABC, abstractmethod
import polars as pl
from datetime import date
from ...data.plugins.wsdata import get_cnstk_tdday


class FeatureBuilder(ABC):
    """
    Abstract base class for all factor builders.

    Builder responsibility:
    DataView -> raw factor exposure
    """

    # ====== core API ======
    def build(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Run the factor builder.
        """
        # check input
        self._validate_input(data)
        # sort and calc
        ret = self.calc(data.sort("date", "sym"))
        # check output
        self._validate_output(ret)
        return ret

    @abstractmethod
    def calc(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Actual computation logic.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def required_fields(self) -> list[str]:
        """
        Required fields for the factor computation.
        """
        raise NotImplementedError

    def required_date_period(self, date_begin: date, date_end: date) -> tuple[date, date]:
        """
        Required datalens for the factor computation.
        """
        n = self.window_size if hasattr(self, "window_size") else 1
        date_begin = get_cnstk_tdday().filter(pl.col("date") <= date_begin)[-n, "date"]
        date_end = get_cnstk_tdday().filter(pl.col("date") <= date_end)[-1, "date"]
        return date_begin, date_end
    
    # ====== helpers ======
    def _validate_input(self, data:pl.DataFrame):
        """
        Validate required fields exist.
        """
        missing = set(self.required_fields + ["date", "sym"]) - set(data.schema.keys())
        if missing:
            raise ValueError(
                f"{self.__class__.__name__}(): missing required fields: {missing}"
            )
    
    def _validate_output(self, data:pl.DataFrame):
        """
        Validate output schema.
        """
        if "feature" not in data.schema.keys():
            raise ValueError(
                f"""{self.__class__.__name__}(): output field "feature" not found"""
            )

@dataclass
class MockBuilder(FeatureBuilder):
    
    @property
    def required_fields(self) -> list[str]:
        return []
    
    def build(self, data:pl.DataFrame|None=None) -> pl.DataFrame:
        return pl.DataFrame({"date": [], "sym": [], "feature": []}) if data is None else data