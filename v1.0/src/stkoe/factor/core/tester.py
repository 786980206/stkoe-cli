from datetime import date
from dataclasses import dataclass, fields
import polars as pl
import pickle
import importlib
from pathlib import Path
from typing import Any
import orjson
from .feature import Feature
from .funcs import *


@dataclass(frozen=True)
class FactorTesterSpec:
    """因子测试器配置"""
    by_group: bool = False
    quantiles: int = 5
    periods: list[int] = (1, 5, 10)
    # 测试窗口, 注意结束日期应该小于`最新日期-max(periods)`以避免可能出现的空值
    date_range: tuple[date, date] = date(2023, 1, 1), date(2026, 1, 1)
    sample_range: pl.Expr|None = None
    # 核心指标滚动窗口. 例如过去 252 日的滚动平均 AC, 滚动平均 Prop(|t|>2)
    rolling_window: int = 252


class FactorTester(object):
    
    def __init__(self, factor: Feature, spec: FactorTesterSpec|None=None):
        self.factor = factor
        self.spec = spec if isinstance(spec, FactorTesterSpec) else FactorTesterSpec()
        self.factor_data = None

    def perpare_data(
        self,
        sf_base: pl.DataFrame,
        returns: pl.Expr = pl.col("r"),
        groupby: pl.Expr | None = pl.col("ic"),
        marketcap: pl.Expr | None = pl.col("fv"),
    ) -> "FactorTester":
        self.factor_data = (
            sf_base.join(self.factor.data, on=["date", "sym"], how="left")
            .with_columns(
                returns.alias("returns"),
                groupby.alias("group"),
                marketcap.alias("marketcap"),
                pl.col("feature").alias("factor"),
                pl.when(
                    self.spec.sample_range if self.spec.sample_range is not None else pl.col("sample")>0
                ).then( 1 ).otherwise( 0 ).alias("sample")
            )
            .sort("date", "sym")
        )
        self.get_clean_factor_and_forward_returns()
        return self

    def get_clean_factor_and_forward_returns(self) -> "FactorTester":
        """生成标准因子数据
        输出字段:
        - `dn`: 未来`n`日的累计收益率;
        - `factor_quantile`: 因子分箱结果, 在日期截面上划分, 支持按行业划分`by_group=True`;
        - `sample`: 标识是否是截面样本, 分成3中情况: 1.样本数据且因子值不为空, -1.样本数据但因子值为空, 0.非样本数据;

        其他说明:
        - 未对数据进行任何剔除, 数据索引还是和`cnstk_ixday`对齐;
        """
        # 1. 收益率计算: 此时还没有剔除非样本, 否者收益率计算会存在偏差
        self.factor_data = (
            self.factor_data.sort("sym", "date").select(
                [
                    "date",
                    "sym",
                    "sample",
                    "returns",
                    "marketcap",
                    "group",
                    "factor",
                    *( ts_cret(pl.col("returns"), -d).alias(f"d{d}") for d in self.spec.periods ),
                ]
            )
        ).filter(pl.col("date").is_between(*self.spec.date_range))
        # 2. 因子分箱, 区分是否按行业中性
        self.factor_data = self.factor_data.with_columns(
            pl.col("factor")
            .qcut(
                self.spec.quantiles,
                allow_duplicates=True,
                labels=[f"{i + 1}" for i in range(self.spec.quantiles)],
            )
            .over(["date", "group", "sample"] if self.spec.by_group else ["date","sample"])
            .alias("factor_quantile")
            .cast(pl.String)
            .cast(pl.Int64)
        )
        # 3. 将可能的空值统一处理成非样本数据
        # date, sym, returns, sample 来自 sf_base, 保证一定不为空
        # group 可能会有空值, 但不对因子值产生任何影响
        # dn 只会由于计算未来收益率可能为空, 但应该由 FactorTesterSpec.date_range 控制, 这里不做任何处理
        # factor 值可能为空, 为空的视为非样本数据
        self.factor_data = self.factor_data.with_columns(
            pl.when( (c.sample==1) & ( c.factor.is_null() ) ).then( -1 ).otherwise( c.sample ).alias("sample")
        )
        return self

    @staticmethod
    def dump_results(path: str | Path, /, *results) -> Path:
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        manifest: dict[str, dict] = {}
        for result in results:
            cls = type(result)
            cls_name = cls.__name__
            manifest[cls_name] = {"module": cls.__module__, "fields": {}}
            for f in fields(result):
                value = getattr(result, f.name)
                stem = f"{cls_name}.{f.name}"
                if isinstance(value, pl.DataFrame):
                    fname = f"{stem}.parquet"
                    value.write_parquet(path / fname)
                    manifest[cls_name]["fields"][f.name] = {"type": "parquet", "file": fname}
                else:
                    fname = f"{stem}.pkl"
                    with open(path / fname, "wb") as fh:
                        pickle.dump(value, fh, protocol=pickle.HIGHEST_PROTOCOL)
                    manifest[cls_name]["fields"][f.name] = {"type": "pickle", "file": fname}

        (path / "manifest.json").write_bytes(
            orjson.dumps(manifest, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
        )
        return path

    @staticmethod
    def load_results(path: str | Path) -> dict[str, Any]:
        path = Path(path)

        if not (path / "manifest.json").exists():
            return {}
        
        manifest: dict[str, dict] = orjson.loads((path / "manifest.json").read_bytes())

        results: dict[str, Any] = {}
        for cls_name, info in manifest.items():
            module = importlib.import_module(info["module"])
            cls = getattr(module, cls_name)
            field_values = {}
            for field_name, field_info in info["fields"].items():
                fpath = path / field_info["file"]
                if field_info["type"] == "parquet":
                    field_values[field_name] = pl.read_parquet(fpath)
                else:
                    with open(fpath, "rb") as fh:
                        field_values[field_name] = pickle.load(fh)
            results[cls_name] = cls(**field_values)

        return results