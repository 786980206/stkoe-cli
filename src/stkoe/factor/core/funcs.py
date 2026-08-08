import polars as pl
import polynx as plx
import numpy as np
import polars_ols as pls
import polars_ds as plds
from datetime import datetime
from functools import wraps
from typing import Iterable
from polars.datatypes import *


pl.DataFrame.query = plx.core.plx_query
class _PolarsColMeta(type):
    def __getattr__(cls, name: str) -> pl.Expr:
        if name.startswith("__"):
            raise AttributeError
        return pl.col(name)

    def __getitem__(cls, name: str | list[str]) -> pl.Expr:
        return pl.col(name)


class c(metaclass=_PolarsColMeta):
    pass


def ds_rank(x: pl.Expr) -> pl.Expr:
    return (x.rank("average") / (x.count() + 1)).over(["date", "sample"])


def ds_rank2norm(col: pl.Expr) -> pl.Expr:
    z_left = (-2 * col.log()).sqrt()
    z_right = (-2 * (1 - col).log()).sqrt()
    left = (
        (
            (
                ((-7.784894002430293e-03 * z_left - 3.223964580411365e-01) * z_left
                - 2.400758277161838e00)
                * z_left
                - 2.549732539343734e00
            )
            * z_left
            + 4.374664141464968e00
        )
        * z_left
        + 2.938163982698783e00
    ) / (
        (
            (
                ((7.784695709041462e-03 * z_left + 3.224671290700398e-01) * z_left
                + 2.445134137142996e00)
                * z_left
                + 3.754408661907416e00
            )
            * z_left
            + 1.0
        )
    )
    q = col - 0.5
    r = q * q
    center = (
        (
            (
                (
                    (-3.969683028665376e01 * r + 2.209460984245205e02) * r
                    - 2.759285104469687e02
                )
                * r
                + 1.383577518672690e02
            )
            * r
            - 3.066479806614716e01
        )
        * r
        + 2.506628277459239e00
    ) * q / (
        (
            (
                ((-5.447609879822406e01 * r + 1.615858368580409e02) * r
                - 1.556989798598866e02)
                * r
                + 6.680131188771972e01
            )
            * r
            - 1.328068155288572e01
        )
        * r
        + 1.0
    )
    right = -(
        (
            (
                ((-7.784894002430293e-03 * z_right - 3.223964580411365e-01) * z_right
                - 2.400758277161838e00)
                * z_right
                - 2.549732539343734e00
            )
            * z_right
            + 4.374664141464968e00
        )
        * z_right
        + 2.938163982698783e00
    ) / (
        (
            (
                ((7.784695709041462e-03 * z_right + 3.224671290700398e-01) * z_right
                + 2.445134137142996e00)
                * z_right
                + 3.754408661907416e00
            )
            * z_right
            + 1.0
        )
    )
    return pl.when(col < 0.02425).then(left).when(col > 0.97575).then(right).otherwise(center)


def fint(x: pl.Expr) -> pl.Expr:
    return x.cast(Int64)


def ffloat(x: pl.Expr) -> pl.Expr:
    return x.cast(Float64)


def fbool(x: pl.Expr) -> pl.Expr:
    return x.cast(Boolean)


def fstr(x: pl.Expr) -> pl.Expr:
    return x.cast(String)


def fsum(x: pl.Expr) -> pl.Expr:
    return x.sum()


def abs(x: pl.Expr) -> pl.Expr:
    return x.abs()


def flog(x: pl.Expr) -> pl.Expr:
    return x.log()


def sign(x: pl.Expr) -> pl.Expr:
    return x.sign()


def when_then(cond: pl.Expr, x: pl.Expr, y: pl.Expr) -> pl.Expr:
    return pl.when(cond).then(x).otherwise(y)


def min(x: pl.Expr, y: pl.Expr) -> pl.Expr:
    return pl.when(x < y).then(x).otherwise(y)


def max(x: pl.Expr, y: pl.Expr) -> pl.Expr:
    return pl.when(x > y).then(x).otherwise(y)


def pow(x: pl.Expr, a: int) -> pl.Expr:
    return x.pow(a)


def signpow(x: pl.Expr, a: int) -> pl.Expr:
    return x.sign() * x.abs().pow(a)


def cs_calc(x: pl.Expr, only_sample=False, fcol=None) -> pl.Expr:
    return x.over("date")


def cs_scale(x: pl.Expr, a: int = 1) -> pl.Expr:
    return (x / x.abs().sum() * a).over("sample", "date") * pl.col("sample")


def cs_zscore(x: pl.Expr) -> pl.Expr:
    return (x - x.mean() / x.std()).over("sample", "date") * pl.col("sample")


def cs_minmax(x: pl.Expr, a: float = 0, b: float = 1) -> pl.Expr:
    return (a + (b - a) * (x - x.min()) / (x.max() - x.min())).over("sample", "date") * pl.col("sample")


def cs_softmax(x: pl.Expr, a: float = 0, b: float = 1) -> pl.Expr:
    raise NotImplementedError


def cs_3sigma(x: pl.Expr) -> pl.Expr:
    E = x.mean().over("sample", "date")
    std = x.std().over("sample", "date")
    return (
        pl.when(x > (E + 3 * std))
        .then(E + 3 * std)
        .when(x < (E - 3 * std))
        .then(E - 3 * std)
        .otherwise(x)
    ) * pl.col("sample")


def cs_winsorize(x: pl.Expr, d: float = 0.05, u: float = 0.95) -> pl.Expr:
    th_u = x.quantile(u).over("sample", "date")
    th_d = x.quantile(d).over("sample", "date")
    return pl.when(x > th_u).then(th_u).when(x < th_d).then(th_d).otherwise(x) * pl.col("sample")


def cs_indneutralize(x: pl.Expr, ind: pl.Expr) -> pl.Expr:
    return (x - x.mean().over(ind, "sample", "date")) / x.std().over(ind, "sample", "date") * pl.col("sample")


def cs_capneutralize(x: pl.Expr, cap: pl.Expr) -> pl.Expr:
    return x.least_squares.ols(cap, add_intercept=True, mode="residuals").over("sample", "date") * pl.col("sample")


def cs_rank(x: pl.Expr) -> pl.Expr:
    return ((x.rank(method="dense") - 1) / (x.n_unique() - 1)).over("sample", "date") * pl.col("sample")


def cs_quantile(x: pl.Expr, q: float) -> pl.Expr:
    return x.quantile(q).over("sample", "date") * pl.col("sample")


def cs_qcut(x: pl.Expr, q: int = 5) -> pl.Expr:
    return x.qcut(q, allow_duplicates=True, labels=[f"q{i + 1}" for i in range(q)]).over("sample", "date") * pl.col("sample")


def cs_corr(x: pl.Expr, y: pl.Expr, *args, **kwargs) -> pl.Expr:
    return pl.corr(x, y, *args, **kwargs).over("sample", "date")


def ts_lag(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.shift(d).over("sym")


def ts_diff(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.diff(d).over("sym")


def ts_ret(c: pl.Expr, d: int, fcol: str | None = None) -> pl.Expr:
    d = int(np.floor(d))
    return c.pct_change(d).over("sym") if d > 0 else c.pct_change(-d).shift(d).over("sym")


def ts_cret(r: pl.Expr, d: int, fcol: str | None = None) -> pl.Expr:
    d = int(np.floor(d))
    return (r + 1).log().rolling_sum(d).exp().over("sym") - 1 if d > 0 else (r + 1).log().rolling_sum(-d).exp().shift(d).over("sym") - 1


def ts_corr(x: pl.Expr, y: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return pl.rolling_corr(x, y, window_size=d).over("sym")


def ts_cov(x: pl.Expr, y: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return pl.rolling_cov(x, y, window_size=d).over("sym")


def ts_min(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.rolling_min(window_size=d).over("sym")


def ts_max(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.rolling_max(window_size=d).over("sym")


def ts_sum(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.rolling_sum(window_size=d).over("sym")


def ts_std(x: pl.Expr, d: int) -> pl.Expr:
    d = int(np.floor(d))
    return x.rolling_std(window_size=d).over("sym")


def ts_avg(x: pl.Expr, d: int) -> pl.Expr:
    return x.rolling_mean(d).over("sym")


def ts_median(x: pl.Expr, d: int) -> pl.Expr:
    return x.rolling_median(d).over("sym")


def ts_var(x: pl.Expr, d: int) -> pl.Expr:
    return x.rolling_var(d).over("sym")


def ts_quantile(x: pl.Expr, d: int, q: float) -> pl.Expr:
    return x.rolling_quantile(q, window_size=d).over("sym")


def ts_kurtosis(x: pl.Expr, d: int, q: float) -> pl.Expr:
    return x.rolling_kurtosis(window_size=d).over("sym")


def ts_skew(x: pl.Expr, d: int, q: float) -> pl.Expr:
    return x.rolling_skew(window_size=d).over("sym")


def ts_ema(x: pl.Expr, n: int) -> pl.Expr:
    return x.ewm_mean(span=n).over("sym")


def ts_shift(x: pl.Expr, n: int) -> pl.Expr:
    return x.shift(n).over("sym")


def ts_ols(Y: pl.Expr, *args, **kwargs) -> pl.Expr:
    return Y.least_squares.rolling_ols(*args, **kwargs).over("sym")


def df_filter(c: pl.Expr) -> pl.Expr:
    return c


def df_qry(c: pl.Expr) -> pl.Expr:
    return c


def df_del(c: pl.Expr) -> pl.Expr:
    return ~c


def df_delanynull(*cols: pl.Expr) -> pl.Expr:
    return ~pl.any_horizontal([col.is_null() for col in cols])


def df_delallnull(*cols: pl.Expr) -> pl.Expr:
    return ~pl.all_horizontal([col.is_null() for col in cols])


def cint(x: pl.Expr) -> pl.Expr:
    return x.cast(pl.Int64)


def cbool(x: pl.Expr) -> pl.Expr:
    return x.cast(pl.Boolean)


def is_sample(ex_i: int = 22, ex_s: int = 30, ex_st: int = 252) -> pl.Expr:
    return ~cbool((pl.col("i") < ex_i) | (ts_sum(cint(pl.col("s") == "S"), ex_s) > 10) | (ts_sum(cint(pl.col("st")), ex_st) > 0))
