"""factor_tester tester：测试数据集准备 + 六类测试器（纯 polars 计算，无绘图）

对话 v1.0 ``factor/core/tester.py``（数据准备 + 前向收益 + 分位）与
``factor/testers/``（bucket_returns / factor_returns / bucket_turnover /
autocorrelation / ic / coverage）。

测试数据集 Schema（v1.0 FactorTesterData）：
    date / sym / sample / returns / group / marketcap / factor / d{no} / factor_quantile
"""
from __future__ import annotations

import polars as pl

from .spec import FactorTesterSpec


# ---------- 数据准备 ----------

def ts_cret(returns: pl.Expr, d: int) -> pl.Expr:
    """sym 内 d 日前向累计收益：``ts_cret(returns, -d)``

    用对数收益累加 + shift 实现（要求在 sym 内按 date 升序排列）：
        fwd_d(t) = exp( cum_log(t+d) - cum_log(t) ) - 1
    """
    logr = (1 + returns).log()
    cum = logr.cum_sum().over("sym")
    return (cum.shift(-d).over("sym") - cum).exp() - 1


def _quantile_expr(factor: pl.Expr, keys: list[str], quantiles: int) -> pl.Expr:
    """截面分位（qcut 近似）：rank/count 按 date(+group) 分桶取 1..quantiles"""
    rank = factor.rank("average").over(keys)
    cnt = factor.count().over(keys)
    return (rank.truediv(cnt) * quantiles).ceil().cast(pl.Int32).clip(1, quantiles)


def prepare_factor_data(frame: pl.DataFrame, spec: FactorTesterSpec) -> pl.DataFrame:
    """把 (date/sym/sample/returns/group/marketcap/factor) 底表加工成测试数据集

    - 前向收益 ``d{no} = ts_cret(returns, -no)``（sym 内按 date 升序）
    - 样本重整：sample=1(观测)/0(非观测)/-1(因子为空被剔除)；因子为空则剔除因子
    - 因子分位 ``factor_quantile``：按 date（by_group 时 +group）截面分位
    - 过滤 date_range，按 date/sym 排序
    """
    df = frame.sort(["sym", "date"])
    out = (
        df.with_columns(
            *[ts_cret(pl.col("returns"), d).alias(f"d{d}") for d in spec.periods]
        )
        .with_columns(
            pl.when(pl.col("sample") > 0).then(1).otherwise(0).alias("sample")
        )
        .with_columns(
            pl.when((pl.col("sample") == 1) & pl.col("factor").is_null())
            .then(-1)
            .otherwise(pl.col("sample"))
            .alias("sample")
        )
        .with_columns(
            pl.when(pl.col("sample") == -1).then(None).otherwise(pl.col("factor"))
            .alias("factor")
        )
    )
    keys = ["date", "group"] if spec.by_group else ["date"]
    out = out.with_columns(
        _quantile_expr(pl.col("factor"), keys, spec.quantiles).alias("factor_quantile")
    )
    out = out.filter(
        (pl.col("date") >= pl.lit(spec.date_range[0]))
        & (pl.col("date") <= pl.lit(spec.date_range[1]))
    ).sort(["date", "sym"])
    return out.with_columns(pl.col("factor_quantile").cast(pl.Int32))


# ---------- 测试器 ----------

def bucket_returns(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """分组收益（bucket_returns）：按 date×quantile 聚合前向收益均值/标准误

    - ``rtn_date``：date×quantile 的 E(d{no}) 与 SE(d{no})（组内标准误 = std/sqrt(n)）
    - ``exr_date``：分组收益相对当日截面均值的超额收益（E - date 均值）
    - ``gbr_date``：分组收益相对 group 内均值的超额（group-based return）
    """
    goods = data.filter(pl.col("sample") == 1)
    agg = []
    for d in spec.periods:
        agg += [pl.col(f"d{d}").mean().alias(f"E(d{d})"),
                (pl.col(f"d{d}").std() / pl.col(f"d{d}").count().sqrt()).alias(f"SE(d{d})")]
    rtn = (
        goods.group_by(["date", "factor_quantile"])
        .agg(agg)
        .sort(["date", "factor_quantile"])
    )
    exr_cols = []
    for d in spec.periods:
        mean = pl.col(f"E(d{d})").mean().over("date")
        exr_cols.append((pl.col(f"E(d{d})") - mean).alias(f"EXR(d{d})"))
    exr = rtn.with_columns(exr_cols)
    exr = exr.select(["date", "factor_quantile"] +
                     [f"EXR(d{d})" for d in spec.periods])
    gbr_cols = []
    for d in spec.periods:
        gbr_cols.append((pl.col(f"E(d{d})") - pl.col(f"E(d{d})").mean().over("date", "group")).alias(f"GBR(d{d})"))
    gbr = (
        goods.group_by(["date", "group", "factor_quantile"])
        .agg([pl.col(f"d{d}").mean().alias(f"E(d{d})") for d in spec.periods])
        .with_columns(gbr_cols)
        .sort(["date", "group", "factor_quantile"])
    )
    gbr = gbr.select(["date", "group", "factor_quantile"] +
                     [f"GBR(d{d})" for d in spec.periods])
    return {"rtn_date": rtn, "exr_date": exr, "gbr_date": gbr}


def _factor_weight_returns(goods: pl.DataFrame, spec: FactorTesterSpec) -> pl.DataFrame:
    """每 date 截面：因子加权 / 等权 / 行业中性 多空组合日收益

    权重 = 截面 rank 中心化（long-short 权重 Σw≈0），行业中性先组内去均值。
    """
    d1 = "d1" if "d1" in goods.columns else f"d{spec.periods[0]}"
    f = (
        goods.with_columns(pl.col(d1).fill_null(0.0).alias("r"))
        .with_columns(pl.col("factor").rank("average").over("date").alias("rank"))
        .with_columns(
            (pl.col("rank") - pl.col("rank").mean().over("date")).alias("w"),
            (pl.col("r") - pl.col("r").mean().over("date", "group")).alias("r_ind"),
        )
    )
    daily = (
        f.group_by("date")
        .agg([
            (pl.col("w") * pl.col("r")).sum().truediv(pl.col("w").abs().sum()).alias("fw_ls"),
            (pl.col("w").clip(0, None) * pl.col("r")).sum()
            .truediv(pl.col("w").clip(0, None).sum()).alias("fw_raw"),
            (pl.col("w") * pl.col("r_ind")).sum().truediv(pl.col("w").abs().sum()).alias("fw_ind"),
            (pl.col("w").clip(0, None) * pl.col("r_ind")).sum()
            .truediv(pl.col("w").clip(0, None).sum()).alias("fw_ind_raw"),
            pl.col("r").mean().alias("eq_raw"),
            pl.col("r_ind").mean().alias("eq_ind"),
            (pl.col("r").filter(pl.col("factor_quantile") == spec.quantiles).mean()
             - pl.col("r").filter(pl.col("factor_quantile") == 1).mean()).alias("ls"),
            pl.col("r").filter(pl.col("factor_quantile") == spec.quantiles).mean().alias("top_raw"),
            pl.col("r").filter(pl.col("factor_quantile") == 1).mean().alias("bottom_raw"),
            (pl.col("r_ind").filter(pl.col("factor_quantile") == spec.quantiles).mean()
             - pl.col("r_ind").filter(pl.col("factor_quantile") == 1).mean()).alias("ls_ind"),
            pl.col(f"d{spec.periods[0]}").mean().alias("hold"),
            (pl.col("r").mean()).alias("mkt"),
        ])
        .sort("date")
    )
    return daily


def factor_returns(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """因子收益（factor_returns）：每周期 12 个日度 + 12 个累计序列

    日度序列（权重/组合）：fw_ls/fw_raw/fw_ind/fw_ind_raw（因子加权）、
    eq_raw/eq_ind（等权）、ls/top_raw/bottom_raw/ls_ind（分位多空）、
    hold（全截面持有）、mkt（市场等权）；累计 = (1+daily) 累乘。
    """
    goods = data.filter(pl.col("sample") == 1)
    out: dict[str, pl.DataFrame] = {}
    for d in spec.periods:
        cols = ["fw_ls", "fw_raw", "fw_ind", "fw_ind_raw", "eq_raw", "eq_ind",
                "ls", "top_raw", "bottom_raw", "ls_ind", "hold", "mkt"]
        daily = _factor_weight_returns(goods, spec)
        cum = daily.select(
            ["date"] + [((1 + pl.col(c)).cum_prod() - 1).alias(f"{c}_cum") for c in cols]
        )
        out[f"fr_d{d}"] = daily.join(cum, on="date", how="left")
    return out


def bucket_turnover(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """分组换手率（bucket_turnover）：date×quantile 内样本在 d 期后仍在同组的占比"""
    goods = data.filter(pl.col("sample") == 1).sort(["sym", "date"])
    out: dict[str, pl.DataFrame] = {}
    for d in spec.periods:
        tr = (
            goods.with_columns(
                pl.col("factor_quantile").shift(-d).over("sym").alias("q_fwd")
            )
            .filter(pl.col("q_fwd").is_not_null())
            .group_by(["date", "factor_quantile"])
            .agg((pl.col("factor_quantile") == pl.col("q_fwd")).mean().alias(f"TR(d{d})"))
            .sort(["date", "factor_quantile"])
        )
        out[f"tr_d{d}"] = tr
    return out


def autocorrelation(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """自相关（autocorrelation）：date 截面上 factor 与其 d 期滞后/前推的相关系数"""
    goods = data.filter(pl.col("sample") == 1).sort(["sym", "date"])
    out: dict[str, pl.DataFrame] = {}
    for d in spec.periods:
        ac = (
            goods.with_columns(
                pl.col("factor").shift(-d).over("sym").alias("f_fwd")
            )
            .group_by("date")
            .agg([
                pl.corr("factor", "f_fwd").alias(f"AC(d{d})"),
                pl.corr("factor", "f_fwd", method="spearman").alias(f"RankAC(d{d})"),
            ])
            .sort("date")
        )
        out[f"ac_d{d}"] = ac
    return out


def ic(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """IC 测试：date 截面 factor 与 d 期前向收益的相关（IC / RankIC / GIC / RankGIC）

    GIC（group IC）= 组内去均值后（factor 与 forward return 各自减 date×group 均值）
    的截面相关。
    """
    goods = data.filter(pl.col("sample") == 1)
    out: dict[str, pl.DataFrame] = {}
    for d in spec.periods:
        dcol = f"d{d}"
        ics = (
            goods.with_columns(
                (pl.col("factor") - pl.col("factor").mean().over("date", "group")).alias("factor_g"),
                (pl.col(dcol) - pl.col(dcol).mean().over("date", "group")).alias(f"{dcol}_g"),
            )
            .group_by("date")
            .agg([
                pl.corr("factor", dcol).alias(f"IC(d{d})"),
                pl.corr("factor", dcol, method="spearman").alias(f"RankIC(d{d})"),
                pl.corr("factor_g", f"{dcol}_g").alias(f"GIC(d{d})"),
                pl.corr("factor_g", f"{dcol}_g", method="spearman").alias(f"RankGIC(d{d})"),
            ])
            .sort("date")
        )
        out[f"ic_d{d}"] = ics
    return out


def coverage(data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    """覆盖率（coverage）：每 date 截面的样本/因子覆盖率

    - SF2S：有效因子样本 / 样本数（sample>0）
    - F2T：有效因子 / 全部标的（含非样本）
    - S2T：样本数 / 全部标的
    - X2S：因子为空剔除样本 / 样本数
    """
    cvg = (
        data.with_columns(
            (pl.col("factor").is_not_null() & (pl.col("sample") > 0)).cast(pl.Int32).alias("f_ok"),
            (pl.col("sample") > 0).cast(pl.Int32).alias("s_ok"),
            (pl.col("sample") == -1).cast(pl.Int32).alias("x_ok"),
        )
        .group_by("date")
        .agg([
            pl.len().alias("total"),
            pl.col("s_ok").sum().alias("SNo"),
            pl.col("f_ok").sum().alias("FNo"),
            pl.col("x_ok").sum().alias("XNo"),
        ])
        .with_columns([
            (pl.col("FNo") / pl.col("SNo")).alias("SF2S"),
            (pl.col("FNo") / pl.col("total")).alias("F2T"),
            (pl.col("SNo") / pl.col("total")).alias("S2T"),
            (pl.col("XNo") / pl.col("SNo")).alias("X2S"),
        ])
        .select(["date", "SF2S", "F2T", "S2T", "X2S"])
        .sort("date")
    )
    return {"cvg_date": cvg}


TESTERS = {
    "bucket_returns": bucket_returns,
    "factor_returns": factor_returns,
    "bucket_turnover": bucket_turnover,
    "autocorrelation": autocorrelation,
    "ic": ic,
    "coverage": coverage,
}

TESTER_KINDS = tuple(sorted(TESTERS))


def run_tester(kind: str, data: pl.DataFrame, spec: FactorTesterSpec) -> dict[str, pl.DataFrame]:
    fn = TESTERS.get(kind)
    if fn is None:
        raise ValueError(f"未知测试器: {kind}（可用: {', '.join(TESTER_KINDS)}）")
    return fn(data, spec)


__all__ = ["prepare_factor_data", "run_tester", "TESTERS", "TESTER_KINDS"]