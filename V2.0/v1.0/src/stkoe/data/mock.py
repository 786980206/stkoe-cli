"""mock 数据生成工具：参数化生成演示数据并写入 tables/ 后 scan 注册

用法（CLI）：``stkoe mock <name> [--kind klday|tdcal|feature|common]``
用法（SDK）：``data.mock.write("demo", data.mock.klday(n_syms=50), partition_by="year")``
"""
import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl

from . import get_root
from .catalog.spec import TableScanReport
from .table import scan

# 申万一级行业（与旧 plugins/mock 一致）
INDUSTRIES = [
    "/inc/sw2021/农林牧渔", "/inc/sw2021/基础化工", "/inc/sw2021/钢铁",
    "/inc/sw2021/有色金属", "/inc/sw2021/电子", "/inc/sw2021/汽车",
    "/inc/sw2021/家用电器", "/inc/sw2021/食品饮料", "/inc/sw2021/纺织服饰",
    "/inc/sw2021/轻工制造", "/inc/sw2021/医药生物", "/inc/sw2021/公用事业",
    "/inc/sw2021/交通运输", "/inc/sw2021/房地产", "/inc/sw2021/商贸零售",
    "/inc/sw2021/社会服务", "/inc/sw2021/银行", "/inc/sw2021/非银金融",
    "/inc/sw2021/综合", "/inc/sw2021/建筑材料", "/inc/sw2021/建筑装饰",
    "/inc/sw2021/电力设备", "/inc/sw2021/国防军工", "/inc/sw2021/计算机",
    "/inc/sw2021/传媒", "/inc/sw2021/通信", "/inc/sw2021/煤炭",
    "/inc/sw2021/石油石化", "/inc/sw2021/环保", "/inc/sw2021/美容护理",
    "/inc/sw2021/机械设备",
]


def _dates(start: str = "2020-01-01", end: str = "2023-12-31") -> pl.Series:
    """自然日序列（简化：不剔除节假日，仅周一~周五）"""
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)
    days = [d for i in range((e - s).days + 1) if (d := s + dt.timedelta(days=i)).weekday() < 5]
    return pl.Series("date", days, dtype=pl.Date)


def _syms(n: int) -> list[str]:
    """生成 n 个股票代码（.SZ/.SH 各半）"""
    half = n // 2
    return [f"{i + 1:06d}.SZ" for i in range(half)] + [f"6{i:05d}.SH" for i in range(n - half)]


def tdcal(start: str = "2020-01-01", end: str = "2023-12-31") -> pl.DataFrame:
    """交易日历（date）"""
    return _dates(start, end).to_frame()


def common(n_syms: int = 100, start: str = "2020-01-01", end: str = "2023-12-31",
           seed: int = 42) -> pl.DataFrame:
    """公共数据：date, sym, ic（行业）"""
    rng = np.random.default_rng(seed)
    syms = _syms(n_syms)
    ics = rng.integers(0, len(INDUSTRIES), size=n_syms)
    ic_map = {s: INDUSTRIES[ics[i]] for i, s in enumerate(syms)}
    return _panel(syms, start, end, {"ic": {s: ic_map[s] for s in syms}})


def index(n_syms: int = 100, start: str = "2020-01-01", end: str = "2023-12-31") -> pl.DataFrame:
    """索引面板：date, sym 两列（股票池 × 交易日，作 dataset 的 index 表）"""
    return _panel(_syms(n_syms), start, end, {})


def feature(name: str, n_syms: int = 100, start: str = "2020-01-01", end: str = "2023-12-31",
            seed: int | None = None) -> pl.DataFrame:
    """特征数据：date, sym, {name}（每 sym 固定均值/波动率，截面有区分度）"""
    syms = _syms(n_syms)
    rng = np.random.default_rng(seed if seed is not None else abs(hash(name)) % (2**31))
    means = rng.standard_normal(n_syms) * 0.1
    stds = rng.uniform(0.3, 1.0, size=n_syms)
    cols = {
        s: rng.standard_normal(_n_days(start, end)) * stds[i] + means[i]
        for i, s in enumerate(syms)
    }
    return _panel(syms, start, end, {name: cols})


def klday(n_syms: int = 100, start: str = "2020-01-01", end: str = "2023-12-31",
          seed: int = 12345) -> pl.DataFrame:
    """行情面板：date, sym, r, ic, fv, sample, optime（optime 为工具字段，默认 ignore_cols）"""
    syms = _syms(n_syms)
    dates = _dates(start, end)
    n_dates, n_s = len(dates), len(syms)
    rng = np.random.default_rng(seed)

    r_matrix = rng.normal(0, 0.03, size=(n_dates, n_s))
    limit_mask = rng.random(size=(n_dates, n_s)) < 0.02
    r_matrix[limit_mask] = rng.choice([-0.10, 0.10], size=limit_mask.sum())

    base_mv = np.exp(rng.normal(4.0, 1.5, size=n_s))
    cum = np.cumprod(1 + r_matrix, axis=0)
    fv_matrix = base_mv[None, :] * cum

    rng2 = np.random.default_rng(seed + 1)
    ic_idx = rng2.integers(0, len(INDUSTRIES), size=n_s)
    sample_matrix = (rng2.random(size=(n_dates, n_s)) < 0.95).astype(np.int8)
    optime = dt.datetime(2026, 8, 6, 8, 0, 0)

    dates_arr = np.repeat(dates.to_numpy(), n_s)
    syms_arr = np.tile(np.array(syms), n_dates)
    ic_arr = np.tile(np.array([INDUSTRIES[i] for i in ic_idx]), n_dates)

    return pl.DataFrame({
        "date": dates_arr,
        "sym": syms_arr,
        "r": r_matrix.ravel(),
        "ic": ic_arr,
        "fv": fv_matrix.ravel(),
        "sample": sample_matrix.ravel(),
        "optime": optime,
    })


def _n_days(start: str, end: str) -> int:
    return len(_dates(start, end))


def _panel(syms: list[str], start: str, end: str, per_sym: dict[str, dict]) -> pl.DataFrame:
    """把 per_sym 的 {sym: [len(dates)] 或 标量} 列展开为长表（date × sym）"""
    dates = _dates(start, end)
    n_dates, n_s = len(dates), len(syms)
    dates_arr = np.repeat(dates.to_numpy(), n_s)
    syms_arr = np.tile(np.array(syms), n_dates)
    df = pl.DataFrame({"date": dates_arr, "sym": syms_arr})
    for name, cols in per_sym.items():
        first = cols[syms[0]]
        if isinstance(first, str) or np.isscalar(first):
            values = np.tile(np.array([cols[s] for s in syms]), n_dates)
        else:
            values = np.array([cols[s] for s in syms]).T.ravel()
        df = df.with_columns(pl.Series(name, values))
    return df


def write(name: str, df: pl.DataFrame, *, partition_by: str | None = None) -> TableScanReport:
    """写 tables/<name>/ 并 scan 注册（返回 TableScanReport）"""
    d = get_root() / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    if partition_by:
        if partition_by not in df.columns and "date" in df.columns:
            df = df.with_columns(pl.col("date").dt.year().alias(partition_by))
        df.write_parquet(d / "data", partition_by=[partition_by])
    else:
        df.write_parquet(d / f"{name}.parquet")
    return scan(name)


def write_demo(root: Path | None = None, *, n_syms: int = 100,
               start: str = "2020-01-01", end: str = "2023-12-31",
               seed: int = 12345) -> list[TableScanReport]:
    """生成一套演示表：mock_tdcal / mock_common / mock_klday / mock_feature"""
    from . import configure
    if root is not None:
        configure(root)
    reports = [
        write("mock_tdcal", tdcal(start, end)),
        write("mock_common", common(n_syms, start, end, seed)),
        write("mock_klday", klday(n_syms, start, end, seed), partition_by="year"),
        write("mock_feature", feature("zscore", n_syms, start, end, seed + 2)),
    ]
    return reports
