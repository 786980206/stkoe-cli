"""mock 演示数据生成：把 ``scripts/gen_example_data.py`` 的造数能力内建为 stkoe 命令

参考 v1.0 ``data/mock.py`` 的生成器设计（tdcal/common/index/feature/klday + write），
生成器只产出 polars DataFrame，``write`` 把表写到 ``<data_dir>/tables/<name>/``。

- ``stkoe mock demo``：生成 example.md 演练用的两张演示源表 index + m1
  （默认 300 只股票 × 500 个交易日 = 15 万行；``--n-syms/--n-days`` 可调）
  到 ``tables/index/`` + ``tables/m1/``
- ``stkoe mock gen <name> --kind <kind>``：参数化生成单张表（kind：
  tdcal / common / index / feature / klday / m1）

mock 只写磁盘 parquet、**不做 catalog 注册**——stkoe 是「发现资产」语义，
写盘后仍需 ``table add <name>`` 登记（与 example.md §1 一致）。
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl

# 申万一级行业（与 v1.0 plugins/mock 一致）
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

# ---- example.md 演示数据参数（默认 300 只 × 500 个交易日 = 15 万行）----

DEMO_N_SYMS = 300
DEMO_N_DAYS = 500


def _dates(start: str = "2024-01-01", end: str = "2024-01-03",
           n_days: int | None = None) -> pl.Series:
    """交易日序列（简化：只保留周一~周五的自然日）

    日期用**字符串**形态（如 ``"2024-01-01"``）：sample/feature 的公式过滤
    形如 ``(date >= '2024-01-02')``，与 tests/test_sample.py 的约定一致。

    ``n_days`` 指定从 ``start`` 起向后数的交易日个数，优先于 ``end``。
    """
    s = dt.date.fromisoformat(start)
    if n_days is not None:
        days: list[str] = []
        d = s
        while len(days) < n_days:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d += dt.timedelta(days=1)
        return pl.Series("date", days, dtype=pl.Utf8)
    e = dt.date.fromisoformat(end)
    days = [d for i in range((e - s).days + 1)
            if (d := s + dt.timedelta(days=i)).weekday() < 5]
    return pl.Series("date", [d.isoformat() for d in days], dtype=pl.Utf8)


def _syms(n: int) -> list[str]:
    """生成 n 个股票代码（.SZ/.SH 各半）"""
    half = n // 2
    return [f"{i + 1:06d}.SZ" for i in range(half)] + \
           [f"6{i:05d}.SH" for i in range(n - half)]


def _panel(syms: list[str], start: str, end: str, per_sym: dict[str, dict],
           n_days: int | None = None) -> pl.DataFrame:
    """把 per_sym 的 {sym: [len(dates)] 或 标量} 列展开为 date × sym 长表"""
    dates = _dates(start, end, n_days=n_days)
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


def tdcal(start: str = "2024-01-01", end: str = "2024-01-03",
          n_days: int | None = None) -> pl.DataFrame:
    """交易日历（date）"""
    return _dates(start, end, n_days=n_days).to_frame()


def common(n_syms: int = 10, start: str = "2024-01-01", end: str = "2024-01-03",
           n_days: int | None = None, seed: int = 42) -> pl.DataFrame:
    """公共数据：date/sym/ic（行业分类，每 sym 固定）"""
    rng = np.random.default_rng(seed)
    syms = _syms(n_syms)
    ics = rng.integers(0, len(INDUSTRIES), size=n_syms)
    ic_map = {s: INDUSTRIES[ics[i]] for i, s in enumerate(syms)}
    return _panel(syms, start, end, {"ic": ic_map}, n_days=n_days)


def index(n_syms: int = 10, start: str = "2024-01-01", end: str = "2024-01-03",
          n_days: int | None = None, seed: int = 1) -> pl.DataFrame:
    """索引面板：date/sym/r/ic/fv/x（作 dataset 的 index 表 + 因子测试必需列）"""
    syms = _syms(n_syms)
    rng = np.random.default_rng(seed)
    n_days_ = len(_dates(start, end, n_days=n_days))
    ic_map = {s: str(c) for s, c in
              zip(syms, rng.choice(["G1", "G2"], size=n_syms))}
    fv_map = {s: float(v) for s, v in
              zip(syms, rng.uniform(1.0, 10.0, size=n_syms))}
    x_map = {s: float(v) for s, v in
             zip(syms, rng.uniform(1.0, 10.0, size=n_syms))}
    r_per_sym = {s: rng.normal(0, 0.03, n_days_) for s in syms}
    return _panel(syms, start, end,
                  {"r": r_per_sym, "ic": ic_map, "fv": fv_map, "x": x_map},
                  n_days=n_days)


def m1(n_syms: int = 10, start: str = "2024-01-01", end: str = "2024-01-03",
       n_days: int | None = None, seed: int = 42) -> pl.DataFrame:
    """证券资料：date/sym/name/industry（与 index 同键可 join）"""
    syms = _syms(n_syms)
    rng = np.random.default_rng(seed)
    names = {s: f"股票{i + 1:06d}" for i, s in enumerate(syms)}
    inds = {s: INDUSTRIES[int(rng.integers(0, len(INDUSTRIES)))] for s in syms}
    return _panel(syms, start, end, {"name": names, "industry": inds},
                  n_days=n_days)


def feature(name: str = "x", n_syms: int = 10, start: str = "2024-01-01",
            end: str = "2024-01-03", n_days: int | None = None,
            seed: int | None = None) -> pl.DataFrame:
    """特征数据：date/sym/{name}（每 sym 固定均值/波动率，截面有区分度）"""
    syms = _syms(n_syms)
    rng = np.random.default_rng(seed if seed is not None
                                else abs(hash(name)) % (2**31))
    means = rng.standard_normal(n_syms) * 0.1
    stds = rng.uniform(0.3, 1.0, size=n_syms)
    n_days_ = len(_dates(start, end, n_days=n_days))
    cols = {
        s: rng.standard_normal(n_days_) * stds[i] + means[i]
        for i, s in enumerate(syms)
    }
    return _panel(syms, start, end, {name: cols}, n_days=n_days)


def klday(n_syms: int = 10, start: str = "2024-01-01", end: str = "2024-01-03",
          n_days: int | None = None, seed: int = 12345) -> pl.DataFrame:
    """行情面板：date/sym/r/ic/fv/sample/optime（optime 为工具字段）"""
    syms = _syms(n_syms)
    dates = _dates(start, end, n_days=n_days)
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


# ---- 演示数据（example.md 用）----


def demo_index(n_syms: int = DEMO_N_SYMS, n_days: int = DEMO_N_DAYS,
               seed: int = 1) -> pl.DataFrame:
    """演示 index 表（默认 300 只 × 500 个交易日 = 15 万行）：sym/date/r/ic/fv/x"""
    return index(n_syms=n_syms, n_days=n_days, seed=seed)


def demo_m1(n_syms: int = DEMO_N_SYMS, n_days: int = DEMO_N_DAYS,
            seed: int = 42) -> pl.DataFrame:
    """演示 m1 表（默认 15 万行）：sym/date/name/industry"""
    return m1(n_syms=n_syms, n_days=n_days, seed=seed)


# ---- 写盘 ----

def resolve_data_dir(data_dir=None) -> Path:
    """生效数据目录：显式 data_dir > 配置 data_dir（与 dispatch 的 data_dir 透传一致）"""
    if data_dir:
        return Path(data_dir).expanduser()
    from ..settings import load_config

    return Path(load_config().data_dir).expanduser()


def write(data_dir, name: str, df: pl.DataFrame) -> dict:
    """写 ``tables/<name>/data.parquet``（不注册，供 table add 发现）"""
    d = resolve_data_dir(data_dir) / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    path = d / "data.parquet"
    df.write_parquet(path)
    return {"name": name, "path": str(path), "rows": df.height,
            "columns": df.columns}


def demo(data_dir=None, n_syms: int = DEMO_N_SYMS,
         n_days: int = DEMO_N_DAYS) -> list[dict]:
    """生成 example.md 演示源表 index + m1，返回写入清单"""
    return [write(data_dir, "index", demo_index(n_syms=n_syms, n_days=n_days)),
            write(data_dir, "m1", demo_m1(n_syms=n_syms, n_days=n_days))]


def gen(name: str, kind: str, *, data_dir=None, n_syms: int = 10,
        start: str = "2024-01-01", end: str = "2024-01-03",
        n_days: int | None = None, seed: int | None = None,
        col: str | None = None) -> dict:
    """参数化生成单张表并写盘（kind：tdcal/common/index/feature/klday/m1）"""
    kw = {"n_syms": int(n_syms), "start": start, "end": end, "n_days": n_days}
    if kind == "tdcal":
        df = tdcal(start, end, n_days=n_days)
    elif kind == "common":
        df = common(seed=42 if seed is None else seed, **kw)
    elif kind == "index":
        df = index(seed=1 if seed is None else seed, **kw)
    elif kind == "feature":
        df = feature(col or "x", seed=seed, **kw)
    elif kind == "klday":
        df = klday(seed=12345 if seed is None else seed, **kw)
    elif kind == "m1":
        df = m1(seed=42 if seed is None else seed, **kw)
    else:
        raise ValueError(f"未知 mock kind: {kind}（可用: "
                         "tdcal/common/index/feature/klday/m1）")
    return write(data_dir, name, df)


__all__ = ["INDUSTRIES", "tdcal", "common", "index", "m1", "feature", "klday",
           "demo", "demo_index", "demo_m1", "write", "gen", "resolve_data_dir"]
