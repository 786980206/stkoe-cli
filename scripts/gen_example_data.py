"""example.md 的 mock 造数脚本：生成演示 parquet 源表到 <data-dir>/tables/。

用法（在仓库根目录，data-dir 与 stkoe 配置一致）：
    uv run python scripts/gen_example_data.py [data-dir]

data-dir 缺省取 ./example-data；生成 index（行情）+ m1（证券资料）两张表。
"""
from __future__ import annotations

import pathlib
import sys

import polars as pl


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    root = pathlib.Path(args[0]) if args else pathlib.Path("example-data")
    tables = root / "tables"
    (tables / "index").mkdir(parents=True, exist_ok=True)
    (tables / "m1").mkdir(parents=True, exist_ok=True)

    syms = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ",
            "000006.SZ", "000007.SZ", "000008.SZ", "000009.SZ", "000010.SZ"]
    dates = ["2024-01-01"] * 10 + ["2024-01-02"] * 10 + ["2024-01-03"] * 10
    pl.DataFrame({
        "sym": syms * 3,
        "date": dates,
        "r": [0.01, -0.02, 0.03, -0.01, 0.02, 0.00, 0.01, -0.03, 0.02, 0.01] * 3,
        "ic": ["G1", "G1", "G1", "G2", "G2", "G1", "G2", "G1", "G2", "G2"] * 3,
        "fv": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0] * 3,
        "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0] * 3,
    }).write_parquet(tables / "index" / "data.parquet")

    names = ["平安银行", "万科A", "国农科技", "国华网安", "ST星源",
             "深振业A", "全新好", "神州高铁", "中国宝安", "深康佳A"]
    industries = ["银行", "地产", "农业", "软件", "综合",
                  "地产", "综合", "机械", "综合", "家电"]
    pl.DataFrame({
        "sym": syms * 3,
        "date": dates,
        "name": names * 3,
        "industry": industries * 3,
    }).write_parquet(tables / "m1" / "data.parquet")

    print(f"mock tables written: {tables / 'index' / 'data.parquet'}, "
          f"{tables / 'm1' / 'data.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
