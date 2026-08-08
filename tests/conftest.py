"""测试夹具：独立数据根目录 + 常用数据生成"""
import sys
from pathlib import Path

import polars as pl
import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import stkoe.data as data  # noqa: E402


@pytest.fixture()
def root(tmp_path):
    """每个测试独立的数据根目录，结束时释放 catalog 连接"""
    root = tmp_path / "local"
    data.configure(root)
    data.init()
    yield root
    data.configure(tmp_path / "gone")


def make_df(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [r[0] for r in rows],
            "sym": [r[1] for r in rows],
            "r": [r[2] for r in rows],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


def write_single(root: Path, name: str, df: pl.DataFrame) -> Path:
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.parquet"
    df.write_parquet(path)
    return path


def write_hive(root: Path, name: str, df: pl.DataFrame, partition_by: str = "year") -> Path:
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    out = d / "data"
    df.with_columns(pl.col("date").dt.year().alias(partition_by)).write_parquet(
        out, partition_by=[partition_by]
    )
    return out
