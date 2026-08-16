"""存储层：读取接口——目录（hive 分区还原）/ 单文件 parquet → LazyFrame。

从 graph/materialize.py 的 ``scan_materialized`` 泛化——资产层读取物化/源头
数据的唯一入口（panel/sample/fieldset/factor/tester 的增量旧桶读取、get 读取
统一走这里），替换底层引擎（polars → DuckDB 等）时只改本层内部。
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from .query import to_expr


def scan(root: Path | str | list | tuple, *, partition: str | None = None,
         columns: list[str] | None = None, where=None,
         exclude: tuple[str, ...] = ("part",)) -> pl.LazyFrame:
    """读取 parquet 数据（lazy）：目录（hive 分区还原）/ 单文件 / 文件列表。

    - ``root`` 为目录：``scan_parquet(dir, hive_partitioning=True)``（多文件/分区
      目录）；为单文件：直接扫描该文件；为路径列表：批量扫描（catalog 文件级
      裁剪后的场景）；
    - ``partition``：按内部物化桶列 ``part`` 前缀过滤（如 ``"2024"`` 取
      ``part=2024`` 桶——物化时间桶布局，见 ``write_all``）；
    - ``where``：字符串谓词（见 ``to_expr``）或 pl.Expr 过滤；
    - ``columns``：列裁剪（先于 where 应用）；
    - ``exclude``：剔除内部列（默认物化桶列 ``part``——对外列集合与实时视图
      一致；传 ``()`` 保留）。
    """
    if isinstance(root, (list, tuple)):
        lf = pl.scan_parquet([str(p) for p in root], hive_partitioning=True)
    elif Path(root).is_file():
        lf = pl.scan_parquet(root)
    else:
        lf = pl.scan_parquet(root, hive_partitioning=True)
    if partition is not None:
        lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    if columns is not None:
        lf = lf.select(*columns)
    elif exclude:
        lf = lf.select(pl.all().exclude(*exclude))
    return lf


def row_count(path: Path) -> int:
    """parquet 行数（只读 footer 元数据，不读数据页；失败返回 0）。"""
    try:
        return pl.scan_parquet(path).select(pl.len()).collect().item()
    except Exception:
        return 0


__all__ = ["scan", "row_count"]
