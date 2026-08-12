"""统计计算：calc_stats（分组/非分组列统计）+ calc_storage（表文件存续信息）

- calc_stats（要素对齐 v1.0 stat 模块），输出列（ALL_COLS）：
    group | field | data_type | count | null_count | nunique |
    min | q25 | q50 | q75 | max | mean | min_date | max_date
- calc_storage（``stat scan --kind storage``），输出列（STORAGE_COLS）：
    partition_by | partition_value | storage_size | file_no
"""
from __future__ import annotations

from pathlib import PurePosixPath

import polars as pl

ALL_COLS = ["group", "field", "data_type", "count", "null_count", "nunique",
            "min", "q25", "q50", "q75", "max", "mean", "min_date", "max_date"]

STORAGE_COLS = ["partition_by", "partition_value", "storage_size", "file_no"]


def _hive_value(rel: str, key: str) -> str:
    """从相对路径提取 hive 分区值（``year=2024/month=1/data.parquet`` + key=year → ``2024``）"""
    for part in PurePosixPath(rel).parts[:-1]:
        if part.startswith(key + "="):
            return part.split("=", 1)[1]
    return ""


def calc_storage(files: list[tuple[str, int]], group_key: str | None = None) -> pl.DataFrame:
    """表文件存续信息（按 hive 分区键/值聚合存储占用与文件数）

    ``files``：``[(rel_path, size)]``（相对表根）；``group_key`` 为 None 时输出
    全表一行 ``partition_by=__all__ / partition_value=__all__``；否则按该分区键的目录值
    各一行（值取 hive 目录 ``key=value`` 的 value）。输出列见 STORAGE_COLS。
    """
    rows: list[tuple[str, str, int, int]] = []
    if group_key is None:
        rows.append(("__all__", "__all__", sum(s for _, s in files), len(files)))
    else:
        by: dict[str, list[int]] = {}
        for rel, size in files:
            by.setdefault(_hive_value(rel, group_key), []).append(size)
        for val in sorted(by):
            sizes = by[val]
            rows.append((group_key, val, sum(sizes), len(sizes)))
    df = pl.DataFrame(rows, schema=STORAGE_COLS, orient="row")
    return df.with_columns(pl.col("storage_size").cast(pl.Int64),
                           pl.col("file_no").cast(pl.Int64))


def calc_stats(data: pl.LazyFrame | pl.DataFrame, group_col: str | None = None) -> pl.LazyFrame:
    """计算所有列的统计信息（支持分组/非分组；返回 LazyFrame）

    ``group_col`` 为 None 时全量一行 ``group=all``；否则按该列不同取值各一组，
    分组列名作为首列列名。
    """
    lf = data.lazy() if isinstance(data, pl.DataFrame) else data
    schema = lf.collect_schema()
    numeric_cols = [c for c, d in schema.items() if d.is_numeric()]
    string_cols = [c for c, d in schema.items() if d == pl.String]
    temporal_cols = [c for c, d in schema.items() if d.is_temporal()]
    group_col = group_col or "all"
    has_group = group_col != "all"
    g = "_g"
    if not has_group:
        lf = lf.with_columns(pl.lit("all").alias(g))
    else:
        lf = lf.with_columns(pl.col(group_col).cast(pl.String, strict=False).alias(g))

    stats_list = []
    if numeric_cols:
        stats_list.append(
            lf.unpivot(index=[g], on=numeric_cols, variable_name="field")
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").min().alias("min"), pl.col("value").quantile(0.25).alias("q25"),
                  pl.col("value").median().alias("q50"), pl.col("value").quantile(0.75).alias("q75"),
                  pl.col("value").max().alias("max"), pl.col("value").mean().alias("mean"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("numeric").alias("data_type"),
                           pl.col("min").cast(pl.String).alias("min"),
                           pl.col("max").cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.String).alias("min_date"),
                           pl.lit(None).cast(pl.String).alias("max_date")])
            .select([g, *ALL_COLS[1:]]))
    if string_cols:
        stats_list.append(
            lf.unpivot(index=[g], on=string_cols, variable_name="field")
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("string").alias("data_type"),
                           pl.lit(None).cast(pl.String).alias("min"),
                           pl.lit(None).cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.String).alias("min_date"),
                           pl.lit(None).cast(pl.String).alias("max_date"),
                           pl.lit(None).cast(pl.Float64).alias("q25"),
                           pl.lit(None).cast(pl.Float64).alias("q50"),
                           pl.lit(None).cast(pl.Float64).alias("q75"),
                           pl.lit(None).cast(pl.Float64).alias("mean")])
            .select([g, *ALL_COLS[1:]]))
    if temporal_cols:
        stats_list.append(
            lf.select([g, *temporal_cols])
            .unpivot(index=[g], on=temporal_cols, variable_name="field")
            .with_columns(pl.col("value").cast(pl.Datetime, strict=False))
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").min().alias("min_date"),
                  pl.col("value").max().alias("max_date"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("temporal").alias("data_type"),
                           pl.col("min_date").cast(pl.String).alias("min_date"),
                           pl.col("max_date").cast(pl.String).alias("max_date"),
                           pl.lit(None).cast(pl.String).alias("min"),
                           pl.lit(None).cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.Float64).alias("q25"),
                           pl.lit(None).cast(pl.Float64).alias("q50"),
                           pl.lit(None).cast(pl.Float64).alias("q75"),
                           pl.lit(None).cast(pl.Float64).alias("mean")])
            .select([g, *ALL_COLS[1:]]))
    gname = group_col if has_group else "group"
    if stats_list:
        result = pl.concat(stats_list, how="vertical")
    else:
        result = pl.LazyFrame({g: [], **{c: [] for c in ALL_COLS[1:]}})
    result = result.rename({g: gname})
    order = {col: i for i, col in enumerate(schema.names())}
    result = result.with_columns(
        pl.col("field").replace_strict(order, default=999).alias("_order")
    ).sort(["_order", gname]).drop("_order")
    return result