"""统计计算：calc_stats（分组/非分组列统计）+ calc_storage（表文件存续信息）

- calc_stats（要素对齐 v1.0 stat 模块），输出列（ALL_COLS）：
    group | field | data_type | count | null_count | nunique |
    min | q25 | q50 | q75 | max | mean | min_date | max_date
- calc_storage（``stat scan --kind storage``），输出列（STORAGE_COLS）：
    partition_by | partition_value | storage_size | file_no

内存策略（大表 coverage 统计）：
- 计算全程 LazyFrame；聚合阶段用流式（controller 侧 ``sink_parquet`` 落盘）
- **按 dtype 类别聚合、再对小的聚合结果 unpivot**：多列一次性 group_by 得到
  每列每指标一列的窄表，再对窄表按指标逐列 unpivot + join 拼成 ALL_COLS 长表，
  避免对原始数据 unpivot（行数×列数 的内存爆炸）
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


# ---------- calc_stats（流式友好实现） ----------

def _metric_specs(kind: str, c: str, st: pl.DataType | None = None) -> list[tuple[str, str, pl.Expr]]:
    """某 dtype 类别单列的指标表达式列表：``(输出列名, 聚合别名后缀, 表达式)``

    别名 ``{c}#{suffix}`` 参与窄表聚合与后续 unpivot 字段还原（``field`` 值回
    映射为原始列名）。``st`` 为数值类的 supertype：逐列先 cast 再聚合，与旧实现
    （unpivot 列超类型提升）的数值口径一致。
    """
    if kind == "numeric":
        col = pl.col(c) if st is None else pl.col(c).cast(st)
        return [
            ("null_count", "null", col.null_count()),
            ("min", "min", col.min()),
            ("q25", "q25", col.quantile(0.25)),
            ("q50", "q50", col.median()),
            ("q75", "q75", col.quantile(0.75)),
            ("max", "max", col.max()),
            ("mean", "mean", col.mean()),
            ("nunique", "nuniq", col.n_unique()),
        ]
    if kind == "string":
        return [
            ("null_count", "null", pl.col(c).null_count()),
            ("nunique", "nuniq", pl.col(c).n_unique()),
        ]
    dt = pl.col(c).cast(pl.Datetime, strict=False)
    return [
        ("null_count", "null", dt.null_count()),
        ("min_date", "min", dt.min()),
        ("max_date", "max", dt.max()),
        ("nunique", "nuniq", dt.n_unique()),
    ]


def _class_stats(base: pl.LazyFrame, g: str, cols: list[str],
                 kind: str) -> pl.LazyFrame:
    """同一 dtype 类别的分组统计（流式友好）

    多列一次性 ``group_by`` 聚合（每列每指标一个聚合列），得到按组行数=组数的窄表；
    再对窄表按指标逐列 unpivot 成 ``(g, count, field, <metric>)`` 并以
    ``(g, count, field)`` 内 join 拼成一行一字段的统计行。窄表行数=组数（小），
    全程不对原始数据 unpivot。
    """
    st = None
    if kind == "numeric":
        try:
            st = base.select(cols).unpivot(on=cols).collect_schema().get("value")
        except Exception:
            st = None
        if st == pl.Object:
            st = None
    aggs: list[pl.Expr] = [pl.len().alias("_count")]
    for c in cols:
        for _, suffix, expr in _metric_specs(kind, c, st):
            aggs.append(expr.alias(f"{c}#{suffix}"))
    small = base.select([g, *cols]).group_by(g).agg(*aggs)

    metrics = [(m, s) for m, s, _ in _metric_specs(kind, cols[0], st)]
    parts: list[pl.LazyFrame] = []
    for metric, suffix in metrics:
        mcols = [f"{c}#{suffix}" for c in cols]
        parts.append(
            small.select([g, "_count", *mcols])
            .unpivot(index=[g, "_count"], on=mcols,
                     variable_name="field", value_name=metric)
            .with_columns(pl.col("field").replace_strict(
                {f"{c}#{suffix}": c for c in cols})))
    joined = parts[0]
    for p in parts[1:]:
        joined = joined.join(p, on=[g, "_count", "field"], how="inner")
    joined = joined.with_columns(pl.col("_count").alias("count"))

    if kind == "numeric":
        joined = joined.with_columns([
            pl.lit("numeric").alias("data_type"),
            pl.col("min").cast(pl.String).alias("min"),
            pl.col("max").cast(pl.String).alias("max"),
            pl.lit(None, dtype=pl.String).alias("min_date"),
            pl.lit(None, dtype=pl.String).alias("max_date"),
        ])
    elif kind == "string":
        joined = joined.with_columns([
            pl.lit("string").alias("data_type"),
            pl.lit(None, dtype=pl.String).alias("min"),
            pl.lit(None, dtype=pl.String).alias("max"),
            pl.lit(None, dtype=pl.String).alias("min_date"),
            pl.lit(None, dtype=pl.String).alias("max_date"),
            pl.lit(None, dtype=pl.Float64).alias("q25"),
            pl.lit(None, dtype=pl.Float64).alias("q50"),
            pl.lit(None, dtype=pl.Float64).alias("q75"),
            pl.lit(None, dtype=pl.Float64).alias("mean"),
        ])
    else:
        joined = joined.with_columns([
            pl.lit("temporal").alias("data_type"),
            pl.col("min_date").cast(pl.String).alias("min_date"),
            pl.col("max_date").cast(pl.String).alias("max_date"),
            pl.lit(None, dtype=pl.String).alias("min"),
            pl.lit(None, dtype=pl.String).alias("max"),
            pl.lit(None, dtype=pl.Float64).alias("q25"),
            pl.lit(None, dtype=pl.Float64).alias("q50"),
            pl.lit(None, dtype=pl.Float64).alias("q75"),
            pl.lit(None, dtype=pl.Float64).alias("mean"),
        ])
    return joined.select([g, *ALL_COLS[1:]])


def calc_stats(data: pl.LazyFrame | pl.DataFrame, group_col: str | None = None) -> pl.LazyFrame:
    """计算所有列的统计信息（支持分组/非分组；返回 LazyFrame）

    ``group_col`` 为 None 时全量一行 ``group=all``；否则按该列不同取值各一组，
    分组列名作为首列列名。数值/字符串/时间三类各做一次流式聚合（见 _class_stats），
    结果可直接 ``collect(engine="streaming")`` 或 ``sink_parquet``。
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
        base = lf.with_columns(pl.lit("all").alias(g))
    else:
        base = lf.with_columns(pl.col(group_col).cast(pl.String, strict=False).alias(g))

    stats: list[pl.LazyFrame] = []
    if numeric_cols:
        stats.append(_class_stats(base, g, numeric_cols, "numeric"))
    if string_cols:
        stats.append(_class_stats(base, g, string_cols, "string"))
    if temporal_cols:
        stats.append(_class_stats(base, g, temporal_cols, "temporal"))

    if stats:
        result = pl.concat(stats, how="vertical")
    else:
        result = pl.LazyFrame({g: [], **{c: [] for c in ALL_COLS[1:]}})
    gname = group_col if has_group else "group"
    result = result.rename({g: gname})
    order = {col: i for i, col in enumerate(schema.names())}
    result = result.with_columns(
        pl.col("field").replace_strict(order, default=999).alias("_order")
    ).sort(["_order", gname]).drop("_order")
    return result
