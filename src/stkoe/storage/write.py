"""存储层：物化写入接口——全量落盘 / 增量桶重写 / flat 增量合并。

从 graph/materialize.py 的 ``write_partitioned``/``rewrite_buckets`` 迁移并
**收拢 4 个资产（panel/fieldset/factor/tester）重复的 flat 增量写回逻辑**
（``write_incremental_flat``）——物化写盘（含时间桶 hive 分区布局）是资产层
调用的标准接口，替换底层引擎（polars → DuckDB 等）时只改本层内部。
"""
from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl


def write_all(df_or_lf: pl.DataFrame | pl.LazyFrame, out_dir: Path,
              partition_keys: list[str],
              gran: str = "", dt_col: str = "",
              *, clean: bool = False) -> None:
    """**全量物化**：按分区键写 hive 目录 ``key=value/``；无分区键 → 单文件。

    ``partition_keys=["part"]``（时间桶）：按 ``materialize_partition`` 粒度从
    ``dt_col`` 提取桶值（yearly→YYYY、monthly→YYYY-MM、daily→YYYY-MM-DD，
    String/ISO 前缀切片）生成 ``part`` 列后写 ``part=<v>/`` 目录。

    **原生 Hive 分区写出**（polars ≥1.43 的 ``pl.PartitionBy``）：一次流式求值
    即按桶落盘——不整表入内存、不逐桶重算上游 lazy 计划（对 LazyFrame 逐桶
    ``filter(...).sink_parquet()`` 会让每桶重新执行整条 join 链，粗桶 × 大表时
    成本随桶数线性放大，曾致 1852 万行 × yearly 37 桶的 panel 全量物化卡死）。
    ``include_key=True``：**文件内保留 part 列**（String，由 with_columns 生成）
    ——hive_partitioning 读取时文件列优先，part 恒为 String；若 include_key=False，
    目录值会被推断为 Int64（如 ``part=2024``），与增量数据（String）类型不一致，
    增量合并/``is_in`` 过滤会失败。

    ``clean=True``（全量语义）：写前清空 ``out_dir``——``PartitionBy`` 只写
    数据里存在的桶、**不删除新数据中已消失的旧桶目录**（整年数据被删后全量
    重写会残留陈旧桶的 phantom 行）；数据为空时落一个保留 schema 的空
    ``data.parquet``（hive 桶目录不存在，读路径不因"无 parquet 文件"报错）。
    """
    empty = isinstance(df_or_lf, pl.DataFrame) and df_or_lf.height == 0
    if clean:
        shutil.rmtree(out_dir, ignore_errors=True)
    lf = df_or_lf.lazy() if isinstance(df_or_lf, pl.DataFrame) else df_or_lf
    out_dir.mkdir(parents=True, exist_ok=True)
    if not partition_keys:
        lf.sink_parquet(out_dir / "data.parquet")
        return
    key = partition_keys[0]
    if key == "part":
        cut = {"yearly": 4, "monthly": 7, "daily": 10}.get(gran, 4)
        lf = lf.with_columns(
            pl.col(dt_col).cast(pl.String).str.slice(0, cut).alias("part"))
    if empty:
        lf.sink_parquet(out_dir / "data.parquet")
        return
    lf.sink_parquet(pl.PartitionBy(out_dir, key=key, include_key=True),
                    mkdir=True)


def write_file(df_or_lf: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    """**单文件写盘**（无分区语义）：自定义文件名场景（如 tester 命名输出）。
    """
    lf = df_or_lf.lazy() if isinstance(df_or_lf, pl.DataFrame) else df_or_lf
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path)


def write_incremental(old: pl.LazyFrame | pl.DataFrame, inc: pl.DataFrame,
                      dt_expr: pl.Expr, pkeys: list[str], out_dir: Path,
                      gran: str, dt_col: str,
                      *, sym_expr: pl.Expr | None = None,
                      sort_cols: list[str] | None = None) -> pl.DataFrame:
    """**增量物化（时间桶分区）**：删受影响桶后，把桶内区间外旧行与增量行合并写回。

    时间桶粒度（yearly/monthly/daily）粗于增量区间（天级）——直接删桶会丢掉
    桶内未变化的行；且增量新日期可能与旧数据**同桶**（affected 取两边的并集）。
    故：受影响桶 = 旧数据命中「区间（×标的）」行的桶 ∪ 增量数据所在桶；保留
    受影响桶内 ``~(dt_expr [& sym_expr])`` 旧行，与增量合并后整体重写这些桶。
    ``sym_expr`` 给出时（事件带 symbol_scope）命中判定收窄到变化标的，
    未变化的标的行不重算。**惰性过滤**：受影响桶判定只读 part 列、keep 行级
    裁剪后才 collect（大表增量避免全量读入内存）；``sort_cols`` 给出时合并
    结果按时间优先排序写盘。
    """
    key = pkeys[0]
    cut = {"yearly": 4, "monthly": 7, "daily": 10}.get(gran, 4)
    part_expr = pl.col(dt_col).cast(pl.String).str.slice(0, cut).alias(key)
    inc_parts = inc.with_columns(part_expr)[key].unique().to_list()
    hit = dt_expr & sym_expr if sym_expr is not None else dt_expr
    lf = old.lazy() if isinstance(old, pl.DataFrame) else old
    affected = sorted(set(
        lf.filter(hit).select(key).unique().collect()[key].to_list())
        | set(inc_parts))
    keep = lf.filter(~hit).filter(pl.col(key).is_in(affected)).collect() \
        if affected else None
    for v in affected:
        shutil.rmtree(out_dir / f"{key}={v}", ignore_errors=True)
    if keep is not None:
        # 增量行补同款 part 列（与 keep 列数对齐；write_all 会再覆盖同值）
        merged = pl.concat(
            [keep, inc.with_columns(part_expr)], how="vertical_relaxed")
    else:
        merged = inc
    if sort_cols:
        merged = merged.sort(sort_cols)
    write_all(merged, out_dir, pkeys, gran=gran, dt_col=dt_col)
    return merged


def write_incremental_flat(out_path: Path, inc: pl.DataFrame,
                           dt_expr: pl.Expr, keys: list[str],
                           *, sym_expr: pl.Expr | None = None,
                           sort_cols: list[str] | None = None) -> pl.DataFrame:
    """**增量物化（flat 单文件）**：删区间（×标的）命中行 + 增量合并写回。

    惰性过滤只读 keep 行（未命中标的/区间外的旧行），与增量 ``inc`` 合并后
    按 ``keys`` 去重（保留新值）写回单文件；``sort_cols`` 给出时按时间优先
    排序。panel/fieldset/factor/tester 四个资产的 flat 增量分支统一走这里。
    """
    keep = pl.scan_parquet(out_path).filter(
        ~(dt_expr & sym_expr) if sym_expr is not None else ~dt_expr
    ).collect()
    df = pl.concat([keep, inc], how="vertical_relaxed"
                   ).unique(subset=keys, keep="last")
    if sort_cols:
        df = df.sort(sort_cols)
    df.write_parquet(out_path)
    return df


__all__ = ["write_all", "write_file", "write_incremental", "write_incremental_flat"]
