"""物化共享基础设施：时间桶分区计划 + hive 分区读写（panel/fieldset/factor/tester 共用）。

下游物化统一继承其 index 的 ``materialize_partition``（yearly/monthly/daily，
默认 yearly）按**时间桶**落盘（``part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/``）；
增量删桶合并写回（``rewrite_buckets``）；全量写前清空（``write_partitioned``
的 ``clean=True``）。本模块无资产业务，只承载"物化布局/落盘"这一共享机制。
"""
from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

from .model import node_id


def index_node(store, node: dict) -> dict | None:
    """沿血缘链找该资产依赖的 index 节点（Cypher 变长上游遍历，一次拿全链；
    取最接近的 index，不找 table）。"""
    nid = node.get("id") or node_id(node["type"], node["name"])
    for d in store.upstream(nid):
        if d["type"] == "index":
            return store.get_node(d["id"])
    return None


def partition_plan(store, node: dict, dt_col: str = "") -> tuple[list[str], str]:
    """下游物化分区方案 = 继承 index 的 ``materialize_partition`` 时间桶。

    - yearly/monthly/daily（默认 yearly）：**无论 index 物理是否分区**，下游都按
      时间粒度分桶落盘（``part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]``，见
      ``write_partitioned``）；``dt_col`` 为时间键（keys 末列）；
    - gran 未知 / 无 index / 无时间键 → 单文件（``([], "")``）。
    """
    idx = index_node(store, node)
    if idx is None or not dt_col:
        return [], ""
    gran = (idx.get("materialize_partition") or "yearly").strip().lower()
    if gran in ("yearly", "monthly", "daily"):
        return ["part"], gran
    return [], ""


def scan_materialized(root: Path, partition: str | None = None) -> pl.LazyFrame:
    """读物化 parquet（hive 分区还原），**剔除内部分区桶列 part**——
    保持对外列集合与实时视图一致（part 仅供物化增量删桶，不对外暴露）。
    ``partition`` 按 part 桶前缀过滤（如 ``--partition 2024`` 取 2024 年桶）。"""
    lf = pl.scan_parquet(root, hive_partitioning=True)
    if partition is not None:
        lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
    return lf.select(pl.all().exclude("part"))


def write_partitioned(df_or_lf: pl.DataFrame | pl.LazyFrame, out_dir: Path,
                      partition_keys: list[str],
                      gran: str = "", dt_col: str = "",
                      *, clean: bool = False) -> None:
    """物化落盘：按分区键写 hive 目录 ``key=value/``；无分区键 → 单文件。

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

    ``clean=True``（**全量物化**）：写前清空 ``out_dir``——``PartitionBy`` 只写
    数据里存在的桶、**不删除新数据中已消失的旧桶目录**（整年数据被删后全量
    重写会残留陈旧桶的 phantom 行；旧逐桶写同样如此）；数据为空时落一个保留
    schema 的空 ``data.parquet``（hive 桶目录不存在，读路径不因"无 parquet
    文件"报错）。增量合并（``rewrite_buckets``）不传 clean——已按受影响桶
    精确删除，不能动未受影响桶。
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


def rewrite_buckets(old: pl.LazyFrame | pl.DataFrame, df_inc: pl.DataFrame,
                    dt_expr: pl.Expr, pkeys: list[str], out_dir: Path,
                    gran: str, dt_col: str,
                    *, sym_expr: pl.Expr | None = None,
                    sort_cols: list[str] | None = None) -> pl.DataFrame:
    """分区级增量写回：删受影响桶后，把**桶内区间外旧行**与增量行合并写回。

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
    inc_parts = df_inc.with_columns(part_expr)[key].unique().to_list()
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
        # 增量行补同款 part 列（与 keep 列数对齐；write_partitioned 会再覆盖同值）
        merged = pl.concat(
            [keep, df_inc.with_columns(part_expr)], how="vertical_relaxed")
    else:
        merged = df_inc
    if sort_cols:
        merged = merged.sort(sort_cols)
    write_partitioned(merged, out_dir, pkeys, gran=gran, dt_col=dt_col)
    return merged
