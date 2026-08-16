"""数据存储访问层（storage）：polars parquet 读写与数据计算的标准接口。

**定位**：资产层（table/index/panel/fieldset/sample/feature/factor/tester/stat）
不再直接碰 polars 文件 API（``scan_parquet``/``read_parquet``/``write_parquet``/
``sink_parquet``/footer 等）——统一经本层访问与落盘。**替换底层引擎
（polars → DuckDB 等）时只改本层内部实现，公共接口不变**（读取仍返回
polars LazyFrame/DataFrame，DuckDB 可通过 ``duckdb.sql(...).pl()`` 桥接）。

**公共接口**：

- 读（get 语义——名称/路径 → 分区或单文件数据）：
  ``scan(root, *, partition, columns, where, exclude) -> LazyFrame``
  root 为目录（hive 分区还原）或单文件；partition 按物化桶列 part 前缀过滤；
  where 字符串谓词/pl.Expr；exclude 剔除内部列（默认 part）。
- 全量物化：``write_all(df_or_lf, out_dir, partition_keys, gran, dt_col, *,
  clean)``——无分区键 → 单文件；``["part"]`` → 时间桶 hive 分区（PartitionBy）；
  clean=True 写前清空（防陈旧桶），空数据落 schema 空文件。
- 增量物化：``write_incremental(old, inc, dt_expr, pkeys, out_dir, gran,
  dt_col, *, sym_expr, sort_cols)``（时间桶分区：删受影响桶 + 保留桶内区间外
  旧行合并写回）；``write_incremental_flat(out_path, inc, dt_expr, keys, *,
  sym_expr, sort_cols)``（flat 单文件：删区间命中行 + 合并去重写回）。
- 布局/元数据：``disk_files``/``detect_layout``/``partition_of``/``hive_value``
  （layout）；``footer``/``signature``/``diff_files``/``columns_union``（meta）。
- 查询裁剪：``to_expr``/``parse_pred``/``prune_files``（query，基于
  stkoe_file_stats 的 SQL 文件级裁剪，入参 sqlite connection）。
- 数据计算：``calc_stats``（列统计 ALL_COLS）/``calc_storage``（存续统计
  STORAGE_COLS）。

**依赖方向**：本层只依赖 polars/pyarrow/标准库，不依赖资产层（graph/table/
panel/...）；资产层单向依赖本层。
"""
from __future__ import annotations

from .calc import ALL_COLS, STORAGE_COLS, calc_storage, calc_stats
from .layout import (detect_layout, disk_files, hive_value, iter_parquets,
                     now, partition_of)
from .meta import columns_union, diff_files, footer, norm_stat, signature
from .query import NUMERIC_DTYPES, is_numeric_col, lit, parse_pred, prune_files, to_expr
from .read import row_count, scan
from .spec import ColumnMeta, FileDiff, FileInfo, TableLayout
from .write import write_all, write_file, write_incremental, write_incremental_flat

__all__ = [
    # 读
    "scan", "row_count",
    # 写（物化）
    "write_all", "write_file", "write_incremental", "write_incremental_flat",
    # 布局
    "iter_parquets", "disk_files", "detect_layout", "partition_of", "hive_value",
    # 元数据
    "footer", "norm_stat", "signature", "columns_union", "diff_files",
    # 查询裁剪
    "NUMERIC_DTYPES", "parse_pred", "lit", "to_expr", "is_numeric_col", "prune_files",
    # 数据计算
    "ALL_COLS", "STORAGE_COLS", "calc_stats", "calc_storage",
    # 数据类
    "TableLayout", "FileInfo", "FileDiff", "ColumnMeta",
]
