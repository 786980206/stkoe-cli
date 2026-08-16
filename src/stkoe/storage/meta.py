"""存储层：parquet 元数据（footer / 签名 / 差异 / 列并集）。

从 table/util.py 迁移——footer 只读元数据不读数据页，签名用于快检与失效判定。
"""
from __future__ import annotations

import hashlib

import polars as pl
import pyarrow.parquet as pq

from .spec import ColumnMeta, FileDiff, FileInfo


def signature(files: list[FileInfo]) -> str:
    """对象签名 = sha256(sorted rel_path|size|mtime_ns)，快检与失效判定依据"""
    lines = sorted(f"{f.rel_path}|{f.size}|{f.mtime_ns}" for f in files)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def norm_stat(v) -> str | None:
    """统计值转规范字符串（ISO 日期/时间、str 数字），同类型内字典序可比"""
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def footer(path) -> dict:
    """读 parquet footer：行数/字节/schema/min-max/null_count（不读数据页）"""
    schema = {k: str(v) for k, v in pl.scan_parquet(path).collect_schema().items()}
    md = pq.ParquetFile(path).metadata
    lo: dict[str, object] = {}
    hi: dict[str, object] = {}
    nulls: dict[str, int] = {}
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for j in range(rg.num_columns):
            col = rg.column(j)
            name = col.path_in_schema
            s = col.statistics
            if s is None:
                continue
            nulls[name] = nulls.get(name, 0) + (s.null_count or 0)
            if s.has_min_max:
                lo[name] = s.min if name not in lo else min(lo[name], s.min)
                hi[name] = s.max if name not in hi else max(hi[name], s.max)
    stats = {
        name: (schema[name], norm_stat(lo.get(name)), norm_stat(hi.get(name)), nulls.get(name, 0))
        for name in schema
    }
    return {
        "row_count": md.num_rows,
        "file_bytes": sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups)),
        "schema": schema,
        "stats": stats,
    }


def columns_union(items: list[tuple[str, dict]], ignore: set[str]) -> list[ColumnMeta]:
    """按首次出现顺序合并所有文件的列；``ignore`` 中命中的列标记 is_tool"""
    dtypes: dict[str, str] = {}
    for _, ftr in items:
        for col, dtype in ftr["schema"].items():
            dtypes.setdefault(col, dtype)
    return [
        ColumnMeta(name=col, display_name=col, data_type=dtypes[col], is_tool=col in ignore)
        for col in dtypes
    ]


def diff_files(disk: list[FileInfo], cat: dict[str, dict]) -> list[FileDiff]:
    """磁盘 vs catalog 清单差异（added / removed / changed）。

    ``cat``：rel_path -> 含 size/mtime_ns 的 dict（来自 stkoe_data_files 行）。
    """
    disk_map = {f.rel_path: f for f in disk}
    diffs: list[FileDiff] = []
    for rel in sorted(cat.keys() - disk_map.keys()):
        r = cat[rel]
        diffs.append(FileDiff(rel, "removed", catalog_size=r["size"], catalog_mtime_ns=r["mtime_ns"]))
    for rel in sorted(disk_map.keys() - cat.keys()):
        f = disk_map[rel]
        diffs.append(FileDiff(rel, "added", disk_size=f.size, disk_mtime_ns=f.mtime_ns))
    for rel in sorted(disk_map.keys() & cat.keys()):
        f, r = disk_map[rel], cat[rel]
        if (f.size, f.mtime_ns) != (r["size"], r["mtime_ns"]):
            diffs.append(FileDiff(rel, "changed", catalog_size=r["size"], disk_size=f.size,
                                  catalog_mtime_ns=r["mtime_ns"], disk_mtime_ns=f.mtime_ns))
    return diffs


__all__ = ["signature", "norm_stat", "footer", "columns_union", "diff_files"]
