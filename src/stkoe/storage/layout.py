"""存储层：目录布局识别（磁盘扫描 / hive 分区解析）。

从 table/util.py 迁移——parquet 文件的目录/布局操作集中在 storage 层，
替换底层引擎（polars → DuckDB 等）时本层是唯一改动面。
"""
from __future__ import annotations

import datetime
from pathlib import Path, PurePosixPath

from .spec import FileInfo, TableLayout


def now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def iter_parquets(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def disk_files(root: Path) -> list[FileInfo]:
    """列目录做 stat 指纹（不读 footer）"""
    files = []
    for p in iter_parquets(root):
        st = p.stat()
        files.append(FileInfo(p.relative_to(root).as_posix(), st.st_size, st.st_mtime_ns))
    return files


def detect_layout(rel_paths: list[str]) -> tuple[TableLayout, list[str]]:
    """从相对路径识别资产形态：SINGLE / FLAT / HIVE（返回布局 + 分区键）"""
    keys: list[str] = []
    for rel in rel_paths:
        for part in PurePosixPath(rel).parts[:-1]:
            if "=" in part:
                key = part.split("=", 1)[0]
                if key not in keys:
                    keys.append(key)
    if not rel_paths:
        return TableLayout.SINGLE, []
    if keys:
        return TableLayout.HIVE, keys
    if len(rel_paths) == 1:
        return TableLayout.SINGLE, []
    return TableLayout.FLAT, []


def partition_of(rel: str) -> str:
    parts = PurePosixPath(rel).parts[:-1]
    return "/".join(p for p in parts if "=" in p)


def hive_value(rel: str, key: str) -> str:
    """从相对路径提取 hive 分区值（``year=2024/month=1/data.parquet`` + key=year → ``2024``）"""
    for part in PurePosixPath(rel).parts[:-1]:
        if part.startswith(key + "="):
            return part.split("=", 1)[1]
    return ""


__all__ = ["now", "iter_parquets", "disk_files", "detect_layout", "partition_of",
           "hive_value"]
