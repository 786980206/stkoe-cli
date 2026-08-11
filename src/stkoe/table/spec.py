"""表数据公共数据类型"""
from __future__ import annotations

from dataclasses import dataclass, field, fields as _fields
from enum import Enum


class TableLayout(Enum):
    """表资产形态"""

    SINGLE = "single"  # 根目录单文件
    FLAT = "flat"      # 根目录平铺多文件（无分区）
    HIVE = "hive"      # key=value/ 分区目录


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    display_name: str = ""
    description: str = ""
    data_type: str | None = None
    unit: str | None = None
    formula: str | None = None
    tags: tuple[str, ...] = ()
    as_index: bool = False
    is_tool: bool = False  # 工具字段（处理数据时忽略）

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "data_type": self.data_type,
            "unit": self.unit,
            "formula": self.formula,
            "tags": list(self.tags),
            "as_index": self.as_index,
            "is_tool": self.is_tool,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnMeta":
        names = {f.name for f in _fields(cls)}
        kw = {k: v for k, v in d.items() if k in names}
        if "tags" in kw and not isinstance(kw["tags"], tuple):
            kw["tags"] = tuple(kw["tags"] or ())
        return cls(**kw)

    def patch(self, **kw) -> "ColumnMeta":
        """返回浅拷贝修改版（frozen dataclass 更新元数据字段用）"""
        return ColumnMeta.from_dict({**self.to_dict(), **kw})


@dataclass(frozen=True)
class FileMeta:
    """文件元数据（signature 校验的一部分：只保留路径/分区/大小/时间戳）"""

    rel_path: str
    partition_path: str = ""
    size: int | None = None
    mtime_ns: int | None = None


@dataclass(frozen=True)
class TableMeta:
    """表元数据（``table meta`` 输出）"""

    name: str
    version: int
    layout: TableLayout
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    source: str = "local"
    extra: dict = field(default_factory=dict)
    partition_by: tuple[str, ...] = ()
    partition_count: int = 0
    files: tuple[FileMeta, ...] = ()
    columns: tuple[ColumnMeta, ...] = ()
    consistent: bool = True  # catalog 与磁盘一致（只读对账，不触发扫描）
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "layout": self.layout.value,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "source": self.source,
            "extra": self.extra,
            "partition_by": list(self.partition_by),
            "partition_count": self.partition_count,
            "files": [{"rel_path": f.rel_path, "partition": f.partition_path,
                       "size": f.size, "mtime_ns": f.mtime_ns} for f in self.files],
            "columns": [c.to_dict() for c in self.columns],
            "consistent": self.consistent,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class FileDiff:
    rel_path: str
    kind: str  # added | removed | changed
    catalog_size: int | None = None
    disk_size: int | None = None
    catalog_mtime_ns: int | None = None
    disk_mtime_ns: int | None = None


@dataclass(frozen=True)
class TableScanReport:
    """``table add``/``table scan`` 结果：差异/版本/布局"""

    name: str
    version_before: int
    version_after: int
    layout: TableLayout
    partition_by: tuple[str, ...]
    partition_count: int
    diffs: tuple[FileDiff, ...]
    changed: bool
    implicit_registered: bool = False

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "layout": self.layout.value,
            "partition_by": list(self.partition_by),
            "partition_count": self.partition_count,
            "diffs": [
                {
                    "rel_path": d.rel_path,
                    "kind": d.kind,
                    "catalog_size": d.catalog_size,
                    "disk_size": d.disk_size,
                    "catalog_mtime_ns": d.catalog_mtime_ns,
                    "disk_mtime_ns": d.disk_mtime_ns,
                }
                for d in self.diffs
            ],
            "changed": self.changed,
            "implicit_registered": self.implicit_registered,
        }


__all__ = [
    "TableLayout", "ColumnMeta", "FileMeta", "TableMeta",
    "FileDiff", "TableScanReport",
]
