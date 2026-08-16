"""表数据公共数据类型（活代码引用部分；V2.0 TableMeta/TableScanReport 已随
TableController 废弃，见 table/errors.py 说明）"""
from __future__ import annotations

from dataclasses import dataclass, fields as _fields
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
    source_table: str | None = None  # dataset 列来源表
    source_field: str | None = None  # dataset 列来源字段

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
            "source_table": self.source_table,
            "source_field": self.source_field,
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
class FileDiff:
    rel_path: str
    kind: str  # added | removed | changed
    catalog_size: int | None = None
    disk_size: int | None = None
    catalog_mtime_ns: int | None = None
    disk_mtime_ns: int | None = None


__all__ = ["TableLayout", "ColumnMeta", "FileDiff"]
