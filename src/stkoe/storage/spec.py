"""存储层数据类：文件指纹 / 布局 / 差异 / 列元数据（与具体资产类型无关）。

从 table/spec.py 迁移——storage 层是 parquet 文件访问的唯一入口，
其数据类定义在此，资产层（table/panel/...）只消费不重复定义。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TableLayout(Enum):
    """资产物理布局：单文件 / 多文件平铺 / hive 分区目录"""

    SINGLE = "single"
    FLAT = "flat"
    HIVE = "hive"


@dataclass(frozen=True)
class FileInfo:
    """磁盘文件指纹（stat-only，不打开文件）"""

    rel_path: str
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class FileDiff:
    """catalog 清单 vs 磁盘差异：added / removed / changed"""

    rel_path: str
    kind: str
    catalog_size: int | None = None
    disk_size: int | None = None
    catalog_mtime_ns: int | None = None
    disk_mtime_ns: int | None = None


@dataclass
class ColumnMeta:
    """列元数据（表物理列；graph 层有独立的列节点模型）"""

    name: str
    display_name: str = ""
    description: str = ""
    data_type: str = ""
    unit: str | None = None
    formula: str | None = None
    tags: list[str] = field(default_factory=list)
    as_index: bool = False
    is_tool: bool = False
    source_table: str | None = None
    source_field: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "display_name": self.display_name or self.name,
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


__all__ = ["TableLayout", "FileInfo", "FileDiff", "ColumnMeta"]
