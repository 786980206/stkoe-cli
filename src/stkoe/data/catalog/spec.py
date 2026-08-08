"""数据框架公共数据类型"""
from dataclasses import dataclass, field
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
    is_tool: bool = False  # 工具字段（处理数据时忽略，见 data.settings）
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
        fields = cls.__dataclass_fields__
        return cls(**{k: v for k, v in d.items() if k in fields})


@dataclass(frozen=True)
class TableMeta:
    name: str
    version: int
    layout: TableLayout
    partition_by: tuple[str, ...] = ()
    partition_count: int = 1
    columns: tuple[ColumnMeta, ...] = ()
    row_count: int | None = None
    file_count: int = 0
    bytes: int = 0
    as_index: bool = False
    has_data: bool = False
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    source: str = "local"
    extra: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class FileDiff:
    rel_path: str
    kind: str  # added | removed | changed
    catalog_size: int | None = None
    disk_size: int | None = None
    catalog_mtime_ns: int | None = None
    disk_mtime_ns: int | None = None


@dataclass(frozen=True)
class TableStatus:
    name: str
    registered: bool
    consistent: bool
    signature_catalog: str | None
    signature_disk: str | None
    diffs: tuple[FileDiff, ...] = ()


@dataclass(frozen=True)
class SniffReport:
    name: str
    version_before: int
    version_after: int
    layout: TableLayout
    partition_by: tuple[str, ...]
    partition_count: int
    diffs: tuple[FileDiff, ...]
    changed: bool
    implicit_registered: bool = False


@dataclass(frozen=True)
class TaskHandle:
    task_id: str
    type: str
    object_ref: str
    status: str = "submitted"
    progress: float = 0.0
    stage: str = ""
    error: str | None = None
    result_ref: str | None = None


@dataclass(frozen=True)
class TaskLog:
    id: int
    task_id: str
    seq: int
    ts: str
    level: str
    message: str


@dataclass(frozen=True)
class DatasetMeta:
    name: str
    version: int
    index_table: str
    tables: tuple[str, ...]
    keys: tuple[str, ...]
    columns: tuple[ColumnMeta, ...]
    partition_by: tuple[str, ...] = ()
    partition_gran: str = ""  # 分区粒度：''(flat)/year/month/date/identity(镜像 index)
    materialized: bool = False
    materialized_at: str | None = None
    dependency_hash: str | None = None
    partition_deps: dict = field(default_factory=dict)  # 分区 -> 依赖源文件签名
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "index_table": self.index_table,
            "tables": list(self.tables),
            "keys": list(self.keys),
            "columns": [c.to_dict() for c in self.columns],
            "partition_by": list(self.partition_by),
            "partition_gran": self.partition_gran,
            "materialized": self.materialized,
            "materialized_at": self.materialized_at,
            "dependency_hash": self.dependency_hash,
            "partition_deps": self.partition_deps,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: dict, *, name: str, version: int) -> "DatasetMeta":
        return cls(
            name=name, version=version,
            index_table=d.get("index_table", ""),
            tables=tuple(d.get("tables", [])),
            keys=tuple(d.get("keys", [])),
            columns=tuple(ColumnMeta.from_dict(c) for c in d.get("columns", [])),
            partition_by=tuple(d.get("partition_by", [])),
            partition_gran=d.get("partition_gran", ""),
            materialized=bool(d.get("materialized", False)),
            materialized_at=d.get("materialized_at"),
            dependency_hash=d.get("dependency_hash"),
            partition_deps=dict(d.get("partition_deps", {})),
            display_name=d.get("display_name", ""),
            description=d.get("description", ""),
            tags=tuple(d.get("tags", [])),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )


@dataclass(frozen=True)
class DatasetStatus:
    name: str
    registered: bool
    materialized: bool
    materializing: bool
    consistent: bool  # dependency_hash == 当前源表签名
    partition_by: tuple[str, ...]
    dependency_hash: str | None
    current_hash: str | None
    pending_partitions: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatasetSniffReport:
    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    incremental: bool
    partition_by: tuple[str, ...]
    rebuilt_partitions: tuple[str, ...] = ()


@dataclass(frozen=True)
class StatMeta:
    """统计对象元数据（catalog type='stat'；name=所属 dataset 名）"""
    name: str
    version: int
    dataset: str
    groups: tuple[str, ...] = ()  # 已物化分组（"all" + 索引列）
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "dataset": self.dataset,
            "groups": list(self.groups),
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: dict, *, name: str, version: int) -> "StatMeta":
        return cls(
            name=name, version=version,
            dataset=d.get("dataset", ""),
            groups=tuple(d.get("groups", [])),
            display_name=d.get("display_name", ""),
            description=d.get("description", ""),
            tags=tuple(d.get("tags", [])),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )


@dataclass(frozen=True)
class StatStatus:
    name: str
    registered: bool
    dataset: str = ""
    groups: tuple[str, ...] = ()
    consistent: bool = True  # 所有缓存分组 data_key 与当前一致
    stale_groups: tuple[str, ...] = ()
