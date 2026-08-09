"""数据框架公共数据类型"""
import datetime
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
        names = {f.name for f in _fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in names})

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
    """表元数据（`table meta` 输出；不含统计信息——统计见 stat 模块）"""

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
    """``table scan`` 结果：差异/版本/布局 + 下游触发"""

    name: str
    version_before: int
    version_after: int
    layout: TableLayout
    partition_by: tuple[str, ...]
    partition_count: int
    diffs: tuple[FileDiff, ...]
    changed: bool
    implicit_registered: bool = False
    triggered: tuple[str, ...] = ()  # 自动触发的下游对象（dataset/stat）

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
            "triggered": list(self.triggered),
        }


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
    """dataset 逻辑数据集元数据（dataset meta 输出）"""

    name: str
    version: int
    index_table: str
    tables: tuple[str, ...]
    keys: tuple[str, ...]
    columns: tuple[ColumnMeta, ...]
    partition_by: tuple[str, ...] = ()
    partition_gran: str = ""  # ''（flat）/year/month/date/identity（镜像 index）
    materialized: bool = False
    materialized_at: str | None = None
    curated: bool = False            # 物化数据与当前源一致（读前自物化用）
    pending_partitions: tuple[str, ...] = ()
    validation: dict | None = None   # 最近一次 validate 结果（读路径，不参与版本）
    extra: dict = field(default_factory=dict)  # 调用方透传的自定义字段（如 category）
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
            "curated": self.curated,
            "pending_partitions": list(self.pending_partitions),
            "validation": self.validation,
            "extra": self.extra,
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class DatasetScanReport:
    """dataset scan（重物化）结果"""

    name: str
    version_before: int
    version_after: int
    materialized: bool
    changed: bool
    incremental: bool
    partition_by: tuple[str, ...]
    rebuilt_partitions: tuple[str, ...] = ()
    triggered: tuple[str, ...] = ()  # 自动触发的下游（stat 等）

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "materialized": self.materialized,
            "changed": self.changed,
            "incremental": self.incremental,
            "partition_by": list(self.partition_by),
            "rebuilt_partitions": list(self.rebuilt_partitions),
            "triggered": list(self.triggered),
        }


@dataclass(frozen=True)
class StatMeta:
    """统计资产元数据（stat meta 输出）"""

    name: str                    # 资产名（= 目标对象名）
    version: int
    target_type: str             # table | dataset
    target_name: str
    groups: tuple[str, ...] = ()  # 物化分组（"all"/"file"/列名）
    stale_groups: tuple[str, ...] = ()
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "target_type": self.target_type,
            "target_name": self.target_name,
            "groups": list(self.groups),
            "stale_groups": list(self.stale_groups),
            "display_name": self.display_name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class DepMeta:
    """依赖边（祖->孙）：供 meta/血缘展示"""

    dep_type: str
    dep_name: str
    fields: tuple[str, ...] = ()  # 字段级：依赖方用到该上游哪些字段
    detail: dict = field(default_factory=dict)