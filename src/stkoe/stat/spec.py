"""stat 公共数据类型：StatFile / StatMeta / StatScanReport"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatFile:
    """一个统计分组产物文件"""

    partition: str        # "all" 或分组列名
    rel_path: str         # 相对 stats 根（如 panel/ds1/coverage/all.parquet）
    rows: int
    size: int

    def to_dict(self) -> dict:
        return {"partition": self.partition, "rel_path": str(self.rel_path),
                "rows": self.rows, "size": self.size}


@dataclass(frozen=True)
class StatMeta:
    """``stat meta`` 输出：目标 + kind + 已生成分组文件"""

    target_type: str
    target_name: str
    kind: str
    partitions: tuple[str, ...] = ()
    files: tuple[StatFile, ...] = ()
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "target_type": self.target_type,
            "target_name": self.target_name,
            "kind": self.kind,
            "partitions": list(self.partitions),
            "files": [f.to_dict() for f in self.files],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class StatScanReport:
    """``stat scan``（生成统计分组产物）结果"""

    target_type: str
    target_name: str
    kind: str
    partitions: tuple[str, ...] = ()
    files: tuple[StatFile, ...] = ()

    def to_dict(self) -> dict:
        return {
            "target_type": self.target_type,
            "target_name": self.target_name,
            "kind": self.kind,
            "partitions": list(self.partitions),
            "files": [f.to_dict() for f in self.files],
        }


__all__ = ["StatFile", "StatMeta", "StatScanReport"]