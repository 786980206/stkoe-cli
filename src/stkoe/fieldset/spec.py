"""fieldset 公共数据类型：FieldMeta（活代码仅 engine 接口类型提示使用）

V2.0 的 FieldsetMeta/FieldsetScanReport/FieldsetCheckResult 已随
FieldsetController 废弃（业务统一走 graph/service.py 的 GraphService）。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldMeta:
    """一个衍生指标（字段）：基于源 dataset 列 + 公式计算，check 通过后 validated=True"""

    name: str
    formula: str = ""
    display_name: str = ""
    description: str = ""
    unit: str | None = None
    tags: tuple[str, ...] = ()
    validated: bool = False  # 是否通过 check（看公式与源数据可逐行计算）

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "formula": self.formula,
            "display_name": self.display_name,
            "description": self.description,
            "unit": self.unit,
            "tags": list(self.tags),
            "validated": self.validated,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FieldMeta":
        kw = dict(d)
        kw["tags"] = tuple(d.get("tags") or ())
        return cls(**kw)


__all__ = ["FieldMeta"]
