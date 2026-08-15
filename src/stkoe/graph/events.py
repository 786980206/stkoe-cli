"""DataChangeEvent 合并与积累（血缘传播的核心算法）。

规则（v3.0-def.py PanelHandler.update 语义）：
- 上游 ``version_list`` 中 ``version > 边.required_version`` 的事件 = 积累的更新事件；
- 按 action（upsert/delete）分两类合并：
  - symbol_scope / datetime_scope 取并集（None = 全集，吞并一切）
  - field_scope 取交集（None = 全集）
- 输出恒为 ``{"upsert": event|None, "delete": event|None}``。
"""
from __future__ import annotations

from typing import Any

from .model import DataChangeEvent


def _union(a: list | None, b: list | None) -> list | None:
    """scope 并集：None 表示全集。"""
    if a is None or b is None:
        return None
    seen, out = set(), []
    for v in [*a, *b]:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _intersect(a: list | None, b: list | None) -> list | None:
    """scope 交集：None 表示全集。"""
    if a is None:
        return list(b) if b is not None else None
    if b is None:
        return list(a)
    return [v for v in a if v in b]


def merge_events(events: list[DataChangeEvent]) -> dict[str, DataChangeEvent | None]:
    """把一批事件按 action 合并为 ``{"upsert": ..., "delete": ...}``。

    同类事件 scope 合并：symbol/datetime 并集、field 交集；
    一个 action 的事件全为空 scope 时直接返回该 action 的全量事件。
    """
    out: dict[str, DataChangeEvent | None] = {"upsert": None, "delete": None}
    for action in ("upsert", "delete"):
        group = [e for e in events if e.action == action]
        if not group:
            continue
        # 以组内第一个事件为起点（避免把"未合并"的 None scope 误当全集）
        merged = group[0]
        for e in group[1:]:
            merged = DataChangeEvent(
                action=action,
                symbol_scope=_union(merged.symbol_scope, e.symbol_scope),
                datetime_scope=_union(merged.datetime_scope, e.datetime_scope),
                field_scope=_intersect(merged.field_scope, e.field_scope),
            )
        out[action] = merged
    return out


def events_after(version_list: dict, required_version: int) -> list[DataChangeEvent]:
    """取出 ``version > required_version`` 的事件（按版本升序）。"""
    events = []
    for v in sorted(int(k) for k in version_list):
        if v > required_version:
            events.append(DataChangeEvent.from_dict(version_list[str(v)]))
    return events


def accumulate(
    version_list: dict,
    required_version: int,
) -> dict[str, DataChangeEvent | None]:
    """计算依赖方积累的更新事件（v3.0-def.py ``on_change`` 输出形态）。"""
    return merge_events(events_after(version_list, required_version))


def event_from_kwargs(**kw: Any) -> DataChangeEvent:
    """从 handler 参数构造 DataChangeEvent（字段缺失容错）。"""
    return DataChangeEvent(
        action=kw.get("action", "upsert"),
        field_scope=kw.get("field_scope"),
        symbol_scope=kw.get("symbol_scope"),
        datetime_scope=kw.get("datetime_scope"),
    )


__all__ = ["merge_events", "events_after", "accumulate", "event_from_kwargs"]
