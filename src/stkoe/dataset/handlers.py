"""dataset TaskHandler：旧别名 → 转发 panel 实现（source="dataset"）

V3.0 起 dataset 概念改名为 panel；实现统一在 ``panel/handlers.py``，
本模块只把同一组 handler 注册到 "dataset" source（行为与 Execute 的
``e:dataset ...`` 一致，返回 name 用 "panel"）。
"""
from __future__ import annotations

from ..panel.handlers import (
    PanelAddHandler,
    PanelDeleteHandler,
    PanelGetHandler,
    PanelListHandler,
    PanelMetaHandler,
    PanelSetHandler,
    PanelUpdateHandler,
)


def register(registry) -> None:
    for action, handler in (
        ("add", PanelAddHandler()),
        ("get", PanelGetHandler()),
        ("meta", PanelMetaHandler()),
        ("list", PanelListHandler()),
        ("", PanelListHandler()),
        ("set", PanelSetHandler()),
        ("update", PanelUpdateHandler()),
        ("delete", PanelDeleteHandler()),
        ("del", PanelDeleteHandler()),
    ):
        registry.register("dataset", action, handler)
