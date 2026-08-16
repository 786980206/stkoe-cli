"""图数据导出为 Cytoscape elements payload（Execute/CLI 与 portal 血缘共用）。

``build_payload`` / ``node_summaries`` 为纯函数：给定 GraphStore 即返回可序列化结构。
"""
from __future__ import annotations

import time

from .model import node_id


def _node_data(props: dict, with_meta: bool) -> dict:
    """节点属性 → Cytoscape node data。"""
    name = props.get("name", "")
    display = props.get("display_name") or name
    data = {
        "id": props.get("id") or node_id(props.get("type", ""), name),
        "type": props.get("type", ""),
        "name": name,
        "label": display,
        "version": props.get("version", 0),
        "valid": bool(props.get("valid", True)),
        "materialized": bool(props.get("materialized", False)),
    }
    if with_meta:
        data["meta"] = {k: v for k, v in props.items() if k != "id"}
    return data


def _edge_data(src: str, tgt: str, edge: dict) -> dict:
    """边属性 → Cytoscape edge data。"""
    detail = edge.get("detail") or {}
    return {
        "id": f"{src}->{tgt}",
        "source": src,
        "target": tgt,
        "role": detail.get("role", ""),
        "join": detail.get("join"),
        "required_version": edge.get("required_version", 0),
    }


def build_payload(store, center: str | None = None, depth: int | None = None,
                  with_meta: bool = True) -> dict:
    """构建 Cytoscape elements payload。

    - center=None：全图；center="type:name"：该节点上下游子图（depth 限制深度）。
    - 返回 {"graph": {...}, "elements": {"nodes": [...], "edges": [...]}}。
    """
    if center is not None:
        nodes, edges = _subgraph(store, center, depth, with_meta)
    else:
        nodes, edges = _all(store, with_meta)
    types = sorted({n["data"]["type"] for n in nodes})
    return {
        "graph": {
            "exported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "center": center,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "types": types,
        },
        "elements": {"nodes": nodes, "edges": edges},
    }


def _all(store, with_meta: bool) -> tuple[list, list]:
    nodes = [{"data": _node_data(p, with_meta)} for p in store.list_nodes()]
    edges: dict[str, dict] = {}
    for props in store.list_nodes():
        for e in store.deps_of(props["id"]):
            edges[e["source"] + "->" + e["target"]] = \
                {"data": _edge_data(e["source"], e["target"], e)}
    return nodes, list(edges.values())


def _subgraph(store, center: str, depth: int | None, with_meta: bool) -> tuple[list, list]:
    ctype, cname = center.split(":", 1)
    keep = {center}
    for d in store.upstream(center, depth=depth):
        keep.add(d["id"])
    for d in store.downstream(center, depth=depth):
        keep.add(d["id"])
    nodes = []
    for nid in sorted(keep):
        props = store.get_node(nid)
        if props is not None:
            nodes.append({"data": _node_data(props, with_meta)})
    edges: dict[str, dict] = {}
    for nid in keep:
        for e in store.deps_of(nid):
            if e["target"] in keep:
                edges[e["source"] + "->" + e["target"]] = \
                    {"data": _edge_data(e["source"], e["target"], e)}
    return nodes, list(edges.values())


def node_summaries(store, asset_type: str | None = None) -> list[dict]:
    """节点摘要列表（前端中心节点选择器用）。"""
    label = asset_type.capitalize() if asset_type else None
    out = []
    for p in store.list_nodes(label):
        name = p.get("name", "")
        out.append({
            "id": p.get("id") or node_id(p.get("type", ""), name),
            "type": p.get("type", ""),
            "name": name,
            "display_name": p.get("display_name") or name,
            "version": p.get("version", 0),
            "valid": bool(p.get("valid", True)),
            "materialized": bool(p.get("materialized", False)),
        })
    return out


__all__ = ["build_payload", "node_summaries"]
