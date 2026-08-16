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


def _edge_data(src: str, tgt: str, edge: dict, etype: str = "DEPENDS") -> dict:
    """边属性 → Cytoscape edge data（``type`` 标注边类型：DEPENDS/DERIVES/BELONGS_TO）。"""
    detail = edge.get("detail") or {}
    return {
        "id": f"{src}->{tgt}",
        "source": src,
        "target": tgt,
        "type": etype,
        "role": detail.get("role", ""),
        "join": detail.get("join"),
        "required_version": edge.get("required_version", 0),
    }


def build_payload(store, center: str | None = None, depth: int | None = None,
                  with_meta: bool = True, with_columns: bool = False) -> dict:
    """构建 Cytoscape elements payload。

    - center=None：全图；center="type:name"：该节点上下游子图（depth 限制深度）。
    - with_columns=True：叠加**列级血缘**（涉及资产的 Column 节点 + DERIVES 边）。
    - 返回 {"graph": {...}, "elements": {"nodes": [...], "edges": [...]}}。
    """
    if center is not None:
        nodes, edges = _subgraph(store, center, depth, with_meta, with_columns)
    else:
        nodes, edges = _all(store, with_meta, with_columns)
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


def column_payload(store, column_id: str, depth: int | None = None,
                   with_meta: bool = True) -> dict:
    """列级血缘子图：以某列（``column:<资产 id>.<列名>``）为中心，
    上游来源列（DERIVES 出边）+ 下游派生列（DERIVES 入边）；附带所属资产节点作上下文。

    ``column_id`` 可省略 ``column:`` 前缀（命令层习惯写 ``<type:name.col>``，
    自动补全为完整列节点 id）。
    """
    if not column_id.startswith("column:"):
        column_id = f"column:{column_id}"
    empty = {
        "graph": {"exported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                  "center": column_id, "node_count": 0, "edge_count": 0,
                  "types": []},
        "elements": {"nodes": [], "edges": []},
    }
    if store.get_node(column_id) is None:
        return empty
    keep = {column_id}
    keep |= {d["id"] for d in store.column_upstream(column_id, depth=depth)}
    keep |= {d["id"] for d in store.column_downstream(column_id, depth=depth)}
    nodes = []
    owner_ids: set[str] = set()
    for cid in sorted(keep):
        props = store.get_node(cid)
        if props is not None:
            nodes.append({"data": _node_data(props, with_meta)})
            owner_ids.add(props.get("asset", ""))
    for aid in sorted(owner_ids - keep):
        props = store.get_node(aid)
        if props is not None:
            nodes.append({"data": _node_data(props, with_meta)})
    edges: dict[str, dict] = {}
    for cid in keep:
        for e in store.deps_of(cid, rel_type="DERIVES"):
            if e["target"] in keep:
                edges[cid + "->" + e["target"]] = \
                    {"data": _edge_data(cid, e["target"], e, "DERIVES")}
        owner = (store.get_node(cid) or {}).get("asset", "")
        if owner:
            edges[f"{cid}->{owner}"] = \
                {"data": _edge_data(cid, owner, {}, "BELONGS_TO")}
    types = sorted({n["data"]["type"] for n in nodes})
    return {
        "graph": {"exported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                  "center": column_id, "node_count": len(nodes),
                  "edge_count": len(edges), "types": types},
        "elements": {"nodes": nodes, "edges": list(edges.values())},
    }


def _all(store, with_meta: bool, with_columns: bool = False) -> tuple[list, list]:
    nodes = [{"data": _node_data(p, with_meta)} for p in store.list_nodes()
             if with_columns or p.get("type") != "column"]
    edges: dict[str, dict] = {}
    for props in store.list_nodes():
        for e in store.deps_of(props["id"]):
            edges[e["source"] + "->" + e["target"]] = \
                {"data": _edge_data(e["source"], e["target"], e, "DEPENDS")}
        if with_columns and props.get("type") == "column":
            for e in store.deps_of(props["id"], rel_type="DERIVES"):
                edges[e["source"] + "->" + e["target"]] = \
                    {"data": _edge_data(e["source"], e["target"], e, "DERIVES")}
            owner = props.get("asset", "")
            if owner:
                edges[f"{props['id']}->{owner}"] = \
                    {"data": _edge_data(props["id"], owner, {}, "BELONGS_TO")}
    return nodes, list(edges.values())


def _subgraph(store, center: str, depth: int | None, with_meta: bool,
              with_columns: bool = False) -> tuple[list, list]:
    ctype, cname = center.split(":", 1)
    keep = {center}
    for d in store.upstream(center, depth=depth):
        keep.add(d["id"])
    for d in store.downstream(center, depth=depth):
        keep.add(d["id"])
    col_ids: set[str] = set()
    if with_columns:
        for nid in keep:
            col_ids |= {c["id"] for c in store.columns_of(nid)}
        # DERIVES 闭包：目标列可能属 keep 外资产（如 test 跨依赖引用 sample 列），一并纳入
        pending = list(col_ids)
        while pending:
            nxt = []
            for cid in pending:
                for e in store.deps_of(cid, rel_type="DERIVES"):
                    if e["target"] not in col_ids:
                        col_ids.add(e["target"])
                        nxt.append(e["target"])
            pending = nxt
        # BELONGS_TO：列所属资产并入节点集（跨层接图——列层与资产层连成一张图）
        for cid in col_ids:
            owner = (store.get_node(cid) or {}).get("asset", "")
            if owner:
                keep.add(owner)
    nodes = []
    for nid in sorted(keep | col_ids):
        props = store.get_node(nid)
        if props is not None:
            nodes.append({"data": _node_data(props, with_meta)})
    edges: dict[str, dict] = {}
    for nid in keep:
        for e in store.deps_of(nid):
            if e["target"] in keep:
                edges[e["source"] + "->" + e["target"]] = \
                    {"data": _edge_data(e["source"], e["target"], e, "DEPENDS")}
    if with_columns:
        for cid in col_ids:
            for e in store.deps_of(cid, rel_type="DERIVES"):
                if e["target"] in col_ids:
                    edges[cid + "->" + e["target"]] = \
                        {"data": _edge_data(cid, e["target"], e, "DERIVES")}
            owner = (store.get_node(cid) or {}).get("asset", "")
            if owner and owner in keep:
                edges[f"{cid}->{owner}"] = \
                    {"data": _edge_data(cid, owner, {}, "BELONGS_TO")}
    return nodes, list(edges.values())


def node_summaries(store, asset_type: str | None = None) -> list[dict]:
    """节点摘要列表（前端中心节点选择器用）；默认只列资产节点，
    ``--type column`` 显式指定时才列列节点。"""
    label = asset_type.capitalize() if asset_type else None
    want_cols = (asset_type or "").lower() == "column"
    out = []
    for p in store.list_nodes(label):
        if p.get("type") == "column" and not want_cols:
            continue
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


__all__ = ["build_payload", "column_payload", "node_summaries"]
