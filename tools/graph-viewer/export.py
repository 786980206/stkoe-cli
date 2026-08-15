#!/usr/bin/env python
"""导出图数据为 Cytoscape.js elements JSON，供 tools/graph-viewer/index.html 可视化。

用法：
    python tools/graph-viewer/export.py <graph.db> [选项]

选项：
    --node <type:name>   只导出该节点及其上下游子图（默认导出全图）
    --depth N            上下游血缘深度（默认不限）
    --output <path>      输出文件（默认 graph-data.json）
    --pretty             美化 JSON（默认紧凑）
    --no-meta            节点不携带全量 meta（详情面板将只显示基础字段）

输出结构（Cytoscape elements）：
    {
      "graph":  {"exported_at", "source_db", "center", "node_count", "edge_count", "types"},
      "elements": {
        "nodes": [{"data": {"id", "type", "name", "label", "version", "valid",
                            "materialized", "meta"}}],
        "edges": [{"data": {"id", "source", "target", "role", "join",
                            "required_version"}}]
      }
    }
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

# 仓库根 / src 入 path（脚本可从任意 cwd 运行）
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from stkoe.graph import GraphController, GraphStore  # noqa: E402
from stkoe.graph.model import node_id  # noqa: E402


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
        # 全量 meta（除 id/version_list 冗余外保留，供详情面板展示）
        meta = {k: v for k, v in props.items() if k != "id"}
        data["meta"] = meta
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


def export_all(ctrl: GraphController, with_meta: bool) -> tuple[list, list]:
    """全图导出。"""
    nodes = [{"data": _node_data(p, with_meta)} for p in ctrl.store.list_nodes()]
    edges: dict[str, dict] = {}
    for props in ctrl.store.list_nodes():
        nid = props["id"]
        for e in ctrl.store.deps_of(nid):
            edges[e["source"] + "->" + e["target"]] = \
                {"data": _edge_data(e["source"], e["target"], e)}
    return nodes, list(edges.values())


def export_subgraph(ctrl: GraphController, center: str, depth: int | None,
                    with_meta: bool) -> tuple[list, list]:
    """导出中心节点 + 上下游子图（含子图内全部边）。"""
    ctype, cname = center.split(":", 1)
    ctrl.meta(ctype, cname)  # 校验存在
    keep = {center}
    for d in ctrl.store.upstream(center, depth=depth):
        keep.add(d["id"])
    for d in ctrl.store.downstream(center, depth=depth):
        keep.add(d["id"])
    nodes = []
    for nid in sorted(keep):
        props = ctrl.store.get_node(nid)
        if props is not None:
            nodes.append({"data": _node_data(props, with_meta)})
    edges: dict[str, dict] = {}
    for nid in keep:
        for e in ctrl.store.deps_of(nid):
            if e["target"] in keep:
                edges[e["source"] + "->" + e["target"]] = \
                    {"data": _edge_data(e["source"], e["target"], e)}
    return nodes, list(edges.values())


def main() -> int:
    ap = argparse.ArgumentParser(description="导出 stkoe 血缘图数据为 Cytoscape elements JSON")
    ap.add_argument("db", help="graphqlite 图数据库文件路径（如 <data-dir>/graph.db）")
    ap.add_argument("--node", help="中心节点（type:name），导出其上下游子图")
    ap.add_argument("--depth", type=int, default=None, help="上下游深度")
    ap.add_argument("--output", default="graph-data.json", help="输出文件")
    ap.add_argument("--pretty", action="store_true", help="美化输出")
    ap.add_argument("--no-meta", action="store_true", help="不带全量 meta")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"error: 数据库不存在: {args.db}", file=sys.stderr)
        return 2

    ctrl = GraphController(GraphStore(args.db))
    try:
        if args.node:
            nodes, edges = export_subgraph(ctrl, args.node, args.depth, not args.no_meta)
        else:
            nodes, edges = export_all(ctrl, not args.no_meta)
    finally:
        ctrl.store.close()

    types = sorted({n["data"]["type"] for n in nodes})
    payload = {
        "graph": {
            "exported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source_db": os.path.abspath(args.db),
            "center": args.node,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "types": types,
        },
        "elements": {"nodes": nodes, "edges": edges},
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False,
                  indent=2 if args.pretty else None)
    print(f"已导出 {len(nodes)} 节点 / {len(edges)} 边 -> {args.output}")
    print("在 tools/graph-viewer/ 下启动静态服务后打开页面：")
    print("  python -m http.server 8080 --directory tools/graph-viewer")
    print("  浏览器访问 http://127.0.0.1:8080/ 并选择 graph-data.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
