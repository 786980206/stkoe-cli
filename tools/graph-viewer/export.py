#!/usr/bin/env python
"""导出图数据为 Cytoscape.js elements JSON，供 tools/graph-viewer/index.html 可视化。

复用 ``src/stkoe/graph/export.py`` 的 build_payload / node_summaries 纯函数。

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

# 仓库根 / src 入 path（脚本可从任意 cwd 运行）
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from stkoe.graph import GraphController, GraphStore  # noqa: E402
from stkoe.graph.export import build_payload  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="导出 stkoe 血缘图数据为 Cytoscape elements JSON")
    ap.add_argument("db", help="graphqlite 图数据库文件路径（如 <data-dir>/catalog.db）")
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
        payload = build_payload(ctrl.store, center=args.node, depth=args.depth,
                                with_meta=not args.no_meta)
    finally:
        ctrl.store.close()

    payload["graph"]["source_db"] = os.path.abspath(args.db)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False,
                  indent=2 if args.pretty else None)
    print(f"已导出 {payload['graph']['node_count']} 节点 / "
          f"{payload['graph']['edge_count']} 边 -> {args.output}")
    print("在 tools/graph-viewer/ 下启动静态服务后打开页面：")
    print("  python -m http.server 8080 --directory tools/graph-viewer")
    print("  浏览器访问 http://127.0.0.1:8080/ 并选择 graph-data.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
