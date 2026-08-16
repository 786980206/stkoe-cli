"""图算法与影响分析（纯 Python，不依赖 graphqlite 内置算法）。

- ``page_rank`` / ``degree_centrality`` / ``weak_components``：资产图（DEPENDS
  有向边，依赖方 → 被依赖方）上的经典图算法；
- ``asset_graph``：提取资产子图（节点 + 边；``--node`` 时取该节点上下游闭包）；
- ``asset_impact`` / ``column_impact``：下游影响分析（资产级 DEPENDS 闭包 /
  列级 DERIVES 闭包，复用 ``store._walk`` 的逐层批量遍历）。

供 `e:graph analyze` / `e:graph impact`（dispatch 薄包装）使用。
"""
from __future__ import annotations

from collections import defaultdict

from .model import split_column_node_id

# 列节点 id 前缀（资产图算法排除列节点）
_COLUMN_PREFIX = "column:"


def page_rank(node_ids: list[str], edges: list[tuple[str, str]],
              *, damping: float = 0.85, max_iter: int = 100,
              tol: float = 1e-6) -> dict[str, float]:
    """PageRank（有向图，标准迭代实现）。

    边方向与 DEPENDS 一致（依赖方 → 被依赖方），rank 沿边流向被依赖方——被更多
    下游依赖的资产得分更高（血缘场景的"基础性/重要性"）。返回 ``{node: score}``。
    """
    n = len(node_ids)
    if n == 0:
        return {}
    idx = {nid: i for i, nid in enumerate(node_ids)}
    out: dict[int, list[int]] = defaultdict(list)
    for s, t in edges:
        si, ti = idx.get(s), idx.get(t)
        if si is not None and ti is not None:
            out[si].append(ti)
    pr = {i: 1.0 / n for i in range(n)}
    base = (1.0 - damping) / n
    for _ in range(max_iter):
        nxt = {i: base for i in range(n)}
        for j, outs in out.items():
            share = damping * pr[j] / len(outs)
            for ti in outs:
                nxt[ti] += share
        diff = sum(abs(nxt[i] - pr[i]) for i in range(n))
        pr = nxt
        if diff < tol:
            break
    return {node_ids[i]: pr[i] for i in range(n)}


def degree_centrality(node_ids: list[str],
                      edges: list[tuple[str, str]]) -> list[dict]:
    """度中心性：``in_degree``（入边 = 被依赖的下游数）/ ``out_degree``（出边 =
    上游依赖数）/ ``degree``（合计）；按 degree 降序返回 ``[{node, in_degree,
    out_degree, degree}]``。"""
    idset = set(node_ids)
    indeg = {n: 0 for n in node_ids}
    outdeg = {n: 0 for n in node_ids}
    for s, t in edges:
        if s in idset and t in idset:
            outdeg[s] += 1
            indeg[t] += 1
    rows = [{"node": n, "in_degree": indeg[n], "out_degree": outdeg[n],
             "degree": indeg[n] + outdeg[n]} for n in node_ids]
    return sorted(rows, key=lambda r: (r["degree"], r["in_degree"]), reverse=True)


def weak_components(node_ids: list[str],
                    edges: list[tuple[str, str]]) -> list[dict]:
    """弱连通分量（边按无向处理，并查集）：``[{id, size, nodes}]`` 按 size 降序。"""
    parent = {n: n for n in node_ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    idset = set(node_ids)
    for s, t in edges:
        if s in idset and t in idset:
            rs, rt = find(s), find(t)
            if rs != rt:
                parent[rt] = rs
    groups: dict[str, list[str]] = defaultdict(list)
    for n in node_ids:
        groups[find(n)].append(n)
    ordered = sorted(groups.values(), key=len, reverse=True)
    return [{"id": i, "size": len(ns), "nodes": sorted(ns)}
            for i, ns in enumerate(ordered)]


def analyze(node_ids: list[str], edges: list[tuple[str, str]]) -> dict:
    """一次算全：``{"page_rank": [{node, score}]（降序）, "degree": [...],
    "components": [...]}``。"""
    return {
        "page_rank": [{"node": n, "score": round(s, 6)}
                      for n, s in sorted(page_rank(node_ids, edges).items(),
                                         key=lambda kv: kv[1], reverse=True)],
        "degree": degree_centrality(node_ids, edges),
        "components": weak_components(node_ids, edges),
    }


def asset_graph(store, centers: str | list[str] | None = None):
    """资产子图：全部资产节点 + DEPENDS 边；``centers``（可多个）时取各自上下
    游闭包的**并集**（含自身，逗号/列表多中心批量）。

    列节点（Column）不参与。返回 ``(node_ids, [(source, target), ...])``。
    兼容旧签名：单中心传字符串等价 ``[center]``。
    """
    if isinstance(centers, str):
        centers = [centers]
    if not centers:
        nodes = [n["id"] for n in store.list_nodes()
                 if not n["id"].startswith(_COLUMN_PREFIX)]
    else:
        ids: set[str] = set()
        for c in centers:
            if not c or not store.has_node(c):
                continue  # 不存在的中心直接跳过（不虚造节点）
            ids.add(c)
            ids |= {d["id"] for d in store.downstream(c)}
            ids |= {d["id"] for d in store.upstream(c)}
        nodes = [n for n in ids if not n.startswith(_COLUMN_PREFIX)]
    idset = set(nodes)
    edges = []
    for nid in nodes:
        for dep in store.deps_of(nid):
            if dep["target"] in idset:
                edges.append((nid, dep["target"]))
    return nodes, edges


def asset_impact(store, node_id: str, *, depth: int | None = None) -> dict:
    """资产级下游影响：DEPENDS 下游闭包（``assets``，带 depth）+ 该资产全部列的
    DERIVES 下游列闭包（``columns``）。返回 ``{"assets", "columns"}``。"""
    assets = [{"id": r["id"], "type": r["type"], "name": r["name"],
               "depth": r["depth"]}
              for r in store.downstream(node_id, depth=depth)]
    seen: set[str] = set()
    cols = []
    for c in store.columns_of(node_id):
        for r in store.column_downstream(c["id"], depth=depth):
            if r["id"] not in seen:
                seen.add(r["id"])
                cols.append({"id": r["id"], "depth": r["depth"]})
    return {"assets": assets, "columns": cols}


def column_impact(store, column_id: str, *, depth: int | None = None) -> dict:
    """列级下游影响：DERIVES 下游列闭包（``columns``）+ 受影响列所属资产
    （``assets``，去重按最小 depth）。返回 ``{"columns", "assets"}``。"""
    seen: set[str] = set()
    cols = []
    for r in store.column_downstream(column_id, depth=depth):
        if r["id"] not in seen:
            seen.add(r["id"])
            cols.append({"id": r["id"], "depth": r["depth"]})
    best: dict[str, int] = {}
    for r in cols:
        asset, _ = split_column_node_id(r["id"])
        if asset not in best or r["depth"] < best[asset]:
            best[asset] = r["depth"]
    assets = [{"id": a, "depth": d} for a, d in sorted(best.items())]
    return {"columns": cols, "assets": assets}


__all__ = ["analyze", "asset_graph", "asset_impact", "column_impact",
           "page_rank", "degree_centrality", "weak_components"]
