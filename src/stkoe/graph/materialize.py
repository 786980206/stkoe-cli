"""物化计划（图侧）：时间桶分区方案 + 沿链 index 定位（panel/fieldset/factor/tester 共用）。

物化的**读写落盘**已下沉到 ``storage`` 层（``storage.scan`` / ``storage.write_all`` /
``storage.write_incremental``）——替换底层引擎只动 storage；本模块只承载与图
血缘相关的**物化布局计划**（下游统一继承其 index 的 ``materialize_partition``：
yearly/monthly/daily 按时间桶落盘 ``part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/``）。
"""
from __future__ import annotations

from .model import node_id


def index_node(store, node: dict) -> dict | None:
    """沿血缘链找该资产依赖的 index 节点（Cypher 变长上游遍历，一次拿全链；
    取最接近的 index，不找 table）。"""
    nid = node.get("id") or node_id(node["type"], node["name"])
    for d in store.upstream(nid):
        if d["type"] == "index":
            return store.get_node(d["id"])
    return None


def partition_plan(store, node: dict, dt_col: str = "") -> tuple[list[str], str]:
    """下游物化分区方案 = 继承 index 的 ``materialize_partition`` 时间桶。

    - yearly/monthly/daily（默认 yearly）：**无论 index 物理是否分区**，下游都按
      时间粒度分桶落盘（``part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]``，见
      ``storage.write_all``）；``dt_col`` 为时间键（keys 末列）；
    - gran 未知 / 无 index / 无时间键 → 单文件（``([], "")``）。
    """
    idx = index_node(store, node)
    if idx is None or not dt_col:
        return [], ""
    gran = (idx.get("materialize_partition") or "yearly").strip().lower()
    if gran in ("yearly", "monthly", "daily"):
        return ["part"], gran
    return [], ""


__all__ = ["index_node", "partition_plan"]
