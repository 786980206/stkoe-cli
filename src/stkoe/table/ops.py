"""table 资产业务：登记（发现资产）/ 读取 / 元数据 / 重扫对账。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——图登记/依赖/版本
走 ``svc``（GraphService）暴露的图能力（``_scan_disk``/``_meta_dict`` 等）；
本模块只承载 table 自身的操作逻辑。函数第一参数 ``svc`` 为 GraphService 实例。
"""
from __future__ import annotations

import polars as pl

from ..graph.errors import AssetNotFoundError
from ..graph.model import node_id
from .errors import TableExistsError
from . import util as T


def table_add(svc, name: str, *, all: bool = False, meta: dict | None = None):
    """注册表（发现资产）：目录必须存在；已注册报 TableExistsError。

    配置了 ``dbt-manifest-file`` 时先应用 manifest 元数据（description/列说明等），
    参数显式指定的值（``--display_name/--description/...``）覆盖 manifest。
    """
    if all:
        if not svc.tables_root.exists():
            return []
        out = []
        for d in sorted(x for x in svc.tables_root.iterdir() if x.is_dir()):
            if svc.store.get_node(node_id("table", d.name)) is None \
                    and any(d.rglob("*.parquet")):
                m, cols = svc._manifest_meta(d.name)
                out.append(svc._scan_disk(
                    "table", d.name, meta={**m, **(meta or {})}, col_meta=cols))
        return out
    if not name:
        raise ValueError("add 需要表名（或 --all 批量发现）")
    root = svc._root(name)
    if not root.exists():
        raise AssetNotFoundError(f"table dir not found: {root}")
    if svc.store.get_node(node_id("table", name)) is not None:
        raise TableExistsError(f"table already registered: {name} (use scan to refresh)")
    m, cols = svc._manifest_meta(name)
    return svc._scan_disk("table", name, meta={**m, **(meta or {})}, col_meta=cols)


def table_get(svc, name: str, *, columns=None, where=None, partition=None,
              exclude_tool: bool = False, limit=None, offset=None,
              count_total: bool = False):
    df, total = svc._get_data("table", name, columns=columns, where=where,
                              partition=partition, exclude_tool=exclude_tool,
                              limit=limit, offset=offset, count_total=count_total)
    return (df, total) if count_total else df


def table_lazy(svc, name: str, *, where=None, exclude_tool: bool = False) -> pl.LazyFrame:
    """table 读取 lazy 视图（stat 等下游消费）。"""
    return svc._read_lazy("table", name, where=where, exclude_tool=exclude_tool)


def table_meta(svc, name: str) -> dict:
    return svc._meta_dict("table", name)


def table_list(svc, *, candidate: bool = False) -> list:
    if candidate:
        if not svc.tables_root.exists():
            return []
        out = []
        for d in sorted(x for x in svc.tables_root.iterdir() if x.is_dir()):
            if svc.store.get_node(node_id("table", d.name)) is None \
                    and any(d.rglob("*.parquet")):
                out.append(d.name)
        return out
    return [svc._meta_dict("table", n["name"]) for n in svc.graph.list("table")]


def table_set(svc, name: str, **kw) -> dict:
    svc._require_node("table", name)
    return svc.graph.set("table", name, **kw)


def table_col(svc, name: str, column: str, **kw) -> dict:
    return svc.graph.col("table", name, column, **svc._norm_col_kw(kw))


def table_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("table", name)
    svc.graph.delete("table", name, force=force)
    svc.store.fingerprint_clear(node_id("table", name))
    return {"deleted": name}


def table_update(svc, name: str, *, all: bool = False) -> dict | list:
    """源头表更新：重扫对账（物理变化 → 版本递增 + 下游置脏）。

    源头（table/index）无上游，天然就绪；`--all` 批量重扫全部已登记表。
    """
    if all:
        return [svc._scan_disk("table", n["name"]) for n in svc.graph.list("table")]
    return svc._scan_disk("table", name)


def table_data_key(svc, name: str) -> str:
    """当前数据标识：快检后返回签名（未登记则 ''）。"""
    root = svc._root(name)
    if not root.exists():
        return ""
    svc._ensure_fresh("table", name)
    node = svc.store.get_node(node_id("table", name))
    return node.get("signature", "") if node else T.signature(T.disk_files(root))
