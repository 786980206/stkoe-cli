"""index 资产业务：登记（发现资产 + 键唯一性校验）/ 读取 / 元数据 / 重扫对账。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——图登记/依赖/版本走
``svc`` 暴露的图能力（``_scan_disk``/``_meta_dict``/``_ensure_fresh`` 等）。
``_partition_hint`` 为物化粒度引导（默认 yearly 桶下数据跨多年时提示细化）。
"""
from __future__ import annotations

import polars as pl

from ..graph.errors import AssetNotFoundError
from ..graph.model import node_id
from ..table.errors import TableExistsError
from ..table import util as T


def _partition_hint(span: tuple[str, str] | None, gran: str) -> str:
    """物化粒度引导：默认 yearly 时间桶下 index 数据跨多年 → 提示细化粒度。

    增量重写按桶整桶替换——yearly 桶粒度粗，跨年数据的大范围/频繁增量会反复
    重写整个年份桶；monthly/daily 桶可细分（见 graph.materialize.write_partitioned）。
    ``span`` 为 ``(datetime 最小, 最大)``（字符串/ISO 字典序），解析失败返回空。
    """
    if gran != "yearly" or not span or len(span) != 2:
        return ""
    lo, hi = str(span[0]), str(span[1])
    if lo and hi and lo[:4] != hi[:4]:
        return (f"index 数据跨 {lo[:4]}-{hi[:4]} 年：yearly 时间桶下增量重写会重写"
                f"整个年份桶，数据量大或增量频繁建议 materialize_partition=monthly/"
                f"daily（index set 调整，下游物化继承）")
    return ""


def _check_index_unique(svc, name: str, *, symbol_col: str | None = None,
                        datetime_col: str | None = None) -> tuple[str, str] | None:
    """校验 index 物理数据的 ``(symbol_col, datetime_col)`` 组合唯一（V3.0 设计
    ``IndexHandler.add``：index 是时间×标的的索引，不允许重复键）。

    返回 ``(datetime 最小, 最大)``（登记时一次扫描顺带取到，供物化粒度引导
    ``_partition_hint``）；无时间列/无数据 → None。
    """
    node = svc.store.get_node(node_id("index", name)) or {}
    sym = symbol_col or node.get("symbol_col") or "sym"
    dt = datetime_col or node.get("datetime_col") or "date"
    lf = pl.scan_parquet(svc._index_root(name), hive_partitioning=True)
    if dt not in lf.collect_schema().names():
        return None
    df = lf.select(sym, dt).collect()
    total = df.height
    uniq = df.unique().height
    if uniq != total:
        raise ValueError(
            f"index {name} 的 ({sym}, {dt}) 组合不唯一: {total} 行 / {uniq} 组唯一"
            f"（index 要求 symbol+datetime 键唯一）")
    vals = df[dt].drop_nulls()
    if vals.len() == 0:
        return None
    return str(vals.min()), str(vals.max())


def index_add(svc, name: str, *, all: bool = False, symbol_col: str = "sym",
              datetime_col: str = "date", materialize_partition: str = "yearly",
              meta: dict | None = None) -> dict | list:
    """注册 index（发现资产）：目录必须存在；已注册报 TableExistsError。

    ``--all`` 批量发现：扫描 ``index/`` 下未登记且含 parquet 的目录（同 table add --all）。
    单表登记前校验 ``(symbol, datetime)`` 组合唯一（V3.0 设计）。
    配置了 ``dbt-manifest-file`` 时先应用 manifest 元数据（参数显式指定覆盖）。
    """
    if all:
        if not svc.indexs_root.exists():
            return []
        out = []
        for d in sorted(x for x in svc.indexs_root.iterdir() if x.is_dir()):
            if svc.store.get_node(node_id("index", d.name)) is None \
                    and any(d.rglob("*.parquet")):
                m, cols = svc._manifest_meta(d.name)
                r = svc._scan_disk(
                    "index", d.name, meta={**m, **(meta or {})}, col_meta=cols,
                    extra_data={"symbol_col": symbol_col, "datetime_col": datetime_col,
                                "materialize_partition": materialize_partition})
                hint = _partition_hint(
                    _check_index_unique(svc, d.name, symbol_col=symbol_col,
                                        datetime_col=datetime_col),
                    materialize_partition)
                if hint:
                    r["partition_hint"] = hint
                out.append(r)
        return out
    if not name:
        raise ValueError("add 需要 index 名（或 --all 批量发现）")
    root = svc._index_root(name)
    if not root.exists():
        raise AssetNotFoundError(f"index dir not found: {root}")
    if svc.store.get_node(node_id("index", name)) is not None:
        raise TableExistsError(f"index already registered: {name}")
    span = _check_index_unique(svc, name, symbol_col=symbol_col,
                               datetime_col=datetime_col)
    m, cols = svc._manifest_meta(name)
    r = svc._scan_disk(
        "index", name, meta={**m, **(meta or {})}, col_meta=cols,
        extra_data={"symbol_col": symbol_col, "datetime_col": datetime_col,
                    "materialize_partition": materialize_partition})
    # 物化粒度引导：yearly 默认粒度下数据跨多年 → 报告带 partition_hint
    hint = _partition_hint(span, materialize_partition)
    if hint:
        r["partition_hint"] = hint
    return r


def index_get(svc, name: str, *, columns=None, where=None, partition=None,
              exclude_tool: bool = False, limit=None, offset=None,
              count_total: bool = False):
    df, total = svc._get_data("index", name, columns=columns, where=where,
                              partition=partition, exclude_tool=exclude_tool,
                              limit=limit, offset=offset, count_total=count_total)
    return (df, total) if count_total else df


def index_meta(svc, name: str) -> dict:
    return svc._meta_dict("index", name)


def index_list(svc, *, candidate: bool = False) -> list:
    """index 清单；candidate=True 返回未登记为 index 但含 parquet 的表目录。"""
    if candidate:
        if not svc.indexs_root.exists():
            return []
        out = []
        for d in sorted(x for x in svc.indexs_root.iterdir() if x.is_dir()):
            if svc.store.get_node(node_id("index", d.name)) is None \
                    and any(d.rglob("*.parquet")):
                out.append(d.name)
        return out
    return [svc._meta_dict("index", n["name"]) for n in svc.graph.list("index")]


def index_set(svc, name: str, **kw) -> dict:
    svc._require_node("index", name)
    return svc.graph.set("index", name, **kw)


def index_col(svc, name: str, column: str, **kw) -> dict:
    return svc.graph.col("index", name, column, **svc._norm_col_kw(kw))


def index_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("index", name)
    svc.graph.delete("index", name, force=force)
    svc.store.fingerprint_clear(node_id("index", name))
    return {"deleted": name}


def index_update(svc, name: str, *, all: bool = False) -> dict | list:
    """源头 index 更新：重扫对账（物理变化 → 版本递增 + 下游置脏）。

    （symbol, datetime）唯一性校验只在 ``index_add`` 登记时执行；update 是
    重扫对账语义（信任磁盘现状），跳过全表 unique 校验——大表下每次 update
    全表扫描代价高（2000 万行 ~6s），且对账本身不改变数据。
    """
    if all:
        return [svc._scan_disk("index", n["name"])
                for n in svc.graph.list("index")]
    return svc._scan_disk("index", name)


def index_data_key(svc, name: str) -> str:
    root = svc._index_root(name)
    if not root.exists():
        return ""
    svc._ensure_fresh("index", name)
    node = svc.store.get_node(node_id("index", name))
    return node.get("signature", "") if node else T.signature(T.disk_files(root))
