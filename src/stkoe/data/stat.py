"""STAT 模块：原始表 / 数据集的统计资产（add/get/del/set/meta/list/scan）

定位：
- 统计是独立资产：与数据解耦（dataset 只管数据，统计归统计）
- 目标：table（原始表）或 dataset（逻辑数据集）；catalog type='stat'，name=目标名；
  身份 (type,name) 复合唯一，与目标对象共存
- 存储：stats/<name>/group=<all|col>/stats.parquet；有效性在 catalog object meta
- 缓存语义：data_key（目标数据标识）变化 → 分组失配 → scan/read 时重算；
  get 默认读缓存，缺失/过期惰性重算；版本只在 data_key 变化时 bump
- 依赖：stat → 目标（table/dataset），detail 记 fields（字段级血缘复用 stkoe_depends）
- 触发：上游 table scan / dataset scan 变更后级联 stat scan（见 table/dataset 模块）
"""
from __future__ import annotations

import builtins
import shutil
from pathlib import Path

import polars as pl
from loguru import logger

from . import catalog, dataset, get_root, table
from .catalog import access
from .catalog.json import loads
from .catalog.spec import StatMeta
from .task import TaskControl, TaskHandle, conn_txn, defer
from .util import now


class StatNotFoundError(FileNotFoundError):
    pass


def _root(name: str) -> Path:
    return get_root() / "stats" / name


def _object(conn, name: str):
    return access.get_object(conn, name, "stat")


def _meta(conn, obj) -> StatMeta:
    meta = loads(obj["meta"])
    target = meta.get("target") or {"type": "dataset", "name": obj["name"]}
    groups = meta.get("groups", {})
    t_type, t_name = target.get("type", "dataset"), target.get("name", obj["name"])
    try:
        cur = _target_key(t_type, t_name)
        stale = tuple(g for g, e in groups.items() if e.get("data_key") != cur)
    except Exception:
        stale = ()
    return StatMeta(
        name=obj["name"],
        version=obj["version"],
        target_type=t_type,
        target_name=t_name,
        groups=tuple(groups),
        stale_groups=stale,
        display_name=meta.get("display_name", obj["name"]),
        description=meta.get("description", ""),
        tags=tuple(meta.get("tags", [])),
        created_at=obj["created_at"],
        updated_at=obj["updated_at"],
    )


def _target_key(target_type: str, target_name: str) -> str:
    """目标数据标识（缓存有效性判定）"""
    if target_type == "table":
        return table.data_key(target_name)
    return dataset.data_key(target_name)


def _groups(conn, name: str) -> dict:
    obj = _object(conn, name)
    return loads(obj["meta"]).get("groups", {}) if obj is not None else {}


def _group_file(name: str, group: str) -> Path:
    return _root(name) / f"group={group}" / "stats.parquet"


def _stat_target(name: str) -> tuple[str, str]:
    """"目标解析：已注册读 catalog；未注册同名 dataset 优先，其次 table"""
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is not None:
        t = loads(obj["meta"]).get("target") or {}
        return t.get("type", "dataset"), t.get("name", name)
    if table._object(conn, name) is not None:
        return "table", name
    if dataset._object(conn, name) is not None:
        return "dataset", name
    raise StatNotFoundError(f"no dataset or table named: {name}")


def _get_dm(conn, name: str, target_type: str, target_name: str):
    if target_type == "dataset":
        return dataset._dataset_meta(conn, dataset._object(conn, target_name))
    return None


def _resolve_groups(conn, name: str, target_type: str, target_name: str, *,
                    group_cols: list[str] | None = None, all_: bool = False) -> list[str]:
    """请求分组：--all → ['all'] + 逐键/逐列；--group-col → 指定列；缺省 ['all']"""
    if all_:
        cols = _get_cols(target_type, target_name)
        return ["all", *cols]
    if group_cols:
        return [*group_cols]
    return ["all"]


def _get_cols(target_type: str, target_name: str) -> list[str]:
    if target_type == "table":
        return [c.name for c in table.meta(target_name).columns if not c.is_tool]
    dm = dataset.describe(target_name)
    return [c.name for c in dm.columns if not c.is_tool]


def _validate_groups(target_type, target_name, groups: list[str]) -> None:
    cols = builtins.set(_get_cols(target_type, target_name))
    bad = [g for g in groups if g != "all" and g not in cols]
    if bad:
        raise ValueError(f"stat group cols not in {target_type} columns: {bad}")


def _select_lf(target_type, target_name):
    if target_type == "table":
        return table.get_lazy(target_name, exclude_tool=True)
    return dataset.get_lazy(target_name)


def _compute_for(target_type, target_name, group: str):
    lf = _select_lf(target_type, target_name)
    return calc_stats(lf, group_col=None if group == "all" else group).collect()


def _need_recompute(conn, name: str, group: str, target: tuple[str, str]) -> bool:
    file = _group_file(name, group)
    if not file.exists():
        logger.debug(f"stat {name} group={group}: cache file missing, recompute")
        return True
    entry = _groups(conn, name).get(group)
    try:
        cur = _target_key(*target)
    except Exception:
        logger.debug(f"stat {name} group={group}: data_key unavailable, recompute")
        return True
    stale = entry is None or entry.get("data_key") != cur
    logger.debug(f"stat {name} group={group}: cache {'stale' if stale else 'fresh'} "
                 f"(entry_data_key={entry.get('data_key') if entry else None}, cur={cur})")
    return stale


def _cache_write(conn, name: str, group: str, df: pl.DataFrame, key: str) -> None:
    """写分组缓存 + catalog meta 记录 data_key（变化才 bump version）"""
    file = _group_file(name, group)
    file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file)
    obj = _object(conn, name)
    if obj is None:
        _ensure_registered(conn, name)
        obj = _object(conn, name)
    meta = loads(obj["meta"])
    groups = meta.setdefault("groups", {})
    old = groups.get(group)
    groups[group] = {"data_key": key, "computed_at": now()}
    access.update_object_meta(conn, obj["id"], meta, now_str=now(),
                              bump=(old is None or old.get("data_key") != key))


def _ensure_registered(conn, name: str) -> dict:
    """注册 stat 对象（幂等；目标解析在第一次写入时固化）"""
    obj = _object(conn, name)
    if obj is None:
        target_type, target_name = _stat_target(name)
        meta = {"target": {"type": target_type, "name": target_name},
                "display_name": name, "description": "", "tags": [], "groups": {}}
        access.insert_object(conn, "stat", name, meta, "", now())
        access.set_deps(conn, "stat", name,
                        [(target_type, target_name,
                          {"fields": _get_cols(target_type, target_name)})])
        obj = _object(conn, name)
    return loads(obj["meta"])


# ---------- 统计计算 ----------

ALL_COLS = ["group", "field", "data_type", "count", "null_count", "nunique",
            "min", "q25", "q50", "q75", "max", "mean", "min_date", "max_date"]


def calc_stats(data: pl.LazyFrame, group_col: str | None = None) -> pl.LazyFrame:
    """计算所有列的统计信息（支持分组/非分组；返回 LazyFrame）"""
    schema = data.collect_schema()
    numeric_cols = [c for c, d in schema.items() if d.is_numeric()]
    string_cols = [c for c, d in schema.items() if d == pl.String]
    temporal_cols = [c for c, d in schema.items() if d.is_temporal()]
    group_col = group_col or "all"
    has_group = group_col != "all"
    if not has_group:
        data = data.with_columns(pl.lit("all").alias("_g"))
        g = "_g"
    else:
        data = data.with_columns(pl.col(group_col).cast(pl.String, strict=False).alias("_g"))
        g = "_g"
    stats_list = []
    if numeric_cols:
        stats_list.append(
            data.unpivot(index=[g], on=numeric_cols, variable_name="field")
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").min().alias("min"), pl.col("value").quantile(0.25).alias("q25"),
                  pl.col("value").median().alias("q50"), pl.col("value").quantile(0.75).alias("q75"),
                  pl.col("value").max().alias("max"), pl.col("value").mean().alias("mean"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("numeric").alias("data_type"),
                           pl.col("min").cast(pl.String).alias("min"),
                           pl.col("max").cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.String).alias("min_date"),
                           pl.lit(None).cast(pl.String).alias("max_date")])
            .select([g, *ALL_COLS[1:]]))
    if string_cols:
        stats_list.append(
            data.unpivot(index=[g], on=string_cols, variable_name="field")
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("string").alias("data_type"),
                           pl.lit(None).cast(pl.String).alias("min"),
                           pl.lit(None).cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.String).alias("min_date"),
                           pl.lit(None).cast(pl.String).alias("max_date"),
                           pl.lit(None).cast(pl.Float64).alias("q25"),
                           pl.lit(None).cast(pl.Float64).alias("q50"),
                           pl.lit(None).cast(pl.Float64).alias("q75"),
                           pl.lit(None).cast(pl.Float64).alias("mean")])
            .select([g, *ALL_COLS[1:]]))
    if temporal_cols:
        stats_list.append(
            data.select([g, *temporal_cols])
            .unpivot(index=[g], on=temporal_cols, variable_name="field")
            .with_columns(pl.col("value").cast(pl.Datetime, strict=False))
            .group_by([g, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").min().alias("min_date"),
                  pl.col("value").max().alias("max_date"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("temporal").alias("data_type"),
                           pl.col("min_date").cast(pl.String).alias("min_date"),
                           pl.col("max_date").cast(pl.String).alias("max_date"),
                           pl.lit(None).cast(pl.String).alias("min"),
                           pl.lit(None).cast(pl.String).alias("max"),
                           pl.lit(None).cast(pl.Float64).alias("q25"),
                           pl.lit(None).cast(pl.Float64).alias("q50"),
                           pl.lit(None).cast(pl.Float64).alias("q75"),
                           pl.lit(None).cast(pl.Float64).alias("mean")])
            .select([g, *ALL_COLS[1:]]))
    result = pl.concat(stats_list, how="vertical")
    gname = group_col if has_group else "group"
    result = result.rename({g: gname})
    order = {col: i for i, col in enumerate(data.collect_schema().names())}
    result = result.with_columns(
        pl.col("field").replace_strict(order, default=999).alias("_order")
    ).sort(["_order", gname]).drop("_order")
    return result


# ---------- add / set / del ----------

def _prepare(conn, name: str) -> tuple[str, str]:
    with conn_txn(conn):
        _ensure_registered(conn, name)
    return _stat_target(name)


def add(name: str, *, group_col: list[str] | None = None, all_: bool = False,
        refresh: bool = False, background: bool | None = None
        ) -> StatMeta | TaskHandle:
    """创建统计资产：为目标（table/dataset）预计算分组（缺省 ['all']）。

    ``--group-col`` 指定列分组；``--all`` 含逐列分组；``refresh`` 强制重算已有分组。
    """
    def _run(conn, ctl):
        cx = conn if conn is not None else catalog().conn
        target = _prepare(cx, name)
        groups = _resolve_groups(cx, name, *target, group_cols=group_col, all_=all_)
        _validate_groups(*target, groups)
        logger.info(f"stat add[{name}]: target={target[0]}:{target[1]}, groups={groups}"
                    f"{' (refresh)' if refresh else ''}")
        for i, g in enumerate(groups):
            ctl.check()
            ctl.stage(f"stat group={g} ({i + 1}/{len(groups)})")
            if not refresh and not _need_recompute(cx, name, g, target):
                logger.debug(f"stat add[{name}] group={g}: cache fresh, skip")
                continue
            logger.debug(f"stat add[{name}] group={g}: computing stats")
            df = _compute_for(*target, g)
            with conn_txn(cx):
                _cache_write(cx, name, g, df, _target_key(*target))
            ctl.progress((i + 1) / len(groups), msg=f"group={g} done")
            ctl.flush(cx)
        return _meta(cx, _object(cx, name))
    return defer("stat_add", name, _run, background=background)


def set(name: str, *, display_name: str | None = None, description: str | None = None,
        tags: list[str] | None = None, background: bool | None = None
        ) -> StatMeta | TaskHandle:
    """修改统计资产的描述性元数据"""
    def _run(conn, ctl):
        cx = conn if conn is not None else catalog().conn
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is None:
                raise StatNotFoundError(f"stat not registered: {name}")
            meta = loads(obj["meta"])
            if display_name is not None:
                meta["display_name"] = display_name
            if description is not None:
                meta["description"] = description
            if tags is not None:
                meta["tags"] = tags
            access.update_object_meta(cx, obj["id"], meta, now_str=now())
        return _meta(cx, _object(cx, name))
    return defer("stat_set", name, _run, background=background)


# ---------- meta / list ----------

def meta(name: str) -> StatMeta:
    """统计资产元数据（含分组/stale 状态）"""
    obj = _object(catalog().conn, name)
    if obj is None:
        raise StatNotFoundError(f"stat not registered: {name}")
    return _meta(catalog().conn, obj)


def list() -> list[StatMeta]:
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='stat' "
                        "ORDER BY name").fetchall()
    return [_meta(conn, r) for r in rows]


# ---------- get / del ----------

def get(name: str, *, group_col: str | None = None, all_: bool = False,
        refresh: bool = False, background: bool | None = None
        ) -> pl.DataFrame | dict[str, pl.DataFrame] | TaskHandle:
    """读取统计（默认读缓存，缺失/过期自动重算；--refresh 强制）。"""
    def _run(conn, ctl):
        cx = conn if conn is not None else catalog().conn
        target = _prepare(cx, name)
        groups = _resolve_groups(cx, name, *target,
                                 group_cols=[group_col] if group_col else None,
                                 all_=all_)
        _validate_groups(*target, groups)
        out: dict[str, pl.DataFrame] = {}
        for g in groups:
            if not refresh and not _need_recompute(cx, name, g, target):
                try:
                    out[g] = pl.read_parquet(_group_file(name, g))
                    logger.debug(f"stat get[{name}] group={g}: cache hit, read from disk")
                    continue
                except Exception:
                    pass
            logger.debug(f"stat get[{name}] group={g}: recompute (cache miss/stale)")
            df = _compute_for(*target, g)
            with conn_txn(cx):
                _cache_write(cx, name, g, df, _target_key(*target))
            out[g] = df
        return out if all_ else out[groups[0]]
    return defer("stat_get", name, _run, background=background)


def del_(name: str, *, background: bool | None = None) -> TaskHandle:
    """删除统计资产：注册 + 产物（stats/<name>/）+ 依赖边"""
    def _run(conn, ctl):
        cx = conn if conn is not None else catalog().conn
        with conn_txn(cx):
            _drop_cascade(cx, name)
    return defer("stat_del", name, _run, background=background)


def _drop_cascade(conn, name: str) -> None:
    """级联删除 stat（供 stat.del / dataset.del / table.del force 复用）"""
    obj = _object(conn, name)
    if obj is None:
        return
    conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
    access.clear_deps(conn, "stat", name)
    shutil.rmtree(_root(name), ignore_errors=True)


# ---------- scan ----------

def scan(name: str | None = None, *, all: bool = False, refresh: bool = False,
         background: bool | None = None
         ) -> dict | list[dict] | TaskHandle:
    """重算 data_key 失配的分组（幂等）；--refresh 强制全量；``all=True`` 全部已注册。"""
    if all and name:
        raise ValueError("--all 与 name 互斥")
    return defer("stat_scan", name or "*",
                 lambda conn, ctl: _execute_scan_one(name, refresh),
                 background=background)


def _execute_scan(name: str, refresh: bool = False) -> dict:
    """上游级联触发入口（dataset/table scan 变更时调用，无任务登记）"""
    return _execute_scan_one(name, refresh)


def _execute_scan_one(name: str | None, refresh: bool = False) -> dict | list[dict]:
    """同步扫描单资产/全部（无任务登记）；返回失配/重算分组明细"""
    if name is not None:
        return _scan_single(name, refresh)
    conn = catalog().conn
    names = [r["name"] for r in conn.execute(
        "SELECT name FROM stkoe_objects WHERE type='stat' ORDER BY name").fetchall()]
    return [_scan_single(n, refresh) for n in names]


def _scan_single(name: str, refresh: bool = False) -> dict:
    conn = catalog().conn
    target = _stat_target(name)
    recomputed, fresh = [], []
    for g in builtins.list(_groups(conn, name)):
        if refresh or _need_recompute(conn, name, g, target):
            logger.debug(f"stat scan[{name}] group={g}: recompute")
            df = _compute_for(*target, g)
            with conn_txn(conn):
                _cache_write(conn, name, g, df, _target_key(*target))
            recomputed.append(g)
        else:
            fresh.append(g)
    logger.info(f"stat scan[{name}]: recomputed={recomputed}, fresh={fresh}")
    return {"name": name, "target": f"{target[0]}:{target[1]}",
            "recomputed": recomputed, "fresh": fresh}


def _rename_cascade(conn, old: str, new: str) -> bool:
    """级联改名 stat（供 stat/rename 与 dataset rename 复用）；未注册返回 False"""
    obj = _object(conn, old)
    if obj is None:
        return False
    src, dst = _root(old), _root(new)
    if src.exists():
        if dst.exists():
            raise FileExistsError(f"target dir already exists: {dst}")
        src.rename(dst)
    conn.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
    access.rename_obj(conn, "stat", old, new)
    access.rename_dep(conn, "stat", old, new)
    return True


def rename(old: str, new: str, *, background: bool | None = None) -> StatMeta | TaskHandle:
    """改名：目录 stats/old → stats/new + catalog/依赖边（dataset 改名时级联）"""
    def _run(conn, ctl):
        cx = conn if conn is not None else catalog().conn
        with conn_txn(cx):
            if not _rename_cascade(cx, old, new):
                raise StatNotFoundError(f"stat not registered: {old}")
        return _meta(cx, _object(cx, new))
    return defer("stat_rename", f"{old}->{new}", _run, background=background)


class StatError(ValueError):
    pass