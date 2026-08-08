"""stat 模块：dataset 统计的独立物化视图（与 dataset 解耦，经 stkoe_depends 记录依赖）

- 统计物化数据写 ``stats/<dataset>/group=<all|col>/stats.parquet``，与数据集物化（datasets/）隔离
- catalog 注册 type='stat'（name=所属 dataset 名），meta 记录 dataset/groups/每分组 data_key
- 依赖：stat → dataset（stkoe_depends），供后续触发（dataset 变更 → stat sniff）
- stat create：预计算指定分组（--all = "all" + 逐索引列分组）；stat select：默认读缓存，--refresh 重算
"""
from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

from . import catalog, get_root
from .catalog import access
from .catalog.json import loads
from .catalog.spec import StatMeta, StatStatus
from .task import TaskControl, TaskHandle, conn_txn, run_task
from .util import now


class StatNotFoundError(FileNotFoundError):
    pass


def _root(name: str) -> Path:
    return get_root() / "stats" / name


def _group_file(name: str, group: str) -> Path:
    return _root(name) / f"group={group}" / "stats.parquet"


def _object(conn, name: str):
    return access.get_object(conn, name, "stat")


def _meta(conn, obj) -> StatMeta:
    return StatMeta.from_dict(loads(obj["meta"]), name=obj["name"], version=obj["version"])


def _groups(conn, name: str) -> dict:
    """catalog meta 中的分组表：{group: {data_key, computed_at}}"""
    obj = _object(conn, name)
    if obj is None:
        return {}
    return loads(obj["meta"]).get("groups", {})


# ---------- 分组解析 / 校验 ----------

def _resolve_groups(dm, *, group_cols: list[str] | None = None, all_: bool = False) -> list[str]:
    """请求分组：--all → ['all'] + 逐索引列；--group-col → 指定列；缺省 → ['all']"""
    if all_:
        return ["all", *dm.keys]
    if group_cols:
        return [*group_cols]
    return ["all"]


def _validate_groups(dm, groups: list[str]) -> None:
    cols = {c.name for c in dm.columns}
    bad = [g for g in groups if g != "all" and g not in cols]
    if bad:
        raise ValueError(f"stat group cols not in dataset columns: {bad}")


# ---------- 计算 / 缓存 ----------

def calc_stats(data: pl.LazyFrame, group_col: str | None = None) -> pl.LazyFrame:
    """计算所有列的统计信息（支持分组/非分组；返回 LazyFrame）"""
    schema = data.collect_schema()
    numeric_cols = [c for c, d in schema.items() if d.is_numeric()]
    string_cols = [c for c, d in schema.items() if d == pl.String]
    temporal_cols = [c for c, d in schema.items() if d.is_temporal()]
    if group_col is None:
        data = data.with_columns(pl.lit("all").alias("group"))
        group_col = "group"
    stats_list = []
    ALL_COLS = [group_col, "field", "data_type", "count", "null_count", "nunique",
                "min", "q25", "q50", "q75", "max", "mean", "min_date", "max_date"]
    if numeric_cols:
        stats_list.append(
            data.unpivot(index=[group_col], on=numeric_cols, variable_name="field")
            .group_by([group_col, "field"])
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
            .select(ALL_COLS))
    if string_cols:
        stats_list.append(
            data.unpivot(index=[group_col], on=string_cols, variable_name="field")
            .group_by([group_col, "field"])
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
            .select(ALL_COLS))
    if temporal_cols:
        stats_list.append(
            data.unpivot(index=[group_col], on=temporal_cols, variable_name="field")
            .group_by([group_col, "field"])
            .agg([pl.len().alias("count"), pl.col("value").null_count().alias("null_count"),
                  pl.col("value").min().alias("min"), pl.col("value").max().alias("max"),
                  pl.col("value").n_unique().alias("nunique")])
            .with_columns([pl.lit("temporal").alias("data_type"),
                           pl.col("min").cast(pl.String).alias("min"),
                           pl.col("max").cast(pl.String).alias("max"),
                           pl.col("min").cast(pl.String).alias("min_date"),
                           pl.col("max").cast(pl.String).alias("max_date"),
                           pl.lit(None).cast(pl.Float64).alias("q25"),
                           pl.lit(None).cast(pl.Float64).alias("q50"),
                           pl.lit(None).cast(pl.Float64).alias("q75"),
                           pl.lit(None).cast(pl.Float64).alias("mean")])
            .select(ALL_COLS))
    result = pl.concat(stats_list, how="vertical")
    order = {col: i for i, col in enumerate(schema.names())}
    result = result.with_columns(
        pl.col("field").replace_strict(order, default=999).alias("_order")
    ).sort(["_order", group_col]).drop("_order")
    return result


def _compute(dm, group: str) -> pl.DataFrame:
    """按分组重算统计（group='all' 未分组；数据源与 dataset.select 语义一致）"""
    lf = _select_lf(dm)
    return calc_stats(lf, group_col=None if group == "all" else group).collect()


def _select_lf(dm):
    from . import dataset
    return dataset.select(dm.name)


def _need_recompute(conn, name: str, group: str) -> bool:
    """分组缓存是否缺失/过期（data_key 与当前数据标识不一致）"""
    from . import dataset
    file = _group_file(name, group)
    if not file.exists():
        return True
    entry = _groups(conn, name).get(group)
    return entry is None or entry.get("data_key") != dataset.data_key(name)


def _cache_write(conn, name: str, group: str, df: pl.DataFrame) -> None:
    """写分组缓存（stats.parquet + catalog meta 记录 data_key；data_key 变化才 bump version）"""
    from . import dataset
    file = _group_file(name, group)
    file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file)
    obj = _object(conn, name)
    if obj is None:
        access.insert_object(conn, "stat", name, {"dataset": name, "groups": {}}, "", now())
        obj = _object(conn, name)
    cur = loads(obj["meta"])
    groups = cur.get("groups", {})
    new_key = dataset.data_key(name)
    old = groups.get(group)
    groups[group] = {"data_key": new_key, "computed_at": now()}
    cur["groups"] = groups
    access.update_object_meta(conn, obj["id"], cur, now_str=now(),
                              bump=(old is None or old.get("data_key") != new_key))


# ---------- 定义管理 ----------

def create(name: str, *, group_col: list[str] | None = None, all_: bool = False,
           background: bool = False, force: bool = False) -> TaskHandle:
    """预计算 stat 分组（缺省 'all'；--all 含逐索引列分组；幂等，force 强制重算）

    统计对象在 catalog 注册（type='stat'），依赖登记 stat → dataset。
    """
    from . import dataset
    dm = dataset.describe(name)
    groups = _resolve_groups(dm, group_cols=group_col, all_=all_)
    _validate_groups(dm, groups)

    def _run(conn, ctl: TaskControl):
        if force:
            shutil.rmtree(_root(name), ignore_errors=True)
        with conn_txn(conn):
            if _object(conn, name) is None:
                access.insert_object(conn, "stat", name, {"dataset": name, "groups": {}}, "", now())
        for i, group in enumerate(groups):
            if ctl:
                ctl.check()
                ctl.stage(f"stat group={group} ({i + 1}/{len(groups)})")
            if not force and not _need_recompute(conn, name, group):
                continue
            df = _compute(dm, group)
            with conn_txn(conn):
                _cache_write(conn, name, group, df)
            if ctl:
                ctl.progress((i + 1) / len(groups), msg=f"group={group} done")
        with conn_txn(conn):
            cur_groups = [*_groups(conn, name)]
            access.set_deps(conn, "stat", name, [("dataset", name, {"groups": cur_groups})])

    return run_task("stat_create", name, _run, background=background)


def list() -> list[StatMeta]:
    rows = catalog().conn.execute(
        "SELECT * FROM stkoe_objects WHERE type='stat' ORDER BY name").fetchall()
    return [_meta(catalog().conn, r) for r in rows]


def describe(name: str) -> StatMeta:
    obj = _object(catalog().conn, name)
    if obj is None:
        raise StatNotFoundError(f"stat not registered: {name}")
    return _meta(catalog().conn, obj)


def status(name: str) -> StatStatus:
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        return StatStatus(name=name, registered=False)
    from . import dataset
    groups = loads(obj["meta"]).get("groups", {})
    cur_key = dataset.data_key(name)
    stale = [g for g, e in groups.items() if e.get("data_key") != cur_key]
    return StatStatus(name=name, registered=True, dataset=obj["name"],
                      groups=tuple(groups), consistent=not stale, stale_groups=tuple(stale))


def drop(name: str) -> TaskHandle:
    """删注册 + 依赖边；统计产物为框架自持派生数据，一并删除"""

    def _run(conn, ctl):
        with conn_txn(conn):
            _drop_cascade(conn, name)

    return run_task("stat_drop", name, _run)


def _drop_cascade(conn, name: str) -> None:
    """级联删除 stat（供 stat.drop 与 dataset.drop 复用；未注册则忽略）"""
    obj = _object(conn, name)
    if obj is None:
        return
    conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
    access.clear_deps(conn, "stat", name)
    shutil.rmtree(_root(name), ignore_errors=True)


def rename(old: str, new: str) -> TaskHandle:
    """改名：目录 stats/old → stats/new + catalog/依赖边同步"""

    def _run(conn, ctl):
        with conn_txn(conn):
            if not _rename_cascade(conn, old, new):
                raise StatNotFoundError(f"stat not registered: {old}")

    return run_task("stat_rename", f"{old}->{new}", _run)


def _rename_cascade(conn, old: str, new: str) -> bool:
    """级联改名 stat（供 stat.rename 与 dataset.rename 复用）；未注册返回 False"""
    obj = _object(conn, old)
    if obj is None:
        return False
    if _object(conn, new) is not None:
        raise ValueError(f"stat already registered: {new}")
    src, dst = _root(old), _root(new)
    if src.exists():
        if dst.exists():
            raise FileExistsError(f"target dir already exists: {dst}")
        src.rename(dst)
    conn.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
    access.rename_obj(conn, "stat", old, new)
    access.rename_dep(conn, "stat", old, new)
    return True


# ---------- 读取 / 同步 ----------

def select(name: str, *, group_col: str | None = None, all_: bool = False,
           refresh: bool = False) -> pl.DataFrame | dict[str, pl.DataFrame]:
    """读 stat：默认读缓存（缺失/过期自动重算），``refresh=True`` 强制重算。

    ``all_=True`` 返回 {group: df} 字典（"all" + 逐索引列），否则返回单个 DataFrame。
    """
    from . import dataset
    dm = dataset.describe(name)
    groups = _resolve_groups(dm, group_cols=[group_col] if group_col else None, all_=all_)
    _validate_groups(dm, groups)
    out: dict[str, pl.DataFrame] = {}
    for group in groups:
        out[group] = _select_group(dm, group, refresh=refresh)
    return out if all_ else out[groups[0]]


def _select_group(dm, group: str, *, refresh: bool = False) -> pl.DataFrame:
    conn = catalog().conn
    if not refresh and not _need_recompute(conn, dm.name, group):
        try:
            return pl.read_parquet(_group_file(dm.name, group))
        except Exception:
            pass
    df = _compute(dm, group)
    with catalog().txn() as conn:
        _cache_write(conn, dm.name, group, df)
        access.add_dep(conn, "stat", dm.name, "dataset", dm.name, detail=None)
    return df


def sniff(name: str) -> dict:
    """重算 data_key 失配的分组（幂等）；返回 {recomputed, fresh}"""
    conn = catalog().conn
    if _object(conn, name) is None:
        raise StatNotFoundError(f"stat not registered: {name}")
    from . import dataset
    dm = dataset.describe(name)
    recomputed, fresh = [], []
    for group in [*_groups(conn, name)]:
        if _need_recompute(conn, name, group):
            df = _compute(dm, group)
            with conn_txn(conn):
                _cache_write(conn, name, group, df)
            recomputed.append(group)
        else:
            fresh.append(group)
    return {"name": name, "dataset": dm.name, "recomputed": recomputed, "fresh": fresh}
