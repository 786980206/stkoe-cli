"""DATASET 模块：索引表 + 多表 join 的逻辑数据集（add/get/del/set/meta/list/scan）

设计要点：
- 注册在 catalog（type='dataset'），meta JSON 存 objects.meta；列带 source_table/source_field
  映射 → 字段级血缘
- **物化对用户透明**（核心设计）：
  - 何时物化：add 后自动（CLI 同步 / REPL 后台）；scan 依赖变化增量重物化；
    get 读前若未物化/过期 → 先增量物化再读（永不读到过期数据）
  - 怎么更新：增量按分区粒度（partition_deps 记录每分区依赖源文件签名，只重算失配分区）
  - 分区策略：镜像 index HIVE 分区键 → 数据量+时间键选 year/month/date → flat
- 物化产物为框架自持派生数据，写 datasets/<name>/（无 .materialized/ 嵌套层级）
- 依赖登记 stkoe_depends：dataset → 成员 table（detail 记 keys + 字段映射）
- 触发：dataset scan 有实际变更后级联其依赖方 stat scan
"""
from __future__ import annotations

import builtins
import datetime
import hashlib
import shutil
import threading
from pathlib import Path

import polars as pl

from . import catalog, get_root, table
from .catalog import access
from .catalog.json import loads
from .catalog.spec import (
    ColumnMeta,
    DatasetMeta,
    DatasetScanReport,
    TaskHandle,
)
from .query import prune_files
from .task import TaskControl, conn_txn, defer
from .table import DependencyError, _with_conn
from .util import now

_MAT_LOCKS: dict[str, threading.Lock] = {}
_MAT_LOCK = threading.Lock()


class DatasetNotFoundError(FileNotFoundError):
    pass


class DatasetExistsError(ValueError):
    pass


def _root(name: str) -> Path:
    return get_root() / "datasets" / name


def _mat_dir(name: str) -> Path:
    return _root(name)


def _object(conn, name: str):
    return access.get_object(conn, name, "dataset")


# ---------- 元数据 ----------

def _meta_dict(conn, name: str) -> dict:
    """catalog meta JSON 直读（物化引擎内部状态，不暴露进 DatasetMeta）"""
    obj = _object(conn, name)
    return loads(obj["meta"]) if obj is not None else {}


def _dataset_meta(conn, obj) -> DatasetMeta:
    meta = loads(obj["meta"])
    materialized = bool(meta.get("materialized", False))
    dep_hash = meta.get("dependency_hash") or ""
    cur = ""
    if materialized and obj["signature"]:
        cur = obj["signature"]
    curated = materialized and cur != "" and cur == dep_hash
    return DatasetMeta(
        name=obj["name"],
        version=obj["version"],
        index_table=meta.get("index_table", ""),
        tables=tuple(meta.get("tables", [])),
        keys=tuple(meta.get("keys", [])),
        columns=tuple(ColumnMeta.from_dict(c) for c in meta.get("columns", [])),
        partition_by=tuple(meta.get("partition_by", [])),
        partition_gran=meta.get("partition_gran", ""),
        materialized=materialized,
        materialized_at=meta.get("materialized_at"),
        curated=curated,
        pending_partitions=tuple(k for k in (meta.get("partition_deps") or {})
                                 if k != ""),
        validation=meta.get("validation"),
        extra=meta.get("extra") or {},
        display_name=meta.get("display_name", obj["name"]),
        description=meta.get("description", ""),
        tags=tuple(meta.get("tags", [])),
        created_at=obj["created_at"],
        updated_at=obj["updated_at"],
    )


# ---------- 依赖签名 / 源表 ----------

def _source_hash(tables: list[str]) -> str:
    """底层表依赖签名：sha256(sorted 表+磁盘签名)；同时触发读前快检保鲜"""
    parts = []
    for t in sorted(builtins.set(tables)):
        parts.append(f"{t}:{table.data_key(t)}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _read_source_hash(dm: DatasetMeta) -> str:
    """当前源签名（与物化记录比对判断是否过期）"""
    return _source_hash([dm.index_table, *dm.tables])


# ---------- 列映射 / 实时 join ----------

def scan_spec(index_table: str, *tables: str, keys: list[str] | None = None) -> dict:
    """校验 index + 成员表，自动推导 join 键与列映射

    **join 键由 index 表定义**：keys 缺省 = index 表的全部列；每个键必须存在于所有成员表，
    缺列明确报错（杜绝 join 键静默退化导致结果膨胀）。
    """
    members = [index_table, *tables]
    metas = [table.meta(t) for t in members]
    for t, m in zip(members, metas):
        if not m.files:
            return {"ok": False, "message": f"table has no data: {t}"}
    index_names = [c.name for c in metas[0].columns]
    if keys is None:
        keys = [*index_names]
    else:
        missing = [k for k in keys if k not in index_names]
        if missing:
            return {"ok": False, "message": f"join keys must be columns of index '{index_table}'; "
                                            f"not in index: {missing}"}
    if not keys:
        return {"ok": False, "message": f"index '{index_table}' has no columns to use as join keys"}

    colsets = [{c.name for c in m.columns} for m in metas]
    for t, cs in zip(members[1:], colsets[1:]):
        missing = [k for k in keys if k not in cs]
        if missing:
            return {"ok": False, "message": f"member table '{t}' missing join keys: {missing}"}

    index_by_name = {c.name: c for c in metas[0].columns}
    columns: list[ColumnMeta] = []
    used: set[str] = builtins.set()
    for k in keys:
        c = index_by_name[k]
        columns.append(ColumnMeta(name=k, data_type=c.data_type, as_index=True,
                                  source_table=index_table, source_field=k))
        used.add(k)
    for t, m in zip(members, metas):
        for c in m.columns:
            if c.name in keys:
                continue
            out = c.name if c.name not in used else f"{c.name}__{t}"
            used.add(out)
            columns.append(ColumnMeta(name=out, data_type=c.data_type,
                                      source_table=t, source_field=c.name))
    return {"ok": True, "keys": keys, "columns": columns, "tables": members,
            "index_table": index_table, "tables_meta": metas}


def _align_keys(lf: pl.LazyFrame, keys: list[str]) -> pl.LazyFrame:
    """join 键 dtype 归一：datetime 时区元数据不一致时不 cast 会 join 失败

    （如 mock/csv 混入：一侧 ``datetime[μs]`` 另一侧 ``datetime[μs, UTC]``），
    统一 cast 为无时区指明时间的同一精度。
    """
    casts = []
    schema = lf.collect_schema()
    for k in keys:
        dt = schema.get(k)
        if isinstance(dt, pl.Datetime):
            casts.append(pl.col(k).cast(pl.Datetime(dt.time_unit, time_zone=None)))
    if casts:
        lf = lf.with_columns(*casts)
    return lf


def _view_lf(dm: DatasetMeta) -> pl.LazyFrame:
    """实时 join 视图（lazy）：按列映射重命名后 inner join on keys"""
    by_src: dict[str, list[ColumnMeta]] = {}
    for c in dm.columns:
        by_src.setdefault(c.source_table, []).append(c)

    def frame(t: str) -> pl.LazyFrame:
        lf = table.get_lazy(t)
        used_src = {c.source_field for c in by_src.get(t, [])}
        exprs = [pl.col(c.source_field).alias(c.name) for c in by_src.get(t, [])]
        exprs += [pl.col(k).alias(k) for k in dm.keys if k not in used_src]
        return _align_keys(lf.select(*exprs), dm.keys)

    frames = [frame(dm.index_table)]
    for t in dm.tables:
        frames.append(frame(t))
    joined = frames[0]
    for f in frames[1:]:
        joined = joined.join(f, on=[*dm.keys], how="inner")
    return joined.select(*[c.name for c in dm.columns])


# ---------- 自动分区 ----------

_PARTITION_TARGET_ROWS = 500_000
_PARTITION_MIN_ROWS = 1_000_000
_GRANS = {"year": 365, "month": 30, "date": 1}


def _est_rows(index_table: str) -> int:
    """index 表行数估算（catalog row_count 汇总，不读数据页）"""
    conn = catalog().conn
    obj = table._object(conn, index_table)
    if obj is None:
        return 0
    r = conn.execute("SELECT COALESCE(SUM(row_count),0) AS n FROM stkoe_data_files "
                     "WHERE object_id=?", (obj["id"],)).fetchone()
    return int(r["n"] or 0)


def _pick_granularity(span_days: float, rows: int) -> str:
    est = rows / _PARTITION_TARGET_ROWS
    best = "year"
    best_d = float("inf")
    for g, days in _GRANS.items():
        n = max(1, int(span_days / days))
        d = abs(n - est)
        if d < best_d:
            best, best_d = g, d
    return best


def _partition_plan(dm: DatasetMeta, lf: pl.LazyFrame) -> dict | None:
    """自动分区决策：镜像 index HIVE 分区键 → 数据量+时间键选 year/month/date → None(flat)"""
    try:
        im = table.meta(dm.index_table)
    except Exception:
        im = None
    schema = lf.collect_schema()
    tkeys = [k for k in dm.keys if schema[k].is_temporal()]
    if im is not None and im.layout.value == "hive":
        for k in im.partition_by:
            if k in schema and schema[k].is_temporal():
                return {"gran": "identity", "dm_key": k, "lo": None, "hi": None}
    if not tkeys:
        return None
    key = tkeys[0]
    lo, hi = _minmax(lf, key)
    rows = _est_rows(dm.index_table)
    if rows < _PARTITION_MIN_ROWS:
        return None
    gran = _pick_granularity((hi - lo).days, rows)
    return {"gran": gran, "dm_key": key, "lo": lo, "hi": hi}


def _minmax(lf: pl.LazyFrame, col: str) -> tuple[object, object]:
    r = lf.select(pl.col(col).min().alias("_min"), pl.col(col).max().alias("_max")).collect().row(0)
    return r[0], r[1]


def _as_date(v):
    return v.date() if isinstance(v, datetime.datetime) else v


def _bucket_range(gran: str, value: str):
    if gran == "identity":
        return None, None
    if gran == "year":
        y = int(value)
        return datetime.date(y, 1, 1), datetime.date(y + 1, 1, 1)
    if gran == "month":
        y, m = (int(x) for x in value.split("-"))
        hi = datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1)
        return datetime.date(y, m, 1), hi
    d = datetime.date.fromisoformat(value)
    return d, d + datetime.timedelta(days=1)


def _bucket_values(gran: str, lo, hi) -> list[str]:
    if gran == "identity":
        return []
    out: list[str] = []
    d = _as_date(lo)
    hi = _as_date(hi)
    while d < hi:
        if gran == "year":
            out.append(str(d.year))
            d = d.replace(year=d.year + 1)
        elif gran == "month":
            out.append(d.strftime("%Y-%m"))
            d = d.replace(year=d.year + 1, month=1) if d.month == 12 else d.replace(month=d.month + 1)
        else:
            out.append(d.isoformat())
            d += datetime.timedelta(days=1)
    return out


def _table_ids(conn, tab: str) -> list[int]:
    obj = table._object(conn, tab)
    if obj is None:
        return []
    return [r["id"] for r in conn.execute(
        "SELECT id FROM stkoe_data_files WHERE object_id=?", (obj["id"],)).fetchall()]


def _files_for_range(conn, tab: str, col: str, lo, hi) -> set[int]:
    """喂给分区范围 [lo,hi) 的源文件 id（无统计列则退化为包含全部）"""
    obj = table._object(conn, tab)
    if obj is None:
        return builtins.set()
    cand: set[int] | None = None
    for bound in (lo, hi):
        if bound is None:
            continue
        rows = prune_files(conn, obj["id"],
                           where=f"{col}>{'=' if bound is lo else ''}{bound.isoformat()}")
        ids = {r["id"] for r in rows}
        cand = ids if cand is None else (cand & ids)
    if cand is None:
        cand = {r["id"] for r in conn.execute(
            "SELECT id FROM stkoe_data_files WHERE object_id=?", (obj["id"],)).fetchall()}
    return cand


def _parse_value(dtype, s: str):
    if dtype.is_integer():
        return int(s)
    if dtype in (pl.Float32, pl.Float64):
        return float(s)
    if dtype == pl.Date:
        return datetime.date.fromisoformat(s)
    if dtype.is_temporal():
        return datetime.datetime.fromisoformat(s)
    return s


def _dep_signature(conn, tabs: list[str], ids: dict[str, set[int]]) -> str:
    parts: list[str] = []
    for tab in sorted(tabs):
        ids_ = ids.get(tab, builtins.set())
        if not ids_:
            continue
        ph = ",".join("?" for _ in ids_)
        rows = conn.execute(
            f"SELECT rel_path, size, mtime_ns FROM stkoe_data_files WHERE object_id="
            f"(SELECT id FROM stkoe_objects WHERE type='table' AND name=?) AND id IN ({ph})",
            (tab, *ids_),
        ).fetchall()
        parts += [f"{tab}|{r['rel_path']}|{r['size']}|{r['mtime_ns']}" for r in rows]
    return hashlib.sha256("\n".join(sorted(parts)).encode()).hexdigest()


# ---------- 物化引擎 ----------

def materialize_job(dm: DatasetMeta, conn, ctl: TaskControl | None,
                    resync: bool = False) -> DatasetScanReport:
    """全量/增量物化；幂等（依赖未变的分区跳过，不 bump version）。

    ``conn``：None=主连接（同步），或 worker 连接（后台任务）。
    """
    cx = _with_conn(conn)
    meta = _meta_dict(cx, dm.name)
    prev_deps: dict[str, str] = dict(meta.get("partition_deps", {}))
    prev_materialized = bool(meta.get("materialized", False))
    version_before = dm.version

    out_dir = _mat_dir(dm.name)
    out_dir.mkdir(parents=True, exist_ok=True)
    tabs = [dm.index_table, *dm.tables]
    lf = _view_lf(dm)
    plan = _partition_plan(dm, lf)
    rebuilt: list[str] = []
    changed = False
    incremental = bool(prev_deps)

    if plan is None:
        # flat：单文件全量
        target = out_dir / "data.parquet"
        dep = _dep_signature(cx, tabs, {t: builtins.set(_table_ids(cx, t)) for t in tabs})
        if resync or prev_deps.get("") != dep or not target.exists():
            if ctl:
                ctl.stage("materializing (flat)")
            lf.sink_parquet(target)
            rebuilt.append("")
            changed = True
        new_deps = {"": dep}
        partition_by, partition_gran = (), ""
    else:
        gran = plan["gran"]
        part_key = plan["dm_key"]
        schema = lf.collect_schema()
        partition_by, partition_gran = (part_key,), gran
        if gran == "identity":
            idx_obj = table._object(cx, dm.index_table)
            buckets = [r["partition_path"].rsplit("=", 1)[-1] for r in cx.execute(
                "SELECT DISTINCT partition_path FROM stkoe_data_files WHERE object_id=? "
                "AND partition_path!=''", (idx_obj["id"],)).fetchall()]
        else:
            buckets = _bucket_values(gran, plan["lo"], plan["hi"])
        new_deps: dict[str, str] = {}
        for i, value in enumerate(buckets):
            if ctl:
                ctl.check()
                ctl.stage(f"materializing part={value} ({i + 1}/{len(buckets)})")
            if gran == "identity":
                ids: dict[str, set[int]] = {}
                for t in tabs:
                    if t == dm.index_table:
                        rows = cx.execute(
                            "SELECT id FROM stkoe_data_files WHERE object_id=? AND partition_path=?",
                            (idx_obj["id"], f"{part_key}={value}")).fetchall()
                        ids[t] = {r["id"] for r in rows}
                    else:
                        ids[t] = builtins.set(_table_ids(cx, t))
                part_filter = pl.col(part_key) == _parse_value(schema[part_key], value)
            else:
                lo, hi = _bucket_range(gran, value)
                ids = {t: _files_for_range(cx, t, part_key, lo, hi) for t in tabs}
                part_filter = (pl.col(part_key) >= lo) & (pl.col(part_key) < hi)
            dep = _dep_signature(cx, tabs, ids)
            part_file = out_dir / f"part={value}" / "data.parquet"
            if not resync and prev_deps.get(value) == dep and part_file.exists():
                new_deps[value] = dep
                continue
            part_file.parent.mkdir(parents=True, exist_ok=True)
            lf.filter(part_filter).sink_parquet(part_file)
            new_deps[value] = dep
            rebuilt.append(value)
            changed = True
            if ctl:
                ctl.progress((i + 1) / len(buckets), msg=f"part={value} done")
                ctl.flush(cx)

    dm2 = _update_meta(cx, dm, new_deps, partition_by=partition_by,
                       partition_gran=partition_gran, bump=prev_materialized and changed)
    return DatasetScanReport(
        name=dm.name, version_before=version_before, version_after=dm2.version,
        materialized=True, changed=changed, incremental=incremental,
        partition_by=dm2.partition_by, rebuilt_partitions=tuple(rebuilt))


def _update_meta(conn, dm: DatasetMeta, partition_deps: dict,
                 *, partition_by: tuple[str, ...], partition_gran: str,
                 bump: bool = False) -> DatasetMeta:
    """写物化态：dependency_hash=当前源签名（首次物化不 bump；实际变更 +1）"""
    cur_hash = _read_source_hash(dm)
    with conn_txn(conn):
        obj = _object(conn, dm.name)
        if obj is None:
            raise DatasetNotFoundError(f"dataset not registered: {dm.name}")
        meta = loads(obj["meta"])
        meta["materialized"] = True
        meta["materialized_at"] = now()
        meta["dependency_hash"] = cur_hash
        meta["partition_deps"] = partition_deps
        meta["partition_by"] = builtins.list(partition_by)
        meta["partition_gran"] = partition_gran
        access.update_object_meta(conn, obj["id"], meta, signature=cur_hash,
                                  now_str=now(), bump=bump)
        obj = _object(conn, dm.name)
    return _dataset_meta(conn, obj)


# ---------- add / list ----------

def _register(conn, name: str, index_table: str, tables: list[str], keys: list[str],
              columns: list[ColumnMeta], meta: dict | None = None):
    cur = {
        "display_name": name,
        "description": "",
        "tags": [],
        "index_table": index_table,
        "tables": tables,
        "keys": keys,
        "columns": [c.to_dict() for c in columns],
        "partition_by": [],
        "partition_gran": "",
        "materialized": False,
        "materialized_at": None,
        "dependency_hash": None,
        "partition_deps": {},
    }
    if meta:
        cur["extra"] = meta
    obj = access.insert_object(conn, "dataset", name, cur, "", now())
    # 资源依赖：dataset → 全部成员表（含 index 表；detail 记 keys + 字段映射，供血缘/触发）
    deps = []
    for t in dict.fromkeys([index_table, *tables]):
        fields = [c.source_field for c in columns if c.source_table == t]
        deps.append(("table", t, {"keys": keys, "fields": fields}))
    access.set_deps(conn, "dataset", name, deps)
    return obj


def add(name: str, index_table: str, *tables: str, keys: list[str] | None = None,
        materialize: bool = True, force: bool = False,
        background: bool | None = None, **meta_extra
        ) -> DatasetMeta | TaskHandle:
    """创建 dataset：校验 join 规格 → 注册 → 自动物化（CLI 同步 / REPL 后台）。

    - ``materialize=False`` 只注册不物化（get 时仍会自动物化，用户无感知）
    - ``force=True`` 覆盖重建（删旧定义 + 清空旧物化产物）
    - 返回物化完成后的 DatasetMeta（同步模式）；异步模式返回 TaskHandle
    """
    spec = scan_spec(index_table, *tables, keys=keys)
    if not spec["ok"]:
        raise ValueError(spec["message"])
    if not force and _object(catalog().conn, name) is not None:
        raise DatasetExistsError(f"dataset already registered: {name} "
                                 f"(use force=True to redefine)")
    _root(name).mkdir(parents=True, exist_ok=True)

    def _run(conn, ctl):
        cx = _with_conn(conn)
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is not None:
                cx.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
                access.clear_deps(cx, "dataset", name)
            _register(cx, name, index_table, [*tables], spec["keys"], spec["columns"],
                      meta=meta_extra)
        if force:
            shutil.rmtree(_root(name), ignore_errors=True)
        if materialize:
            dm = _dataset_meta(cx, _object(cx, name))
            return materialize_job(dm, cx, ctl)
        return None

    return defer("dataset_add", name, _run, background=background,
                 result_fn=lambda r: _describe(catalog().conn, name))


def _describe(conn, name: str) -> DatasetMeta:
    obj = _object(conn, name)
    return _dataset_meta(conn, obj)


def list() -> list[DatasetMeta]:
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='dataset' "
                        "ORDER BY name").fetchall()
    return [_dataset_meta(conn, r) for r in rows]


# ---------- 元数据 / 读取（透明物化） ----------

def describe(name: str) -> DatasetMeta:
    """dataset 元数据（物化状态/curated 是否与当前源一致）"""
    obj = _object(catalog().conn, name)
    if obj is None:
        raise DatasetNotFoundError(f"dataset not registered: {name}")
    return _dataset_meta(catalog().conn, obj)


def meta(name: str) -> DatasetMeta:
    """dataset 元数据（describe 别名，接口统一）"""
    return describe(name)


def get_lazy(name: str, *, columns: list[str] | None = None,
             where: pl.Expr | str | None = None,
             partition: str | None = None) -> pl.LazyFrame:
    """读取 dataset（lazy）。物化缺失/过期时先增量物化再读（透明，用户无感知）。"""
    with _lock(name):
        dm = describe(name)
        if not dm.materialized or not dm.curated:
            scan_impl(dm, conn=catalog().conn)
            dm = describe(name)
    if dm.materialized:
        lf = pl.scan_parquet(_mat_dir(name), hive_partitioning=True)
        if partition is not None:
            names = builtins.set(lf.collect_schema().names())
            if "part" not in names:
                raise ValueError(f"dataset not partitioned; cannot filter partition={partition}")
            # hive 分区值可能是 Date/Int，统一 cast String 前缀匹配
            lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
    else:
        lf = _view_lf(dm)
    if where is not None:
        from .query import to_expr
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    if columns is not None:
        lf = lf.select(*columns)
    return lf


def get(name: str, *, columns: list[str] | None = None,
        where: pl.Expr | str | None = None, partition: str | None = None,
        limit: int | None = None) -> pl.DataFrame:
    """读 dataset（collect；读前自动增量物化，保证与最新源一致）"""
    lf = get_lazy(name, columns=columns, where=where, partition=partition)
    if limit is not None:
        lf = lf.limit(limit)
    return lf.collect()


def _lock(name: str) -> threading.Lock:
    with _MAT_LOCK:
        return _MAT_LOCKS.setdefault(name, threading.Lock())


# ---------- set / rename ----------

def set(name: str, *, display_name: str | None = None, description: str | None = None,
        tags: list[str] | None = None, new_name: str | None = None,
        background: bool | None = None, **extra) -> DatasetMeta | TaskHandle:
    """修改 dataset 级元数据（display_name/description/tags）；``new_name`` 改变名。"""
    if new_name:
        return rename(name, new_name, background=background)
    def _run(conn, ctl):
        cx = _with_conn(conn)
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            meta = loads(obj["meta"])
            if display_name is not None:
                meta["display_name"] = display_name
            if description is not None:
                meta["description"] = description
            if tags is not None:
                meta["tags"] = tags
            meta.setdefault("extra", {}).update(extra)
            access.update_object_meta(cx, obj["id"], meta, now_str=now())
        return _dataset_meta(cx, _object(cx, name))
    return defer("dataset_set", name, _run, background=background)


def rename(old: str, new: str, *, background: bool | None = None) -> DatasetMeta | TaskHandle:
    """改名：目录 datasets/old → new + catalog/依赖边同步 + 关联 stat 级联改名"""
    def _run(conn, ctl):
        cx = _with_conn(conn)
        src, dst = _root(old), _root(new)
        with conn_txn(cx):
            obj = _object(cx, old)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {old}")
            if _object(cx, new) is not None:
                raise ValueError(f"dataset already registered: {new}")
            if src.exists():
                if dst.exists():
                    raise FileExistsError(f"target dir already exists: {dst}")
                src.rename(dst)
            cx.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
            access.rename_obj(cx, "dataset", old, new)
            _repoint_field_dataset(cx, old, new)
            access.rename_dep(cx, "dataset", old, new)
            from .stat import _rename_cascade
            _rename_cascade(cx, old, new)
        return _dataset_meta(cx, _object(cx, new))
    return defer("dataset_rename", f"{old}->{new}", _run, background=background)


# ---------- del ----------

def del_(name: str, *, force: bool = False, with_data: bool = True,
         background: bool | None = None) -> TaskHandle:
    """删除 dataset：校验依赖（被 stat 引用默认报错；force 级联删除）。

    物化产物为框架自持派生数据，默认一并删除；成员表（用户数据）从不删除。
    """
    def _run(conn, ctl):
        cx = _with_conn(conn)
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            dependents = access.dependents(cx, "dataset", name)
            if dependents and not force:
                raise DependencyError("dependencies exist: " + ", ".join(
                    f"{d['obj_type']}:{d['obj_name']}" for d in dependents)
                    + " (use --force to cascade)")
            cx.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            access.clear_deps(cx, "dataset", name)
            if force:
                _drop_field_dependents(cx, dependents)
        if force:
            from .stat import _drop_cascade
            with catalog().txn() as conn2:
                _drop_cascade(conn2, name)
        if with_data:
            shutil.rmtree(_root(name), ignore_errors=True)
    return defer("dataset_del", name, _run, background=background)


def _drop_field_dependents(cx, dependents: list[dict]) -> None:
    """force 级联：删除绑定本 dataset 的 field（注册 + 出边 + 物化产物）"""
    for d in dependents:
        if d["obj_type"] != "field":
            continue
        fobj = access.get_object(cx, d["obj_name"], "field")
        if fobj is not None:
            cx.execute("DELETE FROM stkoe_objects WHERE id=?", (fobj["id"],))
            access.clear_deps(cx, "field", d["obj_name"])
            shutil.rmtree(get_root() / "fields" / d["obj_name"], ignore_errors=True)


def _repoint_field_dataset(cx, old: str, new: str) -> None:
    """dataset 改名：字段 meta["dataset"] 同步指向新名（依赖边已由 rename_dep 迁移）"""
    for d in access.dependents(cx, "dataset", old):
        if d["obj_type"] != "field":
            continue
        fobj = access.get_object(cx, d["obj_name"], "field")
        if fobj is not None:
            fmeta = loads(fobj["meta"])
            fmeta["dataset"] = new
            access.update_object_meta(cx, fobj["id"], fmeta, now_str=now())


# ---------- scan（增量重物化 + 触发下游） ----------

def scan(name: str | None = None, *, all: bool = False, resync: bool = False,
         cascade: bool = True, background: bool | None = None
         ) -> DatasetScanReport | list[DatasetScanReport] | TaskHandle:
    """检查依赖 → 增量重物化（幂等）；变更后级联触发下游 stat scan。

    ``all=True`` 全部已注册 dataset；REPL 默认后台（返回 TaskHandle）。
    """
    if all:
        if name:
            raise ValueError("--all 与 name 互斥")
        def _all(conn, ctl):
            out = []
            for dm in list():
                out.append(scan_impl(dm, conn=conn, ctl=ctl, resync=resync, cascade=cascade))
            return out
        return defer("dataset_scan", "*", _all, background=background)
    def _one(conn, ctl):
        cx = _with_conn(conn)
        dm = _dataset_meta(cx, _object(cx, name))
        if dm.name != name:
            raise DatasetNotFoundError(f"dataset not registered: {name}")
        return scan_impl(dm, conn=cx, ctl=ctl, resync=resync, cascade=cascade)
    return defer("dataset_scan", name, _one, background=background)


def scan_impl(dm: DatasetMeta, *, conn=None, ctl=None, resync: bool = False,
              cascade: bool = True) -> DatasetScanReport:
    """增量重物化（幂等）；变更后按 stkoe_depends 级联下游 stat scan"""
    cx = _with_conn(conn)
    report = materialize_job(dm, cx, ctl, resync=resync)
    if cascade and report.changed:
        _notify_stat_downstream(report.name)
    return report


def _notify_stat_downstream(name: str) -> None:
    from . import stat
    conn = catalog().conn
    for d in access.dependents(conn, "dataset", name):
        if d["obj_type"] == "stat":
            try:
                stat._execute_scan(d["obj_name"])
            except Exception:
                pass


# ---------- 数据标识（供 stat 缓存有效性） ----------

def data_key(name: str) -> str:
    """当前数据标识：物化完成 = dependency_hash（物化时点），未物化 = 当前源签名"""
    dm = describe(name)
    if dm.materialized:
        return _meta_dict(catalog().conn, name).get("dependency_hash") or ""
    return _read_source_hash(dm)


def materialized_payload(name: str, *, elapsed_ms: int = 0) -> dict:
    """物化任务 portal 契约：{datasetId, columns, rows, dataFile, elapsedMs}

    供 gRPC RunTask 的 dataset scan/materialize 分支返回；rows 直接数产物。
    """
    import time

    dm = describe(name)
    fp = _mat_dir(dm.name)
    if not dm.partition_by:
        fp = fp / "data.parquet"
    t0 = time.time()
    rows = 0
    if fp.exists():
        lf = pl.scan_parquet(fp, hive_partitioning=True) if fp.is_dir() else pl.scan_parquet(fp)
        rows = int(lf.select(pl.len()).collect()[0, 0])
    columns = [c.name for c in (dm.columns or [])]
    return {
        "datasetId": dm.name,
        "columns": columns,
        "rows": rows,
        "dataFile": str(fp) if fp.exists() else "",
        "elapsedMs": int(elapsed_ms or (time.time() - t0) * 1000),
    }



# ---------- 校验（portal 集合一致性） ----------

def _table_schema_cols(name: str) -> set[str]:
    """物理表列名（只读 schema，不读数据页）"""
    t = table.get_lazy(name)
    return builtins.set(t.collect_schema().names())


def validate(name: str, *, mode: str = "full") -> dict:
    """校验 dataset 与依赖表的索引契约；结果写入 catalog meta（不写数据文件）。

    - ``mode='fast'``：仅检查索引字段在每张表存在性 + 行数
    - ``mode='full'``：额外检查索引组合唯一性（索引即主键）

    返回 ``{name, valid, tables: [{name, missing_index, index_unique,
    row_count}], checked_at}``；结果存 meta[\"validation\"]，后续读 meta 直接取。
    """
    dm = describe(name)
    keys = [*dm.keys]
    report = []
    valid = True
    for t in [dm.index_table, *dm.tables]:
        cols = _table_schema_cols(t)
        missing = [k for k in keys if k not in cols]
        row = {"name": t, "missing": missing, "index_unique": True, "row_count": 0}
        if missing or not keys:
            if missing:
                valid = False
            if not keys:
                row["index_unique"] = None
            report.append(row)
            continue
        lf = table.get_lazy(t, columns=keys)
        try:
            row["row_count"] = int(lf.select(pl.len()).collect()[0, 0])
        except Exception:
            row["row_count"] = -1
        if mode == "full":
            try:
                n = int(lf.select(pl.len()).collect()[0, 0])
                u = int(lf.unique().select(pl.len()).collect()[0, 0])
                row["index_unique"] = n == u
                if not row["index_unique"]:
                    valid = False
            except Exception:
                row["index_unique"] = None
        report.append(row)
    out = {"name": name, "valid": valid, "tables": report}
    with catalog().txn() as conn:
        obj = _object(conn, name)
        if obj is not None:
            meta_for = loads(obj["meta"])
            meta_for["validation"] = out
            access.update_object_meta(conn, obj["id"], meta_for, now_str=now())
    return out