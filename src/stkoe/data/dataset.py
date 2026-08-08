"""dataset 模块：索引表 + 多表 join 的逻辑数据集（接口对齐 table）

设计要点：
- 注册在 catalog（type='dataset'），元数据 JSON 存 objects.meta；接口对齐 table API
- ``select`` 不触发物化：物化已完成走物化数据，否则实时 join
- 物化自动管理：create 后后台自动物化+统计（REPL/daemon）；增量按分区粒度（partition_deps
  记录每个分区依赖的源文件签名，只重算失配分区）；幂等重跑不 bump version
- 自动分区：优先镜像 index 表 HIVE 分区键；否则按数据量 + 时间键自动选 year/month/date；否则 flat
- 物化产物为框架自持派生数据，写 ``datasets/<name>/``；框架绝不写底层表
- 资源依赖登记在 ``stkoe_depends``（dataset → 成员表），供后续触发/级联使用
- 统计物化独立于 dataset（``stat`` 模块，写 ``stats/<name>/``，经 stkoe_depends 记录 stat → dataset）
"""
from __future__ import annotations

import datetime
import hashlib
import shutil
from pathlib import Path

import polars as pl

from . import catalog, get_root
from .catalog import access
from .catalog.json import loads
from .catalog.spec import (
    ColumnMeta,
    DatasetMeta,
    DatasetSniffReport,
    DatasetStatus,
    TaskHandle,
)
from .query import prune_files
from .task import TaskControl, conn_txn, run_task
from .util import now


class DatasetNotFoundError(FileNotFoundError):
    pass


class DatasetExistsError(ValueError):
    pass


def _root(name: str) -> Path:
    return get_root() / "datasets" / name


def _mat_dir(name: str) -> Path:
    """物化产物目录：datasets/<name>/（不再嵌套 .materialized/ 层级）"""
    return _root(name)


# ---------- 依赖签名 / 源表 ----------

def _source_hash(tables: list[str]) -> str:
    """底层表依赖签名：sha256(sorted 表名+磁盘签名)；同时触发读前快检保鲜"""
    from . import table
    parts = []
    for t in sorted(set(tables)):
        table.describe(t)  # 触发 _ensure_fresh
        parts.append(f"{t}:{table.status(t).signature_disk}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _object(conn, name: str):
    return access.get_object(conn, name, "dataset")


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
    # 资源依赖：dataset → 全部成员表（stkoe_depends，供后续触发/级联）
    deps = [("table", t, {"keys": keys}) for t in dict.fromkeys(tables)]
    access.set_deps(conn, "dataset", name, deps)
    return obj


def _meta(conn, obj) -> DatasetMeta:
    return DatasetMeta.from_dict(loads(obj["meta"]), name=obj["name"], version=obj["version"])


# ---------- 列映射 / 实时 join ----------

def scan(index_table: str, *tables: str, keys: list[str] | None = None) -> dict:
    """校验 index + 成员表，自动推导 join 键与列映射

    **join 键由 index 表定义**：keys 缺省 = index 表的全部列；每个键必须存在于所有成员表，
    缺列明确报错（杜绝缺键列时静默退化导致结果膨胀）。``keys=`` 可显式指定 index 列的子集。
    返回 ``{keys, columns, tables, ok, message}``。
    """
    from . import table
    members = [index_table, *tables]
    metas = [table.describe(t) for t in members]
    for t, m in zip(members, metas):
        if not m.has_data:
            return {"ok": False, "message": f"table has no data: {t}"}
    index_names = [c.name for c in metas[0].columns]
    if keys is None:
        keys = [*index_names]
    else:
        missing = [k for k in keys if k not in index_names]
        if missing:
            return {"ok": False, "message": f"join keys must be columns of index '{index_table}'; "
                                            f"not in index: {missing}"}
        keys = [*keys]
    if not keys:
        return {"ok": False, "message": f"index '{index_table}' has no columns to use as join keys"}

    colsets = [{c.name for c in m.columns} for m in metas]
    for t, cs in zip(members[1:], colsets[1:]):
        missing = [k for k in keys if k not in cs]
        if missing:
            return {"ok": False, "message": f"member table '{t}' missing join keys: {missing}"}

    index_by_name = {c.name: c for c in metas[0].columns}
    columns: list[ColumnMeta] = []
    used: set[str] = set()
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


def _view_lf(dm: DatasetMeta) -> pl.LazyFrame:
    """实时 join 视图（lazy）：按列映射重命名后 inner join on keys"""
    from . import table
    by_src: dict[str, list[ColumnMeta]] = {}
    for c in dm.columns:
        by_src.setdefault(c.source_table, []).append(c)

    def frame(t: str) -> pl.LazyFrame:
        lf = table.select(t)
        exprs = [pl.col(c.source_field).alias(c.name) for c in by_src.get(t, [])]
        exprs += [pl.col(k).alias(k) for k in dm.keys if k not in {c.source_field for c in by_src.get(t, [])}]
        return lf.select(*exprs)

    frames = [frame(dm.index_table)]
    for t in dm.tables:
        frames.append(frame(t))
    joined = frames[0]
    for f in frames[1:]:
        joined = joined.join(f, on=[*dm.keys], how="inner")
    cols = [c.name for c in dm.columns]
    return joined.select(*cols)


# ---------- 自动分区 ----------

_PARTITION_TARGET_ROWS = 500_000
_PARTITION_MIN_ROWS = 1_000_000
_GRANS = {"year": 365, "month": 30, "date": 1}  # 粒度 -> 近似天数


def _temporal_keys(dm: DatasetMeta, schema: pl.Schema) -> list[str]:
    return [k for k in dm.keys if schema[k].is_temporal()]


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
    """自动分区决策：返回 ``{gran, dm_key, lo, hi}`` 或 None(flat)

    优先镜像 index HIVE 分区键（取第一个时间键）；否则按数据量+时间键选 year/month/date。
    """
    from . import table
    try:
        im = table.describe(dm.index_table)
    except Exception:
        im = None
    schema = lf.collect_schema()
    tkeys = _temporal_keys(dm, schema)
    if im is not None and im.layout.value == "hive":
        for k in im.partition_by:
            if k in schema and schema[k].is_temporal():
                return {"gran": "identity", "dm_key": k, "lo": None, "hi": None}
    if not tkeys:
        return None
    key = tkeys[0]
    lo, hi = _minmax(lf, key)
    rows = _est_rows(dm)
    if rows < _PARTITION_MIN_ROWS:
        return None
    gran = _pick_granularity((hi - lo).days, rows)
    return {"gran": gran, "dm_key": key, "lo": lo, "hi": hi}


def _minmax(lf: pl.LazyFrame, col: str) -> tuple[object, object]:
    r = lf.select(pl.col(col).min().alias("_min"), pl.col(col).max().alias("_max")).collect().row(0)
    return r[0], r[1]


def _est_rows(dm: DatasetMeta) -> int:
    from . import table
    try:
        return table.describe(dm.index_table).row_count or 0
    except Exception:
        return 0


# ---------- 增量（分区依赖） ----------

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
    obj = access.get_object(conn, tab, "table")
    if obj is None:
        return []
    return [r["id"] for r in conn.execute(
        "SELECT id FROM stkoe_data_files WHERE object_id=?", (obj["id"],)).fetchall()]


def _files_for_range(conn, tab: str, col: str, lo, hi) -> set[int]:
    """喂给分区范围 [lo,hi) 的源文件 id（无统计列的退化包含全部）"""
    obj = access.get_object(conn, tab, "table")
    if obj is None:
        return set()
    cand: set[int] | None = None
    for bound in (lo, hi):
        if bound is None:
            continue
        rows = prune_files(conn, obj["id"], where=f"{col}>{'=' if bound is lo else ''}{bound.isoformat()}")
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
        ids_ = ids.get(tab, set())
        if not ids_:
            continue
        placeholders = ",".join("?" for _ in ids_)
        rows = conn.execute(
            f"SELECT rel_path, size, mtime_ns FROM stkoe_data_files WHERE object_id="
            f"(SELECT id FROM stkoe_objects WHERE type='table' AND name=?) AND id IN ({placeholders})",
            (tab, *ids_),
        ).fetchall()
        parts += [f"{tab}|{r['rel_path']}|{r['size']}|{r['mtime_ns']}" for r in rows]
    return hashlib.sha256("\n".join(sorted(parts)).encode()).hexdigest()


# ---------- 物化引擎 ----------

def _materialize_job(dm: DatasetMeta, conn, ctl: TaskControl | None, resync: bool = False) -> DatasetSniffReport:
    """全量/增量物化；返回报告。幂等：依赖未变的分区跳过，不 bump version"""
    from . import table
    out_dir = _mat_dir(dm.name)
    out_dir.mkdir(parents=True, exist_ok=True)
    tabs = [dm.index_table, *dm.tables]
    cur_hash = _source_hash(tabs)

    version_before = dm.version
    lf = _view_lf(dm)
    plan = _partition_plan(dm, lf)
    rebuilt: list[str] = []
    changed = False
    incremental = bool(dm.partition_deps)
    partition_by: tuple[str, ...] = ()
    partition_gran = ""
    partition_deps: dict[str, str] = {}

    if plan is None:
        # flat：单文件全量
        target = out_dir / "data.parquet"
        dep = _dep_signature(conn, tabs, {t: set(_table_ids(conn, t)) for t in tabs})
        if resync or dm.partition_deps.get("") != dep or not target.exists():
            if ctl:
                ctl.stage("materializing (flat)")
            lf.sink_parquet(target)
            rebuilt.append("")
            changed = True
        partition_deps = {"": dep}
    else:
        gran = plan["gran"]
        part_key = plan["dm_key"]
        schema = lf.collect_schema()
        partition_by = (part_key,)
        partition_gran = gran
        if gran == "identity":
            idx_obj = access.get_object(conn, dm.index_table, "table")
            buckets = [r["partition_path"].rsplit("=", 1)[-1] for r in conn.execute(
                "SELECT DISTINCT partition_path FROM stkoe_data_files WHERE object_id=? "
                "AND partition_path!=''", (idx_obj["id"],)).fetchall()]
        else:
            buckets = _bucket_values(gran, plan["lo"], plan["hi"])
        for i, value in enumerate(buckets):
            if ctl:
                ctl.check()
                ctl.stage(f"materializing part={value} ({i + 1}/{len(buckets)})")
            if gran == "identity":
                # 镜像 index HIVE 分区：源文件按 partition_path 精确对齐（分区列不在文件内，无法按统计裁剪）
                ids: dict[str, set[int]] = {}
                for t in tabs:
                    if t == dm.index_table:
                        rows = conn.execute(
                            "SELECT id FROM stkoe_data_files WHERE object_id=? AND partition_path=?",
                            (idx_obj["id"], f"{part_key}={value}")).fetchall()
                        ids[t] = {r["id"] for r in rows}
                    else:
                        ids[t] = set(_table_ids(conn, t))
                part_filter = pl.col(part_key) == _parse_value(schema[part_key], value)
            else:
                lo, hi = _bucket_range(gran, value)
                ids = {t: _files_for_range(conn, t, part_key, lo, hi) for t in tabs}
                part_filter = (pl.col(part_key) >= lo) & (pl.col(part_key) < hi)
            dep = _dep_signature(conn, tabs, ids)
            part_file = out_dir / f"part={value}" / "data.parquet"
            if not resync and dm.partition_deps.get(value) == dep and part_file.exists():
                partition_deps[value] = dep
                continue
            part_file.parent.mkdir(parents=True, exist_ok=True)
            lf.filter(part_filter).sink_parquet(part_file)
            partition_deps[value] = dep
            rebuilt.append(value)
            changed = True
            if ctl:
                ctl.progress((i + 1) / len(buckets), msg=f"part={value} done")
                ctl.flush(conn)

    dm = _update_meta(conn, dm, cur_hash, partition_deps, materialized=True,
                      partition_by=partition_by, partition_gran=partition_gran,
                      bump=dm.materialized and changed)
    return DatasetSniffReport(
        name=dm.name, version_before=version_before, version_after=dm.version,
        materialized=dm.materialized, changed=changed, incremental=incremental,
        partition_by=dm.partition_by, rebuilt_partitions=tuple(rebuilt))


def _update_meta(conn, dm: DatasetMeta, cur_hash: str, partition_deps: dict,
                 *, materialized: bool, partition_by: tuple[str, ...] | None = None,
                 partition_gran: str | None = None, bump: bool = False) -> DatasetMeta:
    obj = _object(conn, dm.name)
    cur = loads(obj["meta"])
    cur["materialized"] = materialized
    cur["dependency_hash"] = cur_hash
    cur["partition_deps"] = partition_deps
    cur["materialized_at"] = now()
    cur["updated_at"] = now()
    if partition_by is not None:
        cur["partition_by"] = [*partition_by]
    if partition_gran is not None:
        cur["partition_gran"] = partition_gran
    # 首次物化不 bump（对齐表隐式注册语义）；有实际变更才 +1（幂等重跑不变）
    access.update_object_meta(conn, obj["id"], cur, signature=cur_hash, now_str=now(), bump=bump)
    conn.commit()
    return _meta(conn, _object(conn, dm.name))


def _materialize_task(dm_name: str):
    def _run(conn, ctl: TaskControl):
        obj = _object(conn, dm_name)
        dm = _meta(conn, obj)
        _materialize_job(dm, conn, ctl)
    return _run


def materialize(name: str, *, resync: bool = False, background: bool = True) -> TaskHandle:
    """物化 dataset（增量；resync=True 全量重算）。默认后台执行"""
    return run_task("dataset_materialize", name, _materialize_task(name),
                    background=background)


# ---------- 定义管理 ----------

def create(name: str, index_table: str, *tables: str, keys: list[str] | None = None,
           materialize: bool = True, background: bool = True,
           force: bool = False, **meta) -> TaskHandle:
    """注册 dataset；REPL/daemon 下默认后台自动物化

    ``materialize=False`` 只注册不物化；``background=False`` 同步执行物化。
    已存在且非 ``force`` 抛 ``DatasetExistsError``（须 ``force=True`` 覆盖重建：
    删旧定义 + 清空旧物化产物，再注册新定义）。依赖登记进 ``stkoe_depends``。
    """
    from . import table
    scan_ret = scan(index_table, *tables, keys=keys)
    if not scan_ret["ok"]:
        raise ValueError(scan_ret["message"])
    if not force and _object(catalog().conn, name) is not None:
        raise DatasetExistsError(f"dataset already registered: {name} (use force=True to redefine)")
    _root(name).mkdir(parents=True, exist_ok=True)

    def _run(conn, ctl):
        with conn_txn(conn):
            obj = _object(conn, name)
            if obj is not None:
                conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            _register(conn, name, index_table, [*tables], scan_ret["keys"],
                      scan_ret["columns"], meta=meta)
        if force:
            # 清空旧物化产物，避免与新定义混读（尤其分区策略变化时残留 part=*/data.parquet）
            shutil.rmtree(_root(name), ignore_errors=True)

    h = run_task("dataset_create", name, _run)
    if materialize:
        h = run_task("dataset_materialize", name, _materialize_task(name), background=background)
    return h


def describe(name: str) -> DatasetMeta:
    """dataset 元数据"""
    obj = _object(catalog().conn, name)
    if obj is None:
        raise DatasetNotFoundError(f"dataset not registered: {name}")
    return _meta(catalog().conn, obj)


def list() -> list[DatasetMeta]:
    rows = catalog().conn.execute(
        "SELECT * FROM stkoe_objects WHERE type='dataset' ORDER BY name").fetchall()
    return [_meta(catalog().conn, r) for r in rows]


def update(name: str, *, display_name: str | None = None, description: str | None = None,
           tags: list[str] | None = None, **extra) -> DatasetMeta:
    """更新描述性元数据（物化配置由 sniff/自动策略管理）"""
    with catalog().txn() as conn:
        obj = _object(conn, name)
        if obj is None:
            raise DatasetNotFoundError(f"dataset not registered: {name}")
        cur = loads(obj["meta"])
        if display_name is not None:
            cur["display_name"] = display_name
        if description is not None:
            cur["description"] = description
        if tags is not None:
            cur["tags"] = tags
        access.update_object_meta(conn, obj["id"], cur, now_str=now())
    return describe(name)


def drop(name: str, *, with_data: bool = False) -> TaskHandle:
    """删注册；with_data 同时删除物化产物（框架自持派生数据）。依赖边与关联 stat 同步清理"""

    def _run(conn, ctl):
        with conn_txn(conn):
            obj = _object(conn, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            access.clear_deps(conn, "dataset", name)
        if with_data:
            shutil.rmtree(_root(name), ignore_errors=True)
        from .stat import _drop_cascade
        with conn_txn(conn):
            _drop_cascade(conn, name)

    return run_task("dataset_drop", name, _run)


def rename(old: str, new: str) -> TaskHandle:
    """改名：目录 datasets/old → datasets/new + catalog/依赖边同步（关联 stat 一并改名）"""

    def _run(conn, ctl):
        src, dst = _root(old), _root(new)
        with conn_txn(conn):
            obj = _object(conn, old)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {old}")
            if _object(conn, new) is not None:
                raise ValueError(f"dataset already registered: {new}")
            if src.exists():
                if dst.exists():
                    raise FileExistsError(f"target dir already exists: {dst}")
                src.rename(dst)
            conn.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
            access.rename_obj(conn, "dataset", old, new)
            access.rename_dep(conn, "dataset", old, new)
            from .stat import _rename_cascade
            _rename_cascade(conn, old, new)

    return run_task("dataset_rename", f"{old}->{new}", _run)


# ---------- 状态 / 读取 ----------

def status(name: str) -> DatasetStatus:
    """只读对账：依赖是否过期 / 物化是否完成 / 是否在物化中"""
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        return DatasetStatus(name=name, registered=False, materialized=False,
                             materializing=False, consistent=False, partition_by=(),
                             dependency_hash=None, current_hash=None)
    dm = _meta(conn, obj)
    running = catalog().conn.execute(
        "SELECT COUNT(*) AS n FROM stkoe_tasks WHERE type='dataset_materialize' "
        "AND object_ref=? AND status IN ('running','submitted','paused')", (name,)).fetchone()["n"]
    cur = _source_hash([dm.index_table, *dm.tables])
    pending = []
    if dm.materialized and dm.partition_gran not in ("", "identity"):
        for part, dep in dm.partition_deps.items():
            if part == "":
                continue
            lo, hi = _bucket_range(dm.partition_gran, part)
            ids = {t: _files_for_range(conn, t, dm.partition_by[0], lo, hi)
                   for t in [dm.index_table, *dm.tables]}
            if _dep_signature(conn, [dm.index_table, *dm.tables], ids) != dep:
                pending.append(part)
    return DatasetStatus(name=name, registered=True, materialized=dm.materialized,
                         materializing=running > 0, consistent=dm.dependency_hash == cur,
                         partition_by=dm.partition_by,
                         dependency_hash=dm.dependency_hash, current_hash=cur,
                         pending_partitions=tuple(pending))


def schema(name: str) -> pl.Schema:
    """join 视图 schema（实时；不触发物化）"""
    dm = describe(name)
    return _view_lf(dm).collect_schema()


def partitions(name: str) -> list[str]:
    """物化分区列表（如 part=2020）"""
    dm = describe(name)
    if not dm.materialized:
        return []
    root = _mat_dir(name)
    if not root.exists():
        return []
    out = []
    for p in sorted(root.glob("part=*")):
        if any(p.rglob("*.parquet")):
            out.append(p.name)
    return out


def select(name: str, *, columns: list[str] | None = None,
           where: pl.Expr | str | None = None, partition: str | None = None) -> pl.LazyFrame:
    """读取 dataset（lazy）：物化完成走物化数据，否则实时 join；不触发物化"""
    dm = describe(name)
    if _materialized_ready(dm):
        lf = pl.scan_parquet(_mat_dir(name), hive_partitioning=True)
    else:
        lf = _view_lf(dm)
    if partition is not None:
        names = set(lf.collect_schema().names())
        if "part" not in names:
            raise ValueError(f"dataset not partitioned; cannot filter partition={partition}")
        # hive_partitioning 会把 ISO 值推断成 Date/Int，统一 cast String 用前缀匹配
        # （year=2020 前缀命中；month=2020-01 前缀命中；date/identity 全值命中）
        lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
    if where is not None:
        from .query import to_expr
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    if columns is not None:
        lf = lf.select(*columns)
    return lf


def _materialized_ready(dm: DatasetMeta) -> bool:
    if not dm.materialized:
        return False
    running = catalog().conn.execute(
        "SELECT COUNT(*) AS n FROM stkoe_tasks WHERE type='dataset_materialize' "
        "AND object_ref=? AND status IN ('running','submitted','paused')", (dm.name,)).fetchone()["n"]
    return running == 0


# ---------- sniff ----------

def sniff(name: str, *, resync: bool = False) -> DatasetSniffReport:
    """检查依赖 → 增量重物化（幂等）；resync=True 全量"""
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise DatasetNotFoundError(f"dataset not registered: {name}")
    dm = _meta(conn, obj)
    return _materialize_job(dm, conn, None, resync=resync)


def sniff_all() -> list[DatasetSniffReport]:
    return [sniff(dm.name) for dm in list()]


# ---------- 数据标识（供 stat/触发器等派生物判定缓存有效性） ----------

def data_key(name: str) -> str:
    """当前数据标识：物化完成对齐 dependency_hash（物化时点），否则当前源签名"""
    dm = describe(name)
    if dm.materialized and dm.dependency_hash:
        return dm.dependency_hash
    return _source_hash([dm.index_table, *dm.tables])
