"""DATASET 模块：索引表 + 多表 join 的逻辑数据集（DatasetController，async 接口）

设计要点：
- 注册在 catalog（type='dataset'，同 data_dir/catalog.db），meta JSON 存
  DatasetMeta；列带 source_table/source_field 映射 → 字段级血缘
- **物化对用户透明**（核心设计）：
  - add 后自动物化；scan 依赖变化增量重物化；get 读前若未物化/过期 → 先增量物化再读
  - 增量按分区粒度：partition_deps 记录每分区依赖源文件签名，只重算失配分区
  - 分区策略：镜像 index HIVE 分区键（identity）→ 数据量+时间键选 year/month/date → flat
- 物化产物为框架自持派生数据，写 datasets/<name>/（无 .materialized/ 嵌套层级）
- 依赖登记 stkoe_depends：dataset → 成员 table（detail 记 keys + 字段映射）

行数语义（left join 以 index 为基准）：dataset 行数 == index 表行数；成员表缺失的
键行保留（列值 null），成员表多余的键行不参与。
"""
from __future__ import annotations

import asyncio
import builtins
import datetime
import hashlib
import shutil
import threading
from pathlib import Path

import polars as pl

from ..jsonutil import dumps_str, loads
from ..table.controller import TableController, DEFAULT_IGNORE_COLS, DependencyError
from ..table.query import prune_files, to_expr
from ..table.spec import ColumnMeta
from ..table.util import now
from .spec import DatasetMeta, DatasetScanReport


class DatasetNotFoundError(FileNotFoundError):
    pass


class DatasetExistsError(ValueError):
    pass


_PARTITION_TARGET_ROWS = 500_000
_PARTITION_MIN_ROWS = 1_000_000
_GRANS = {"year": 365, "month": 30, "date": 1}

_MAT_LOCKS: dict[str, threading.Lock] = {}
_MAT_LOCK = threading.Lock()


class DatasetController:
    """dataset 控制面：注册/读取/物化/删除/元数据/列表（async 方法，阻塞 IO 走线程）

    复用 TableController 读源表（_get_lazy/_meta_sync/_object），共享同一 catalog。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        self._tc = TableController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._tc.data_dir
        self.catalog = self._tc.catalog
        self.root = self.data_dir / "datasets"
        self.ignore_cols = set(ignore_cols)

    # ---------- 内部转换 ----------

    def _root(self, name: str) -> Path:
        return self.root / name

    def _object(self, conn, name: str):
        from ..table.catalog import get_object

        return get_object(conn, name, "dataset")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _dataset_meta(self, conn, obj) -> DatasetMeta:
        meta = loads(obj["meta"])
        materialized = bool(meta.get("materialized", False))
        dep_hash = meta.get("dependency_hash") or ""
        cur = obj["signature"] or "" if materialized else ""
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

    def _source_hash(self, tables: list[str]) -> str:
        """底层表依赖签名：sha256(sorted 表+磁盘签名)；同时触发读前快检保鲜"""
        parts = []
        for t in sorted(builtins.set(tables)):
            parts.append(f"{t}:{self._tc.data_key(t)}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _read_source_hash(self, dm: DatasetMeta) -> str:
        return self._source_hash([dm.index_table, *dm.tables])

    # ---------- 列映射 / 实时 join ----------

    def scan_spec(self, index_table: str, *tables: str,
                  keys: list[str] | None = None) -> dict:
        """校验 index + 成员表，自动推导 join 键与列映射

        **join 键由 index 表定义**：keys 缺省 = index 表的全部**非工具**列
        （排除 ignore_cols，如 ``optime``）；每个键必须存在于所有成员表，缺列报错。
        """
        members = [index_table, *tables]
        metas = [self._tc._meta_sync(t) for t in members]
        for t, m in zip(members, metas):
            if not m.files:
                return {"ok": False, "message": f"table has no data: {t}"}
        index_by_name = {c.name: c for c in metas[0].columns}
        if keys is None:
            keys = [c.name for c in metas[0].columns if not c.is_tool]
        else:
            missing = [k for k in keys if k not in index_by_name]
            if missing:
                return {"ok": False, "message": f"join keys must be columns of index "
                                                f"'{index_table}'; not in index: {missing}"}
        if not keys:
            return {"ok": False, "message": f"index '{index_table}' has no columns "
                                            f"to use as join keys"}

        colsets = [{c.name for c in m.columns} for m in metas]
        for t, cs in zip(members[1:], colsets[1:]):
            missing = [k for k in keys if k not in cs]
            if missing:
                return {"ok": False, "message": f"member table '{t}' missing join "
                                                f"keys: {missing}"}

        columns: list[ColumnMeta] = []
        used: set[str] = builtins.set()
        for k in keys:
            c = index_by_name[k]
            columns.append(self._inherit_col_meta(c, name=k, as_index=True,
                                                  source_table=index_table,
                                                  source_field=k))
            used.add(k)
        for t, m in zip(members, metas):
            for c in m.columns:
                if c.is_tool or c.name in keys:
                    continue
                out = c.name if c.name not in used else f"{c.name}__{t}"
                used.add(out)
                columns.append(self._inherit_col_meta(c, name=out,
                                                      source_table=t,
                                                      source_field=c.name))
        return {"ok": True, "keys": keys, "columns": columns, "tables": members,
                "index_table": index_table, "tables_meta": metas}

    @staticmethod
    def _inherit_col_meta(c: ColumnMeta, **override) -> ColumnMeta:
        """构造 dataset 列：继承源列元数据（display_name/description/unit/formula/tags）"""
        return ColumnMeta(
            name=override.pop("name"),
            display_name=c.display_name,
            description=c.description,
            data_type=c.data_type,
            unit=c.unit,
            formula=c.formula,
            tags=c.tags,
            as_index=override.pop("as_index", False),
            source_table=override.pop("source_table", c.source_table),
            source_field=override.pop("source_field", c.source_field),
        )

    def _align_keys(self, lf: pl.LazyFrame, keys: list[str]) -> pl.LazyFrame:
        """join 键 dtype 归一：datetime 时区元数据不一致时不 cast 会 join 失败"""
        casts = []
        schema = lf.collect_schema()
        for k in keys:
            dt = schema.get(k)
            if isinstance(dt, pl.Datetime):
                casts.append(pl.col(k).cast(pl.Datetime(dt.time_unit, time_zone=None)))
        if casts:
            lf = lf.with_columns(*casts)
        return lf

    def _view_lf(self, dm: DatasetMeta) -> pl.LazyFrame:
        """实时 join 视图（lazy）：按列映射重命名后 left join on keys（index 为左表）"""
        by_src: dict[str, list[ColumnMeta]] = {}
        for c in dm.columns:
            by_src.setdefault(c.source_table, []).append(c)

        def frame(t: str) -> pl.LazyFrame:
            lf = self._tc._get_lazy(t)
            used_src = {c.source_field for c in by_src.get(t, [])}
            exprs = [pl.col(c.source_field).alias(c.name) for c in by_src.get(t, [])]
            exprs += [pl.col(k).alias(k) for k in dm.keys if k not in used_src]
            return self._align_keys(lf.select(*exprs), dm.keys)

        frames = [frame(dm.index_table)]
        for t in dm.tables:
            frames.append(frame(t))
        joined = frames[0]
        for f in frames[1:]:
            joined = joined.join(f, on=[*dm.keys], how="left")
        return joined.select(*[c.name for c in dm.columns])

    # ---------- 自动分区 / 分区工具 ----------

    def _est_rows(self, index_table: str) -> int:
        from ..table.catalog import get_object

        conn = self.catalog.new_conn()
        try:
            obj = get_object(conn, index_table, "table")
            if obj is None:
                return 0
            r = conn.execute(
                "SELECT COALESCE(SUM(row_count),0) AS n FROM stkoe_data_files "
                "WHERE object_id=?", (obj["id"],)).fetchone()
            return int(r["n"] or 0)
        finally:
            conn.close()

    def _pick_granularity(self, span_days: float, rows: int) -> str:
        est = rows / _PARTITION_TARGET_ROWS
        best = "year"
        best_d = float("inf")
        for g, days in _GRANS.items():
            n = max(1, int(span_days / days))
            d = abs(n - est)
            if d < best_d:
                best, best_d = g, d
        return best

    def _partition_plan(self, dm: DatasetMeta, lf: pl.LazyFrame) -> dict | None:
        """自动分区决策：镜像 index HIVE 分区键 → 数据量+时间键选 gran → None(flat)"""
        try:
            im = self._tc._meta_sync(dm.index_table)
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
        lo, hi = self._minmax(lf, key)
        rows = self._est_rows(dm.index_table)
        if rows < _PARTITION_MIN_ROWS:
            return None
        return {"gran": self._pick_granularity((hi - lo).days, rows),
                "dm_key": key, "lo": lo, "hi": hi}

    def _minmax(self, lf: pl.LazyFrame, col: str) -> tuple[object, object]:
        r = lf.select(pl.col(col).min().alias("_min"),
                      pl.col(col).max().alias("_max")).collect().row(0)
        return r[0], r[1]

    def _bucket_values(self, gran: str, lo, hi) -> list[str]:
        out: list[str] = []
        d = lo if not isinstance(lo, datetime.datetime) else lo.date()
        h = hi if not isinstance(hi, datetime.datetime) else hi.date()
        while d <= h:
            if gran == "year":
                out.append(str(d.year))
                d = d.replace(year=d.year + 1, month=1, day=1)
            elif gran == "month":
                out.append(d.strftime("%Y-%m"))
                d = d.replace(year=d.year + 1, month=1, day=1) if d.month == 12 \
                    else d.replace(month=d.month + 1, day=1)
            else:
                out.append(d.isoformat())
                d += datetime.timedelta(days=1)
        return out

    def _table_ids(self, conn, tab: str) -> list[int]:
        obj = self._tc._object(conn, tab)
        if obj is None:
            return []
        return [r["id"] for r in conn.execute(
            "SELECT id FROM stkoe_data_files WHERE object_id=?", (obj["id"],)).fetchall()]

    def _files_for_range(self, conn, tab: str, col: str, lo, hi) -> set[int]:
        """喂给分区范围 [lo,hi) 的源文件 id（用 stkoe_file_stats 裁剪；无统计列退化全量）"""
        obj = self._tc._object(conn, tab)
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
                "SELECT id FROM stkoe_data_files WHERE object_id=?",
                (obj["id"],)).fetchall()}
        return cand

    def _dep_signature(self, conn, tabs: list[str], ids: dict[str, set[int]]) -> str:
        parts: list[str] = []
        for tab in sorted(tabs):
            ids_ = ids.get(tab, builtins.set())
            if not ids_:
                continue
            ph = ",".join("?" for _ in ids_)
            rows = conn.execute(
                f"SELECT rel_path, size, mtime_ns FROM stkoe_data_files WHERE object_id="
                f"(SELECT id FROM stkoe_objects WHERE type='table' AND name=?) "
                f"AND id IN ({ph})", (tab, *ids_)).fetchall()
            parts += [f"{tab}|{r['rel_path']}|{r['size']}|{r['mtime_ns']}" for r in rows]
        return hashlib.sha256("\n".join(sorted(parts)).encode()).hexdigest()

    # ---------- 物化引擎 ----------

    def _materialize_sync(self, dm: DatasetMeta, conn=None, *,
                          resync: bool = False,
                          on_progress=None) -> DatasetScanReport:
        """全量/增量物化；幂等（依赖未变的分区跳过，不 bump version）。

        ``on_progress(i, total, msg)`` 可选进度回调（同步调用，来自 worker 线程）。
        """
        cx = conn if conn is not None else self.catalog.new_conn()
        own_conn = conn is None
        try:
            meta = self._meta_dict(cx, dm.name)
            prev_deps: dict[str, str] = dict(meta.get("partition_deps", {}))
            prev_materialized = bool(meta.get("materialized", False))
            version_before = dm.version

            out_dir = self._root(dm.name)
            out_dir.mkdir(parents=True, exist_ok=True)
            tabs = [dm.index_table, *dm.tables]
            lf = self._view_lf(dm)
            plan = self._partition_plan(dm, lf)
            rebuilt: list[str] = []
            changed = False
            incremental = bool(prev_deps)

            if plan is None:
                target = out_dir / "data.parquet"
                dep = self._dep_signature(
                    cx, tabs, {t: builtins.set(self._table_ids(cx, t)) for t in tabs})
                if resync or prev_deps.get("") != dep or not target.exists():
                    lf.sink_parquet(target)
                    rebuilt.append("")
                    changed = True
                new_deps = {"": dep}
                partition_by, partition_gran = (), ""
            else:
                gran = plan["gran"]
                part_key = plan["dm_key"]
                partition_by, partition_gran = (part_key,), gran
                if gran == "identity":
                    idx_obj = self._tc._object(cx, dm.index_table)
                    buckets = [r["partition_path"].rsplit("=", 1)[-1]
                               for r in cx.execute(
                                   "SELECT DISTINCT partition_path FROM stkoe_data_files "
                                   "WHERE object_id=? AND partition_path!=''",
                                   (idx_obj["id"],)).fetchall()]
                else:
                    buckets = self._bucket_values(gran, plan["lo"], plan["hi"])
                new_deps: dict[str, str] = {}
                for i, value in enumerate(buckets):
                    if on_progress is not None:
                        on_progress(i + 1, len(buckets), f"{dm.name}: part={value}")
                    if gran == "identity":
                        ids: dict[str, set[int]] = {}
                        for t in tabs:
                            if t == dm.index_table:
                                rows = cx.execute(
                                    "SELECT id FROM stkoe_data_files WHERE object_id=? "
                                    "AND partition_path=?",
                                    (idx_obj["id"], f"{part_key}={value}")).fetchall()
                                ids[t] = {r["id"] for r in rows}
                            else:
                                ids[t] = builtins.set(self._table_ids(cx, t))
                        part_filter = pl.col(part_key).cast(pl.String) == str(value)
                    else:
                        lo, hi = self._bucket_range(gran, value)
                        ids = {t: self._files_for_range(cx, t, part_key, lo, hi)
                               for t in tabs}
                        part_filter = (pl.col(part_key) >= lo) & (pl.col(part_key) < hi)
                    dep = self._dep_signature(cx, tabs, ids)
                    part_file = out_dir / f"part={value}" / "data.parquet"
                    if not resync and prev_deps.get(value) == dep and part_file.exists():
                        new_deps[value] = dep
                        continue
                    part_file.parent.mkdir(parents=True, exist_ok=True)
                    lf.filter(part_filter).sink_parquet(part_file)
                    new_deps[value] = dep
                    rebuilt.append(value)
                    changed = True

            dm2 = self._update_meta(cx, dm, new_deps, partition_by=partition_by,
                                    partition_gran=partition_gran,
                                    bump=prev_materialized and changed)
            return DatasetScanReport(
                name=dm.name, version_before=version_before,
                version_after=dm2.version, materialized=True, changed=changed,
                incremental=incremental, partition_by=dm2.partition_by,
                rebuilt_partitions=tuple(rebuilt))
        finally:
            if own_conn:
                cx.close()

    def _bucket_range(self, gran: str, value: str):
        if gran == "year":
            y = int(value)
            return datetime.date(y, 1, 1), datetime.date(y + 1, 1, 1)
        if gran == "month":
            y, m = (int(x) for x in value.split("-"))
            hi = datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1)
            return datetime.date(y, m, 1), hi
        d = datetime.date.fromisoformat(value)
        return d, d + datetime.timedelta(days=1)

    def _update_meta(self, conn, dm: DatasetMeta, partition_deps: dict, *,
                     partition_by: tuple[str, ...], partition_gran: str,
                     bump: bool = False) -> DatasetMeta:
        """写物化态：dependency_hash=当前源签名（首次物化不 bump；实际变更 +1）"""
        from ..table.catalog import update_object_meta

        cur_hash = self._read_source_hash(dm)
        obj = self._object(conn, dm.name)
        if obj is None:
            raise DatasetNotFoundError(f"dataset not registered: {dm.name}")
        meta = self._meta_dict(conn, dm.name)
        meta["materialized"] = True
        meta["materialized_at"] = now()
        meta["dependency_hash"] = cur_hash
        meta["partition_deps"] = partition_deps
        meta["partition_by"] = builtins.list(partition_by)
        meta["partition_gran"] = partition_gran
        update_object_meta(conn, obj["id"], meta, signature=cur_hash,
                           now_str=now(), bump=bump)
        conn.commit()
        return self._dataset_meta(conn, self._object(conn, dm.name))

    # ---------- add / list ----------

    def _register(self, conn, name: str, index_table: str, tables: list[str],
                  keys: list[str], columns: list[ColumnMeta],
                  meta: dict | None = None):
        from ..table.catalog import insert_object

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
        obj = insert_object(conn, "dataset", name, cur, "", now())
        deps = []
        for t in dict.fromkeys([index_table, *tables]):
            fields = [c.source_field for c in columns if c.source_table == t]
            deps.append(("table", t, {"keys": keys, "fields": fields}))
        self._set_deps(conn, "dataset", name, deps)
        return obj

    def _add_sync(self, name: str, index_table: str, tables: list[str],
                  keys: list[str] | None, materialize: bool) -> DatasetMeta:
        spec = self.scan_spec(index_table, *tables, keys=keys)
        if not spec["ok"]:
            raise ValueError(spec["message"])
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise DatasetExistsError(f"dataset already registered: {name}")
            self._register(conn, name, index_table, [*tables], spec["keys"],
                           spec["columns"])
        dm = DatasetMeta(name=name,
                         version=self._object(self.catalog.new_conn(), name)["version"],
                         index_table=index_table, tables=tuple(tables),
                         keys=tuple(spec["keys"]), columns=tuple(spec["columns"]))
        if materialize:
            conn = self.catalog.new_conn()
            try:
                self._materialize_sync(dm, conn)
            finally:
                conn.close()
        return self._describe_sync(name)

    def _describe_sync(self, name: str) -> DatasetMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            return self._dataset_meta(conn, obj)
        finally:
            conn.close()

    def _list_sync(self) -> list[DatasetMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='dataset' "
                                "ORDER BY name").fetchall()
            return [self._dataset_meta(conn, r) for r in rows]
        finally:
            conn.close()

    # ---------- 读取 ----------

    def _get_lazy_sync(self, name: str, *, columns: list[str] | None = None,
                       where: pl.Expr | str | None = None,
                       partition: str | None = None) -> pl.LazyFrame:
        """读 dataset（lazy）。物化完成且与源一致 → 读物化 parquet；
        未物化/过期 → 实时 join 视图（不隐式物化，物化走显式 scan）"""
        dm = self._describe_sync(name)
        if dm.materialized and dm.curated:
            lf = pl.scan_parquet(self._root(name), hive_partitioning=True)
            if partition is not None:
                names = builtins.set(lf.collect_schema().names())
                if "part" not in names:
                    raise ValueError(f"dataset not partitioned; cannot filter "
                                     f"partition={partition}")
                lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
        else:
            lf = self._view_lf(dm)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if columns is not None:
            lf = lf.select(*columns)
        return lf

    def _get_sync(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None,
                  partition: str | None = None,
                  limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        lf = self._get_lazy_sync(name, columns=columns, where=where, partition=partition)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def _lock(self, name: str) -> threading.Lock:
        with _MAT_LOCK:
            return _MAT_LOCKS.setdefault(name, threading.Lock())

    # ---------- set / scan / delete ----------

    def _set_sync(self, name: str, kw: dict) -> DatasetMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            meta = dict(self._meta_dict(conn, name))
            for key, value in kw.items():
                if key in ("display_name", "description", "source"):
                    meta[key] = str(value)
                elif key == "tags":
                    meta["tags"] = [t.strip() for t in str(value).split(",") if t.strip()]
                else:
                    extra = dict(meta.get("extra") or {})
                    extra[key] = value
                    meta["extra"] = extra
            self._update_object_meta(conn, obj["id"], meta, now_str=now(), bump=False)
            conn.commit()
            return self._dataset_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _scan_sync(self, name: str | None, *, all: bool = False,
                   resync: bool = False,
                   on_progress=None) -> DatasetScanReport | list[DatasetScanReport]:
        if all:
            return [self._scan_one(dm, resync=resync, on_progress=on_progress)
                    for dm in self._list_sync()]
        return self._scan_one(self._describe_sync(name), resync=resync,
                              on_progress=on_progress)

    def _scan_one(self, dm: DatasetMeta, *, resync: bool = False,
                  on_progress=None) -> DatasetScanReport:
        """增量重物化（幂等）；源列定义变化时先同步 columns 再全量重物化"""
        conn = self.catalog.new_conn()
        try:
            meta_changed = self._sync_source_meta(dm, conn)
            if meta_changed:
                dm = self._dataset_meta(conn, self._object(conn, dm.name))
            return self._materialize_sync(dm, conn, resync=(resync or meta_changed),
                                          on_progress=on_progress)
        finally:
            conn.close()

    def _sync_source_meta(self, dm: DatasetMeta, conn) -> bool:
        """对比成员表 meta，源表列定义变化则同步 dataset columns；返回是否变化

        同步范围包括结构（列集合/类型/来源映射）与列元数据
        （display_name/description/unit/formula/tags，来自源表 ``table col``）——
        dataset 列不提供直接修改入口，源表说明变更经 scan 自动覆盖。
        """
        spec = self.scan_spec(dm.index_table, *dm.tables, keys=list(dm.keys))
        if not spec["ok"]:
            return False
        obj = self._object(conn, dm.name)
        if obj is None:
            return False
        meta = self._meta_dict(conn, dm.name)
        old = meta.get("columns", [])
        new = [c.to_dict() for c in spec["columns"]]
        if self._cols_equal(old, new):
            return False
        meta["columns"] = new
        meta.pop("materialized", None)
        self._update_object_meta(conn, obj["id"], meta, now_str=now(), bump=False)
        conn.commit()
        return True

    def _cols_equal(self, a: list[dict], b: list[dict]) -> bool:
        def sig(c: dict) -> tuple:
            return (c.get("name"), c.get("data_type"), c.get("source_table"),
                    c.get("source_field"), c.get("as_index"),
                    c.get("display_name"), c.get("description"),
                    c.get("unit"), c.get("formula"), tuple(c.get("tags") or ()))
        return [sig(x) for x in a] == [sig(x) for x in b]

    def _delete_sync(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise DatasetNotFoundError(f"dataset not registered: {name}")
            dependents = self._dependents(conn, "dataset", name)
            if dependents and not force:
                raise DependencyError(dependents)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            self._clear_deps(conn, "dataset", name)
            conn.commit()
        finally:
            conn.close()
        if with_data:
            shutil.rmtree(self._root(name), ignore_errors=True)
        return {"deleted": name}

    def data_key(self, name: str) -> str:
        """当前数据标识：物化完成 = dependency_hash；未物化 = 当前源签名"""
        dm = self._describe_sync(name)
        if dm.materialized:
            return self._meta_dict(self.catalog.new_conn(), name).get("dependency_hash") or ""
        return self._read_source_hash(dm)

    # ---------- catalog 便捷（dataset 侧依赖图） ----------

    def _set_deps(self, conn, obj_type, obj_name, deps):
        from ..table.catalog import set_deps

        set_deps(conn, obj_type, obj_name, deps)

    def _clear_deps(self, conn, obj_type, obj_name):
        from ..table.catalog import clear_deps

        clear_deps(conn, obj_type, obj_name)

    def _dependents(self, conn, obj_type, name):
        from ..table.catalog import dependents

        return dependents(conn, obj_type, name)

    def _update_object_meta(self, conn, object_id, meta, now_str=None, bump=False):
        from ..table.catalog import update_object_meta

        update_object_meta(conn, object_id, meta, now_str=now_str, bump=bump)

    # ---------- async 接口 ----------

    async def add(self, name: str, index_table: str, *tables: str,
                  keys: list[str] | None = None,
                  materialize: bool = False) -> DatasetMeta:
        """创建 dataset：校验 join 规格 → 注册（不自动物化，物化走显式 scan）"""
        return await asyncio.to_thread(self._add_sync, name, index_table,
                                       list(tables), keys, materialize)

    async def get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None,
                  partition: str | None = None,
                  limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读 dataset（collect）。物化完成读物化数据，否则实时 join 视图；
        不隐式物化（物化走显式 scan）。``count_total=True`` 返回 ``(df, total)``"""
        return await asyncio.to_thread(
            self._get_sync, name, columns=columns, where=where,
            partition=partition, limit=limit, offset=offset, count_total=count_total)

    async def meta(self, name: str) -> DatasetMeta:
        """dataset 元数据（describe 别名，接口统一）"""
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[DatasetMeta]:
        """已注册 dataset 列表"""
        return await asyncio.to_thread(self._list_sync)

    async def set(self, name: str, **kw) -> DatasetMeta:
        """更新 dataset 级元数据（display_name/description/tags；其余键进 extra）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def scan(self, name: str | None = None, *, all: bool = False,
                   resync: bool = False,
                   on_progress=None) -> DatasetScanReport | list[DatasetScanReport]:
        """检查依赖 → 增量重物化（幂等）；``all=True`` 全部已注册 dataset

        ``on_progress(i, total, msg)`` 可选进度回调（worker 线程同步调用，逐分区）。
        """
        return await asyncio.to_thread(self._scan_sync, name, all=all, resync=resync,
                                       on_progress=on_progress)

    async def delete(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        """删除 dataset 注册与物化产物（成员表=用户数据，从不删除）"""
        return await asyncio.to_thread(self._delete_sync, name, force=force,
                                       with_data=with_data)


__all__ = ["DatasetController", "DatasetNotFoundError", "DatasetExistsError"]