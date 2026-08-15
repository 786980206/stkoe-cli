"""FIELDSET 模块：基于 dataset 的衍生指标集（FieldsetController，async 接口）

定位：
- fieldset = 源 dataset 列 + 若干衍生指标（公式计算引擎生成字段），注册于 catalog
  （type='fieldset'），meta JSON 存 FieldsetMeta（含 fields 的 FieldMeta 列表）
- 物化：``fieldset scan`` 把**已校验**指标 + 源 keys 落盘到 ``fieldsets/<name>/``，
  目录布局镜像源 dataset（源已分区则按同分区键/粒度镜像，否则 flat 单文件）
- 依赖：fieldset → 源 dataset（stkoe_depends 登记，删除源 dataset 需 ``--force``）
- 读取：物化完成且与源+公式一致（curated）读物化 parquet；否则实时基于源 dataset
  视图计算（不隐式物化，物化走显式 scan）

指标生命周期：add/set 后 validated=False（未校验）；``check`` 校验通过才参与物化。
"""
from __future__ import annotations

import asyncio
import hashlib
import shutil
from pathlib import Path

import polars as pl

from ..jsonutil import dumps_str, loads
from ..table.catalog import get_object, insert_object
from ..table.controller import DEFAULT_IGNORE_COLS, DependencyError
from ..table.util import now
from .engine import get_engine
from .spec import FieldMeta, FieldsetCheckResult, FieldsetMeta, FieldsetScanReport


class FieldsetNotFoundError(FileNotFoundError):
    pass


class FieldsetExistsError(ValueError):
    pass


META_FIELDS = ("display_name", "description", "source")  # dataset/engine/keys 创建时固定


class FieldsetController:
    """衍生指标集控制面：add/get/meta/list/set/scan/delete + 指标级 add/set/del/meta/check/test"""

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..dataset.controller import DatasetController

        self._dc = DatasetController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._dc.data_dir
        self.catalog = self._dc.catalog
        self.root = self.data_dir / "fieldsets"

    # ---------- 内部转换 ----------

    def _root(self, name: str) -> Path:
        return self.root / name

    def _object(self, conn, name: str):
        return get_object(conn, name, "fieldset")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _fieldset_meta(self, conn, obj) -> FieldsetMeta:
        meta = loads(obj["meta"])
        fields = tuple(FieldMeta.from_dict(f) for f in meta.get("fields", []))
        dm = self._dataset_meta_safe(meta.get("dataset", ""))
        cur_hash = self._current_hash(dm, fields)
        stored = meta.get("dependency_hash") or ""
        materialized = bool(meta.get("materialized", False))
        return FieldsetMeta(
            name=obj["name"],
            version=obj["version"],
            dataset=meta.get("dataset", ""),
            engine=meta.get("engine", "polars"),
            keys=tuple(meta.get("keys", [])),
            fields=fields,
            partition_by=tuple(meta.get("partition_by", [])),
            partition_gran=meta.get("partition_gran", ""),
            materialized=materialized,
            materialized_at=meta.get("materialized_at"),
            curated=materialized and stored == cur_hash,
            columns=tuple(dm.columns),
            extra=meta.get("extra") or {},
            display_name=meta.get("display_name", obj["name"]),
            description=meta.get("description", ""),
            tags=tuple(meta.get("tags", [])),
            source=meta.get("source", "local"),
            created_at=obj["created_at"],
            updated_at=obj["updated_at"],
        )

    def _dataset_meta_safe(self, name: str):
        if not name:
            return None
        try:
            return self._dc._describe_sync(name)
        except Exception:
            return None

    def _current_hash(self, dm, fields) -> str:
        """物化一致性签名 = 源 dataset data_key + 已校验指标（名称+公式）"""
        if dm is None:
            return ""
        parts = [f"dataset:{dm.name}:{self._dc.data_key(dm.name)}"]
        for f in sorted(fields, key=lambda f: f.name):
            if f.validated:
                parts.append(f"{f.name}:{f.formula}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _valid_fields(self, fields) -> list[FieldMeta]:
        return [f for f in fields if f.validated and f.formula]

    # ---------- add / list ----------

    def _register(self, conn, name: str, dataset: str, engine: str,
                  keys: list[str], meta: dict | None = None):
        cur = {
            "dataset": dataset,
            "engine": engine,
            "keys": keys,
            "fields": [],
            "partition_by": [],
            "partition_gran": "",
            "materialized": False,
            "materialized_at": None,
            "dependency_hash": None,
        }
        if meta:
            cur["extra"] = meta.get("extra") or {}
            for key in ("display_name", "description", "source", "tags"):
                if key in meta:
                    cur[key] = meta[key]
        obj = insert_object(conn, "fieldset", name, cur, "", now())
        self._set_deps(conn, "fieldset", name, [("dataset", dataset, {"keys": keys})])
        return obj

    def _add_sync(self, name: str, dataset: str | None, engine: str,
                  meta: dict | None) -> FieldsetMeta:
        if not dataset:
            raise ValueError("fieldset add 需要 --dataset <dataset 名>")
        dm = self._dc._describe_sync(dataset)  # 源 dataset 必须已注册
        meta = self._apply_meta_fields({}, meta or {})
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise FieldsetExistsError(f"fieldset already registered: {name}")
            self._register(conn, name, dataset, engine, list(dm.keys), meta)
        return self._describe_sync(name)

    @staticmethod
    def _apply_meta_fields(fields_out: dict, kw: dict) -> dict:
        """规范化 add/set 的标准元数据键（tags 逗号分隔），其余任意键进 extra"""
        for key, value in kw.items():
            if key == "tags":
                fields_out["tags"] = [t.strip() for t in str(value).split(",") if t.strip()]
            elif key in META_FIELDS:
                fields_out[key] = str(value)
            else:
                extra = dict(fields_out.get("extra") or {})
                extra[key] = value
                fields_out["extra"] = extra
        return fields_out

    def _list_sync(self) -> list[FieldsetMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='fieldset' "
                                "ORDER BY name").fetchall()
            return [self._fieldset_meta(conn, r) for r in rows]
        finally:
            conn.close()

    def _describe_sync(self, name: str) -> FieldsetMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FieldsetNotFoundError(f"fieldset not registered: {name}")
            return self._fieldset_meta(conn, obj)
        finally:
            conn.close()

    # ---------- set / delete ----------

    def _set_sync(self, name: str, kw: dict) -> FieldsetMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FieldsetNotFoundError(f"fieldset not registered: {name}")
            meta = dict(self._meta_dict(conn, name))
            meta = self._apply_meta_fields(meta, kw)
            self._update_object_meta(conn, obj["id"], meta, now_str=now(), bump=False)
            conn.commit()
            return self._fieldset_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _delete_sync(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        from ..table.catalog import dependents

        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FieldsetNotFoundError(f"fieldset not registered: {name}")
            dependents_rows = dependents(conn, "fieldset", name)
            if dependents_rows and not force:
                raise DependencyError(dependents_rows)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.execute("DELETE FROM stkoe_depends WHERE obj_type='fieldset' AND obj_name=?",
                         (name,))
            conn.commit()
        finally:
            conn.close()
        if with_data:
            shutil.rmtree(self._root(name), ignore_errors=True)
        return {"deleted": name}

    # ---------- 指标级操作 ----------

    def _mutate_fields(self, name: str, fn, bump: bool = True) -> FieldsetMeta:
        """在字段列表上执行变更（add/set/del 共用）：取对象 → fn(fields) → 写回"""
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FieldsetNotFoundError(f"fieldset not registered: {name}")
            meta = dict(self._meta_dict(conn, name))
            fields = [FieldMeta.from_dict(f) for f in meta.get("fields", [])]
            fields = fn(fields)
            meta["fields"] = [f.to_dict() for f in fields]
            self._update_object_meta(conn, obj["id"], meta, now_str=now(), bump=bump)
            conn.commit()
            return self._fieldset_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _add_field_sync(self, name: str, field: str, kw: dict) -> FieldsetMeta:
        if field in self._describe_sync(name).keys:
            raise ValueError(f"field 名与源 keys 冲突: {field}")
        if not kw.get("formula"):
            raise ValueError("field add 需要 --formula <表达式>")

        def fn(fields: list[FieldMeta]) -> list[FieldMeta]:
            if any(f.name == field for f in fields):
                raise FieldsetExistsError(f"field already exists: {name}.{field}")
            fields.append(self._build_field(field, kw))
            return fields

        return self._mutate_fields(name, fn)

    def _set_field_sync(self, name: str, field: str, kw: dict) -> FieldsetMeta:
        def fn(fields: list[FieldMeta]) -> list[FieldMeta]:
            idx = next((i for i, f in enumerate(fields) if f.name == field), None)
            if idx is None:
                raise FieldsetNotFoundError(f"field not found: {name}.{field}")
            merged = self._build_field(field, kw, base=fields[idx])
            fields[idx] = merged
            return fields

        return self._mutate_fields(name, fn)

    def _del_field_sync(self, name: str, field: str) -> FieldsetMeta:
        def fn(fields: list[FieldMeta]) -> list[FieldMeta]:
            out = [f for f in fields if f.name != field]
            if len(out) == len(fields):
                raise FieldsetNotFoundError(f"field not found: {name}.{field}")
            return out

        return self._mutate_fields(name, fn)

    @staticmethod
    def _build_field(name: str, kw: dict, base: FieldMeta | None = None) -> FieldMeta:
        """构造/修改 FieldMeta：编辑公式后 validated 复位 False（需重新 check）"""
        cur = base.to_dict() if base is not None else {
            "name": name, "formula": kw.get("formula", ""),
            "display_name": "", "description": "", "unit": None, "tags": [],
            "validated": False,
        }
        for key in ("formula", "display_name", "description", "unit", "tags"):
            if key in kw:
                cur[key] = kw[key]
        if "formula" in kw:
            cur["validated"] = False  # 公式变更 → 需重新校验
        return FieldMeta.from_dict(cur)

    def _field_meta_sync(self, name: str, field: str) -> FieldMeta:
        fm = self._describe_sync(name)
        for f in fm.fields:
            if f.name == field:
                return f
        raise FieldsetNotFoundError(f"field not found: {name}.{field}")

    def _check_sync(self, name: str, field: str | None, *, all_fields: bool = False,
                    on_progress=None) -> list[FieldsetCheckResult]:
        """校验指标公式：源 dataset 视图上逐行计算结果行数一致 → validated=True

        ``field=None`` + ``all_fields=True`` 校验全部指标。
        """
        fs = self._describe_sync(name)
        if all_fields:
            targets = list(fs.fields)
        elif field is not None:
            targets = [fp for fp in fs.fields if fp.name == field]
            if not targets:
                raise FieldsetNotFoundError(f"field not found: {name}.{field}")
        else:
            raise ValueError("check 需要指标名（或 --all）")

        src = self._dc._get_lazy_sync(fs.dataset)
        engine = get_engine(fs.engine)
        results: list[FieldsetCheckResult] = []
        changed = False
        for i, fp in enumerate(targets, start=1):
            if on_progress is not None:
                on_progress(i, len(targets), f"{name}: check {fp.name}")
            ok, message = engine.check(src, fp)
            results.append(FieldsetCheckResult(name, fp.name, ok, message))
            if fp.validated != ok:
                changed = True

        if changed:
            ok_by_field = {r.field: r.ok for r in results}

            def _apply(fields: list[FieldMeta]) -> list[FieldMeta]:
                out = []
                for f in fields:
                    if f.name in ok_by_field and ok_by_field[f.name] != f.validated:
                        f = FieldMeta.from_dict({**f.to_dict(),
                                                 "validated": ok_by_field[f.name]})
                    out.append(f)
                return out

            self._mutate_fields(name, _apply, bump=True)
        return results

    # ---------- 读取 / 物化 ----------

    def _get_lazy_sync(self, name: str, *, columns: list[str] | None = None,
                       where=None, partition: str | None = None,
                       exclude_tool: bool = False,
                       fields_only: bool = False) -> pl.LazyFrame:
        """读 fieldset（lazy）。

        ``fields_only=True`` 只返回衍生数据（keys + 已校验指标）；**默认返回
        dataset + fieldset 已校验指标 join 拼接后的完整视图**（left join on
        keys，dataset 为左表），下游过滤/join 可直接使用无需再拼接。

        物化且 curated 读物化 parquet，否则实时基于源计算（不隐式物化）。
        """
        fs = self._describe_sync(name)
        if fields_only:
            if fs.materialized and fs.curated:
                lf = pl.scan_parquet(self._root(name), hive_partitioning=True)
                if partition is not None:
                    lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
            else:
                lf = self._engine_select_lazy(fs)
        else:
            lf = self._joined_lazy(fs, partition=partition)
        if where is not None:
            from ..table.query import to_expr

            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if columns is not None:
            lf = lf.select(*columns)
        return lf

    def _joined_lazy(self, fs: FieldsetMeta, *,
                     partition: str | None = None) -> pl.LazyFrame:
        """dataset + fieldset 已校验指标 join 拼接视图（left join on keys）"""
        src = self._dc._get_lazy_sync(fs.dataset, partition=partition)
        fields = self._valid_fields(list(fs.fields))
        if not fields:
            return src
        keys = list(fs.keys)
        if fs.materialized and fs.curated:
            mat = pl.scan_parquet(self._root(fs.name), hive_partitioning=True)
            if partition is not None:
                mat = mat.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
        else:
            mat = get_engine(fs.engine).scan(src, keys, fields)
        return src.join(mat.select([*keys, *[f.name for f in fields]]),
                        on=keys, how="left")

    def _engine_select_lazy(self, fs: FieldsetMeta, fields=None) -> pl.LazyFrame:
        """实时计算视图：源 dataset 视图 + 已校验指标（keys + fields）"""
        src = self._dc._get_lazy_sync(fs.dataset)
        engine = get_engine(fs.engine)
        valid = fields if fields is not None else self._valid_fields(list(fs.fields))
        return engine.scan(src, list(fs.keys), valid)

    def _get_sync(self, name: str, *, columns=None, where=None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False,
                  fields_only: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        lf = self._get_lazy_sync(name, columns=columns, where=where,
                                 partition=partition, exclude_tool=exclude_tool,
                                 fields_only=fields_only)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def _test_sync(self, name: str, formula: str) -> pl.DataFrame:
        fs = self._describe_sync(name)
        src = self._dc._get_lazy_sync(fs.dataset)
        return get_engine(fs.engine).test(src, formula)

    def _scan_sync(self, name: str | None, *, all: bool = False,
                   resync: bool = False,
                   on_progress=None) -> FieldsetScanReport | list[FieldsetScanReport]:
        if all:
            return [self._scan_one(fs, resync=resync, on_progress=on_progress)
                    for fs in self._list_sync()]
        return self._scan_one(self._describe_sync(name), resync=resync,
                              on_progress=on_progress)

    def _scan_one(self, fs: FieldsetMeta, *, resync: bool = False,
                  on_progress=None) -> FieldsetScanReport:
        """物化已校验指标（keys + validated fields）；幂等，一致则跳过"""
        dm = self._dataset_meta_safe(fs.dataset)
        fields = self._valid_fields(list(fs.fields))
        cur_hash = self._current_hash(dm, fields)
        conn = self.catalog.new_conn()
        try:
            meta = self._meta_dict(conn, fs.name)
            if not resync and meta.get("dependency_hash") == cur_hash \
                    and meta.get("materialized"):
                return FieldsetScanReport(
                    name=fs.name, version_before=fs.version, version_after=fs.version,
                    materialized=True, changed=False, fields_count=len(fields),
                    partition_by=fs.partition_by)
            out_dir = self._root(fs.name)
            out_dir.mkdir(parents=True, exist_ok=True)
            rebuilt: list[str] = []
            partition_by, gran = self._partition_of_dm(dm)
            if dm is not None and dm.partition_by and dm.materialized:
                buckets = self._partition_buckets(dm)
                for i, value in enumerate(buckets, start=1):
                    if on_progress is not None:
                        on_progress(i, len(buckets), f"{fs.name}: part={value}")
                    part_dir = out_dir / f"part={value}"
                    part_dir.mkdir(parents=True, exist_ok=True)
                    src = self._dc._get_lazy_sync(fs.dataset, partition=str(value))
                    lf = get_engine(fs.engine).scan(src, list(fs.keys), fields)
                    lf.sink_parquet(part_dir / "data.parquet")
                    rebuilt.append(value)
            else:
                if on_progress is not None:
                    on_progress(1, 1, f"{fs.name}: all")
                src = self._dc._get_lazy_sync(fs.dataset)
                lf = get_engine(fs.engine).scan(src, list(fs.keys), fields)
                lf.sink_parquet(out_dir / "data.parquet")
                rebuilt.append("")

            self._write_materialized(conn, fs, cur_hash, partition_by, gran, bump=True)
        finally:
            conn.close()
        return FieldsetScanReport(
            name=fs.name, version_before=fs.version,
            version_after=fs.version + 1, materialized=True, changed=True,
            fields_count=len(fields), partition_by=partition_by,
            rebuilt_partitions=tuple(rebuilt))

    def _partition_of_dm(self, dm) -> tuple[tuple[str, ...], str]:
        if dm is not None and dm.partition_by and dm.materialized:
            return tuple(dm.partition_by), dm.partition_gran
        return (), ""

    def _partition_buckets(self, dm):
        root = self._dc._root(dm.name)
        if not root.exists():
            return []
        vals = set()
        for p in root.rglob("data.parquet"):
            rel = p.relative_to(root).as_posix()
            if "=" in rel:
                seg = rel.split("/")[0]
                if seg.startswith("part="):
                    vals.add(seg.split("=", 1)[1])
        return sorted(vals)

    def _write_materialized(self, conn, fs: FieldsetMeta, dep_hash: str,
                            partition_by: tuple[str, ...], gran: str,
                            bump: bool) -> None:
        obj = self._object(conn, fs.name)
        meta = self._meta_dict(conn, fs.name)
        meta["materialized"] = True
        meta["materialized_at"] = now()
        meta["dependency_hash"] = dep_hash
        meta["partition_by"] = list(partition_by)
        meta["partition_gran"] = gran
        self._update_object_meta(conn, obj["id"], meta, now_str=now(), bump=bump)
        conn.commit()

    # ---------- catalog 便捷 ----------

    def _set_deps(self, conn, obj_type, obj_name, deps):
        from ..table.catalog import set_deps

        set_deps(conn, obj_type, obj_name, deps)

    def _update_object_meta(self, conn, object_id, meta, now_str=None, bump=False):
        from ..table.catalog import update_object_meta

        update_object_meta(conn, object_id, meta, now_str=now_str, bump=bump)

    # ---------- async 接口 ----------

    async def add(self, name: str, dataset: str | None = None, *,
                  engine: str = "polars", **meta) -> FieldsetMeta:
        """创建衍生指标集：依赖一个已注册 dataset；engine 默认 polars"""
        return await asyncio.to_thread(self._add_sync, name, dataset, engine,
                                       meta or None)

    async def get(self, name: str, *, columns: list[str] | None = None,
                  where=None, partition: str | None = None,
                  exclude_tool: bool = False, limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False,
                  fields_only: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读 fieldset（collect）：**默认返回 dataset + fieldset 已校验指标 join 拼接
        的完整视图**；``fields_only=True`` 只返回衍生数据（keys + 已校验指标）。

        物化且一致读物化数据，否则实时基于源计算（不隐式物化）。
        """
        return await asyncio.to_thread(
            self._get_sync, name, columns=columns, where=where, partition=partition,
            exclude_tool=exclude_tool, limit=limit, offset=offset,
            count_total=count_total, fields_only=fields_only)

    async def meta(self, name: str) -> FieldsetMeta:
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[FieldsetMeta]:
        return await asyncio.to_thread(self._list_sync)

    async def set(self, name: str, **kw) -> FieldsetMeta:
        """更新指标集级元数据（display_name/description/tags/source；其余键进 extra）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def delete(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        """删除指标集注册、依赖与物化产物（源 dataset 从不删）"""
        return await asyncio.to_thread(self._delete_sync, name, force=force,
                                       with_data=with_data)

    async def add_field(self, name: str, field: str, **kw) -> FieldsetMeta:
        """添加衍生指标（validated=False，需 check 后参与物化）"""
        return await asyncio.to_thread(self._add_field_sync, name, field, kw)

    async def set_field(self, name: str, field: str, **kw) -> FieldsetMeta:
        """修改衍生指标：公式变更后 validated 复位 False"""
        return await asyncio.to_thread(self._set_field_sync, name, field, kw)

    async def delete_field(self, name: str, field: str) -> FieldsetMeta:
        return await asyncio.to_thread(self._del_field_sync, name, field)

    async def field_meta(self, name: str, field: str) -> FieldMeta:
        return await asyncio.to_thread(self._field_meta_sync, name, field)

    async def check(self, name: str, field: str | None = None, *,
                    all_fields: bool = False, on_progress=None) -> list[FieldsetCheckResult]:
        """校验指标：``field`` 指定单个或 ``all_fields=True`` 全部；通过则 validated=True"""
        return await asyncio.to_thread(self._check_sync, name, field,
                                       all_fields=all_fields, on_progress=on_progress)

    async def test(self, name: str, formula: str) -> pl.DataFrame:
        """测试公式：基于源 dataset 视图求值，返回结果 DataFrame（错误抛异常）"""
        return await asyncio.to_thread(self._test_sync, name, formula)

    async def scan(self, name: str | None = None, *, all: bool = False,
                   resync: bool = False, on_progress=None) -> FieldsetScanReport | list[FieldsetScanReport]:
        """物化全部已校验指标（幂等：依赖签名不变则跳过）；``all=True`` 物化全部指标集"""
        return await asyncio.to_thread(self._scan_sync, name, all=all,
                                       resync=resync, on_progress=on_progress)


__all__ = ["FieldsetController", "FieldsetNotFoundError", "FieldsetExistsError"]