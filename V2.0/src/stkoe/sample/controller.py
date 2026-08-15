"""SAMPLE 模块：基于 dataset 的样本池（SampleController，async 接口）

定位：
- sample = 作用在 ``dataset_with_fieldset``（源 dataset 全列 + 其 fieldset 已校验
  衍生字段的 join 结果）上施加过滤 ``formula`` 之后的产物，**没有物化概念**，
  注册于 catalog（type='sample'）；``get``/``check`` 均动态构造数据。
- 过滤公式：列作用域 polars 布尔表达式（如 ``(date>='2026-01-01')&(sym.is_in([...]))``），
  经 ``sample/engine.py`` 的引擎插件计算（当前仅 polars）；formula 为空 → 返回整个
  ``dataset_with_fieldset``。
- 依赖：sample → 源 dataset（stkoe_depends 登记，删除源 dataset 需 ``--force``）。

``dataset_with_fieldset`` 构造步骤（get / check 共用）：
1. 读源 dataset 视图（物化且 curated 读物化 parquet，否则实时 join 视图）
2. 查找 catalog 中 ``dataset == 源 dataset`` 的全部 fieldset，取其已校验指标
   在源 dataset 视图上逐行计算并按 keys left join 出衍生列
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import polars as pl

from ..jsonutil import loads
from ..table.controller import DEFAULT_IGNORE_COLS
from ..table.spec import ColumnMeta
from ..table.util import now
from .engine import get_engine
from .spec import SampleCheckResult, SampleMeta


class SampleNotFoundError(FileNotFoundError):
    pass


class SampleExistsError(ValueError):
    pass


META_FIELDS = ("display_name", "description", "source", "formula", "engine", "tags")


class SampleController:
    """样本池控制面：add/get/meta/list/set/check/delete（无物化，读取动态构造）

    复用 DatasetController 读源 dataset（_get_lazy_sync/_describe_sync）与
    FieldsetController 的公式引擎计算衍生字段列。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..dataset.controller import DatasetController

        self._dc = DatasetController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._dc.data_dir
        self.catalog = self._dc.catalog

    # ---------- 内部转换 ----------

    def _object(self, conn, name: str):
        from ..table.catalog import get_object

        return get_object(conn, name, "sample")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _sample_meta(self, conn, obj) -> SampleMeta:
        meta = loads(obj["meta"])
        dataset = meta.get("dataset", "")
        dm = self._dataset_meta_safe(dataset)
        keys = tuple(meta.get("keys", []) or ())
        columns = self._resolved_columns(dataset, dm, keys)
        return SampleMeta(
            name=obj["name"],
            version=obj["version"],
            dataset=dataset,
            engine=meta.get("engine", "polars"),
            formula=meta.get("formula", ""),
            keys=keys,
            columns=columns,
            display_name=meta.get("display_name", obj["name"]),
            description=meta.get("description", ""),
            tags=tuple(meta.get("tags", [])),
            source=meta.get("source", "local"),
            extra=meta.get("extra") or {},
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

    def _resolved_columns(self, dataset: str, dm, keys: tuple[str, ...]) -> tuple:
        """``sample meta`` 的 columns = 源 dataset 列 + fieldset 已校验衍生指标列"""
        if dm is None:
            return ()
        cols = list(dm.columns)
        for fs in self._related_fieldsets(dataset):
            for f in fs["valid_fields"]:
                cols.append(ColumnMeta(
                    name=f.name, display_name=f.display_name,
                    description=f.description, data_type=None,
                    unit=f.unit, formula=f.formula, tags=f.tags,
                    source_table=dataset, source_field=f.name))
        return tuple(cols)

    # ---------- dataset_with_fieldset 构造 ----------

    def _related_fieldsets(self, dataset: str) -> list[dict]:
        """catalog 中源 dataset 为 ``dataset`` 的全部 fieldset（含已校验指标列表）"""
        from ..fieldset.spec import FieldMeta

        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT name, meta FROM stkoe_objects "
                                "WHERE type='fieldset'").fetchall()
            out: list[dict] = []
            for r in rows:
                meta = loads(r["meta"])
                if meta.get("dataset") != dataset:
                    continue
                fields = [FieldMeta.from_dict(f) for f in meta.get("fields", [])]
                valid = [f for f in fields if f.validated and f.formula]
                if not valid:
                    continue
                out.append({"name": r["name"], "meta": meta, "valid_fields": valid})
            return out
        finally:
            conn.close()

    def _dataset_with_fieldset_lazy(self, dataset: str, *,
                                    partition: str | None = None) -> pl.LazyFrame:
        """step1: 源 dataset 视图 + 各 fieldset 已校验衍生指标列（join on keys）"""
        lf = self._dc._get_lazy_sync(dataset, partition=partition)
        dm = self._dc._describe_sync(dataset)
        from ..fieldset.engine import get_engine as get_fs_engine

        for fs in self._related_fieldsets(dataset):
            engine = get_fs_engine(fs["meta"].get("engine", "polars"))
            fs_keys = list(fs["meta"].get("keys") or dm.keys)
            src = self._dc._get_lazy_sync(dataset, partition=partition)
            flf = engine.scan(src, fs_keys, fs["valid_fields"])
            derived = [f.name for f in fs["valid_fields"]]
            lf = lf.join(flf.select([*fs_keys, *derived]), on=fs_keys, how="left")
        return lf

    def _sample_lazy(self, sm: SampleMeta, *, columns: list[str] | None = None,
                     where=None, partition: str | None = None) -> pl.LazyFrame:
        """step2: filter formula（空 = 原样返回），再施加 where/columns"""
        from ..table.query import to_expr

        lf = self._dataset_with_fieldset_lazy(sm.dataset, partition=partition)
        lf = get_engine(sm.engine).filter(lf, sm.formula)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if columns is not None:
            lf = lf.select(*columns)
        return lf

    def _get_sync(self, name: str, *, columns: list[str] | None = None,
                  where=None, partition: str | None = None,
                  limit: int | None = None, offset: int | None = None,
                  count_total: bool = False, exclude_tool: bool = False
                  ) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        sm = self._describe_sync(name)
        lf = self._sample_lazy(sm, columns=columns, where=where, partition=partition)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def _check_sync(self, name: str) -> SampleCheckResult:
        """校验样本池：过滤后结果集包含全部索引列且行数 > 0"""
        sm = self._describe_sync(name)
        try:
            df = self._sample_lazy(sm).collect()
        except Exception as e:
            return SampleCheckResult(sample=name, ok=False, rows=0,
                                     columns=tuple(sm.keys), message=f"过滤公式执行失败: {e}")
        missing = [k for k in sm.keys if k not in df.columns]
        if missing:
            return SampleCheckResult(sample=name, ok=False, rows=df.height,
                                     columns=tuple(df.columns),
                                     message=f"结果集缺少索引列: {missing}")
        if df.height == 0:
            return SampleCheckResult(sample=name, ok=False, rows=0,
                                     columns=tuple(df.columns), message="结果行数为 0")
        return SampleCheckResult(sample=name, ok=True, rows=df.height,
                                 columns=tuple(df.columns), message=f"有效（{df.height} 行）")

    # ---------- add / list / meta ----------

    def _register(self, conn, name: str, dataset: str, engine: str, formula: str,
                  keys: list[str], meta: dict | None = None):
        from ..table.catalog import insert_object, set_deps

        cur = {
            "dataset": dataset,
            "engine": engine,
            "formula": formula,
            "keys": keys,
            "display_name": name,
            "description": "",
            "source": "local",
            "tags": [],
        }
        if meta:
            cur.update(meta)
        obj = insert_object(conn, "sample", name, cur, "", now())
        set_deps(conn, "sample", name, [("dataset", dataset, {"keys": keys})])
        return obj

    @staticmethod
    def _apply_meta_fields(meta: dict, kw: dict) -> dict:
        """规范化 add/set 的标准元数据键（tags 逗号分隔），其余任意键进 extra"""
        out = dict(meta)
        for key, value in kw.items():
            if key == "tags":
                out["tags"] = [t.strip() for t in str(value).split(",") if t.strip()]
            elif key in META_FIELDS:
                out[key] = str(value)
            else:
                extra = dict(out.get("extra") or {})
                extra[key] = value
                out["extra"] = extra
        return out

    def _add_sync(self, name: str, dataset: str | None, engine: str,
                  formula: str, meta: dict | None) -> SampleMeta:
        if not dataset:
            raise ValueError("sample add 需要 --dataset <dataset 名>")
        dm = self._dc._describe_sync(dataset)  # 源 dataset 必须已注册
        fields = self._apply_meta_fields({}, meta or {})
        fields.setdefault("dataset", dataset)
        fields.setdefault("engine", engine)
        fields.setdefault("formula", formula)
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise SampleExistsError(f"sample already registered: {name}")
            self._register(conn, name, dataset, engine, formula, list(dm.keys), fields)
        return self._describe_sync(name)

    def _list_sync(self) -> list[SampleMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='sample' "
                                "ORDER BY name").fetchall()
            return [self._sample_meta(conn, r) for r in rows]
        finally:
            conn.close()

    def _describe_sync(self, name: str) -> SampleMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise SampleNotFoundError(f"sample not registered: {name}")
            return self._sample_meta(conn, obj)
        finally:
            conn.close()

    # ---------- set / delete ----------

    def _set_sync(self, name: str, kw: dict) -> SampleMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise SampleNotFoundError(f"sample not registered: {name}")
            from ..table.catalog import update_object_meta

            meta = self._apply_meta_fields(self._meta_dict(conn, name), kw)
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._sample_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _delete_sync(self, name: str, *, force: bool = False) -> dict:
        from ..table.catalog import dependents

        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise SampleNotFoundError(f"sample not registered: {name}")
            dependents_rows = dependents(conn, "sample", name)
            if dependents_rows and not force:
                from ..table.controller import DependencyError

                raise DependencyError(dependents_rows)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.execute("DELETE FROM stkoe_depends WHERE obj_type='sample' AND obj_name=?",
                         (name,))
            conn.commit()
        finally:
            conn.close()
        return {"deleted": name}

    # ---------- async 接口 ----------

    async def add(self, name: str, dataset: str | None = None, *,
                  engine: str = "polars", formula: str = "",
                  **meta) -> SampleMeta:
        """创建样本池：依赖一个已注册 dataset；engine 默认 polars，formula 可为空"""
        return await asyncio.to_thread(self._add_sync, name, dataset, engine,
                                       formula, meta or None)

    async def get(self, name: str, *, columns: list[str] | None = None,
                  where=None, partition: str | None = None,
                  limit: int | None = None, offset: int | None = None,
                  count_total: bool = False,
                  exclude_tool: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读样本池（collect）：动态构造 dataset_with_fieldset + filter formula"""
        return await asyncio.to_thread(
            self._get_sync, name, columns=columns, where=where, partition=partition,
            limit=limit, offset=offset, count_total=count_total,
            exclude_tool=exclude_tool)

    async def meta(self, name: str) -> SampleMeta:
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[SampleMeta]:
        return await asyncio.to_thread(self._list_sync)

    async def set(self, name: str, **kw) -> SampleMeta:
        """更新样本池定义（formula/engine/display_name/description/tags/source；任意键进 extra）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def check(self, name: str) -> SampleCheckResult:
        """校验样本池：过滤后结果集含全部索引列且行数 > 0"""
        return await asyncio.to_thread(self._check_sync, name)

    async def delete(self, name: str, *, force: bool = False) -> dict:
        """删除样本池注册与依赖（源 dataset 从不删）"""
        return await asyncio.to_thread(self._delete_sync, name, force=force)


__all__ = ["SampleController", "SampleNotFoundError", "SampleExistsError"]