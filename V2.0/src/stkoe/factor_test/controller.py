"""FACTOR_TEST 模块：因子测试数据集（FactorTestController，async 接口）

定位：
- test = 在 factor 关联的 sample 视图上，结合测试必需列（returns/groupby/marketcap）
  生成的一份因子测试数据集；注册于 catalog（type='factor_test'）。
- 测试数据集 Schema：date / sym / sample / returns / group / marketcap / factor /
  d{no}（前向收益）/ factor_quantile（截面分位）。
- 物化：``test scan`` 把测试数据集落盘 ``factor_tests/<name>/data.parquet``（flat 单文件）；
  幂等——依赖签名（factor 依赖 hash + spec + 测试列名）不变则跳过。
- 读取：物化完成且 curated 读物化 parquet，否则实时构造（不隐式物化）。
- 依赖：test → factor（stkoe_depends 登记，删除上游需 ``--force``）。
"""
from __future__ import annotations

import asyncio
import hashlib
import shutil
from pathlib import Path

import polars as pl

from ..jsonutil import dumps_str, loads
from ..table.controller import DEFAULT_IGNORE_COLS
from ..table.spec import ColumnMeta
from ..table.util import now
from .spec import (FactorTesterSpec, FactorTestCheckResult, FactorTestMeta,
                   FactorTestScanReport)
from .tester import prepare_factor_data, run_tester


class FactorTestNotFoundError(FileNotFoundError):
    pass


class FactorTestExistsError(ValueError):
    pass


META_FIELDS = ("display_name", "description", "source", "tags")

PANEL_COLS = ("date", "sym")


class FactorTestController:
    """因子测试数据集控制面：add/get/meta/list/set/check/scan/delete

    复用 FactorController 读取 factor 数据（keys + factor_col），
    SampleController 读取 sample 的 dataset_with_fieldset + filter 视图。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..factor.controller import FactorController

        self._fc = FactorController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._fc.data_dir
        self.catalog = self._fc.catalog
        self.root = self.data_dir / "factor_tests"

    # ---------- 内部转换 ----------

    def _root(self, name: str) -> Path:
        return self.root / name

    def _object(self, conn, name: str):
        from ..table.catalog import get_object

        return get_object(conn, name, "factor_test")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _spec(self, meta: dict) -> FactorTesterSpec:
        return FactorTesterSpec.from_dict(meta.get("spec") or {})

    def _factor_meta(self, name: str):
        return self._fc._describe_sync(name)

    def _sample_meta(self, name: str):
        return self._fc._sc._describe_sync(name)

    def _test_meta(self, conn, obj) -> FactorTestMeta:
        meta = loads(obj["meta"])
        sample = meta.get("sample", "")
        sm = None
        if sample:
            try:
                sm = self._sample_meta(sample)
            except Exception:
                sm = None
        spec = self._spec(meta)
        stored = meta.get("dependency_hash") or ""
        cur_hash = self._current_hash(meta)
        materialized = bool(meta.get("materialized", False))
        return FactorTestMeta(
            name=obj["name"],
            version=obj["version"],
            factor=meta.get("factor", ""),
            sample=sample,
            returns=meta.get("returns", "r"),
            groupby=meta.get("groupby", "ic"),
            marketcap=meta.get("marketcap", "fv"),
            spec=spec,
            factor_col=meta.get("factor_col", ""),
            keys=tuple(meta.get("keys", []) or (sm.keys if sm is not None else ())),
            materialized=materialized,
            materialized_at=meta.get("materialized_at"),
            curated=materialized and stored == cur_hash,
            columns=tuple(ColumnMeta.from_dict(c) for c in meta.get("columns", [])),
            extra=meta.get("extra") or {},
            display_name=meta.get("display_name", obj["name"]),
            description=meta.get("description", ""),
            tags=tuple(meta.get("tags", [])),
            source=meta.get("source", "local"),
            created_at=obj["created_at"],
            updated_at=obj["updated_at"],
        )

    def _current_hash(self, meta: dict) -> str:
        """物化一致性签名 = factor 依赖 hash + spec + 测试列名 + factor_col"""
        factor = meta.get("factor", "")
        try:
            f_hash = self._fc.dependency_hash(factor)
        except Exception:
            f_hash = ""
        parts = [
            f"factor:{factor}:{f_hash}",
            f"returns:{meta.get('returns', 'r')}",
            f"groupby:{meta.get('groupby', 'ic')}",
            f"marketcap:{meta.get('marketcap', 'fv')}",
            f"factor_col:{meta.get('factor_col', '')}",
            f"spec:{dumps_str(self._spec(meta).to_dict())}",
        ]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    # ---------- 数据构造 ----------

    def _sample_view(self, sample: str) -> pl.DataFrame:
        """sample 视图（dataset_with_fieldset + filter）collect"""
        sm = self._sample_meta(sample)
        return self._fc._sc._sample_lazy(sm).collect()

    def _build_base(self, meta: dict) -> pl.DataFrame:
        """底表：sample 视图（date/sym/sample/returns/group/marketcap）+ factor 列"""
        factor = meta["factor"]
        fm = self._factor_meta(factor)
        sample = meta.get("sample") or fm.sample
        view = self._sample_view(sample)
        returns = meta.get("returns", "r")
        groupby = meta.get("groupby", "ic")
        marketcap = meta.get("marketcap", "fv")
        need = list(PANEL_COLS) + [returns, groupby, marketcap]
        missing = [c for c in need if c not in view.columns]
        if missing:
            raise ValueError(f"sample 缺少测试必需列: {missing}（需要 "
                             f"{'/'.join(PANEL_COLS)} 与 returns/groupby/marketcap）")
        fdf = self._fc._compute_sync(fm)
        keys = list(fm.keys)
        base = (
            view.select(*[pl.col(c) for c in need])
            .with_columns(pl.lit(1, dtype=pl.Int32).alias("sample"))
            .join(fdf, on=keys, how="left")
            .rename({fm.factor_col: "factor", returns: "returns",
                     groupby: "group", marketcap: "marketcap"})
        )
        return base

    def _build_sync(self, meta: dict) -> pl.DataFrame:
        """测试数据集：底表 + prepare_factor_data（前向收益/样本重整/分位）"""
        base = self._build_base(meta)
        return prepare_factor_data(base, self._spec(meta))

    def _get_lazy_sync(self, name: str, *, where=None) -> pl.LazyFrame:
        """读测试数据集（lazy）：物化且 curated → 读物化 parquet；否则实时构造"""
        tm = self._describe_sync(name)
        if tm.materialized and tm.curated:
            lf = pl.scan_parquet(self._root(name) / "data.parquet")
        else:
            meta = self._catalog_meta(name)
            lf = self._build_sync(meta).lazy()
        if where is not None:
            from ..table.query import to_expr

            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def _catalog_meta(self, name: str) -> dict:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorTestNotFoundError(f"test not registered: {name}")
            return loads(obj["meta"])
        finally:
            conn.close()

    def _get_sync(self, name: str, *, where=None,
                  limit: int | None = None, offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        lf = self._get_lazy_sync(name, where=where)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    # ---------- add / list / meta ----------

    def _validate_sample_cols(self, meta: dict):
        """校验 sample 视图包含测试必需列（date/sym/returns/groupby/marketcap）"""
        factor = meta["factor"]
        fm = self._factor_meta(factor)
        sample = meta.get("sample") or fm.sample
        view = self._sample_view(sample)
        need = list(PANEL_COLS) + [meta.get("returns", "r"),
                                   meta.get("groupby", "ic"),
                                   meta.get("marketcap", "fv")]
        missing = [c for c in need if c not in view.columns]
        if missing:
            raise ValueError(f"sample 缺少测试必需列 {missing}，不能创建测试数据集"
                             f"（需要 {need}）")

    def _register(self, conn, name: str, meta: dict, hash_val: str) -> None:
        from ..table.catalog import insert_object, set_deps

        insert_object(conn, "factor_test", name, meta, hash_val, now_str=now())
        set_deps(conn, "factor_test", name,
                 [("factor", meta["factor"], {})])
        conn.commit()

    def _add_sync(self, name: str, *, factor: str, returns: str, groupby: str,
                  marketcap: str, spec: FactorTesterSpec,
                  meta: dict | None = None) -> FactorTestMeta:
        if not factor:
            raise ValueError("test add 需要 --factor <因子名>")
        conn = self.catalog.new_conn()
        try:
            if self._object(conn, name) is not None:
                raise FactorTestExistsError(f"test already registered: {name}")
            fm = self._factor_meta(factor)
            sample = fm.sample
            kw = {
                "factor": factor,
                "sample": sample,
                "returns": returns,
                "groupby": groupby,
                "marketcap": marketcap,
                "spec": spec.to_dict(),
                "factor_col": fm.factor_col or "",
                "keys": list(fm.keys),
                "materialized": False,
                "materialized_at": None,
                "dependency_hash": None,
                "display_name": name,
                "description": "",
                "source": "local",
                "tags": [],
            }
            if meta:
                for k in META_FIELDS:
                    if k in meta:
                        kw[k] = meta[k]
                if "factor_col" in meta:
                    kw["factor_col"] = meta["factor_col"]
                extras = {k: v for k, v in meta.items()
                          if k not in META_FIELDS and k != "factor_col"}
                if extras:
                    kw["extra"] = extras
            self._validate_sample_cols(kw)
            self._register(conn, name, kw, self._current_hash(kw))
        finally:
            conn.close()
        return self._describe_sync(name)

    def _list_sync(self) -> list[FactorTestMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='factor_test' "
                                "ORDER BY name").fetchall()
            return [self._test_meta(conn, r) for r in rows]
        finally:
            conn.close()

    def _describe_sync(self, name: str) -> FactorTestMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorTestNotFoundError(f"test not registered: {name}")
            return self._test_meta(conn, obj)
        finally:
            conn.close()

    # ---------- set / delete ----------

    def _set_sync(self, name: str, kw: dict) -> FactorTestMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorTestNotFoundError(f"test not registered: {name}")
            from ..table.catalog import update_object_meta

            meta = dict(self._meta_dict(conn, name))
            for key in ("returns", "groupby", "marketcap", "factor_col"):
                if key in kw:
                    meta[key] = kw[key]
                    meta["materialized"] = False
                    meta["dependency_hash"] = None
            spec = self._spec(meta)
            spec_kw = {k: v for k, v in kw.items()
                       if k in ("by_group", "quantiles", "periods",
                                "date_range", "rolling_window")}
            if spec_kw:
                if "periods" in spec_kw:
                    raw = spec_kw["periods"]
                    if isinstance(raw, str):
                        spec_kw["periods"] = tuple(int(p) for p in raw.split(",")
                                                   if p.strip())
                    else:
                        spec_kw["periods"] = tuple(int(p) for p in raw
                                                   if str(p).strip())
                if "date_range" in spec_kw:
                    raw = spec_kw["date_range"]
                    if isinstance(raw, str):
                        spec_kw["date_range"] = tuple(str(x) for x in raw.split(","))
                    else:
                        spec_kw["date_range"] = tuple(str(x) for x in raw)
                meta["spec"] = FactorTesterSpec(
                    **{**spec.to_dict(), **spec_kw}).to_dict()
                meta["materialized"] = False
                meta["dependency_hash"] = None
            if "spec" in kw:
                spec = kw["spec"]
                if isinstance(spec, dict):
                    spec = FactorTesterSpec.from_dict(spec)
                meta["spec"] = spec.to_dict()
                meta["materialized"] = False
                meta["dependency_hash"] = None
            for key in META_FIELDS:
                if key in kw:
                    meta[key] = kw[key]
            extras = {k: v for k, v in kw.items()
                      if k not in ("returns", "groupby", "marketcap",
                                   "factor_col", "spec", *META_FIELDS,
                                   "by_group", "quantiles", "periods",
                                   "date_range", "rolling_window")}
            if extras:
                meta.setdefault("extra", {}).update(extras)
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._test_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _delete_sync(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        from ..table.catalog import dependents

        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorTestNotFoundError(f"test not registered: {name}")
            dependents_rows = dependents(conn, "factor_test", name)
            if dependents_rows and not force:
                from ..table.controller import DependencyError

                raise DependencyError(dependents_rows)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.execute("DELETE FROM stkoe_depends WHERE obj_type='factor_test' "
                         "AND obj_name=?", (name,))
            conn.commit()
        finally:
            conn.close()
        if with_data:
            shutil.rmtree(self._root(name), ignore_errors=True)
        return {"deleted": name}

    # ---------- check / scan ----------

    def _check_sync(self, name: str) -> FactorTestCheckResult:
        tm = self._describe_sync(name)
        meta = self._catalog_meta(name)
        try:
            df = self._build_sync(meta)
        except Exception as e:
            return FactorTestCheckResult(test=name, ok=False, rows=0,
                                         columns=tuple(tm.keys),
                                         message=f"测试数据集构造失败: {e}")
        need = list(PANEL_COLS) + ["sample", "returns", "group", "marketcap",
                                   "factor", "factor_quantile"]
        missing = [c for c in need if c not in df.columns]
        if missing:
            return FactorTestCheckResult(test=name, ok=False, rows=df.height,
                                         columns=tuple(df.columns),
                                         message=f"结果集缺少必需列: {missing}")
        if df.height == 0:
            return FactorTestCheckResult(test=name, ok=False, rows=0,
                                         columns=tuple(df.columns),
                                         message="结果行数为 0")
        return FactorTestCheckResult(test=name, ok=True, rows=df.height,
                                     columns=tuple(df.columns),
                                     message=f"有效（{df.height} 行）")

    def _scan_sync(self, name: str | None, *, all: bool = False,
                   resync: bool = False, on_progress=None
                   ) -> FactorTestScanReport | list[FactorTestScanReport]:
        if all:
            return [self._scan_one(tm, resync=resync, on_progress=on_progress)
                    for tm in self._list_sync()]
        return self._scan_one(self._describe_sync(name), resync=resync,
                              on_progress=on_progress)

    def _scan_one(self, tm: FactorTestMeta, *, resync: bool = False,
                  on_progress=None) -> FactorTestScanReport:
        conn = self.catalog.new_conn()
        try:
            meta = self._meta_dict(conn, tm.name)
            cur_hash = self._current_hash(meta)
            if not resync and meta.get("dependency_hash") == cur_hash \
                    and meta.get("materialized"):
                return FactorTestScanReport(
                    name=tm.name, version_before=tm.version, version_after=tm.version,
                    materialized=True, changed=False,
                    quantiles=self._spec(meta).quantiles,
                    periods=self._spec(meta).periods)
            if on_progress is not None:
                on_progress(1, 1, f"{tm.name}: all")
            df = self._build_sync(meta)
            out_dir = self._root(tm.name)
            out_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out_dir / "data.parquet")
            spec = self._spec(meta)
            cols = tuple(ColumnMeta(name=c, display_name=c,
                                    data_type=str(t)) for c, t in zip(df.columns,
                                                                     df.dtypes))
            from ..table.catalog import update_object_meta

            obj = self._object(conn, tm.name)
            meta["materialized"] = True
            meta["materialized_at"] = now()
            meta["dependency_hash"] = cur_hash
            meta["columns"] = [c.to_dict() for c in cols]
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
        finally:
            conn.close()
        return FactorTestScanReport(
            name=tm.name, version_before=tm.version, version_after=tm.version + 1,
            materialized=True, changed=True, rows=df.height,
            quantiles=spec.quantiles, periods=spec.periods)

    # ---------- 测试器 ----------

    def _tester_scan_sync(self, name: str, kind: str, on_progress=None) -> dict:
        """运行测试器 kind 并把各命名产物写入 ``stats/test/<name>/<kind>/``

        返回 ``{输出名: (StatFile, rows)}`` 供 stat 模块聚合。
        """
        from ..stat.controller import StatController, _ordered, StatNotFoundError
        from ..stat.spec import StatFile

        tm = self._describe_sync(name)
        meta = self._catalog_meta(name)
        out_dir = self.data_dir / "stats" / "test" / name / kind
        if tm.materialized and tm.curated:
            data = pl.read_parquet(self._root(name) / "data.parquet")
        else:
            data = self._build_sync(meta)
        spec = self._spec(meta)
        outputs = run_tester(kind, data, spec)
        out_dir.mkdir(parents=True, exist_ok=True)
        files = []
        total = len(outputs)
        for i, (out_name, df) in enumerate(outputs.items(), start=1):
            if on_progress is not None:
                on_progress(i, total, f"{name}/{kind}: {out_name}")
            p = out_dir / f"{out_name}.parquet"
            df.write_parquet(p)
            files.append(StatFile(partition=out_name,
                                  rel_path=p.relative_to(self.data_dir / "stats"),
                                  rows=df.height, size=p.stat().st_size))
        return _ordered(tuple(files))

    # ---------- async 接口 ----------

    async def add(self, name: str, *, factor: str,
                  returns: str = "r", groupby: str = "ic", marketcap: str = "fv",
                  spec: FactorTesterSpec | None = None,
                  **meta) -> FactorTestMeta:
        """创建测试数据集：依赖已注册 factor，校验 sample 含必需列"""
        return await asyncio.to_thread(
            self._add_sync, name, factor=factor, returns=returns, groupby=groupby,
            marketcap=marketcap, spec=spec or FactorTesterSpec(), meta=meta or None)

    async def get(self, name: str, *, where=None, limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读测试数据集（collect）：物化且一致读物化数据，否则实时构造"""
        return await asyncio.to_thread(
            self._get_sync, name, where=where, limit=limit, offset=offset,
            count_total=count_total)

    async def meta(self, name: str) -> FactorTestMeta:
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[FactorTestMeta]:
        return await asyncio.to_thread(self._list_sync)

    async def set(self, name: str, **kw) -> FactorTestMeta:
        """更新测试配置（returns/groupby/marketcap/spec + 元数据；改动后物化失效）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def check(self, name: str) -> FactorTestCheckResult:
        """校验测试数据集：构造成功、含必需列、行数 > 0"""
        return await asyncio.to_thread(self._check_sync, name)

    async def scan(self, name: str | None = None, *, all: bool = False,
                   resync: bool = False,
                   on_progress=None) -> FactorTestScanReport | list[FactorTestScanReport]:
        """物化测试数据集（幂等：依赖签名不变则跳过）；``all=True`` 物化全部"""
        return await asyncio.to_thread(self._scan_sync, name, all=all,
                                       resync=resync, on_progress=on_progress)

    async def delete(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        """删除测试数据集注册、依赖与物化产物"""
        return await asyncio.to_thread(self._delete_sync, name, force=force,
                                       with_data=with_data)


__all__ = ["FactorTestController", "FactorTestNotFoundError",
           "FactorTestExistsError"]
