"""FACTOR 模块：最终因子（FactorController，async 接口）

定位：
- factor = 在 ``sample``（样本池）视图上经 ``feature``（命名公式）逐行算出因子列，
  再经 ``pipeline``（算子链）变换的最终产物；结构恒为「样本索引列 + 一列因子列」，
  注册于 catalog（type='factor'）。
- 物化：``factor scan`` 把最终因子落盘到 ``factors/<name>/``，目录布局镜像源
  sample 的 dataset（源已分区则按同分区键/粒度镜像，否则 flat 单文件）。
- 依赖：factor → feature、factor → sample（stkoe_depends 登记，删除上游需 ``--force``）
- 读取：物化完成且与源+公式+pipeline 一致（curated）读物化 parquet；否则实时基于
  sample 视图计算（不隐式物化，物化走显式 scan）
"""
from __future__ import annotations

import asyncio
import hashlib
import shutil
from pathlib import Path

import polars as pl

from ..jsonutil import loads
from ..table.controller import DEFAULT_IGNORE_COLS
from ..table.spec import ColumnMeta
from ..table.util import now
from .engine import get_engine, parse_pipeline
from .spec import FactorCheckResult, FactorMeta, FactorScanReport, FieldMeta


class FactorNotFoundError(FileNotFoundError):
    pass


class FactorExistsError(ValueError):
    pass


META_FIELDS = ("display_name", "description", "source", "tags")


class FactorController:
    """最终因子控制面：add/get/meta/list/set/check/scan/delete

    复用 SampleController 读取 sample 的 dataset_with_fieldset + filter 视图，
    FeatureController 读取 feature 公式定义。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..sample.controller import SampleController

        self._sc = SampleController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._sc.data_dir
        self.catalog = self._sc.catalog
        self.root = self.data_dir / "factors"

    # ---------- 内部转换 ----------

    def _root(self, name: str) -> Path:
        return self.root / name

    def _object(self, conn, name: str):
        from ..table.catalog import get_object

        return get_object(conn, name, "factor")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _feature_meta(self, name: str):
        from ..feature.controller import FeatureController

        return FeatureController(data_dir=self.data_dir)._describe_sync(name)

    def _sample_meta_safe(self, name: str):
        if not name:
            return None
        try:
            return self._sc._describe_sync(name)
        except Exception:
            return None

    def _factor_meta(self, conn, obj) -> FactorMeta:
        meta = loads(obj["meta"])
        feature = meta.get("feature", "")
        sample = meta.get("sample", "")
        sm = self._sample_meta_safe(sample)
        factor_col = meta.get("factor_col", "")
        field = meta.get("field")
        cur_hash = self._current_hash(meta)
        stored = meta.get("dependency_hash") or ""
        materialized = bool(meta.get("materialized", False))
        return FactorMeta(
            name=obj["name"],
            version=obj["version"],
            feature=feature,
            sample=sample,
            pipeline=meta.get("pipeline", ""),
            engine=meta.get("engine", "polars"),
            factor_col=factor_col,
            keys=tuple(meta.get("keys", []) or (sm.keys if sm else ())),
            partition_by=tuple(meta.get("partition_by", [])),
            partition_gran=meta.get("partition_gran", ""),
            materialized=materialized,
            materialized_at=meta.get("materialized_at"),
            curated=materialized and stored == cur_hash,
            columns=tuple(sm.columns) if sm is not None else (),
            field=FieldMeta.from_dict(field) if field else None,
            extra=meta.get("extra") or {},
            display_name=meta.get("display_name", obj["name"]),
            description=meta.get("description", ""),
            tags=tuple(meta.get("tags", [])),
            source=meta.get("source", "local"),
            created_at=obj["created_at"],
            updated_at=obj["updated_at"],
        )

    def _current_hash(self, meta: dict) -> str:
        """物化一致性签名 = 源 sample 的 data_key + feature 公式 + pipeline"""
        sample = meta.get("sample", "")
        sm = self._sample_meta_safe(sample)
        parts = []
        if sm is not None:
            dm_key = self._sc._dc.data_key(sm.dataset) if sm.dataset else ""
            parts.append(f"sample:{sample}:{sm.engine}:{sm.formula}:{dm_key}")
        feature = meta.get("feature", "")
        try:
            ft = self._feature_meta(feature)
            parts.append(f"feature:{feature}:{ft.engine}:{ft.formula}")
        except Exception:
            parts.append(f"feature:{feature}")
        parts.append(f"pipeline:{meta.get('pipeline', '')}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def dependency_hash(self, name: str) -> str:
        """按当前注册定义重算 factor 的一致性签名（供下游依赖方读取）"""
        conn = self.catalog.new_conn()
        try:
            meta = self._meta_dict(conn, name)
        finally:
            conn.close()
        return self._current_hash(meta)

    # ---------- add / list / meta ----------

    def _register(self, conn, name: str, feature: str, sample: str,
                  engine: str, pipeline: str, factor_col: str,
                  keys: list[str], meta: dict | None = None):
        from ..table.catalog import insert_object, set_deps

        cur = {
            "feature": feature,
            "sample": sample,
            "engine": engine,
            "pipeline": pipeline,
            "factor_col": factor_col,
            "keys": keys,
            "partition_by": [],
            "partition_gran": "",
            "materialized": False,
            "materialized_at": None,
            "dependency_hash": None,
            "field": None,
            "display_name": name,
            "description": "",
            "source": "local",
            "tags": [],
        }
        if meta:
            cur.update(meta)
        obj = insert_object(conn, "factor", name, cur, "", now())
        set_deps(conn, "factor", name, [
            ("feature", feature, {}),
            ("sample", sample, {"keys": keys}),
        ])
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

    def _add_sync(self, name: str, feature: str | None, sample: str | None,
                  engine: str, pipeline: str, factor_col: str,
                  meta: dict | None) -> FactorMeta:
        if not feature:
            raise ValueError("factor add 需要 --feature <feature 名>")
        if not sample:
            raise ValueError("factor add 需要 --sample <sample 名>")
        ft = self._feature_meta(feature)          # feature 必须已注册
        sm = self._sc._describe_sync(sample)      # sample 必须已注册
        parse_pipeline(pipeline)                  # pipeline 语法/算子校验
        get_engine(engine)
        factor_col = factor_col or feature
        fields = self._apply_meta_fields({}, meta or {})
        fields.setdefault("factor_col", factor_col)
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise FactorExistsError(f"factor already registered: {name}")
            self._register(conn, name, feature, sample, engine, pipeline,
                           factor_col, list(sm.keys), fields)
        return self._describe_sync(name)

    def _list_sync(self) -> list[FactorMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='factor' "
                                "ORDER BY name").fetchall()
            return [self._factor_meta(conn, r) for r in rows]
        finally:
            conn.close()

    def _describe_sync(self, name: str) -> FactorMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorNotFoundError(f"factor not registered: {name}")
            return self._factor_meta(conn, obj)
        finally:
            conn.close()

    # ---------- set / delete ----------

    def _set_sync(self, name: str, kw: dict) -> FactorMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorNotFoundError(f"factor not registered: {name}")
            from ..table.catalog import update_object_meta

            meta = dict(self._meta_dict(conn, name))
            # feature/sample/engine/pipeline/factor_col 为定义键，改动后物化态失效
            for key in ("feature", "sample", "engine", "pipeline", "factor_col"):
                if key in kw:
                    if key == "engine":
                        get_engine(kw[key])
                    meta[key] = kw[key]
                    meta["materialized"] = False
                    meta["dependency_hash"] = None
            meta = self._apply_meta_fields(meta, kw)
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._factor_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _delete_sync(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        from ..table.catalog import dependents

        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FactorNotFoundError(f"factor not registered: {name}")
            dependents_rows = dependents(conn, "factor", name)
            if dependents_rows and not force:
                from ..table.controller import DependencyError

                raise DependencyError(dependents_rows)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.execute("DELETE FROM stkoe_depends WHERE obj_type='factor' AND obj_name=?",
                         (name,))
            conn.commit()
        finally:
            conn.close()
        if with_data:
            shutil.rmtree(self._root(name), ignore_errors=True)
        return {"deleted": name}

    # ---------- 计算 / 读取 ----------

    def _compute_sync(self, fm: FactorMeta, *, partition: str | None = None
                      ) -> pl.DataFrame:
        """实时计算最终因子：sample 视图求 feature 公式 → 拼索引+因子列 → 算子链

        结果列 = 样本索引列 + ``factor_col`` 单列。
        """
        sm = self._sc._describe_sync(fm.sample)
        ft = self._feature_meta(fm.feature)
        lf = self._sc._sample_lazy(sm, partition=partition)
        engine = get_engine(fm.engine)
        field = engine.field(lf, ft.formula)
        src_rows = lf.select(pl.len()).collect().item()
        if field.height != src_rows:
            raise ValueError(f"feature 公式非逐行计算: 结果 {field.height} 行 != "
                             f"样本 {src_rows} 行")
        idx = lf.select(*[pl.col(k) for k in fm.keys]).collect()
        df = idx.hstack(field.rename({"field": fm.factor_col}))
        return engine.transform(df, fm.pipeline)

    def _get_lazy_sync(self, name: str, *, where=None,
                       partition: str | None = None) -> pl.LazyFrame:
        """读 factor（lazy）：物化且 curated → 读物化 parquet；否则实时计算"""
        fm = self._describe_sync(name)
        if fm.materialized and fm.curated:
            lf = pl.scan_parquet(self._root(name), hive_partitioning=True)
            if partition is not None:
                lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
        else:
            lf = self._compute_sync(fm, partition=partition).lazy()
        if where is not None:
            from ..table.query import to_expr

            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def _get_sync(self, name: str, *, where=None, partition: str | None = None,
                  limit: int | None = None, offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        lf = self._get_lazy_sync(name, where=where, partition=partition)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def _check_sync(self, name: str) -> FactorCheckResult:
        """校验因子：计算成功、含全部索引列、因子列恰好一列"""
        fm = self._describe_sync(name)
        try:
            df = self._compute_sync(fm)
        except Exception as e:
            return FactorCheckResult(factor=name, ok=False, rows=0,
                                     columns=tuple(fm.keys),
                                     message=f"因子计算失败: {e}")
        missing = [k for k in fm.keys if k not in df.columns]
        if missing:
            return FactorCheckResult(factor=name, ok=False, rows=df.height,
                                     columns=tuple(df.columns),
                                     message=f"结果集缺少索引列: {missing}")
        factor_cols = [c for c in df.columns if c not in fm.keys]
        if len(factor_cols) != 1:
            return FactorCheckResult(factor=name, ok=False, rows=df.height,
                                     columns=tuple(df.columns),
                                     message=f"因子列应恰好 1 列，实际 {len(factor_cols)} 列")
        if df.height == 0:
            return FactorCheckResult(factor=name, ok=False, rows=0,
                                     columns=tuple(df.columns), message="结果行数为 0")
        return FactorCheckResult(factor=name, ok=True, rows=df.height,
                                 columns=tuple(df.columns),
                                 message=f"有效（{df.height} 行）")

    # ---------- 物化 ----------

    def _scan_sync(self, name: str | None, *, all: bool = False,
                   resync: bool = False,
                   on_progress=None) -> FactorScanReport | list[FactorScanReport]:
        if all:
            return [self._scan_one(fm, resync=resync, on_progress=on_progress)
                    for fm in self._list_sync()]
        return self._scan_one(self._describe_sync(name), resync=resync,
                              on_progress=on_progress)

    def _scan_one(self, fm: FactorMeta, *, resync: bool = False,
                  on_progress=None) -> FactorScanReport:
        """物化最终因子；幂等，依赖签名一致则跳过"""
        conn = self.catalog.new_conn()
        try:
            meta = self._meta_dict(conn, fm.name)
            cur_hash = self._current_hash(meta)
            if not resync and meta.get("dependency_hash") == cur_hash \
                    and meta.get("materialized"):
                return FactorScanReport(
                    name=fm.name, version_before=fm.version, version_after=fm.version,
                    materialized=True, changed=False, partition_by=fm.partition_by)
            out_dir = self._root(fm.name)
            out_dir.mkdir(parents=True, exist_ok=True)
            rebuilt: list[str] = []
            partition_by, gran = self._partition_of_sample(fm)
            if partition_by:
                buckets = self._partition_buckets(fm)
                for i, value in enumerate(buckets, start=1):
                    if on_progress is not None:
                        on_progress(i, len(buckets), f"{fm.name}: part={value}")
                    part_dir = out_dir / f"part={value}"
                    part_dir.mkdir(parents=True, exist_ok=True)
                    df = self._compute_sync(fm, partition=str(value))
                    df.write_parquet(part_dir / "data.parquet")
                    rebuilt.append(value)
            else:
                if on_progress is not None:
                    on_progress(1, 1, f"{fm.name}: all")
                self._compute_sync(fm).write_parquet(out_dir / "data.parquet")
                rebuilt.append("")
            self._write_materialized(conn, fm, cur_hash, partition_by, gran)
        finally:
            conn.close()
        return FactorScanReport(
            name=fm.name, version_before=fm.version, version_after=fm.version + 1,
            materialized=True, changed=True, partition_by=partition_by,
            rebuilt_partitions=tuple(rebuilt))

    def _partition_of_sample(self, fm: FactorMeta) -> tuple[tuple[str, ...], str]:
        """镜像源 sample 的 dataset 分区（物化完成且已分区）"""
        sm = self._sample_meta_safe(fm.sample)
        if sm is not None and sm.dataset:
            dm = self._sc._dc._describe_sync(sm.dataset)
            if dm.materialized and dm.partition_by:
                return tuple(dm.partition_by), dm.partition_gran
        return (), ""

    def _partition_buckets(self, fm: FactorMeta) -> list[str]:
        sm = self._sample_meta_safe(fm.sample)
        if sm is None:
            return []
        root = self._sc._dc._root(sm.dataset)
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

    def _write_materialized(self, conn, fm: FactorMeta, dep_hash: str,
                            partition_by: tuple[str, ...], gran: str) -> None:
        from ..table.catalog import update_object_meta

        obj = self._object(conn, fm.name)
        meta = self._meta_dict(conn, fm.name)
        meta["materialized"] = True
        meta["materialized_at"] = now()
        meta["dependency_hash"] = dep_hash
        meta["partition_by"] = list(partition_by)
        meta["partition_gran"] = gran
        meta["field"] = {
            "name": fm.factor_col,
            "formula": self._feature_meta(fm.feature).formula,
            "display_name": fm.factor_col,
            "description": "",
            "unit": None,
            "tags": [],
        }
        update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
        conn.commit()

    # ---------- async 接口 ----------

    async def add(self, name: str, *, feature: str | None = None,
                  sample: str | None = None, engine: str = "polars",
                  pipeline: str = "nothing()", factor_col: str = "",
                  **meta) -> FactorMeta:
        """创建最终因子：依赖已注册 feature 与 sample；pipeline 默认 nothing()"""
        return await asyncio.to_thread(self._add_sync, name, feature, sample,
                                       engine, pipeline, factor_col, meta or None)

    async def get(self, name: str, *, where=None, partition: str | None = None,
                  limit: int | None = None, offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读因子（collect）：物化且一致读物化数据，否则实时计算"""
        return await asyncio.to_thread(
            self._get_sync, name, where=where, partition=partition,
            limit=limit, offset=offset, count_total=count_total)

    async def meta(self, name: str) -> FactorMeta:
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[FactorMeta]:
        return await asyncio.to_thread(self._list_sync)

    async def set(self, name: str, **kw) -> FactorMeta:
        """更新因子定义（feature/sample/pipeline/factor_col + 元数据；改动定义后物化失效）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def check(self, name: str) -> FactorCheckResult:
        """校验因子：计算成功、含全部索引列、因子列恰好一列"""
        return await asyncio.to_thread(self._check_sync, name)

    async def scan(self, name: str | None = None, *, all: bool = False,
                   resync: bool = False, on_progress=None) -> FactorScanReport | list[FactorScanReport]:
        """物化最终因子（幂等：依赖签名不变则跳过）；``all=True`` 物化全部"""
        return await asyncio.to_thread(self._scan_sync, name, all=all,
                                       resync=resync, on_progress=on_progress)

    async def delete(self, name: str, *, force: bool = False,
                     with_data: bool = True) -> dict:
        """删除因子注册、依赖与物化产物（feature/sample 从不删）"""
        return await asyncio.to_thread(self._delete_sync, name, force=force,
                                       with_data=with_data)


__all__ = ["FactorController", "FactorNotFoundError", "FactorExistsError"]