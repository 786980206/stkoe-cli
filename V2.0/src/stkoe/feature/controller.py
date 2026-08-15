"""FEATURE 模块：因子定义库（FeatureController，async 接口）

定位：
- feature = 公式 + engine 的**纯定义**，注册于 catalog（type='feature'），
  **不涉及数据物化**；效果等同"在 dataset 上创建 field"，区别在于
  feature + sample | pipeline -> factor：公式在指定 **sample**（dataset_with_fieldset
  + filter 的动态视图）上逐行计算因子列。
- 引擎插件制（engine.py）：当前仅 polars（列作用域表达式 eval），参照
  fieldset 的 test/check 实现。
- ``feature test <name> --sample <sample>``：在样本视图上求值并校验——
  公式执行成功且结果行数 == 样本行数（逐行计算）才算合法因子。
- 无依赖边：feature 不与 sample 绑定（test 时指定），删除 sample 不受影响。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import polars as pl

from ..jsonutil import loads
from ..table.controller import DEFAULT_IGNORE_COLS
from ..table.util import now
from .engine import get_engine
from .spec import FeatureMeta, FeatureTestResult


class FeatureNotFoundError(FileNotFoundError):
    pass


class FeatureExistsError(ValueError):
    pass


META_FIELDS = ("display_name", "description", "source", "formula", "engine", "unit", "tags")


class FeatureController:
    """因子定义控制面：add/set/delete/meta/list/test（无物化，test 时动态求值）

    复用 SampleController 读取 sample 的 dataset_with_fieldset + filter 视图。
    """

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        from ..sample.controller import SampleController

        self._sc = SampleController(data_dir=data_dir, ignore_cols=ignore_cols)
        self.data_dir = self._sc.data_dir
        self.catalog = self._sc.catalog

    # ---------- 内部转换 ----------

    def _object(self, conn, name: str):
        from ..table.catalog import get_object

        return get_object(conn, name, "feature")

    def _meta_dict(self, conn, name: str) -> dict:
        obj = self._object(conn, name)
        return loads(obj["meta"]) if obj is not None else {}

    def _feature_meta(self, conn, obj) -> FeatureMeta:
        meta = loads(obj["meta"])
        return FeatureMeta(
            name=obj["name"],
            version=obj["version"],
            engine=meta.get("engine", "polars"),
            formula=meta.get("formula", ""),
            display_name=meta.get("display_name", obj["name"]),
            description=meta.get("description", ""),
            unit=meta.get("unit"),
            tags=tuple(meta.get("tags", [])),
            source=meta.get("source", "local"),
            extra=meta.get("extra") or {},
            created_at=obj["created_at"],
            updated_at=obj["updated_at"],
        )

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

    # ---------- add / list / meta ----------

    def _add_sync(self, name: str, engine: str, formula: str,
                  meta: dict | None) -> FeatureMeta:
        if not formula:
            raise ValueError("feature add 需要 --formula <表达式>")
        fields = self._apply_meta_fields({}, meta or {})
        fields.setdefault("engine", engine)
        fields.setdefault("formula", formula)
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise FeatureExistsError(f"feature already registered: {name}")
            self._register(conn, name, fields)
        return self._describe_sync(name)

    def _register(self, conn, name: str, meta: dict):
        from ..table.catalog import insert_object

        cur = {
            "engine": "polars",
            "formula": "",
            "display_name": name,
            "description": "",
            "source": "local",
            "tags": [],
            "unit": None,
        }
        cur.update(meta)
        insert_object(conn, "feature", name, cur, "", now())

    def _list_sync(self) -> list[FeatureMeta]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='feature' "
                                "ORDER BY name").fetchall()
            return [self._feature_meta(conn, r) for r in rows]
        finally:
            conn.close()

    def _describe_sync(self, name: str) -> FeatureMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FeatureNotFoundError(f"feature not registered: {name}")
            return self._feature_meta(conn, obj)
        finally:
            conn.close()

    # ---------- set / delete ----------

    def _set_sync(self, name: str, kw: dict) -> FeatureMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FeatureNotFoundError(f"feature not registered: {name}")
            from ..table.catalog import update_object_meta

            meta = self._apply_meta_fields(self._meta_dict(conn, name), kw)
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._feature_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    def _delete_sync(self, name: str, *, force: bool = False) -> dict:
        from ..table.catalog import dependents

        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise FeatureNotFoundError(f"feature not registered: {name}")
            dependents_rows = dependents(conn, "feature", name)
            if dependents_rows and not force:
                from ..table.controller import DependencyError

                raise DependencyError(dependents_rows)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.execute("DELETE FROM stkoe_depends WHERE obj_type='feature' AND obj_name=?",
                         (name,))
            conn.commit()
        finally:
            conn.close()
        return {"deleted": name}

    # ---------- test ----------

    def _test_sync(self, name: str, sample: str
                   ) -> tuple[FeatureTestResult, pl.DataFrame | None]:
        """在指定 sample 视图上求值公式 + 校验逐行合法性（行数一致）"""
        ft = self._describe_sync(name)
        sm = self._sc._describe_sync(sample)  # 样本池必须已注册
        lf = self._sc._sample_lazy(sm)        # dataset_with_fieldset + filter
        engine = get_engine(ft.engine)
        try:
            df = engine.test(lf, ft.formula)
        except Exception as e:
            return (FeatureTestResult(feature=name, sample=sample, ok=False,
                                      valid=False, rows=0, columns=(),
                                      message=f"公式执行失败: {e}"), None)
        ok, message = engine.check(lf, ft.formula)
        return (FeatureTestResult(feature=name, sample=sample, ok=True,
                                  valid=ok, rows=df.height,
                                  columns=tuple(df.columns), message=message), df)

    # ---------- async 接口 ----------

    async def add(self, name: str, *, engine: str = "polars", formula: str = "",
                  **meta) -> FeatureMeta:
        """创建因子定义：--formula 必选；engine 默认 polars"""
        return await asyncio.to_thread(self._add_sync, name, engine, formula,
                                       meta or None)

    async def set(self, name: str, **kw) -> FeatureMeta:
        """更新因子定义（formula/engine/display_name/description/unit/tags；任意键进 extra）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    async def delete(self, name: str, *, force: bool = False) -> dict:
        """删除因子定义（纯定义，无数据产物）；下游依赖存在时需 --force"""
        return await asyncio.to_thread(self._delete_sync, name, force=force)

    async def meta(self, name: str) -> FeatureMeta:
        return await asyncio.to_thread(self._describe_sync, name)

    async def list(self) -> list[FeatureMeta]:
        return await asyncio.to_thread(self._list_sync)

    async def test(self, name: str, sample: str
                   ) -> tuple[FeatureTestResult, pl.DataFrame | None]:
        """测试因子：在指定 sample 上求值；返回 (结果, 计算结果 DataFrame)"""
        return await asyncio.to_thread(self._test_sync, name, sample)


__all__ = ["FeatureController", "FeatureNotFoundError", "FeatureExistsError"]