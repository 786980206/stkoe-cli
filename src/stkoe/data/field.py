"""field 指标管理（v0.4.1 迁移：catalog 注册，替换遗留 YAML 实现）

指标是 dataset 之上的派生定义（formula 存 catalog meta，可执行/物化）；
物化产物写入 fields/<name>/data.parquet（单列，按索引列排序）。
接口对齐 table/dataset/stat：create/meta/list/set/del/rename + test/materialize。
"""
from __future__ import annotations

import datetime
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from . import catalog, get_root, logger
from .catalog import access
from .catalog.json import loads


class FieldError(ValueError):
    pass


class FieldExistsError(FieldError):
    pass


class FieldNotFoundError(FieldError):
    pass


@dataclass(frozen=True)
class FieldMeta:
    """指标元数据"""
    name: str
    version: int
    dataset: str
    formula: str | None = None
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    materialized: bool = False
    created_at: str | None = None
    updated_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name, "version": self.version, "dataset": self.dataset,
            "formula": self.formula, "display_name": self.display_name,
            "description": self.description, "tags": list(self.tags),
            "materialized": self.materialized,
            "created_at": self.created_at, "updated_at": self.updated_at,
        }


def _object(conn, name: str):
    return access.get_object(conn, name, "field")


def _meta(conn, obj) -> FieldMeta:
    m = __import__("json").loads(obj["meta"])
    return FieldMeta(
        name=obj["name"], version=obj["version"], dataset=m.get("dataset", ""),
        formula=m.get("formula"), display_name=m.get("display_name", obj["name"]),
        description=m.get("description", ""), tags=tuple(m.get("tags", [])),
        materialized=bool(m.get("materialized", False)),
        created_at=obj["created_at"], updated_at=obj["updated_at"],
    )


def create(name: str, dataset: str, formula: str | None = None, **meta_input) -> FieldMeta:
    """新建指标：绑定 dataset（必须已注册）+ 公式存根，注册 catalog。

    ``**meta_input`` 可覆盖 display_name/description/tags 等。
    """
    from .dataset import describe as describe_dataset
    describe_dataset(dataset)  # 未注册抛 DatasetNotFoundError
    conn = catalog().conn
    if _object(conn, name) is not None:
        raise FieldExistsError(f"field already registered: {name}")
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "dataset": dataset, "formula": formula,
        "display_name": name, "description": "", "tags": [],
        "materialized": False,
        "create_time": now, "update_time": now,
    }
    meta.update(meta_input)
    obj = access.insert_object(conn, "field", name, meta, "", now)
    # 指标依赖 dataset（供血缘/级联）
    access.add_dep(conn, "field", name, "dataset", dataset, {"formula": formula})
    logger.debug(f"field [{name}] created on dataset {dataset}")
    return _meta(conn, obj)


def meta(name: str) -> FieldMeta:
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise FieldNotFoundError(f"field not registered: {name}")
    return _meta(conn, obj)


def list() -> list[FieldMeta]:
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='field' "
                        "ORDER BY name").fetchall()
    return [_meta(conn, r) for r in rows]


def rename(old: str, new: str) -> FieldMeta:
    """改名（catalog + 依赖边）"""
    conn = catalog().conn
    with catalog().txn() as cx:
        obj = _object(cx, old)
        if obj is None:
            raise FieldNotFoundError(f"field not registered: {old}")
        if _object(cx, new) is not None:
            raise FieldExistsError(f"field already registered: {new}")
        cx.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
        access.rename_obj(cx, "field", old, new)
        access.rename_dep(cx, "field", old, new)
    return _meta(conn, _object(conn, new))


def del_(name: str) -> None:
    """删除指标注册与物化产物（fields/<name>/ 为框架自持派生数据）"""
    conn = catalog().conn
    with catalog().txn() as cx:
        obj = _object(cx, name)
        if obj is None:
            raise FieldNotFoundError(f"field not registered: {name}")
        cx.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
        access.clear_deps(cx, "field", name)
    shutil.rmtree(_root(name), ignore_errors=True)


def describe(name: str) -> FieldMeta:
    """兼容别名：旧 field 使用 describe(name) 读取"""
    return meta(name)


# ============================================================================
# 更新 / 公式执行 / 物化
# ============================================================================

def _root(name: str) -> Path:
    return get_root() / "fields" / name


def _raw_meta(name: str) -> dict:
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise FieldNotFoundError(f"field not registered: {name}")
    return loads(obj["meta"])


def set(name: str, *, formula: str | None = None, dataset: str | None = None,
        display_name: str | None = None, description: str | None = None,
        tags: list[str] | None = None, **meta_input) -> FieldMeta:
    """更新指标元数据；formula/绑定变更会清空物化状态（产物需重算）"""
    conn = catalog().conn
    with catalog().txn() as cx:
        obj = _object(cx, name)
        if obj is None:
            raise FieldNotFoundError(f"field not registered: {name}")
        obj_meta = loads(obj["meta"])
        if dataset is not None:
            from .dataset import describe as ds_describe
            ds_describe(dataset)
            obj_meta["dataset"] = dataset
            obj_meta["materialized"] = False
        if formula is not None:
            obj_meta["formula"] = formula
            obj_meta["materialized"] = False
        if display_name is not None:
            obj_meta["display_name"] = display_name
        if description is not None:
            obj_meta["description"] = description
        if tags is not None:
            obj_meta["tags"] = tags
        obj_meta.update(meta_input)
        access.update_object_meta(cx, obj["id"], obj_meta, now_str=_now_str())
        access.set_deps(cx, "field", name, [
            ("dataset", obj_meta.get("dataset", ""), {"formula": obj_meta.get("formula")})])
    return meta(name)


def _now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_calc(code: str):
    """编译用户代码并提取 calc(data) callable（命名空间预置 pl/polars）"""
    ns: dict = {"pl": pl}
    try:
        exec(code, ns)
    except Exception as err:
        raise ValueError(f"代码编译/执行失败: {err}") from err
    calc = ns.get("calc")
    if not callable(calc):
        raise ValueError("代码中必须定义 calc(data) 函数（data 为 polars LazyFrame）")
    return calc


def _dataset_lf(name: str) -> pl.LazyFrame:
    from .dataset import get_lazy
    return get_lazy(name)


def _run_formula(ds: str, code: str, ctl=None) -> pl.DataFrame:
    """加载数据集并执行用户 calc(data)；返回 collect 后的 DataFrame。"""
    if ctl:
        ctl.progress(0.25, "加载数据集")
    data = _dataset_lf(ds)
    calc = _load_calc(code)
    if ctl:
        ctl.progress(0.5, "执行 calc(data)")
    try:
        result = calc(data)
    except Exception as err:
        raise ValueError(f"calc(data) 执行失败: {err}") from err
    if not hasattr(result, "collect"):
        raise ValueError("calc(data) 必须返回 polars LazyFrame 或 DataFrame")
    df = result.collect()
    if df.height == 0:
        raise ValueError("计算结果为空（0 行）")
    return df


def test(name: str, *, limit: int = 200, ctl=None) -> dict:
    """调试执行公式：返回前 ``limit`` 行 + schema + 耗时（中文/指标元数据）"""
    m = _raw_meta(name)
    ds = (m.get("dataset") or "").strip()
    code = (m.get("formula") or "").strip()
    if not ds:
        raise ValueError("指标未绑定数据集")
    if not code:
        raise ValueError(f"指标公式为空（{name}）")
    t0 = time.time()
    if ctl:
        ctl.info(f"测试指标 {name}：加载数据集 {ds} …")
    df = _run_formula(ds, code, ctl)
    if ctl:
        ctl.progress(1.0, "完成")
    return {
        "name": name, "dataset": ds,
        "columns": df.columns,
        "schema": {c: str(t) for c, t in df.schema.items()},
        "rows": df.head(limit).to_dicts(),
        "totalRows": int(df.height),
        "elapsedMs": int((time.time() - t0) * 1000),
    }


def test_code(dataset: str, code: str, *, limit: int = 200, ctl=None) -> dict:
    """调试执行未注册公式（测试-保存前预览）：不写 catalog/磁盘。

    与 ``test`` 同执行路径（dataset 加载 + calc(data)），但代码/数据集
    由调用方显式传入，结果不落盘。
    """
    ds = (dataset or "").strip()
    code = (code or "").strip()
    if not ds:
        raise ValueError("测试代码未指定数据集")
    if not code:
        raise ValueError("测试代码为空")
    t0 = time.time()
    if ctl:
        ctl.info(f"测试代码：加载数据集 {ds} …")
    df = _run_formula(ds, code, ctl)
    if ctl:
        ctl.progress(1.0, "完成")
    return {
        "name": "", "dataset": ds,
        "columns": df.columns,
        "schema": {c: str(t) for c, t in df.schema.items()},
        "rows": df.head(limit).to_dicts(),
        "totalRows": int(df.height),
        "elapsedMs": int((time.time() - t0) * 1000),
    }


def materialize(name: str, *, ctl=None) -> dict:
    """全量执行公式并物化：单列 parquet + 物化状态（已物化）。

    结果必须存在与指标「名称」同名的列（否则报错并列出现有列）；
    按数据集索引列排序；产物为框架自持派生数据（fields/<name>/data.parquet）。
    """
    m = _raw_meta(name)
    ds = (m.get("dataset") or "").strip()
    code = (m.get("formula") or "").strip()
    if not ds:
        raise ValueError("指标未绑定数据集")
    if not code:
        raise ValueError(f"指标公式为空（{name}）")
    col = name  # 物化列 = 指标名（契约：结果须有同名列）
    t0 = time.time()
    if ctl:
        ctl.info(f"物化指标 {name}：数据集 {ds} …")
        ctl.progress(0.1, "加载数据集")
    data = _dataset_lf(ds)
    calc = _load_calc(code)
    if ctl:
        ctl.progress(0.3, "执行 calc(data)")
    try:
        result = calc(data)
    except Exception as err:
        raise ValueError(f"calc(data) 执行失败: {err}") from err
    if not hasattr(result, "collect"):
        raise ValueError("calc(data) 必须返回 polars LazyFrame 或 DataFrame")
    df = result.collect()
    if df.height == 0:
        raise ValueError("计算结果为空（0 行）")
    if col not in df.columns:
        raise ValueError(
            f"calc(data) 返回值中不存在与指标名称「{col}」同名的列"
            f"（现有列: {', '.join(df.columns)}）。"
            f"请为结果列添加 .alias(\"{col}\") 后再物化")
    from .dataset import describe as ds_describe
    index_cols = [c for c in ds_describe(ds).keys if c in df.columns]
    if index_cols:
        if ctl:
            ctl.info(f"按索引列排序: {', '.join(index_cols)}")
        df = df.sort(index_cols, nulls_last=True)
    out_dir = _root(name)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.select([col]).write_parquet(out_path)
    if ctl:
        ctl.progress(0.8, "写出产物")
    with catalog().txn() as cx:
        obj = _object(cx, name)
        if obj is not None:
            meta = loads(obj["meta"])
            meta["materialized"] = True
            meta["materialized_time"] = _now_str()
            access.update_object_meta(cx, obj["id"], meta, now_str=_now_str())
    logger.debug(f"field [{name}] materialized ({df.height} rows)")
    if ctl:
        ctl.progress(1.0, "物化完成")
    return {
        "name": name, "column": col, "rows": int(df.height),
        "dataFile": str(out_path),
        "elapsedMs": int((time.time() - t0) * 1000),
    }