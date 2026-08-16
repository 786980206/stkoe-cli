"""feature 资产业务：因子定义库（纯定义，无物化）/ 即时求值测试。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——sample 视图经
``svc._sample_view_lf``（GraphService 薄委托）。
"""
from __future__ import annotations

import polars as pl

from ..graph.handlers import FeatureHandler
from .engine import get_engine as get_feature_engine


def feature_add(svc, name: str, formula: str, *, engine: str = "polars",
                unit: str | None = None, **kw) -> dict:
    return FeatureHandler.add(svc.graph, name, formula, engine=engine,
                              unit=unit, **kw)


def feature_meta(svc, name: str) -> dict:
    return svc.graph.meta("feature", name)


def feature_list(svc) -> list:
    return svc.graph.list("feature")


def feature_set(svc, name: str, **kw) -> dict:
    svc._require_node("feature", name)
    if "window_size" in kw:
        kw["window_size"] = int(kw["window_size"] or 0)
    return svc.graph.set("feature", name, **kw)


def feature_update(svc, name: str) -> dict:
    """feature 更新：纯定义资产（无上游），标记有效并铸版本（无事件不空 bump）。"""
    svc.graph.assert_ready("feature", name)
    m = svc.graph.resolve("feature", name, mark_materialized=False)
    return {"name": name, "valid": True,
            "version": m["version"]}


def feature_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("feature", name)
    svc.graph.delete("feature", name, force=force)
    return {"deleted": name}


def feature_test(svc, name: str, sample: str):
    node = svc._require_node("feature", name)
    lf = svc._sample_view_lf(sample)
    engine = get_feature_engine(node.get("engine") or "polars")
    try:
        df = engine.test(lf, node.get("formula") or "")
        total = lf.select(pl.len()).collect().item()
        valid = df.height == total
        return ({"feature": name, "sample": sample, "ok": True, "valid": valid,
                 "rows": df.height, "columns": list(df.columns),
                 "message": "" if valid else f"结果行数 {df.height} != 样本行数 {total}"},
                df if df.height else None)
    except Exception as e:
        return ({"feature": name, "sample": sample, "ok": False, "valid": False,
                 "rows": 0, "columns": [], "message": f"公式执行失败: {e}"}, None)