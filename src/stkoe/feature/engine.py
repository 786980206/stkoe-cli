"""feature 因子计算引擎：接口 + 注册表 + polars 实现

公式为运行在列作用域里的表达式（如 ``x*2`` / ``pl.col("x").log1p()``），
由引擎在指定 sample 的视图（LazyFrame）上逐行计算因子列。引擎按名字注册（插件式），
当前仅 polars，参照 fieldset/engine.py 的 CalcEngine 实现。

引擎接口（``FeatureEngine``）：
- ``test(lf, formula)``         返回单列结果 DataFrame（命名 ``field``）
- ``check(lf, formula)``        (ok, message)：校验公式可逐行计算结果行数一致
"""
from __future__ import annotations

import polars as pl


class FeatureEngine:
    """因子计算引擎基类：实现为策略对象，按名字注册供 FeatureController 使用"""

    name: str = ""

    def test(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        raise NotImplementedError

    def check(self, lf: pl.LazyFrame, formula: str) -> tuple[bool, str]:
        raise NotImplementedError


class PolarsEngine(FeatureEngine):
    """polars 表达式引擎：在列作用域（``{col: pl.col(col)}``）里 eval 公式"""

    name = "polars"

    @staticmethod
    def _scope(lf: pl.LazyFrame) -> dict:
        scope = {c: pl.col(c) for c in lf.collect_schema().names()}
        scope["pl"] = pl
        return scope

    @staticmethod
    def _expr(formula: str, scope: dict):
        return eval(f"({formula})", {"__builtins__": {}}, scope)

    def test(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        scope = self._scope(lf)
        return lf.select(self._expr(formula, scope).alias("field")).collect()

    def check(self, lf: pl.LazyFrame, formula: str) -> tuple[bool, str]:
        scope = self._scope(lf)
        try:
            expr = self._expr(formula, scope)
            out_rows = lf.select(expr.alias("_f")).collect().height
        except Exception as e:  # 公式编译/执行失败
            return False, f"公式执行失败: {e}"
        src_rows = lf.select(pl.len()).collect().item()
        if out_rows == src_rows:
            return True, f"行数一致（{out_rows} 行）"
        return False, f"结果行数 {out_rows} != 样本行数 {src_rows}（公式需逐行计算）"


# ---------- 引擎注册表 ----------

_ENGINES: dict[str, FeatureEngine] = {}


def register_engine(cls: type[FeatureEngine]) -> type[FeatureEngine]:
    """装饰器：注册引擎到注册表（按 ``cls.name``）"""
    _ENGINES[cls.name] = cls()
    return cls


@register_engine
class _RegisteredPolarsEngine(PolarsEngine):
    pass


def get_engine(name: str) -> FeatureEngine:
    engine = _ENGINES.get(name)
    if engine is None:
        raise EngineNotFoundError(f"未知计算引擎: {name}（可用: {', '.join(engine_names())}）")
    return engine


def engine_names() -> list[str]:
    return sorted(_ENGINES)


class EngineNotFoundError(ValueError):
    pass


__all__ = ["FeatureEngine", "PolarsEngine", "get_engine", "register_engine",
           "engine_names", "EngineNotFoundError"]