"""sample 过滤引擎：接口 + 注册表 + polars 实现

过滤公式为运行在列作用域里的布尔表达式（如
``(date>='2026-01-01')&(sym.is_in(['000001.SZ','000002.SZ']))``），
由引擎在 ``dataset_with_fieldset`` 的 LazyFrame 上执行 ``filter``。引擎按名字注册（插件式），
当前仅 polars。

引擎接口（``SampleEngine``）：
- ``filter(lf, formula)``  返回施加过滤后的 LazyFrame（formula 为空 → 原样返回）
- ``test(lf, formula)``    单列求值预览（命名 ``field``），供校验/联调用
"""
from __future__ import annotations

import polars as pl


class SampleEngine:
    """过滤引擎基类：实现为策略对象，按名字注册供 SampleController 使用"""

    name: str = ""

    def filter(self, lf: pl.LazyFrame, formula: str) -> pl.LazyFrame:
        raise NotImplementedError

    def test(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        raise NotImplementedError


class PolarsEngine(SampleEngine):
    """polars 过滤引擎：在列作用域（``{col: pl.col(col)}``）里 eval 布尔表达式并 filter"""

    name = "polars"

    @staticmethod
    def _scope(lf: pl.LazyFrame) -> dict:
        scope = {c: pl.col(c) for c in lf.collect_schema().names()}
        scope["pl"] = pl
        return scope

    @staticmethod
    def _expr(formula: str, scope: dict):
        return eval(f"({formula})", {"__builtins__": {}}, scope)

    def filter(self, lf: pl.LazyFrame, formula: str) -> pl.LazyFrame:
        if not formula or not formula.strip():
            return lf
        scope = self._scope(lf)
        return lf.filter(self._expr(formula, scope))

    def test(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        scope = self._scope(lf)
        filtered = self.filter(lf, formula)
        return filtered.select(pl.lit(True).alias("field")).collect()


# ---------- 引擎注册表 ----------

_ENGINES: dict[str, SampleEngine] = {}


def register_engine(cls: type[SampleEngine]) -> type[SampleEngine]:
    """装饰器：注册引擎到注册表（按 ``cls.name``）"""
    _ENGINES[cls.name] = cls()
    return cls


@register_engine
class _RegisteredPolarsEngine(PolarsEngine):
    pass


def get_engine(name: str) -> SampleEngine:
    engine = _ENGINES.get(name)
    if engine is None:
        raise EngineNotFoundError(f"未知过滤引擎: {name}（可用: {', '.join(engine_names())}）")
    return engine


def engine_names() -> list[str]:
    return sorted(_ENGINES)


class EngineNotFoundError(ValueError):
    pass


__all__ = ["SampleEngine", "PolarsEngine", "get_engine", "register_engine",
           "engine_names", "EngineNotFoundError"]