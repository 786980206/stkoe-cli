"""fieldset 公式计算引擎：接口 + 注册表 + polars 实现

公式语言为运行在列作用域里的表达式（如 ``a + 1`` / ``pl.col("a") * 2``），
由引擎在数据集列上逐行计算衍生指标。引擎按名字注册（插件式），当前仅 polars。

引擎接口（``CalcEngine``）：
- ``test(dataset, formula)``         返回单列结果 DataFrame（命名 ``field``）
- ``scan(dataset, keys, fields)``    返回 keys + 各已校验指标列的 LazyFrame
- ``check(dataset, field)``          (ok, message)：校验公式可逐行计算结果行数一致
"""
from __future__ import annotations

import polars as pl

from .spec import FieldMeta


class CalcEngine:
    """公式计算引擎基类：实现为策略对象，按名字注册供 FieldsetController 使用"""

    name: str = ""

    def test(self, dataset: pl.LazyFrame, formula: str) -> pl.DataFrame:
        raise NotImplementedError

    def scan(self, dataset: pl.LazyFrame, keys: list[str],
             fields: list[FieldMeta]) -> pl.LazyFrame:
        raise NotImplementedError

    def check(self, dataset: pl.LazyFrame, field: FieldMeta) -> tuple[bool, str]:
        raise NotImplementedError


class PolarsEngine(CalcEngine):
    """polars 表达式引擎：在列作用域（``{col: pl.col(col)}``）里 eval 公式"""

    name = "polars"

    @staticmethod
    def _scope(dataset: pl.LazyFrame) -> dict:
        scope = {c: pl.col(c) for c in dataset.collect_schema().names()}
        scope["pl"] = pl
        return scope

    @staticmethod
    def _expr(formula: str, scope: dict):
        return eval(f"({formula})", {"__builtins__": {}}, scope)

    def test(self, dataset: pl.LazyFrame, formula: str) -> pl.DataFrame:
        scope = self._scope(dataset)
        return dataset.select(self._expr(formula, scope).alias("field")).collect()

    def scan(self, dataset: pl.LazyFrame, keys: list[str],
             fields: list[FieldMeta]) -> pl.LazyFrame:
        scope = self._scope(dataset)
        exprs = [pl.col(k) for k in keys]
        for f in fields:
            if not f.validated:
                continue
            exprs.append(self._expr(f.formula, scope).alias(f.name))
        return dataset.select(*exprs)

    def check(self, dataset: pl.LazyFrame, field: FieldMeta) -> tuple[bool, str]:
        scope = self._scope(dataset)
        try:
            expr = self._expr(field.formula, scope)
            out_rows = dataset.select(expr.alias("_f")).collect().height
        except Exception as e:  # 公式编译/执行失败
            return False, f"公式执行失败: {e}"
        src_rows = dataset.select(pl.len()).collect().item()
        if out_rows == src_rows:
            return True, f"行数一致（{out_rows} 行）"
        return False, f"结果行数 {out_rows} != 源行数 {src_rows}（公式需逐行计算）"


# ---------- 引擎注册表 ----------

_ENGINES: dict[str, CalcEngine] = {}


def register_engine(cls: type[CalcEngine]) -> type[CalcEngine]:
    """装饰器：注册引擎到注册表（按 ``cls.name``）"""
    _ENGINES[cls.name] = cls()
    return cls


@register_engine
class _RegisteredPolarsEngine(PolarsEngine):
    pass


def get_engine(name: str) -> CalcEngine:
    engine = _ENGINES.get(name)
    if engine is None:
        raise EngineNotFoundError(f"未知计算引擎: {name}（可用: {', '.join(engine_names())}）")
    return engine


def engine_names() -> list[str]:
    return sorted(_ENGINES)


class EngineNotFoundError(ValueError):
    pass


__all__ = ["CalcEngine", "PolarsEngine", "get_engine", "register_engine",
           "engine_names", "EngineNotFoundError"]