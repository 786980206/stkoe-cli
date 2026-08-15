"""factor 公式/算子引擎：接口 + 算子注册表 + polars 实现

因子 = 在样本池视图上经 feature 公式求值得到「样本索引列 + 一列因子列」，
再经 ``pipeline`` 算子链（如 ``nothing()|standardlize()|rankliezd()``）变换。

算子接口（``FactorOperator``）：
- ``apply(df) -> df``：输入/输出均为「样本索引列 + 单因子列」的 DataFrame

pipeline 语法：``|`` 分隔的算子调用（如 ``nothing()``），按名字注册（插件式），
当前仅 ``nothing()``；后续算子（standardlize/rankliezd 等）按 ``register_operator``
注册即可动态扩展。
"""
from __future__ import annotations

import re

import polars as pl


class FactorOperator:
    """因子算子基类：实现为策略对象，按名字注册供 pipeline 解析使用"""

    name: str = ""
    help: str = ""

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError


class NothingOperator(FactorOperator):
    """不做任何处理，原样返回输入（恒等变换）"""

    name = "nothing"
    help = "原样返回输入（恒等变换）"

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df


# ---------- 算子注册表 ----------

_OPERATORS: dict[str, FactorOperator] = {}


def register_operator(op: FactorOperator) -> FactorOperator:
    """注册算子实例到注册表（按 ``op.name``）"""
    _OPERATORS[op.name] = op
    return op


register_operator(NothingOperator())


def get_operator(name: str) -> FactorOperator:
    op = _OPERATORS.get(name)
    if op is None:
        raise OperatorNotFoundError(
            f"未知因子算子: {name}（可用: {', '.join(operator_names())}）")
    return op


def operator_names() -> list[str]:
    return sorted(_OPERATORS)


class OperatorNotFoundError(ValueError):
    pass


def parse_pipeline(pipeline: str) -> list[FactorOperator]:
    """解析 ``nothing()|standardlize()|rankliezd()`` 形式的算子链

    每个算子段为 ``name()``（当前不支持参数，后续算子可在 apply 内约定配置）。
    """
    ops: list[FactorOperator] = []
    for seg in [s.strip() for s in (pipeline or "").split("|") if s.strip()]:
        m = re.fullmatch(r"(\w+)\(\)", seg)
        if m is None:
            raise OperatorNotFoundError(f"无法解析算子: {seg}（期望 name() 形式）")
        ops.append(get_operator(m.group(1)))
    return ops


class FactorEngine:
    """因子引擎基类：实现为策略对象，按名字注册供 FactorController 使用"""

    name: str = ""

    def field(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        """在样本池视图上求值 feature 公式，返回单列 DataFrame（命名 ``field``）"""
        raise NotImplementedError

    def transform(self, df: pl.DataFrame, pipeline: str) -> pl.DataFrame:
        """对「样本索引列 + 因子列」DataFrame 施加算子链"""
        for op in parse_pipeline(pipeline):
            df = op.apply(df)
        return df


class PolarsEngine(FactorEngine):
    """polars 因子引擎：列作用域 eval feature 公式 + polars 算子链"""

    name = "polars"

    @staticmethod
    def _scope(lf: pl.LazyFrame) -> dict:
        scope = {c: pl.col(c) for c in lf.collect_schema().names()}
        scope["pl"] = pl
        return scope

    def field(self, lf: pl.LazyFrame, formula: str) -> pl.DataFrame:
        scope = self._scope(lf)
        expr = eval(f"({formula})", {"__builtins__": {}}, scope)
        return lf.select(expr.alias("field")).collect()


# ---------- 引擎注册表 ----------

_ENGINES: dict[str, FactorEngine] = {}


def register_engine(cls: type[FactorEngine]) -> type[FactorEngine]:
    """装饰器：注册引擎到注册表（按 ``cls.name``）"""
    _ENGINES[cls.name] = cls()
    return cls


@register_engine
class _RegisteredPolarsEngine(PolarsEngine):
    pass


def get_engine(name: str) -> FactorEngine:
    engine = _ENGINES.get(name)
    if engine is None:
        raise EngineNotFoundError(f"未知因子引擎: {name}（可用: {', '.join(engine_names())}）")
    return engine


def engine_names() -> list[str]:
    return sorted(_ENGINES)


class EngineNotFoundError(ValueError):
    pass


__all__ = ["FactorOperator", "NothingOperator", "FactorEngine", "PolarsEngine",
           "get_engine", "register_engine", "engine_names", "EngineNotFoundError",
           "get_operator", "register_operator", "operator_names",
           "OperatorNotFoundError", "parse_pipeline"]