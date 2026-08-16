"""各资产类型 Handler（对应 V3.0 初始设计的 TableHandler/IndexHandler/PanelHandler/...）。

本阶段每个 handler 都是 GraphController 的薄封装：
- 节点/边的图账本（add/delete/set/meta/col/notify_change/update/resolve）全部真实；
- 物理数据读取（``get`` 返回节点元数据）与物化（``materialize`` 走 storage 钩子）
  暂未接真实存储，流程可端到端跑通。

Handler 形态沿用 V3.0 初始设计：classmethod + 第一个参数为 controller。
"""
from __future__ import annotations

from typing import Any

from .controller import GraphController
from .model import ColumnMeta, DataChangeEvent, FieldMeta, node_id


def _cols(cols: list | tuple | None) -> list[dict]:
    """列列表 → dict 列表（容错：输入为 dict 列表或 ColumnMeta 列表）。"""
    out = []
    for c in cols or ():
        out.append(c.to_dict() if isinstance(c, ColumnMeta) else dict(c))
    return out


def _norm_join(join: str) -> str:
    """成员表 join 类型归一化：asof/left → asof_join/left_join，未知值报错。"""
    j = str(join or "").strip().lower()
    if j in ("", "asof", "asof_join"):
        return "asof_join"
    if j in ("left", "left_join"):
        return "left_join"
    raise ValueError(f"未知 join 类型: {join!r}（可选 asof / left）")


# =====================================================================
# Table / Index（数据源头）
# =====================================================================

class TableHandler:
    """原始物理表：数据变化经 notify_change 登记事件并传播下游。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, *,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            type: str = "", columns: list | tuple | None = None,
            **kw: Any) -> dict:
        """创建一个 Table 节点。"""
        return ctrl.add(
            "table", name, display_name=display_name, description=description,
            tags=tags, source=source,
            columns=_cols(columns), type=type, **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        """删除节点与上游边：当且仅当节点没有下游边时才能删除。"""
        return ctrl.delete("table", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        """设置节点属性（定义键变更 → 下游失效）。"""
        return ctrl.set("table", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        """获取节点属性。"""
        return ctrl.meta("table", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        """获取节点对应资产物理数据（本阶段返回节点元数据）。"""
        return ctrl.get("table", name)

    @classmethod
    def col(cls, ctrl: GraphController, name: str, col: str, **kw: Any) -> dict:
        """修改节点列属性。"""
        return ctrl.col("table", name, col, **kw)

    @classmethod
    def notify_change(cls, ctrl: GraphController, name: str, *,
                      event: DataChangeEvent | None = None, **kw: Any) -> dict:
        """登记物理数据变化：铸版本 + 事件入日志 + 下游置脏。"""
        return ctrl.notify_change("table", name, event=event, **kw)


class IndexHandler:
    """索引表：数据源头之一（symbol/datetime 唯一性校验后续接入）。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, *,
            symbol_col: str = "sym", datetime_col: str = "date",
            materialize_partition: str = "yearly",
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            columns: list | tuple | None = None, **kw: Any) -> dict:
        """创建一个 Index 节点。"""
        return ctrl.add(
            "index", name, display_name=display_name, description=description,
            tags=tags, source=source,
            columns=_cols(columns), symbol_col=symbol_col,
            datetime_col=datetime_col, materialize_partition=materialize_partition,
            **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("index", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("index", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("index", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("index", name)

    @classmethod
    def col(cls, ctrl: GraphController, name: str, col: str, **kw: Any) -> dict:
        return ctrl.col("index", name, col, **kw)

    @classmethod
    def notify_change(cls, ctrl: GraphController, name: str, *,
                      event: DataChangeEvent | None = None, **kw: Any) -> dict:
        return ctrl.notify_change("index", name, event=event, **kw)


# =====================================================================
# Panel（逻辑面板，index + 成员表 join）
# =====================================================================

class PanelHandler:
    """逻辑面板：由 Index + 成员表 join 而成，节点与 DEPENDS 边同时创建。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, index: str, *,
            tables: dict[str, str] | list | tuple | None = None,
            keys: list | tuple | None = None,
            column_maps: dict[str, dict] | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            **kw: Any) -> dict:
        """创建一个 Panel 节点和多条边（→ index、→ 每张成员表）。

        tables：{表名: join 类型}、[(表名, join 类型), ...] 或 ["表名:join", ...]；
        join 类型归一化为 ``asof_join``/``left_join``，缺省 ``asof_join``。
        ``column_maps``：{依赖名: {panel 列: 上游列 | [列...]}}，写入 DEPENDS 边
        detail 的 ``columns``（列级血缘 DERIVES 边由 controller.add 物化）。
        """
        table_map: dict[str, str] = {}
        if isinstance(tables, dict):
            table_map = {t: _norm_join(j) for t, j in tables.items()}
        else:
            for item in tables or ():
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    table_map[item[0]] = _norm_join(item[1])
                elif isinstance(item, str):
                    tname, _, j = item.partition(":")
                    table_map[tname] = _norm_join(j) if j else "asof_join"
        deps = [("index", index, {"role": "index",
                                  **({"columns": column_maps["index"]}
                                     if column_maps and column_maps.get("index") else {})})]
        deps += [("table", t, {"role": "member", "join": j,
                               **({"columns": column_maps[t]}
                                  if column_maps and column_maps.get(t) else {})})
                 for t, j in table_map.items()]
        return ctrl.add(
            "panel", name, display_name=display_name, description=description,
            tags=tags, source=source, deps=deps,
            index=node_id("index", index),
            tables=table_map,
            keys=list(keys or ()),
            **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        """删除节点和上游边：当且仅当节点没有下游边时才能删除。"""
        return ctrl.delete("panel", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("panel", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("panel", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        """获取节点数据（本阶段返回节点元数据；物化+有效才返回真实数据）。"""
        return ctrl.get("panel", name)

    @classmethod
    def update(cls, ctrl: GraphController, name: str) -> dict:
        """更新节点数据：查依赖 → 对比 required_version 与 version_list →
        合并积累事件 → 物化 → 出边水位对齐。"""
        return ctrl.resolve("panel", name)

    @classmethod
    def materialize(cls, ctrl: GraphController, name: str,
                    upsert: DataChangeEvent | None = None,
                    delete: DataChangeEvent | None = None) -> dict:
        """物化节点数据（= update；upsert/delete 参数为 storage 钩子预留）。"""
        return ctrl.resolve("panel", name)


# =====================================================================
# Fieldset（衍生指标集）
# =====================================================================

class FieldsetHandler:
    """基于 Panel 的衍生指标集：fields 为命名公式。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, panel: str, *,
            engine: str = "polars", column_maps: dict[str, dict] | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None,
            source: str = "local", **kw: Any) -> dict:
        """创建一个 Fieldset 节点和一条边（→ Panel）。

        ``column_maps``：{依赖名: {fieldset 列: 上游列}}，写入 DEPENDS 边 detail。
        """
        return ctrl.add(
            "fieldset", name, display_name=display_name, description=description,
            tags=tags, source=source,
            deps=[("panel", panel, {"role": "panel",
                                    **({"columns": column_maps["panel"]}
                                       if column_maps and column_maps.get("panel") else {})})],
            panel=node_id("panel", panel),
            engine=engine, fields={}, **kw,
        )

    @classmethod
    def add_field(cls, ctrl: GraphController, name: str, field_name: str,
                  formula: str, **kw: Any) -> dict:
        """添加字段（validated=False，需 check 通过）。"""
        meta = ctrl.meta("fieldset", name)
        fields = dict(meta.get("fields") or {})
        if field_name in fields:
            raise ValueError(f"field already exists: {field_name}")
        fields[field_name] = FieldMeta(
            name=field_name, formula=formula,
            display_name=kw.pop("display_name", ""),
            description=kw.pop("description", ""),
            unit=kw.pop("unit", None),
            tags=tuple(kw.pop("tags", ()) or ()),
            validated=False, engine=kw.pop("engine", "polars"),
        ).to_dict()
        return ctrl.set("fieldset", name, definition=True, fields=fields)

    @classmethod
    def delete_field(cls, ctrl: GraphController, name: str, field_name: str) -> dict:
        meta = ctrl.meta("fieldset", name)
        fields = dict(meta.get("fields") or {})
        if field_name not in fields:
            raise ValueError(f"field not found: {field_name}")
        fields.pop(field_name)
        return ctrl.set("fieldset", name, definition=True, fields=fields)

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("fieldset", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("fieldset", name, **kw)

    @classmethod
    def set_field(cls, ctrl: GraphController, name: str, field_name: str, **kw: Any) -> dict:
        meta = ctrl.meta("fieldset", name)
        fields = dict(meta.get("fields") or {})
        if field_name not in fields:
            raise ValueError(f"field not found: {field_name}")
        old = fields[field_name]
        fields[field_name] = {**old, **kw}
        # 公式变更 → validated 复位
        if "formula" in kw and kw["formula"] != old.get("formula"):
            fields[field_name]["validated"] = False
        return ctrl.set("fieldset", name, definition=True, fields=fields)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("fieldset", name)

    @classmethod
    def meta_field(cls, ctrl: GraphController, name: str, field_name: str) -> dict:
        meta = ctrl.meta("fieldset", name)
        fields = meta.get("fields") or {}
        if field_name not in fields:
            raise ValueError(f"field not found: {field_name}")
        return fields[field_name]

    @classmethod
    def get(cls, ctrl: GraphController, name: str, *, fields_only: bool = False,
            **kw: Any) -> dict:
        """获取节点数据（本阶段返回节点元数据）。"""
        return ctrl.get("fieldset", name)

    @classmethod
    def on_change(cls, ctrl: GraphController, name: str) -> dict:
        """根据积累的数据更新事件合并，输出 upsert/delete 两类事件。"""
        return ctrl.accumulated("fieldset", name)

    @classmethod
    def materialize(cls, ctrl: GraphController, name: str,
                    upsert: DataChangeEvent | None = None,
                    delete: DataChangeEvent | None = None) -> dict:
        return ctrl.resolve("fieldset", name)


# =====================================================================
# Sample / Feature / Factor / Tester
# =====================================================================

class SampleHandler:
    """样本池：fieldset 视图 ∩ 指定 index 键集合（无物化，读取动态构造）。

    血缘链：table/index → panel → fieldset → **sample** → factor；
    sample 另依赖一个 index（role=index）作为样本筛选参照（只保留键存在于
    该 index 数据中的行，不再按公式过滤）。
    """

    @classmethod
    def add(cls, ctrl: GraphController, name: str, fieldset: str, index: str, *,
            column_maps: dict[str, dict] | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            **kw: Any) -> dict:
        """创建一个 Sample 节点和两条边（→ Fieldset、→ Index）。

        ``column_maps``：{依赖名: {sample 列: 上游列}}，写入 DEPENDS 边 detail。
        """
        return ctrl.add(
            "sample", name, display_name=display_name, description=description,
            tags=tags, source=source,
            deps=[("fieldset", fieldset, {"role": "fieldset",
                                          **({"columns": column_maps["fieldset"]}
                                             if column_maps and column_maps.get("fieldset") else {})}),
                  ("index", index, {"role": "index",
                                    **({"columns": column_maps["index"]}
                                       if column_maps and column_maps.get("index") else {})})],
            fieldset=node_id("fieldset", fieldset),
            index=node_id("index", index),
            **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("sample", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("sample", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("sample", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("sample", name)

    @classmethod
    def check(cls, ctrl: GraphController, name: str) -> dict:
        """校验（storage 钩子，默认 no-op 通过）。"""
        node = ctrl.meta("sample", name)
        return ctrl._storage.check(node)  # noqa: SLF001


class FeatureHandler:
    """因子定义库：纯定义（命名公式），无物化、不依赖具体资产。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, formula: str, *,
            engine: str = "polars", unit: str | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            **kw: Any) -> dict:
        """创建一个 Feature 节点（formula 必填）。"""
        if not formula:
            raise ValueError("feature formula is required")
        return ctrl.add(
            "feature", name, display_name=display_name, description=description,
            tags=tags, source=source,
            engine=engine, formula=formula, unit=unit, **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("feature", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("feature", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("feature", name)

    @classmethod
    def test(cls, ctrl: GraphController, name: str, *, sample: str | None = None,
             **kw: Any) -> dict:
        """即时求值（storage 钩子，默认 no-op 通过）。"""
        node = ctrl.meta("feature", name)
        result = ctrl._storage.check(node)  # noqa: SLF001
        result["feature"] = name
        result["sample"] = sample
        return result


class FactorHandler:
    """最终因子：Feature 公式 + Sample 视图 + pipeline 算子链。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, feature: str, sample: str, *,
            engine: str = "polars", pipeline: str = "nothing()",
            factor_col: str | None = None, column_maps: dict[str, dict] | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            **kw: Any) -> dict:
        """创建一个 Factor 节点和两条边（→ Feature、→ Sample）。

        ``column_maps``：{依赖名: {factor 列: 上游列}}，写入 DEPENDS 边 detail。
        """
        return ctrl.add(
            "factor", name, display_name=display_name, description=description,
            tags=tags, source=source,
            deps=[
                ("feature", feature, {"role": "feature"}),
                ("sample", sample, {"role": "sample",
                                    **({"columns": column_maps["sample"]}
                                       if column_maps and column_maps.get("sample") else {})}),
            ],
            feature=node_id("feature", feature),
            sample=node_id("sample", sample),
            engine=engine, pipeline=pipeline,
            factor_col=factor_col or feature, **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("factor", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("factor", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("factor", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("factor", name)

    @classmethod
    def check(cls, ctrl: GraphController, name: str) -> dict:
        node = ctrl.meta("factor", name)
        return ctrl._storage.check(node)  # noqa: SLF001


class TesterHandler:
    """因子测试数据集。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, factor: str, *,
            returns: str = "r", groupby: str = "ic", marketcap: str = "fv",
            factor_col: str | None = None, spec: dict | None = None,
            column_maps: dict[str, dict] | None = None,
            display_name: str | None = None, description: str = "",
            tags: list | tuple | None = None, source: str = "local",
            **kw: Any) -> dict:
        """创建一个 Tester 节点和一条边（→ Factor）。

        ``column_maps``：{依赖名: {test 列: 上游列}}，写入 DEPENDS 边 detail。
        """
        return ctrl.add(
            "tester", name, display_name=display_name, description=description,
            tags=tags, source=source,
            deps=[("factor", factor, {"role": "factor",
                                      **({"columns": column_maps["factor"]}
                                         if column_maps and column_maps.get("factor") else {})})],
            factor=node_id("factor", factor),
            returns=returns, groupby=groupby, marketcap=marketcap,
            factor_col=factor_col or factor,
            spec=spec or {"quantiles": 5, "periods": [1, 5, 10],
                          "date_range": ["2023-01-01", "2026-01-01"],
                          "rolling_window": 252, "by_group": False},
            **kw,
        )

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("tester", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("tester", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("tester", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("tester", name)


# =====================================================================
# Model / Stat（预留）
# =====================================================================

class ModelHandler:
    """模型（预留）。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.add("model", name, **kw)

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("model", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("model", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("model", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("model", name)


class StatHandler:
    """统计资产（预留）。"""

    @classmethod
    def add(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.add("stat", name, **kw)

    @classmethod
    def delete(cls, ctrl: GraphController, name: str, *, force: bool = False) -> dict:
        return ctrl.delete("stat", name, force=force)

    @classmethod
    def set(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.set("stat", name, **kw)

    @classmethod
    def meta(cls, ctrl: GraphController, name: str) -> dict:
        return ctrl.meta("stat", name)

    @classmethod
    def get(cls, ctrl: GraphController, name: str, **kw: Any) -> dict:
        return ctrl.get("stat", name)


# =====================================================================
# Graph（图级查询）
# =====================================================================

class GraphHandler:
    """图级查询：节点列表 / 完整上下游血缘 / 失效清单。"""

    @classmethod
    def list(cls, ctrl: GraphController, asset_type: str | None = None, **kw: Any) -> list:
        """根据指定条件获取节点列表。"""
        return ctrl.list(asset_type)

    @classmethod
    def get(cls, ctrl: GraphController, asset_type: str, name: str,
            depth: int | None = None) -> dict:
        """根据节点获取整个上下游的依赖信息。"""
        return ctrl.lineage(asset_type, name, depth=depth)

    @classmethod
    def upstream(cls, ctrl: GraphController, asset_type: str, name: str,
                 depth: int | None = None) -> list:
        return ctrl.upstream(asset_type, name, depth=depth)

    @classmethod
    def downstream(cls, ctrl: GraphController, asset_type: str, name: str,
                   depth: int | None = None) -> list:
        return ctrl.downstream(asset_type, name, depth=depth)

    @classmethod
    def stale(cls, ctrl: GraphController) -> list:
        """失效（待重算）节点清单。"""
        return ctrl.stale()

    @classmethod
    def scan(cls, ctrl: GraphController) -> dict:
        """图统计。"""
        return ctrl.stats()


HANDLERS: dict[str, Any] = {
    "table": TableHandler,
    "index": IndexHandler,
    "panel": PanelHandler,
    "fieldset": FieldsetHandler,
    "sample": SampleHandler,
    "feature": FeatureHandler,
    "factor": FactorHandler,
    "tester": TesterHandler,
    "model": ModelHandler,
    "stat": StatHandler,
    "graph": GraphHandler,
}


__all__ = [
    "TableHandler", "IndexHandler", "PanelHandler", "FieldsetHandler",
    "SampleHandler", "FeatureHandler", "FactorHandler", "TesterHandler",
    "ModelHandler", "StatHandler", "GraphHandler", "HANDLERS",
]
