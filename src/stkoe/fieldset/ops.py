"""fieldset 资产业务：登记（依赖 panel）/ 字段定义与校验 / 物化 / 读取 / 即时求值。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——图登记/依赖/版本走
``svc`` 暴露的图能力；panel 视图经 ``svc._panel_lazy``（GraphService 薄委托，
便于测试 monkeypatch）；物化落盘走 ``graph.materialize`` 共享基础设施。

模块级工具：``_formula_refs``（公式引用列提取）与 ``_expand_scope``（滚动窗口
范围展开）是窗口/公式语义的原点，factor/tester 模块复用。
"""
from __future__ import annotations

import hashlib
import re

import polars as pl

from ..graph.errors import AssetNotFoundError
from ..graph.events import DataChangeEvent
from ..graph.handlers import FieldsetHandler
from ..graph.materialize import partition_plan
from ..storage import scan, to_expr, write_all, write_incremental, write_incremental_flat
from ..graph.model import FieldMeta
from ..graph.version import now_iso
from ..panel.ops import _panel_columns
from .engine import get_engine as get_fieldset_engine

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _formula_refs(formula: str, candidates: set[str]) -> list[str]:
    """公式引用列：提取公式中的标识符，与候选列名（panel/sample 视图列）求交。

    只保留候选集合内的名字，避免把函数名/字面量误当列引用；保序去重。
    """
    return list(dict.fromkeys(m for m in _IDENT_RE.findall(formula or "")
                              if m in candidates))


def _expand_scope(scope, back: int = 0, forward: int = 0):
    """按窗口展开 datetime 区间：``[lo, hi] → [lo-back, hi+forward]``。

    滚动窗口语义（回看 w）：t 时刻输出用到 [t-w+1, t] 的输入 → 输入在 [lo, hi]
    变化时输出受影响范围是 **[lo, hi+w-1]**（向前延伸）；前向收益类窗口（如
    tester 的 d{no}）则相反向后延伸 lo。非 ISO 日期/解析失败 → 原样返回。
    """
    if not scope or (not back and not forward):
        return scope
    from datetime import date, timedelta
    try:
        lo = (date.fromisoformat(scope[0]) - timedelta(days=back)).isoformat()
        hi = (date.fromisoformat(scope[1]) + timedelta(days=forward)).isoformat()
    except (ValueError, TypeError):
        return scope
    return [lo, hi]


def _panel_keys(svc, panel: str) -> list[str]:
    pnode = svc._require_node("panel", panel)
    return list(pnode.get("keys") or ())


def _fieldset_hash(svc, node: dict) -> str:
    """fieldset 物化签名 = panel 版本 + 已校验字段公式/窗口 + engine。"""
    panel = node.get("panel", "").split(":", 1)[1]
    parts = [f"panel:{panel}:{svc._require_node('panel', panel).get('version', 0)}"]
    for fname, f in (node.get("fields") or {}).items():
        if f.get("validated"):
            parts.append(f"{fname}:{f.get('formula', '')}:{f.get('window_size', 0)}")
    parts.append(f"engine:{node.get('engine', 'polars')}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _fieldset_meta_node(svc, name: str) -> dict:
    node = svc._require_node("fieldset", name)
    meta = svc.graph._meta(node)
    extra = dict(meta.get("extra") or {})
    keys = _panel_keys(svc, node.get("panel", "").split(":", 1)[1])
    materialized = bool(node.get("materialized") or extra.get("materialized"))
    dep_hash = extra.get("dependency_hash") or ""
    meta["keys"] = keys
    meta["materialized"] = materialized
    meta["materialized_at"] = extra.get("materialized_at")
    meta["curated"] = materialized and dep_hash == _fieldset_hash(svc, node)
    meta["extra"] = extra
    return meta


def _fieldset_view_lf(svc, name: str, *, fields_only: bool = False,
                      where: pl.Expr | str | None = None) -> tuple[pl.LazyFrame, list[str]]:
    """fieldset 视图：panel 全列 + 已校验衍生字段（fields_only 时仅 keys+字段）。

    物化且 curated → 衍生字段读物化 parquet（fields_only 直接返回；
    否则与 panel 视图 join）。
    """
    node = svc._require_node("fieldset", name)
    panel = node.get("panel", "").split(":", 1)[1]
    keys = _panel_keys(svc, panel)
    fm = _fieldset_meta_node(svc, name)
    root = svc.data_dir / "fieldset" / name
    if fm["curated"] and (root / "data.parquet").exists() \
            or (fm["curated"] and any(root.glob("*=*"))):
        lf = scan(root)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if fields_only:
            return lf, keys
        base, _ = svc._panel_lazy(panel)
        return base.join(lf, on=keys, how="left"), keys
    base, _ = svc._panel_lazy(panel, where)
    fields = [FieldMeta.from_dict(f) for f in (node.get("fields") or {}).values()
              if f.get("validated")]
    engine = get_fieldset_engine(node.get("engine") or "polars")
    if fields_only:
        return engine.scan(base, keys, fields), keys
    derived = engine.scan(base, keys, fields)
    if derived.select(pl.len()).collect().item():
        base = base.join(derived, on=keys, how="left")
    return base, keys


def _fieldset_view_col_names(svc, fieldset: str) -> list[str]:
    """fieldset 视图列名：其 panel 全列 + 已校验字段（仅元数据，不读数据）。"""
    fnode = svc._require_node("fieldset", fieldset)
    pnode = svc._require_node("panel", fnode.get("panel", "").split(":", 1)[-1])
    names = [c["name"] for c in _panel_columns(svc, pnode)]
    names += [f for f, fd in (fnode.get("fields") or {}).items()
              if fd.get("validated")]
    return names


def fieldset_add(svc, name: str, panel: str, *, engine: str = "polars", **kw) -> dict:
    """fieldset add：面板 keys 透传列级血缘（fieldset keys DERIVES → panel keys）。"""
    pnode = svc._require_node("panel", panel)
    keys = list(pnode.get("keys") or ())
    col_maps = {"panel": {k: k for k in keys}}
    return FieldsetHandler.add(svc.graph, name, panel, engine=engine,
                               column_maps=col_maps, **kw)


def fieldset_add_field(svc, name: str, field: str, formula: str, **kw) -> dict:
    if not formula:
        raise ValueError("fieldset add_field 需要 formula")
    r = FieldsetHandler.add_field(svc.graph, name, field, formula, **kw)
    # 列级血缘：字段列 DERIVES → 公式引用的 panel 列（或同集字段）
    _sync_fieldset_field_derives(svc, name, field, formula)
    return r


def fieldset_set_field(svc, name: str, field: str, **kw) -> dict:
    node = svc._require_node("fieldset", name)
    old_formula = ((node.get("fields") or {}).get(field) or {}).get("formula")
    r = FieldsetHandler.set_field(svc.graph, name, field, **kw)
    if "formula" in kw and kw["formula"] != old_formula:
        svc.graph.clear_derives("fieldset", name, field)
        _sync_fieldset_field_derives(svc, name, field, kw["formula"])
    return r


def fieldset_delete_field(svc, name: str, field: str) -> dict:
    # 字段列节点由 set(fields=...) 对账清理（无 DERIVES 引用的孤立节点删除）
    return FieldsetHandler.delete_field(svc.graph, name, field)


def _fieldset_ref_cols(svc, name: str) -> tuple[set[str], set[str]]:
    """fieldset 公式可引用列：panel 视图列 ∪ 本 fieldset 已定义字段名。"""
    node = svc._require_node("fieldset", name)
    panel = node.get("panel", "").split(":", 1)[-1]
    pnode = svc._require_node("panel", panel)
    pcols = {c["name"] for c in _panel_columns(svc, pnode)}
    ffields = set((node.get("fields") or {}).keys())
    return pcols, ffields


def _sync_fieldset_field_derives(svc, name: str, field: str, formula: str) -> list[str]:
    """字段列级血缘：字段列 DERIVES → 公式引用的 panel 列 / 同集字段列。

    同时把引用列写回字段 meta 的 ``required_fields``（派生信息，不额外置脏）。
    """
    node = svc._require_node("fieldset", name)
    panel = node.get("panel", "").split(":", 1)[-1]
    pcols, ffields = _fieldset_ref_cols(svc, name)
    refs = _formula_refs(formula, pcols | ffields)
    to_panel = [r for r in refs if r in pcols]
    if to_panel:
        svc.graph.sync_derives("fieldset", name, "panel", panel,
                               {field: to_panel})
    to_fields = [r for r in refs if r in ffields]
    if to_fields:
        svc.graph.sync_derives("fieldset", name, "fieldset", name,
                               {field: to_fields})
    fields = dict(node.get("fields") or {})
    cur = fields.get(field)
    if cur is not None and list(cur.get("required_fields") or ()) != refs:
        fields[field] = {**cur, "required_fields": refs}
        # required_fields 是 formula 的派生信息（与 validated 同属状态更新）：
        # add_field/set_field 已按定义变化置脏过自身与下游，此处不重复置脏
        svc.graph.set("fieldset", name, definition=True, fields=fields,
                      self_invalidate=False, propagate=False)
    return refs


def _sync_fieldset_derives_all(svc, name: str) -> None:
    """对账字段列级血缘（历史字段自愈）：按当前公式重算 required_fields 与 DERIVES。

    字段 DERIVES 只在 ``add_field``/``set_field`` 时派发——旧库/升级前登记的
    字段可能缺边或缺 ``required_fields``（血缘图上字段与 panel 源字段之间
    没有关系）。``fieldset update`` 时全量对账：引用集合与已登记不一致 →
    清旧边重派发 + 写回 required_fields（幂等，无变化不动作；不置脏）。
    """
    node = svc._require_node("fieldset", name)
    pcols, ffields = _fieldset_ref_cols(svc, name)
    for field, fd in (node.get("fields") or {}).items():
        formula = (fd or {}).get("formula") or ""
        refs = _formula_refs(formula, pcols | ffields)
        cur = list((fd or {}).get("required_fields") or ())
        if cur != refs:
            svc.graph.clear_derives("fieldset", name, field)
            _sync_fieldset_field_derives(svc, name, field, formula)


def fieldset_meta_field(svc, name: str, field: str) -> dict:
    return FieldsetHandler.meta_field(svc.graph, name, field)


def fieldset_meta(svc, name: str) -> dict:
    return _fieldset_meta_node(svc, name)


def fieldset_list(svc) -> list:
    return [_fieldset_meta_node(svc, n["name"]) for n in svc.graph.list("fieldset")]


def fieldset_set(svc, name: str, **kw) -> dict:
    svc._require_node("fieldset", name)
    return svc.graph.set("fieldset", name, **kw)


def fieldset_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("fieldset", name)
    svc.graph.delete("fieldset", name, force=force)
    return {"deleted": name}


def fieldset_check(svc, name: str, field: str) -> dict:
    """校验单个指标；通过后写回 validated=True（视图/物化只取已校验字段）。"""
    node = svc._require_node("fieldset", name)
    fields = node.get("fields") or {}
    if field not in fields:
        raise AssetNotFoundError(f"field not found: {field}")
    base, keys = svc._panel_lazy(node.get("panel", "").split(":", 1)[1])
    engine = get_fieldset_engine(node.get("engine") or "polars")
    ok, message = engine.check(base, FieldMeta.from_dict(fields[field]))
    if ok and not fields[field].get("validated"):
        new_fields = dict(fields)
        new_fields[field] = {**fields[field], "validated": True}
        # validated 写回是状态更新（非定义变化）→ 不使自身失效
        svc.graph.set("fieldset", name, definition=True,
                      fields=new_fields, self_invalidate=False)
    return {"fieldset": name, "field": field, "ok": ok, "message": message}


def fieldset_get(svc, name: str, *, fields_only: bool = False,
                 columns: list[str] | None = None, where=None,
                 limit=None, offset=None, count_total: bool = False):
    """fieldset 读取（第 1/3 态）：已物化 → 物化字段（+ panel 合并）；未物化 → 报错。"""
    node = svc._require_node("fieldset", name)
    meta = fieldset_meta(svc, name)
    root = svc._require_materialized("fieldset", name, meta)
    lf = scan(root)
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    if not fields_only:
        panel = node.get("panel", "").split(":", 1)[1]
        keys = _panel_keys(svc, panel)
        base, _ = svc._panel_lazy(panel)  # 上游 panel 物化或实时（内部视图）
        lf = base.join(lf, on=keys, how="left")
    return svc._collect_page(lf, columns=columns, limit=limit, offset=offset,
                             count_total=count_total)


def fieldset_update(svc, name: str, *, resync: bool = False) -> dict:
    """fieldset 更新：传导检查上游（panel 链）就绪 → 衍生字段物化落盘
    ``fieldset/<name>/data.parquet``（keys + 已校验字段）+ 铸版本 + 水位对齐。

    增量：源头积累事件有明确 datetime 区间且已有物化 → 只重算该区间字段并合并写回；
    首次 / 无区间 / ``--resync`` → 全量。
    """
    svc.graph.assert_ready("fieldset", name)
    node = svc._require_node("fieldset", name)
    _sync_fieldset_derives_all(svc, name)  # 血缘对账：历史字段缺边/引用变化自愈
    panel = node.get("panel", "").split(":", 1)[1]
    keys = _panel_keys(svc, panel)
    fields = [FieldMeta.from_dict(f) for f in (node.get("fields") or {}).values()
              if f.get("validated")]
    engine = get_fieldset_engine(node.get("engine") or "polars")
    out_dir = svc.data_dir / "fieldset" / name
    dt = keys[-1] if keys else ""
    pkeys, gran = partition_plan(svc.store, node, dt_col=dt)
    out_path = out_dir / ("data.parquet" if not pkeys else "")
    scope = None if resync else svc._upstream_scope(node)
    # 滚动窗口（fieldset 字段 window_size）：输入 [lo, hi] 变化 → 输出受影响
    # [lo, hi+w-1]，增量重算区间与自身事件范围都按最大回看宽度向前展开
    win = max((f.window_size for f in fields), default=0)
    if scope and win > 1:
        (lo, hi), syms = scope
        scope = (_expand_scope([lo, hi], forward=win - 1), syms)
    if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
        (lo, hi), syms = scope
        dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
        sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
        where = dt_expr & sym_expr if sym_expr is not None else dt_expr
        base, _ = svc._panel_lazy(panel, where=where)
        df_inc = engine.scan(base, keys, fields).collect()
        if pkeys:
            write_incremental(scan(out_dir, exclude=()), df_inc, dt_expr, pkeys, out_dir, gran, dt,
                            sym_expr=sym_expr,
                            sort_cols=[dt, keys[0]] if dt else None)
        else:
            df = write_incremental_flat(out_path, df_inc, dt_expr, keys,
                                        sym_expr=sym_expr,
                                        sort_cols=[dt, keys[0]] if dt else None)
        rows = df_inc.height
    else:
        base, _ = svc._panel_lazy(panel)
        out = engine.scan(base, keys, fields)
        if dt:
            out = out.sort([dt, keys[0]])  # 物化存储时间优先
        df = out.collect()  # 一次物化（rows 计数与分桶写盘共用，不重复求值）
        rows = df.height
        write_all(df, out_dir, pkeys, gran=gran, dt_col=dt, clean=True)
    m = svc.graph.resolve("fieldset", name, extra={
        "dependency_hash": _fieldset_hash(svc, node),
        "materialized_at": now_iso(),
    }, own_event=DataChangeEvent(
        action="upsert",
        field_scope=[f.name for f in fields],  # 记录自身重算的字段，而非上游列
        datetime_scope=scope[0] if scope else None,  # 窗口展开后的范围，供下游增量
        symbol_scope=scope[1] if scope else None,    # 变化标的集合，供下游增量
    ))
    return {"name": name, "materialized": True, "valid": True, "rows": rows,
            "fields_count": len(fields), "version": m["version"]}


def fieldset_test(svc, name: str, formula: str):
    node = svc._require_node("fieldset", name)
    base, _ = svc._panel_lazy(node.get("panel", "").split(":", 1)[1])
    engine = get_fieldset_engine(node.get("engine") or "polars")
    df = engine.test(base, formula)
    return {"ok": True, "rows": df.height, "columns": list(df.columns)}, df