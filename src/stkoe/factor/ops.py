"""factor 资产业务：登记（feature 公式 + sample 视图 + pipeline 算子链）/ 物化 / 校验。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——sample 视图经
``svc._sample_view_lf``/``svc._sample_view_cols``（GraphService 薄委托），物化落盘
走 ``graph.materialize`` 共享基础设施；``_factor_meta_dict``/``_factor_hash``/
``_factor_compute`` 是 tester 沿链复用的共享能力，GraphService 上有同名薄委托。

``factor update --all`` 批量语义：同 sample 多因子**共享视图计算、分别物化**
（``_factor_plan`` 纯图内元数据出计划 → ``_factor_batch_compute`` 按 sample 分组
一次 collect + 一次 ``FactorEngine.fields`` → ``_factor_write`` 逐因子写盘）。
"""
from __future__ import annotations

import hashlib

import polars as pl

from ..fieldset.ops import _expand_scope, _formula_refs
from ..graph.events import DataChangeEvent
from ..graph.handlers import FactorHandler
from ..graph.materialize import partition_plan, rewrite_buckets, scan_materialized, \
    write_partitioned
from ..graph.version import now_iso
from ..table.query import to_expr
from .engine import get_engine as get_factor_engine
from .engine import parse_pipeline


def _factor_keys(svc, node: dict) -> list[str]:
    """factor 的 keys = 其 sample 的 keys（fieldset → panel）。"""
    sample = node.get("sample", "").split(":", 1)[1]
    snode = svc._require_node("sample", sample)
    return svc._sample_keys(snode)


def _factor_meta_dict(svc, name: str) -> dict:
    """V2.0 FactorMeta 形态 dict（含 keys/columns/materialized/curated/field）。"""
    node = svc._require_node("factor", name)
    meta = svc.graph._meta(node)
    extra = dict(meta.get("extra") or {})
    keys = _factor_keys(svc, node)
    sample = node.get("sample", "").split(":", 1)[1]
    materialized = bool(node.get("materialized") or extra.get("materialized"))
    dep_hash = extra.get("dependency_hash") or ""
    return {
        "name": name,
        "version": node.get("version", 0),
        "feature": node.get("feature", "").split(":", 1)[1] if node.get("feature") else "",
        "sample": sample,
        "pipeline": node.get("pipeline", ""),
        "engine": node.get("engine", "polars"),
        "factor_col": node.get("factor_col", ""),
        "keys": keys,
        "partition_by": list(extra.get("partition_by") or ()),
        "partition_gran": extra.get("partition_gran", ""),
        "materialized": materialized,
        "materialized_at": extra.get("materialized_at"),
        "curated": materialized and dep_hash == _factor_hash(svc, node),
        "columns": svc._sample_view_cols(sample),
        "field": extra.get("field"),
        "extra": extra,
        "display_name": node.get("display_name") or name,
        "description": node.get("description", ""),
        "tags": list(node.get("tags") or ()),
        "source": node.get("source", "local"),
        "created_at": node.get("create_time", ""),
        "updated_at": node.get("update_time", ""),
    }


def _factor_hash(svc, node: dict) -> str:
    """物化一致性签名 = 上游 feature/sample 版本 + engine/pipeline/factor_col。"""
    feature = node.get("feature", "").split(":", 1)[1] if node.get("feature") else ""
    sample = node.get("sample", "").split(":", 1)[1] if node.get("sample") else ""
    parts = [
        f"feature:{feature}:{svc._require_node('feature', feature).get('version', 0)}",
        f"sample:{sample}:{svc._require_node('sample', sample).get('version', 0)}",
        f"engine:{node.get('engine', 'polars')}",
        f"pipeline:{node.get('pipeline', '')}",
        f"factor_col:{node.get('factor_col', '')}",
    ]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _factor_compute(svc, node: dict, *, partition: str | None = None,
                    dt_range: tuple[str, str] | None = None,
                    symbols: list[str] | None = None,
                    view_df: pl.DataFrame | None = None) -> pl.DataFrame:
    """实时计算最终因子：sample 视图求 feature 公式 → 拼索引+因子列 → 算子链。

    ``dt_range=(lo, hi)`` / ``symbols`` 给出时只计算「时间区间 × 标的集合」
    内的行（增量物化用，字符串/ISO 可比）；``view_df`` 为调用方已构建并
    过滤的 sample 视图（tester 沿链复用，避免同一视图重复 join/collect）。
    物化前做**列投影**（keys + 公式引用列）——宽表 panel（200+ 列）下避免
    全列 collect，join 时只取所需列。
    """
    feature = node.get("feature", "").split(":", 1)[1]
    sample = node.get("sample", "").split(":", 1)[1]
    fnode = svc._require_node("feature", feature)
    keys = _factor_keys(svc, node)
    lf = view_df.lazy() if view_df is not None \
        else svc._sample_view_lf(sample)
    if dt_range:
        dt = keys[-1]
        lo, hi = dt_range
        lf = lf.filter(pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi)))
    if symbols:
        lf = lf.filter(pl.col(keys[0]).is_in(symbols))
    view_cols = set(lf.collect_schema().names())
    need = list(dict.fromkeys(
        keys + _formula_refs(fnode.get("formula") or "", view_cols)))
    df = lf.select(*need).collect()  # 只物化所需列（宽表 panel 收益明显）
    engine = get_factor_engine(node.get("engine") or "polars")
    field = engine.field(df.lazy(), fnode.get("formula") or "")
    src_rows = df.height
    if field.height != src_rows:
        raise ValueError(f"feature 公式非逐行计算: 结果 {field.height} 行 != 样本 {src_rows} 行")
    idx = df.select(*[pl.col(k) for k in keys])
    factor_col = node.get("factor_col") or feature
    out = idx.hstack(field.rename({"field": factor_col}))
    return engine.transform(out, node.get("pipeline") or "nothing()")


def _factor_view_lf(svc, name: str, *, where=None,
                    partition: str | None = None) -> pl.LazyFrame:
    """读 factor（lazy，第 1/3 态）：已物化（curated）→ 读物化 parquet；
    未物化 → 报错提示先 update（不再实时计算回退）。"""
    node = svc._require_node("factor", name)
    fm = _factor_meta_dict(svc, name)
    root = svc._require_materialized("factor", name, fm)
    lf = scan_materialized(root, partition=partition)
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    return lf


def factor_add(svc, name: str, feature: str, sample: str, *,
               engine: str = "polars", pipeline: str = "nothing()",
               factor_col: str | None = None, **kw) -> dict:
    """创建最终因子：校验 feature/sample 已注册 + pipeline/engine 合法。

    列级血缘**只保留因子列**：factor_col DERIVES → feature 公式引用的
    sample 视图列（一条或多条边，DEPENDS 边 detail.columns）——keys
    （sym/date）是索引透传，不建字段级映射（对资产血缘无信息量，因子列
    指向 sample 数据字段已表达"因子用了哪些字段"）。
    """
    if not feature:
        raise ValueError("factor add 需要 --feature <feature 名>")
    if not sample:
        raise ValueError("factor add 需要 --sample <sample 名>")
    fnode = svc._require_node("feature", feature)
    snode = svc._require_node("sample", sample)
    parse_pipeline(pipeline)
    get_factor_engine(engine)
    fcol = factor_col or feature
    view = set(svc._fieldset_view_col_names(
        snode.get("fieldset", "").split(":", 1)[-1]))
    refs = _formula_refs(fnode.get("formula") or "", view)
    col_maps = {"sample": {fcol: refs}} if refs else None
    FactorHandler.add(svc.graph, name, feature, sample, engine=engine,
                      pipeline=pipeline, factor_col=factor_col,
                      column_maps=col_maps, **kw)
    return _factor_meta_dict(svc, name)


def factor_get(svc, name: str, *, where=None, partition: str | None = None,
               limit=None, offset=None, count_total: bool = False):
    lf = _factor_view_lf(svc, name, where=where, partition=partition)
    return svc._collect_page(lf, limit=limit, offset=offset, count_total=count_total)


def factor_meta(svc, name: str) -> dict:
    return _factor_meta_dict(svc, name)


def factor_list(svc) -> list:
    return [_factor_meta_dict(svc, n["name"]) for n in svc.graph.list("factor")]


def factor_set(svc, name: str, **kw) -> dict:
    svc._require_node("factor", name)
    return svc.graph.set("factor", name, **kw)


def factor_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("factor", name)
    svc.graph.delete("factor", name, force=force)
    import shutil
    shutil.rmtree(svc.data_dir / "factor" / name, ignore_errors=True)
    return {"deleted": name}


def factor_check(svc, name: str) -> dict:
    """校验因子：计算成功、含全部索引列、因子列恰好一列、行数 > 0。"""
    node = svc._require_node("factor", name)
    keys = _factor_keys(svc, node)
    try:
        df = _factor_compute(svc, node)
    except Exception as e:
        return {"factor": name, "ok": False, "rows": 0, "columns": list(keys),
                "message": f"因子计算失败: {e}"}
    missing = [k for k in keys if k not in df.columns]
    if missing:
        return {"factor": name, "ok": False, "rows": df.height,
                "columns": list(df.columns), "message": f"结果集缺少索引列: {missing}"}
    factor_cols = [c for c in df.columns if c not in keys]
    if len(factor_cols) != 1:
        return {"factor": name, "ok": False, "rows": df.height,
                "columns": list(df.columns),
                "message": f"因子列应恰好 1 列，实际 {len(factor_cols)} 列"}
    if df.height == 0:
        return {"factor": name, "ok": False, "rows": 0,
                "columns": list(df.columns), "message": "结果行数为 0"}
    return {"factor": name, "ok": True, "rows": df.height,
            "columns": list(df.columns), "message": f"有效（{df.height} 行）"}


def factor_update(svc, name: str | None = None, *, all: bool = False,
                  resync: bool = False) -> dict | list[dict]:
    """factor 更新：传导检查上游（sample/feature 全链）就绪 → 物化 factors/<name>/。

    ``--all`` 批量：同 sample 多因子**共享视图计算、分别物化**（见
    ``_factor_scan_many``）。幂等（依赖签名一致则跳过）；update 成功后节点置
    valid=True。
    """
    if all:
        return _factor_scan_many(svc, resync=resync)
    if not name:
        raise ValueError("factor update 需要因子名（或 --all）")
    svc.graph.assert_ready("factor", name)
    return _factor_scan_one(svc, name, resync=resync)


def _factor_plan(svc, node: dict, *, resync: bool = False) -> dict:
    """因子物化计划（**纯图内元数据，不触数据计算**）：幂等判定 + keys/分区
    方案 + 增量范围（feature 窗口展开）+ 物化存在性。

    供单因子 ``_factor_scan_one`` 与批量 ``_factor_scan_many`` 共用——
    批量先给全部因子出计划（阶段 1），再共享计算（阶段 2）、分别写盘（阶段 3）。
    """
    extra = dict(node.get("extra") or {})
    name = node["name"]
    keys = _factor_keys(svc, node)
    dt = keys[-1] if keys else ""
    pkeys, gran = partition_plan(svc.store, node, dt_col=dt)
    out_dir = svc.data_dir / "factor" / name
    out_path = out_dir / ("data.parquet" if not pkeys else "")
    feature = node.get("feature", "").split(":", 1)[1]
    fnode = svc._require_node("feature", feature)
    # 增量物化：最近上游积累事件有明确 datetime 区间且已有物化 → 只重算该区间
    # （分区场景只替换受影响分区文件，flat 场景删区间+合并）
    # feature 滚动窗口：输入 [lo, hi] 变化 → factor 输出受影响 [lo, hi+w-1]
    scope = None if resync else svc._upstream_scope(node)
    fwin = int(fnode.get("window_size") or 0)
    if scope and fwin > 1:
        (lo, hi), syms = scope
        scope = (_expand_scope([lo, hi], forward=fwin - 1), syms)
    has_old = (pkeys and out_dir.exists()) or (not pkeys and out_path.exists())
    cur_hash = _factor_hash(svc, node)
    return {
        "node": node, "name": name, "keys": keys, "dt": dt,
        "pkeys": pkeys, "gran": gran, "scope": scope,
        "incremental": bool(scope) and has_old,
        "out_dir": out_dir, "out_path": out_path,
        "feature": feature, "fnode": fnode, "cur_hash": cur_hash,
        "version_before": node.get("version", 0),
        "skip": (not resync and node.get("valid")
                 and extra.get("dependency_hash") == cur_hash
                 and (node.get("materialized") or extra.get("materialized"))),
    }


def _factor_skip_report(plan: dict) -> dict:
    """幂等跳过的 update 报告（changed=False，版本不推进）。"""
    return {"name": plan["name"], "version_before": plan["version_before"],
            "version_after": plan["version_before"], "materialized": True,
            "changed": False, "partition_by": list(plan["pkeys"])}


def _factor_write(svc, plan: dict, df: pl.DataFrame) -> dict:
    """按物化计划写盘（增量/全量）+ resolve 收口；返回 update 报告。

    单因子路径（``_factor_scan_one``）与批量路径（``_factor_scan_many``）共用
    同一写盘语义——共享计算出的 df 逐因子**分别物化**。
    """
    node = plan["node"]
    name, keys, dt = plan["name"], plan["keys"], plan["dt"]
    pkeys, gran, scope = plan["pkeys"], plan["gran"], plan["scope"]
    out_dir, out_path = plan["out_dir"], plan["out_path"]
    fnode = plan["fnode"]
    if plan["incremental"]:
        (lo, hi), syms = scope
        dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
        sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
        if pkeys:
            old = pl.scan_parquet(out_dir, hive_partitioning=True)
            rewrite_buckets(old, df, dt_expr, pkeys, out_dir, gran, dt,
                            sym_expr=sym_expr,
                            sort_cols=[dt, keys[0]] if dt else None)
        else:
            keep = pl.scan_parquet(out_path).filter(
                ~(dt_expr & sym_expr) if sym_expr is not None else ~dt_expr
            ).collect()
            df = pl.concat([keep, df], how="vertical_relaxed"
                           ).unique(subset=keys, keep="last")
            if dt:
                df = df.sort([dt, keys[0]])  # 时间优先（先时间后标的）
            df.write_parquet(out_path)
    else:
        if dt:
            df = df.sort([dt, keys[0]])  # 物化存储时间优先
        write_partitioned(df, out_dir, pkeys, gran=gran, dt_col=dt, clean=True)
    # 物化成功 → resolve 收口：铸版本并记录**消费的合并事件**（own_event 带
    # 窗口展开后的 datetime_scope，供下游 tester 沿链增量）+ 出边水位对齐
    m = svc.graph.resolve("factor", name, extra={
        "dependency_hash": plan["cur_hash"], "partition_by": pkeys,
        "partition_gran": gran, "materialized_at": now_iso(),
        "field": {"name": node.get("factor_col") or plan["feature"],
                  "formula": fnode.get("formula") or "",
                  "display_name": node.get("factor_col") or plan["feature"],
                  "description": "", "unit": None, "tags": [],
                  "window_size": int(fnode.get("window_size") or 0)},
    }, own_event=DataChangeEvent(
        action="upsert",
        field_scope=[node.get("factor_col") or plan["feature"]],
        datetime_scope=scope[0] if scope else None,
        symbol_scope=scope[1] if scope else None,
    ))
    return {"name": name, "version_before": plan["version_before"],
            "version_after": m["version"], "materialized": True, "changed": True,
            "partition_by": list(pkeys), "rebuilt_partitions": [""]}


def _factor_batch_compute(svc, plans: list[dict]) -> dict[str, pl.DataFrame]:
    """同 sample 多因子**共享视图批量计算**（``factor update --all`` 阶段 2）。

    按 sample 分组：每组只构建一次 sample 视图（join 链 lazy 计划 + **一次
    collect**，列投影 = keys + 组内全部 feature 公式引用列并集）；组内按引擎
    分组，一次 ``FactorEngine.fields`` 算齐全部公式列（polars 单 select，
    同公式去重共享一列）；每个因子再按**自己的增量范围**过滤、施加各自
    pipeline 算子链。返回 ``{因子名: DataFrame}``——物化写盘由调用方逐因子
    执行（共享计算、分别物化）。
    """
    out: dict[str, pl.DataFrame] = {}
    groups: dict[str, list[dict]] = {}
    for p in plans:
        sample = p["node"].get("sample", "").split(":", 1)[1]
        groups.setdefault(sample, []).append(p)
    for sample, items in groups.items():
        keys = items[0]["keys"]
        # 联合范围：任一因子全量（无范围/首次/resync）→ 整视图；否则并集区间
        # 与标的（任一因子全集 → 视图不按标的过滤，各自再精确过滤）
        incs = [p for p in items if p["incremental"]]
        if len(incs) == len(items):
            los = [p["scope"][0][0] for p in incs]
            his = [p["scope"][0][1] for p in incs]
            lo, hi = min(los), max(his)
            sym_sets = [p["scope"][1] for p in incs]
            syms = list(dict.fromkeys(
                s for sl in sym_sets if sl for s in sl)) \
                if all(sym_sets) else None
        else:
            lo = hi = None
            syms = None
        lf = svc._sample_view_lf(sample)
        if lo is not None and keys:
            dt = keys[-1]
            lf = lf.filter(pl.col(dt).cast(pl.String)
                           .is_between(pl.lit(lo), pl.lit(hi)))
        if syms and keys:
            lf = lf.filter(pl.col(keys[0]).is_in(syms))
        view_cols = set(lf.collect_schema().names())
        need = list(dict.fromkeys(
            keys + [r for p in items
                    for r in _formula_refs(p["fnode"].get("formula") or "",
                                           view_cols)]))
        df = lf.select(*need).collect()  # 每组一次 collect（共享视图）
        by_engine: dict[str, list[dict]] = {}
        for p in items:
            by_engine.setdefault(p["node"].get("engine") or "polars", []).append(p)
        for eng_name, eng_items in by_engine.items():
            engine = get_factor_engine(eng_name)
            formula_cols: dict[str, str] = {}  # 公式 → 临时列名（同公式共享一列）
            formulas: dict[str, str] = {}      # 临时列名 → 公式（fields 入参）
            for p in eng_items:
                formula = p["fnode"].get("formula") or ""
                temp = formula_cols.setdefault(
                    formula, f"__f{len(formula_cols)}")
                p["_temp"] = temp
                formulas[temp] = formula
            field_df = engine.fields(df.lazy(), formulas)
            if field_df.height != df.height:
                raise ValueError(
                    f"feature 公式非逐行计算: 结果 {field_df.height} 行 != "
                    f"样本 {df.height} 行")
            for p in eng_items:
                node = p["node"]
                fcol = node.get("factor_col") or p["feature"]
                fdf = df.select(*[pl.col(k) for k in keys]).hstack(
                    field_df.select(pl.col(p["_temp"]).alias(fcol)))
                if p["incremental"] and keys:
                    (lo1, hi1), syms1 = p["scope"]
                    dt = keys[-1]
                    fdf = fdf.filter(
                        pl.col(dt).cast(pl.String)
                        .is_between(pl.lit(lo1), pl.lit(hi1)))
                    if syms1:
                        fdf = fdf.filter(pl.col(keys[0]).is_in(syms1))
                out[p["name"]] = engine.transform(
                    fdf, node.get("pipeline") or "nothing()")
    return out


def _factor_scan_many(svc, *, resync: bool = False) -> list[dict]:
    """``factor update --all``：同 sample 多因子**共享视图批量计算**，分别物化。

    阶段 1（纯图内元数据，无计算）：``_factor_plan`` 逐因子判幂等、取 keys/
    分区方案/增量范围；阶段 2（共享计算）：``_factor_batch_compute`` 按 sample
    分组——每组构建一次视图 + 一次 collect，组内全部因子列一次算齐
    （``FactorEngine.fields``）；阶段 3（分别物化）：逐因子增量/全量写盘 +
    resolve 收口（``_factor_write``，与单因子路径同语义）。
    """
    plans = [_factor_plan(svc, svc._require_node("factor", n["name"]),
                          resync=resync)
             for n in svc.graph.list("factor")]
    reports = [_factor_skip_report(p) for p in plans if p["skip"]]
    active = [p for p in plans if not p["skip"]]
    if active:
        computed = _factor_batch_compute(svc, active)
        reports += [_factor_write(svc, p, computed[p["name"]]) for p in active]
    return reports


def _factor_scan_one(svc, name: str, *, resync: bool = False) -> dict:
    node = svc._require_node("factor", name)
    plan = _factor_plan(svc, node, resync=resync)
    if plan["skip"]:
        return _factor_skip_report(plan)
    if plan["incremental"]:
        (lo, hi), syms = plan["scope"]
        df = _factor_compute(svc, node, dt_range=(lo, hi), symbols=syms)
    else:
        df = _factor_compute(svc, node)
    return _factor_write(svc, plan, df)