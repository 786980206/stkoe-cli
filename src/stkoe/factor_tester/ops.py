"""tester（因子测试数据集）资产业务：登记（依赖 factor）/ 构造 / 物化 / 校验。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——factor 的元数据/
计算经 ``svc._factor_meta_dict``/``svc._factor_hash``/``svc._factor_compute``
（GraphService 薄委托），物化落盘走 ``graph.materialize`` 共享基础设施；
tester 是资产链末端（factor → sample → fieldset → panel → index），无下游。
"""
from __future__ import annotations

import hashlib

import polars as pl

from ..fieldset.ops import _expand_scope, _formula_refs
from ..graph.handlers import TesterHandler
from ..graph.materialize import partition_plan, rewrite_buckets, scan_materialized, \
    write_partitioned
from ..graph.model import node_id
from ..graph.version import now_iso
from ..jsonutil import dumps_str
from ..table.query import to_expr
from .spec import FactorTesterSpec
from .tester import prepare_factor_data


def _tester_spec(svc, node: dict) -> FactorTesterSpec:
    return FactorTesterSpec.from_dict(node.get("spec") or {})


def _tester_meta_dict(svc, name: str) -> dict:
    """V2.0 FactorTestMeta 形态 dict。"""
    node = svc._require_node("tester", name)
    meta = svc.graph._meta(node)
    extra = dict(meta.get("extra") or {})
    materialized = bool(node.get("materialized") or extra.get("materialized"))
    dep_hash = extra.get("dependency_hash") or ""
    return {
        "name": name,
        "version": node.get("version", 0),
        "factor": node.get("factor", "").split(":", 1)[1] if node.get("factor") else "",
        "sample": node.get("sample", "").split(":", 1)[1] if node.get("sample") else "",
        "returns": node.get("returns", "r"),
        "groupby": node.get("groupby", "ic"),
        "marketcap": node.get("marketcap", "fv"),
        "spec": _tester_spec(svc, node).to_dict(),
        "factor_col": node.get("factor_col", ""),
        "keys": list(node.get("keys") or ()),
        "materialized": materialized,
        "materialized_at": extra.get("materialized_at"),
        "curated": materialized and dep_hash == _tester_hash(svc, node),
        "columns": list(extra.get("columns") or ()),
        "extra": extra,
        "display_name": node.get("display_name") or name,
        "description": node.get("description", ""),
        "tags": list(node.get("tags") or ()),
        "source": node.get("source", "local"),
        "created_at": node.get("create_time", ""),
        "updated_at": node.get("update_time", ""),
    }


def _tester_hash(svc, node: dict) -> str:
    """物化一致性签名 = factor 的 hash + spec + 测试列名。"""
    factor = node.get("factor", "").split(":", 1)[1] if node.get("factor") else ""
    fnode = svc._require_node("factor", factor)
    parts = [
        f"factor:{factor}:{svc._factor_hash(fnode)}",
        f"returns:{node.get('returns', 'r')}",
        f"groupby:{node.get('groupby', 'ic')}",
        f"marketcap:{node.get('marketcap', 'fv')}",
        f"factor_col:{node.get('factor_col', '')}",
        f"spec:{dumps_str(_tester_spec(svc, node).to_dict())}",
    ]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _tester_build(svc, node: dict, *, dt_range: tuple[str, str] | None = None,
                  symbols: list[str] | None = None) -> pl.DataFrame:
    """测试数据集：sample 视图（含测试必需列）+ factor 列 → prepare_factor_data。

    ``dt_range=(lo, hi)`` / ``symbols`` 给出时只构造「时间区间 × 标的集合」
    内的行（增量物化用）。sample 视图只 collect 一次并做**列投影**（测试
    必需列 + keys + factor 公式引用列），再传给 ``svc._factor_compute`` 复用
    （宽表 panel 下避免全列物化）。
    """
    factor = node.get("factor", "").split(":", 1)[1]
    fnode = svc._require_node("factor", factor)
    fm = svc._factor_meta_dict(factor)
    sample = node.get("sample", "").split(":", 1)[1] if node.get("sample") else fm["sample"]
    view = svc._sample_view_lf(sample)
    keys = list(fm["keys"])
    if dt_range:
        dt = keys[-1]
        lo, hi = dt_range
        view = view.filter(pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi)))
    if symbols:
        view = view.filter(pl.col(keys[0]).is_in(symbols))
    returns = node.get("returns", "r")
    groupby = node.get("groupby", "ic")
    marketcap = node.get("marketcap", "fv")
    # 键列跟随 factor keys（index 的 symbol/datetime 列名可自定义）
    sym_col = keys[0] if keys else "sym"
    dt_col = keys[-1] if keys else "date"
    need = [dt_col, sym_col, returns, groupby, marketcap]
    view_cols = set(view.collect_schema().names())
    feat_name = fm.get("feature") or fnode.get("feature", "").split(":", 1)[1]
    formula = svc._require_node("feature", feat_name).get("formula") or ""
    proj = list(dict.fromkeys(
        [c for c in need if c in view_cols] + keys
        + _formula_refs(formula, view_cols)))
    view_df = view.select(*proj).collect()
    missing = [c for c in need if c not in view_df.columns]
    if missing:
        raise ValueError(f"sample 缺少测试必需列: {missing}（需要 date/sym 与 "
                         f"returns/groupby/marketcap）")
    fdf = svc._factor_compute(fnode, dt_range=dt_range, symbols=symbols,
                              view_df=view_df)
    base = (
        view_df.select(*[pl.col(c) for c in need])
        .with_columns(pl.lit(1, dtype=pl.Int32).alias("sample"))
        .join(fdf, on=keys, how="left")
        .rename({fm["factor_col"]: "factor", returns: "returns",
                 groupby: "group", marketcap: "marketcap"})
    )
    return prepare_factor_data(base, _tester_spec(svc, node), keys=keys)


def _tester_view_lf(svc, name: str, *, where=None) -> pl.LazyFrame:
    """读测试数据集（lazy，第 1/3 态）：已物化（curated）→ 读物化 parquet；
    未物化 → 报错提示先 update。"""
    node = svc._require_node("tester", name)
    tm = _tester_meta_dict(svc, name)
    root = svc._require_materialized("tester", name, tm)
    lf = scan_materialized(root)
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    return lf


def tester_data(svc, name: str) -> pl.DataFrame:
    """测试数据集 DataFrame（stat 测试器用，第 1/3 态）：已物化 → 读物化；
    未物化 → 报错提示先 update（不再实时构造回退）。"""
    node = svc._require_node("tester", name)
    tm = _tester_meta_dict(svc, name)
    root = svc._require_materialized("tester", name, tm)
    return scan_materialized(root).collect()


def tester_add(svc, name: str, factor: str, *, returns: str = "r",
               groupby: str = "ic", marketcap: str = "fv",
               factor_col: str | None = None, spec: dict | None = None,
               **kw) -> dict:
    """创建测试数据集：依赖已注册 factor，校验 sample 视图含必需列。"""
    if not factor:
        raise ValueError("tester add 需要 --factor <因子名>")
    fnode = svc._require_node("factor", factor)
    fm = svc._factor_meta_dict(factor)
    sample = fm["sample"]
    keys = list(fm["keys"])
    # 键列从 factor/sample keys 推断（不硬编码 date/sym）：index 的
    # symbol_col/datetime_col 可自定义，tester 必须跟随实际键列名
    sym_col = keys[0] if keys else "sym"
    dt_col = keys[-1] if keys else "date"
    # 校验 sample 视图含测试必需列
    view_cols = {c["name"] for c in svc._sample_view_cols(sample)}
    need = [dt_col, sym_col, returns, groupby, marketcap]
    missing = [c for c in need if c not in view_cols]
    if missing:
        raise ValueError(f"sample 缺少测试必需列 {missing}，不能创建测试数据集（需要 {need}）")
    spec_d = FactorTesterSpec.from_dict(spec or {}).to_dict() if spec else \
        {"quantiles": 5, "periods": [1, 5, 10],
         "date_range": ["2023-01-01", "2026-01-01"], "rolling_window": 252}
    keys = list(fm["keys"])
    # tester **不做列级血缘**（列节点/DERIVES/BELONGS_TO 均不建）：tester 是
    # 测试面板（keys/returns/group/marketcap/d{no}/factor_quantile 等派生字段
    # 对资产血缘无信息量），其资产级 DEPENDS → factor 已表达"因子数据来源"；
    # 字段 schema 在 tester meta（columns）里展示，不进入列节点图
    TesterHandler.add(
        svc.graph, name, factor,
        returns=returns, groupby=groupby, marketcap=marketcap,
        factor_col=factor_col or fm["factor_col"] or factor,
        spec=spec_d, sample=node_id("sample", sample),
        keys=keys, **kw)
    return _tester_meta_dict(svc, name)


def tester_get(svc, name: str, *, where=None, limit=None, offset=None,
               count_total: bool = False):
    lf = _tester_view_lf(svc, name, where=where)
    return svc._collect_page(lf, limit=limit, offset=offset, count_total=count_total)


def tester_meta(svc, name: str) -> dict:
    return _tester_meta_dict(svc, name)


def tester_list(svc) -> list:
    return [_tester_meta_dict(svc, n["name"]) for n in svc.graph.list("tester")]


def tester_set(svc, name: str, **kw) -> dict:
    svc._require_node("tester", name)
    return svc.graph.set("tester", name, **kw)


def tester_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("tester", name)
    svc.graph.delete("tester", name, force=force)
    import shutil
    shutil.rmtree(svc.data_dir / "factor_tester" / name, ignore_errors=True)
    return {"deleted": name}


def tester_check(svc, name: str) -> dict:
    """校验测试数据集：构造成功、含必需列、行数 > 0。"""
    node = svc._require_node("tester", name)
    keys = list(node.get("keys") or ())
    try:
        df = _tester_build(svc, node)
    except Exception as e:
        return {"tester": name, "ok": False, "rows": 0, "columns": list(keys),
                "message": f"测试数据集构造失败: {e}"}
    # 输出列 = 实际键列名（index symbol/datetime 可自定义）+ 固定测试列
    need = ([keys[-1], keys[0]] if keys else ["date", "sym"]) \
        + ["sample", "returns", "group", "marketcap",
           "factor", "factor_quantile"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        return {"tester": name, "ok": False, "rows": df.height,
                "columns": list(df.columns), "message": f"结果集缺少必需列: {missing}"}
    if df.height == 0:
        return {"tester": name, "ok": False, "rows": 0,
                "columns": list(df.columns), "message": "结果行数为 0"}
    return {"tester": name, "ok": True, "rows": df.height,
            "columns": list(df.columns), "message": f"有效（{df.height} 行）"}


def tester_update(svc, name: str | None = None, *, all: bool = False,
                  resync: bool = False) -> dict | list[dict]:
    """tester 更新：传导检查上游（factor 全链）就绪 → 物化 factor_tester/<name>/。

    幂等；update 成功后节点置 valid=True。
    """
    if all:
        return [_tester_scan_one(svc, n["name"], resync=resync)
                for n in svc.graph.list("tester")]
    if not name:
        raise ValueError("tester update 需要测试集名（或 --all）")
    svc.graph.assert_ready("tester", name)
    return _tester_scan_one(svc, name, resync=resync)


def _tester_scan_one(svc, name: str, *, resync: bool = False) -> dict:
    node = svc._require_node("tester", name)
    extra = dict(node.get("extra") or {})
    cur_hash = _tester_hash(svc, node)
    spec = _tester_spec(svc, node)
    version_before = node.get("version", 0)
    # 幂等仅当节点有效：上游变化置脏（valid=False）后 update 必须强制重建
    if not resync and node.get("valid") \
            and extra.get("dependency_hash") == cur_hash \
            and (node.get("materialized") or extra.get("materialized")):
        return {"name": name, "version_before": version_before,
                "version_after": version_before, "materialized": True,
                "changed": False, "rows": 0, "quantiles": spec.quantiles,
                "periods": list(spec.periods)}
    out_dir = svc.data_dir / "factor_tester" / name
    keys = list(svc._factor_meta_dict(
        node.get("factor", "").split(":", 1)[1])["keys"])
    dt = keys[-1] if keys else ""
    pkeys, gran = partition_plan(svc.store, node, dt_col=dt)
    out_path = out_dir / ("data.parquet" if not pkeys else "")
    # 增量物化：最近上游积累事件有明确 datetime 区间且已有物化 → 只重算该区间
    # （分区场景只替换受影响分区文件，flat 场景删区间+合并）
    scope = None if resync else svc._upstream_scope(node)
    # d{no} 为前向累计收益（t 时刻输出用到 t..t+no-1 的 returns）：输入在
    # [lo, hi] 变化 → 输出受影响 [hi-no+1, hi]，重算区间按最大 period 向后展开
    max_no = max(spec.periods, default=0)
    if scope and max_no > 1:
        (lo, hi), syms = scope
        scope = (_expand_scope([lo, hi], back=max_no - 1), syms)
    if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
        (lo, hi), syms = scope
        dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
        sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
        inc = _tester_build(svc, node, dt_range=(lo, hi), symbols=syms)
        if pkeys:
            old = pl.scan_parquet(out_dir, hive_partitioning=True)
            df = rewrite_buckets(old, inc, dt_expr, pkeys, out_dir, gran, dt,
                                 sym_expr=sym_expr,
                                 sort_cols=[dt, keys[0]] if dt else None)
        else:
            keep = pl.scan_parquet(out_path).filter(
                ~(dt_expr & sym_expr) if sym_expr is not None else ~dt_expr
            ).collect()
            df = pl.concat([keep, inc], how="vertical_relaxed"
                           ).unique(subset=keys, keep="last")
            if dt:
                df = df.sort([dt, keys[0]])  # 时间优先（先时间后标的）
            df.write_parquet(out_path)
    else:
        df = _tester_build(svc, node)
        if dt:
            df = df.sort([dt, keys[0]])  # 物化存储时间优先
        write_partitioned(df, out_dir, pkeys, gran=gran, dt_col=dt, clean=True)
    cols = [{"name": c, "display_name": c, "data_type": str(t)}
            for c, t in zip(df.columns, df.dtypes)]
    # 物化成功 → resolve 收口：铸版本并记录消费的合并事件（带 datetime_scope，
    # 供下游沿链增量）+ 出边 required_version 对齐 + valid/materialized
    m = svc.graph.resolve("tester", name, extra={
        "dependency_hash": cur_hash, "materialized_at": now_iso(),
        "columns": cols,
    })
    version_after = m["version"]
    return {"name": name, "version_before": version_before,
            "version_after": version_after, "materialized": True, "changed": True,
            "rows": df.height, "quantiles": spec.quantiles,
            "periods": list(spec.periods)}