"""panel 资产业务：登记（index + 成员表 join 视图）/ 元数据 / 物化 / 读取 / 视图构建。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——图登记/依赖/版本走
``svc`` 暴露的图能力（``_require_node``/``graph``/``_read_lazy``/``_upstream_scope``
等），物化落盘走 ``graph.materialize`` 共享基础设施。函数第一参数 ``svc`` 为
GraphService 实例。

``_panel_lazy`` 是下游（fieldset/sample/factor/tester）沿链取 panel 视图的共享
能力：GraphService 上有同名薄委托（fieldset 等经 ``svc._panel_lazy`` 调用，便于
测试 monkeypatch 与统一入口）。
"""
from __future__ import annotations

import hashlib
import shutil

import polars as pl

from ..graph.handlers import PanelHandler
from ..graph.model import node_id
from ..graph.materialize import partition_plan, rewrite_buckets, scan_materialized, \
    write_partitioned
from ..graph.version import now_iso
from ..table.query import to_expr


def _asof_join(left: pl.LazyFrame, right: pl.LazyFrame, keys: list[str]) -> pl.LazyFrame:
    """asof join：等值键 ``keys[:-1]``（by）+ 时间键 ``keys[-1]``（on，backward 就近匹配）。

    on 键为 String 日期形态（如 "2024-01-01"）时 cast 成 Date 做 asof，
    结果列 cast 回 String 保持输出类型（panel 下游公式依赖字符串日期比较）。
    两侧已显式 ``sort(on)``，故 ``check_sortedness=False`` 跳过重复校验
    （by 分组场景 polars 无法校验，会触发 UserWarning）。
    """
    on = keys[-1]
    by = [k for k in keys if k != on]
    schema = left.collect_schema()
    is_str = on in schema and schema[on] == pl.String
    if is_str:
        l = left.with_columns(pl.col(on).str.to_date().alias(on)).sort(on)
        r = right.with_columns(pl.col(on).str.to_date().alias(on)).sort(on)
        out = l.join_asof(r, on=on, by=by, strategy="backward",
                          check_sortedness=False)
        return out.with_columns(pl.col(on).dt.strftime("%Y-%m-%d").alias(on))
    l = left.sort(on)
    r = right.sort(on)
    return l.join_asof(r, on=on, by=by, strategy="backward",
                       check_sortedness=False)


def _table_names(tables) -> list[str]:
    """归一化 panel add 的成员表名清单（与 PanelHandler.add 的解析一致）。"""
    if isinstance(tables, dict):
        return list(tables)
    out = []
    for item in tables or ():
        if isinstance(item, (list, tuple)) and len(item) >= 1:
            out.append(item[0])
        elif isinstance(item, str):
            out.append(item.partition(":")[0])
    return out


def panel_add(svc, name: str, index: str,
              tables: dict[str, str] | list | tuple | None = None, **kw) -> dict:
    """panel add：index 为已注册 Index 节点，tables 为已注册 table 节点。

    keys 由 index 推断（symbol_col + datetime_col，去空去重；兜底 sym/date），
    不再接受显式 ``--keys``（旧参数被忽略）。
    tables 支持 {表名: join}、[(表名, join)]、["表名:join" | "表名"] 混合；
    join 缺省 asof（可选 left），见 PanelHandler.add。
    列级血缘：panel 列 DERIVES → index/成员表列（DEPENDS 边 detail.columns）。
    """
    idx_node = svc._require_node("index", index)
    keys = [c for c in (idx_node.get("symbol_col"), idx_node.get("datetime_col"))
            if c]
    keys = list(dict.fromkeys(keys)) or ["sym", "date"]
    kw.pop("keys", None)  # 忽略旧 --keys 参数，以 index 推断为准
    col_maps = {"index": {c["name"]: c["name"]
                          for c in (idx_node.get("columns") or [])}}
    # 成员表列名冲突校验：与 index 同名成员列跳过（index 优先，既有语义）；
    # **成员表之间同名列报错**——不自动重命名、不静默覆盖（曾静默丢数据）
    member_src: dict[str, str] = {}
    for t in _table_names(tables):
        tnode = svc._require_node("table", t)
        for c in (tnode.get("columns") or []):
            if c["name"] in col_maps["index"]:
                continue
            if c["name"] in member_src and member_src[c["name"]] != t:
                raise ValueError(
                    f"成员表列名冲突: {c['name']} 同时存在于 "
                    f"{member_src[c['name']]} 与 {t}——panel 列名必须唯一"
                    f"（不自动改名；请修改列名或不要同时挂载这两个成员表）")
            member_src.setdefault(c["name"], t)
        col_maps[t] = {c["name"]: c["name"] for c in (tnode.get("columns") or [])
                       if c["name"] not in col_maps["index"]}
    return PanelHandler.add(svc.graph, name, index,
                            tables=tables, keys=keys, column_maps=col_maps, **kw)


def _panel_columns(svc, node: dict) -> list[dict]:
    """派生列清单：index 列优先（keys 标 as_index），member 表列同名跳过。

    列顺序 = index 列 + 成员表列（去重）；**列元数据经列节点图引用解析**
    （``svc._resolve_col_meta``）——完整元数据只在源头（table/index）定义点保存，
    下游不重复存储，改源头列说明全链自动反映。
    """
    names: list[str] = []
    index = node.get("index", "").split(":", 1)[1]
    idx = svc._require_node("index", index)
    for c in idx.get("columns") or []:
        names.append(c["name"])
    for t in (node.get("tables") or {}):
        tnode = svc._require_node("table", t)
        for c in tnode.get("columns") or []:
            if c["name"] not in names:
                names.append(c["name"])
    nid = node_id("panel", node.get("name", ""))
    cols = [svc._resolve_col_meta(nid, cname) for cname in names]
    return svc._norm_cols(cols)


def _panel_hash(svc, node: dict) -> str:
    """panel 物化签名 = 上游 index/table 版本 + tables(join) + keys。"""
    index = node.get("index", "").split(":", 1)[1]
    parts = [f"index:{index}:{svc._require_node('index', index).get('version', 0)}"]
    for t, j in (node.get("tables") or {}).items():
        parts.append(f"table:{t}:{svc._require_node('table', t).get('version', 0)}:{j}")
    parts.append(f"keys:{','.join(node.get('keys') or ())}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def panel_meta(svc, name: str) -> dict:
    node = svc._require_node("panel", name)
    meta = svc.graph._meta(node)
    extra = dict(meta.get("extra") or {})
    materialized = bool(node.get("materialized") or extra.get("materialized"))
    dep_hash = extra.get("dependency_hash") or ""
    meta["columns"] = _panel_columns(svc, node)
    meta["keys"] = list(node.get("keys") or ())
    meta["materialized"] = materialized
    meta["materialized_at"] = extra.get("materialized_at")
    meta["curated"] = materialized and dep_hash == _panel_hash(svc, node)
    keys = list(node.get("keys") or ())
    meta["partition_by"], meta["partition_gran"] = partition_plan(
        svc.store, node, dt_col=keys[-1] if keys else "")
    meta["extra"] = extra
    return meta


def panel_list(svc) -> list:
    return [panel_meta(svc, n["name"]) for n in svc.graph.list("panel")]


def panel_set(svc, name: str, **kw) -> dict:
    svc._require_node("panel", name)
    return svc.graph.set("panel", name, **kw)


def panel_update(svc, name: str) -> dict:
    """panel 更新：传导检查上游（index/成员表）就绪 → join 视图物化落盘
    ``panel/<name>/``（分区布局**镜像 index**：分区 index → hive 目录，flat → 单文件）
    + 铸版本（积累事件）+ 边水位对齐。

    增量：源头积累事件有明确 datetime 区间且已有物化 → 只重算该区间（分区场景只
    替换受影响分区文件，flat 场景删区间+合并）；首次 / 无区间 → 全量物化。
    """
    svc.graph.assert_ready("panel", name)
    node = svc._require_node("panel", name)
    out_dir = svc.data_dir / "panel" / name
    keys = list(node.get("keys") or ())
    dt = keys[-1] if keys else ""
    pkeys, gran = partition_plan(svc.store, node, dt_col=dt)
    out_path = out_dir / ("data.parquet" if not pkeys else "")
    scope = svc._upstream_scope(node)
    if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
        (lo, hi), syms = scope
        dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
        sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
        where = dt_expr & sym_expr if sym_expr is not None else dt_expr
        inc, _ = _panel_lazy(svc, name, where=where, live=True)
        df_inc = inc.collect()
        if pkeys:
            # 分区级增量：删区间涉及的桶并保留桶内区间外旧行 → 合并写回
            # （惰性过滤：受影响桶判定只读 key 列，keep 行级裁剪后 collect）
            old = pl.scan_parquet(out_dir, hive_partitioning=True)
            rewrite_buckets(old, df_inc, dt_expr, pkeys, out_dir, gran, dt,
                            sym_expr=sym_expr,
                            sort_cols=[dt, keys[0]] if dt else None)
        else:
            # flat：惰性过滤只读 keep 行（未命中标的/区间外的旧行）
            keep = pl.scan_parquet(out_path).filter(
                ~(dt_expr & sym_expr) if sym_expr is not None else ~dt_expr
            ).collect()
            df = pl.concat([keep, df_inc], how="vertical_relaxed"
                           ).unique(subset=keys, keep="last")
            if dt:
                df = df.sort([dt, keys[0]])  # 时间优先（先时间后标的）
            df.write_parquet(out_path)
        rows = df_inc.height
    else:
        joined, _ = _panel_lazy(svc, name, live=True)
        if dt:
            joined = joined.sort([dt, keys[0]])  # 物化存储时间优先
        df = joined.collect()  # 一次物化（rows 计数与分桶写盘共用，不重复 join）
        rows = df.height
        write_partitioned(df, out_dir, pkeys, gran=gran, dt_col=dt, clean=True)
    m = svc.graph.resolve("panel", name, extra={
        "dependency_hash": _panel_hash(svc, node),
        "materialized_at": now_iso(),
    })
    return {"name": name, "valid": True, "materialized": True, "rows": rows,
            "version": m["version"]}


def panel_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("panel", name)
    svc.graph.delete("panel", name, force=force)
    shutil.rmtree(svc.data_dir / "panel" / name, ignore_errors=True)
    return {"deleted": name}


def _panel_lazy(svc, name: str, where: pl.Expr | str | None = None,
                live: bool = False) -> tuple[pl.LazyFrame, list[str]]:
    """panel 视图（lazy）：**物化且 curated 读物化 parquet**（下游沿链取上游物化），
    否则实时 join（index 为左表，member 表按各自 join 类型 on keys）。

    ``live=True``：强制实时 join（panel 自身重建时不能用旧物化）。
    """
    node = svc._require_node("panel", name)
    keys = list(node.get("keys") or ())
    if not live:
        fm = panel_meta(svc, name)
        root = svc.data_dir / "panel" / name
        if fm["curated"] and (root / "data.parquet").exists() \
                or (fm["curated"] and any(root.glob("*=*"))):
            lf = scan_materialized(root)
            if where is not None:
                lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
            return lf, keys
    index = node.get("index", "").split(":", 1)[1]
    table_map = dict(node.get("tables") or {})  # {表名: join 类型}
    tables = list(table_map.keys())
    cols = _panel_columns(svc, node)
    by_src: dict[str, list[dict]] = {}
    for c in cols:
        by_src.setdefault(c["source_table"], []).append(c)

    def frame(t: str, asset_type: str) -> pl.LazyFrame:
        lf = svc._read_lazy(asset_type, t)
        used = {c["source_field"] for c in by_src.get(t, [])}
        exprs = [pl.col(c["source_field"]).alias(c["name"]) for c in by_src.get(t, [])]
        exprs += [pl.col(k).alias(k) for k in keys if k not in used]
        return lf.select(*exprs)

    frames = [frame(index, "index")]
    frames += [frame(t, "table") for t in tables]
    joined = frames[0]
    # where 只引用左表（index）列时下推到 join 前——增量物化按「时间×标的」
    # 裁剪时避免全表 join 只为取增量行（宽表 panel 收益明显）；引用右表列或
    # 字符串谓词保持 join 后过滤（语义不变）
    left_push = False
    if where is not None and isinstance(where, pl.Expr) \
            and set(where.meta.root_names()) <= set(frames[0].collect_schema().names()):
        joined = joined.filter(where)
        left_push = True
    for i, t in enumerate(tables):
        f = frames[i + 1]
        if (table_map.get(t) or "asof_join") == "left_join":
            joined = joined.join(f, on=keys, how="left")
        else:
            joined = _asof_join(joined, f, keys)
    joined = joined.select(*[c["name"] for c in cols])
    if where is not None and not left_push:
        joined = joined.filter(to_expr(where) if isinstance(where, str) else where)
    return joined, keys


def panel_get(svc, name: str, *, columns: list[str] | None = None,
              where: pl.Expr | str | None = None, partition=None,
              exclude_tool: bool = False, limit=None, offset=None,
              count_total: bool = False):
    """panel 读取（第 1/3 态）：**已物化（curated）→ 读物化 parquet；
    未物化 → 报错提示先 update**（不再静默回退实时 join）。"""
    meta = panel_meta(svc, name)
    root = svc._require_materialized("panel", name, meta)
    lf = scan_materialized(root)
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    return svc._collect_page(lf, columns=columns, exclude_tool=exclude_tool,
                             limit=limit, offset=offset, count_total=count_total)


def panel_lazy(svc, name: str, *, where=None) -> pl.LazyFrame:
    """panel 实时 join 视图（lazy，stat 等下游消费）。"""
    return _panel_lazy(svc, name, where)[0]