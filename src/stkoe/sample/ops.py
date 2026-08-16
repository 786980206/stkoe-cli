"""sample 资产业务：登记（fieldset 视图 ∩ 筛选 index 键集合，无物化）/ 实时视图 / 校验。

业务实现全部迁自 graph/service.py 的 GraphService 同名方法——fieldset 视图经
``svc._fieldset_view_lf``（GraphService 薄委托）；``_sample_view_lf``/``_sample_view_cols``
是 factor/tester 沿链取 sample 视图的共享能力，GraphService 上有同名薄委托。
"""
from __future__ import annotations

import polars as pl

from ..graph.handlers import SampleHandler
from ..graph.model import FieldMeta, node_id
from ..panel.ops import _panel_columns
from ..storage import scan


def _sample_index_keys(svc, node: dict) -> tuple[str, str]:
    """sample 的筛选 index 键：symbol_col + datetime_col（缺省 sym/date）。"""
    idx = node.get("index", "").split(":", 1)[-1]
    inode = svc._require_node("index", idx)
    return (inode.get("symbol_col") or "sym",
            inode.get("datetime_col") or "date")


def _sample_view_lf(svc, name: str, *, where=None) -> pl.LazyFrame:
    """sample 视图：fieldset 视图 ∩ 指定 index 的键集合（semi join）。

    只保留 (symbol, datetime) 键存在于该 index 数据中的行；index 键列名与
    视图 keys 不同名时按位置映射（symbol → keys[0]，datetime → keys[-1]）。
    """
    node = svc._require_node("sample", name)
    fset = node.get("fieldset", "").split(":", 1)[-1]
    lf, keys = svc._fieldset_view_lf(fset, where=where)
    sym, dt = _sample_index_keys(svc, node)
    key_sym = sym if sym in keys else (keys[0] if keys else sym)
    key_dt = dt if dt in keys else (keys[-1] if len(keys) > 1 else dt)
    idx_lf = scan(
        svc._index_root(node.get("index", "").split(":", 1)[-1]), exclude=())
    idx_lf = idx_lf.select([sym, dt]).unique()
    if sym != key_sym or dt != key_dt:
        idx_lf = idx_lf.rename({sym: key_sym, dt: key_dt})
    return lf.join(idx_lf, on=[key_sym, key_dt], how="semi")


def _sample_keys(svc, node: dict) -> list[str]:
    """sample 的索引列 = 其 fieldset 底层 panel 的 keys。"""
    fset = node.get("fieldset", "").split(":", 1)[-1]
    fnode = svc._require_node("fieldset", fset)
    return svc._panel_keys(fnode.get("panel", "").split(":", 1)[-1])


def sample_add(svc, name: str, fieldset: str, index: str, **kw) -> dict:
    """sample add：列级血缘——视图列透传（sample 列 DERIVES → fieldset 列）
    + 筛选 index 键映射（sample keys DERIVES → index symbol/datetime 列）。"""
    col_maps = {"fieldset": {c: c for c in svc._fieldset_view_col_names(fieldset)}}
    idx_node = svc._require_node("index", index)
    sym = idx_node.get("symbol_col") or "sym"
    dt = idx_node.get("datetime_col") or "date"
    # 视图 keys（panel keys）与 index 键列按位置映射（symbol → keys[0]、datetime → keys[-1]）
    fnode = svc._require_node("fieldset", fieldset)
    keys = svc._panel_keys(fnode.get("panel", "").split(":", 1)[-1])
    key_map: dict[str, str] = {}
    if keys:
        key_map[keys[0]] = sym
    if len(keys) > 1:
        key_map[keys[-1]] = dt
    if key_map:
        col_maps["index"] = key_map
    return SampleHandler.add(svc.graph, name, fieldset, index,
                             column_maps=col_maps, **kw)


def sample_get(svc, name: str, *, columns=None, where=None, limit=None,
               offset=None, count_total: bool = False):
    lf = _sample_view_lf(svc, name, where=where)
    return svc._collect_page(lf, columns=columns, limit=limit, offset=offset,
                             count_total=count_total)


def sample_check(svc, name: str) -> dict:
    node = svc._require_node("sample", name)
    keys = _sample_keys(svc, node)
    try:
        lf = _sample_view_lf(svc, name)
        df = lf.collect()
    except Exception as e:
        return {"sample": name, "ok": False, "rows": 0, "columns": [], "message": str(e)}
    cols = set(df.columns)
    ok = all(k in cols for k in keys) and df.height > 0
    return {"sample": name, "ok": ok, "rows": df.height,
            "columns": list(df.columns),
            "message": "" if ok else "过滤后缺少索引列或行数为 0"}


def sample_meta(svc, name: str) -> dict:
    """sample 元数据（V2.0 形态 dict，§10）：含 keys/columns（完整列元数据）。"""
    node = svc._require_node("sample", name)
    return {
        "name": name,
        "version": node.get("version", 0),
        "fieldset": node.get("fieldset", "").split(":", 1)[-1]
        if node.get("fieldset") else "",
        "index": node.get("index", "").split(":", 1)[-1]
        if node.get("index") else "",
        "keys": _sample_keys(svc, node),
        "valid": bool(node.get("valid")),
        "materialized": False,  # sample 无物化，恒实时构造
        "columns": _sample_view_cols(svc, name),
        "display_name": node.get("display_name") or name,
        "description": node.get("description", ""),
        "tags": list(node.get("tags") or ()),
        "source": node.get("source", "local"),
        "created_at": node.get("create_time", ""),
        "updated_at": node.get("update_time", ""),
    }


def sample_list(svc) -> list:
    return [sample_meta(svc, n["name"]) for n in svc.graph.list("sample")]


def sample_set(svc, name: str, **kw) -> dict:
    svc._require_node("sample", name)
    # 定义键规范化：set --index/--fieldset 存 node_id 形态（与 add 一致）
    if "index" in kw:
        kw["index"] = node_id("index", kw["index"])
    if "fieldset" in kw:
        kw["fieldset"] = node_id("fieldset", kw["fieldset"])
    return svc.graph.set("sample", name, **kw)


def sample_update(svc, name: str) -> dict:
    """sample 更新：传导检查上游（fieldset 链 + 筛选 index）就绪 → 视图可构造 → 铸版本。

    sample 无物化；update = 确认上游就绪并铸版本（消费的积累事件入 version_list，
    无新事件不空 bump），出边水位对齐。
    """
    svc.graph.assert_ready("sample", name)
    _sample_view_lf(svc, name).select(pl.len()).collect()
    m = svc.graph.resolve("sample", name, mark_materialized=False)
    return {"name": name, "valid": True,
            "version": m["version"]}


def sample_delete(svc, name: str, *, force: bool = False) -> dict:
    svc._require_node("sample", name)
    svc.graph.delete("sample", name, force=force)
    return {"deleted": name}


def _sample_view_cols(svc, sample: str) -> list[dict]:
    """sample 视图列（**完整列元数据**，§10）：panel 列继承 ColumnMeta 全键，
    fieldset 衍生字段继承 FieldMeta，未知列回退 name+data_type。"""
    node = svc._require_node("sample", sample)
    lf = _sample_view_lf(svc, sample)
    schema = lf.collect_schema()
    fset = node.get("fieldset", "").split(":", 1)[-1]
    fnode = svc._require_node("fieldset", fset)
    panel = fnode.get("panel", "").split(":", 1)[-1]
    panel_cols = {c["name"]: c for c in
                  _panel_columns(svc, svc._require_node("panel", panel))}
    fs_fields = {f: FieldMeta.from_dict(fd)
                 for f, fd in (fnode.get("fields") or {}).items()}
    out = []
    for c in schema.names():
        if c in panel_cols:
            out.append(panel_cols[c])
        elif c in fs_fields:
            fm = fs_fields[c]
            out.append({
                "name": c, "display_name": fm.display_name or c,
                "description": fm.description, "data_type": str(schema[c]),
                "unit": fm.unit, "formula": fm.formula,
                "tags": list(fm.tags), "validated": fm.validated,
            })
        else:
            out.append({"name": c, "data_type": str(schema[c])})
    return out