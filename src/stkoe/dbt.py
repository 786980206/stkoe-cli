"""dbt manifest.json 元数据桥接：table/index add 时自动应用 dbt 模型的说明性元数据

- 配置键 ``dbt-manifest-file``（stkoe.json，``stkoe config set --dbt-manifest-file <路径>``；
  路径 expanduser，相对路径按当前工作目录解析）
- manifest 为 dbt 编译产物 ``target/manifest.json``：按 ``name``（回退 ``alias``）匹配
  ``nodes``/``sources`` 中的 model/source 节点
- **资产级**：``description`` + ``meta.display_name/source/tags``
- **列级**：``description`` + ``meta.display_name/unit/tags``
- **优先级**：add 参数显式指定 > manifest > 默认（合并发生在 GraphService 的
  ``table_add``/``index_add``，参数直接指定值覆盖 manifest 值）
"""
from __future__ import annotations

from pathlib import Path

from .jsonutil import loads

#: 列级/资产级 meta 里支持的自定义键（manifest 的 ``meta`` 对象）
_META_KEYS = ("display_name", "description", "unit", "tags", "source")


def manifest_path() -> Path | None:
    """配置的 dbt-manifest-file 路径（expanduser + 相对 cwd；未配置返回 None）"""
    from .settings import load_config

    v = load_config().dbt_manifest_file
    if not v:
        return None
    p = Path(str(v)).expanduser()
    return p if p.is_absolute() else Path.cwd() / p


def _index_nodes(path: Path) -> dict[str, dict]:
    """解析 manifest：按 name 索引全部 model/source 节点（仅解析 JSON，不校验 schema）"""
    try:
        data = loads(path.read_bytes())
    except (OSError, ValueError) as e:
        raise ValueError(f"dbt manifest 解析失败 {path}: {e}") from e
    if not isinstance(data, dict):
        raise ValueError(f"dbt manifest 格式错误（应为 JSON 对象）: {path}")
    out: dict[str, dict] = {}
    for group in ("nodes", "sources"):
        for _nid, nd in (data.get(group) or {}).items():
            if not isinstance(nd, dict):
                continue
            rt = nd.get("resource_type")
            if rt not in (None, "model", "source"):
                continue
            key = nd.get("name") or ""
            if key:
                out[key] = nd
    return out


def find_node(path: Path, name: str) -> dict | None:
    """按资产名匹配 manifest 节点：name 优先，回退 alias"""
    nodes = _index_nodes(path)
    if name in nodes:
        return nodes[name]
    for nd in nodes.values():
        if nd.get("alias") == name:
            return nd
    return None


def _pick_meta(node: dict, keys: tuple[str, ...]) -> dict:
    """提取 meta 自定义键（值非空才输出；tags 归一化为 list）"""
    m = node.get("meta") or {}
    out: dict = {}
    for k in keys:
        v = m.get(k)
        if v is None or v == "":
            continue
        if k == "tags":
            out[k] = [t.strip() for t in str(v).split(",") if t.strip()]
        else:
            out[k] = v
    return out


def asset_meta(node: dict) -> dict:
    """资产级元数据：description（节点级）+ meta.display_name/source/tags"""
    out: dict = {}
    if node.get("description"):
        out["description"] = str(node["description"])
    out.update(_pick_meta(node, ("display_name", "source", "tags")))
    return out


def column_meta(node: dict) -> dict[str, dict]:
    """列级元数据：{列名: {description, meta.display_name/unit/tags}}"""
    out: dict[str, dict] = {}
    for cname, c in (node.get("columns") or {}).items():
        if not isinstance(c, dict):
            continue
        col: dict = {}
        if c.get("description"):
            col["description"] = str(c["description"])
        col.update(_pick_meta(c, ("display_name", "unit", "tags")))
        if col:
            out[cname] = col
    return out


__all__ = ["manifest_path", "find_node", "asset_meta", "column_meta"]
