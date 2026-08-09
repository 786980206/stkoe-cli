"""dbt manifest 导入：把 DBT 的 target/manifest.json 中同名模型（表）的元数据
（表描述/标签 + 列描述/列标签/DBT 类型）合并进物理表元数据。

定位 manifest 的规则（`--dbt-manifest` 参数）：
- 传目录（DBT 项目根）→ 自动找 ``<目录>/target/manifest.json``，再退 ``<目录>/manifest.json``；
- 传文件 → 直接使用；
- 不传（None/空串）→ 环境变量 ``STKOE_DBT_MANIFEST``，再退当前目录向上逐层找
  ``target/manifest.json``（适配"在 dbt 项目内运行"的场景）。
同名表匹配：model 的 ``alias`` 优先（DBT 中 alias 才是落库表名），其次 ``name``。
"""
from __future__ import annotations

import json
import os
from pathlib import Path


class DbtManifestError(ValueError):
    """manifest 文件缺失/损坏/不可定位"""


class DbtNodeNotFoundError(ValueError):
    """manifest 中找不到同名表节点"""


def resolve_manifest(path: str | Path | None = None, *, cwd: Path | None = None) -> Path:
    """参数 → manifest.json 绝对路径；找不到抛 DbtManifestError。"""
    if path is None or str(path) == "":
        env = os.environ.get("STKOE_DBT_MANIFEST")
        if env:
            p = Path(env)
        else:
            base = cwd or Path.cwd()
            for d in (base, *base.parents):
                cand = d / "target" / "manifest.json"
                if cand.exists():
                    return cand
            raise DbtManifestError(
                "no dbt manifest found: set STKOE_DBT_MANIFEST or run under a dbt project "
                "with target/manifest.json"
            )
    else:
        p = Path(path)
    if p.is_dir():
        for cand in (p / "target" / "manifest.json", p / "manifest.json"):
            if cand.exists():
                return cand
        raise DbtManifestError(f"no manifest.json under dbt project: {p}")
    if not p.is_file():
        raise DbtManifestError(f"dbt manifest not found: {p}")
    return p


def load_manifest(path: str | Path | None = None) -> dict:
    """读取并校验 manifest（要求含 nodes 字典）"""
    resolved = resolve_manifest(path)
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise DbtManifestError(f"failed to read dbt manifest {resolved}: {e}") from e
    if not isinstance(raw, dict) or not isinstance(raw.get("nodes"), dict):
        raise DbtManifestError(f"invalid dbt manifest (no nodes dict): {resolved}")
    return raw


def find_node(manifest: dict, table_name: str) -> dict | None:
    """按表名找模型节点：alias 优先，其次 name；都无返回 None。"""
    for node in manifest.get("nodes", {}).values():
        if not isinstance(node, dict) or node.get("resource_type") != "model":
            continue
        if node.get("alias") == table_name or node.get("name") == table_name:
            return node
    return None


def apply_table_meta(meta: dict, node: dict, table_name: str) -> tuple[dict, int]:
    """把 manifest 模型节点合并进表 meta（只写 catalog，不动数据文件）。

    规则：
    - 表级：description / tags 非空才覆盖；extra["dbt"] 记录来源（package/模型路径/
      materialized 等，供后续溯源）。
    - 列级：按列名匹配物理表已有列，应用非空 description / tags、以及 data_type
      （存 ``c["dbt_type"]``，不覆盖框架字段类型）；manifest 有但物理表没有的列跳过
      （等 scan 出现后再 apply 一次即可）。
    返回 (新 meta, 实际命中的列数)。
    """
    meta = dict(meta)
    desc = node.get("description")
    if desc:
        meta["description"] = desc
    tags = node.get("tags")
    if tags:
        meta["tags"] = list(tags)

    extra = dict(meta.get("extra", {}))
    cfg = node.get("config") or {}
    extra["dbt"] = {
        "table": table_name,
        "package": node.get("package_name"),
        "model": node.get("original_file_path"),
        "alias": node.get("alias"),
        "resource_type": node.get("resource_type"),
        "materialized": cfg.get("materialized"),
    }
    meta["extra"] = extra

    cols = [dict(c) for c in meta.get("columns", [])]
    by_name = {c["name"]: c for c in cols}
    applied = 0
    for col_name, nd in (node.get("columns") or {}).items():
        c = by_name.get(col_name)
        if c is None:
            continue
        if nd.get("description"):
            c["description"] = nd["description"]
        if nd.get("tags"):
            c["tags"] = list(nd["tags"])
        dt = nd.get("data_type")
        if dt:
            c["dbt_type"] = dt
        applied += 1
    meta["columns"] = cols
    return meta, applied