"""TABLE 模块：原始表资产的元数据管理与更新校验（add/get/del/set/meta/list/scan/col）

职责边界（性能第一）：
- 只保留核心两件事：
  1. 原始表元数据管理 + 更新校验：列目录 stat 指纹 → catalog 清单对账 → 布局/分区/列
     识别 → signature 单调版本。读路径只读 catalog，不读数据页。
  2. 数据更新后触发下游更新：scan 变更 bump 后沿 stkoe_depends 级联 dataset scan（重物化）
     与 stat scan（重算），一次扫描全链保鲜。
- 面向用户的统计（行数/字节/列分布等）不在表模块，全部在 stat 模块；catalog 内保留
  文件级列统计（stkoe_file_stats）作为查询裁剪基础设施（读路径性能）。
- 表数据文件是用户资产：任何操作不写/不删用户 parquet。
- 与 dataset/stat 共享同一套动词接口：add/get/del/set/meta/list/scan。
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

from . import catalog, get_root, ignore_cols
from .catalog import access
from .catalog.json import dumps, loads
from .catalog.spec import (
    ColumnMeta,
    FileMeta,
    TableLayout,
    TableMeta,
    TableScanReport,
    TaskHandle,
)
from .dbt import (
    DbtManifestError,
    DbtNodeNotFoundError,
    apply_table_meta,
    find_node,
    load_manifest,
)
from .query import prune_files, to_expr
from .task import TaskControl, conn_txn, defer
from .util import (
    columns_union,
    detect_layout,
    diff_files,
    disk_files,
    footer,
    now,
    partition_of,
    signature,
)


class TableNotFoundError(FileNotFoundError):
    pass


class TableExistsError(ValueError):
    pass


class DependencyError(ValueError):
    pass


def _root(name: str) -> Path:
    return get_root() / "tables" / name


def _object(conn, name: str):
    return access.get_object(conn, name, "table")


# ---------- 内部构造 / 转换 ----------

def _register(conn, name: str):
    """注册空表行（scan 隐式发现 / add 复用）"""
    meta = {
        "display_name": name,
        "description": "",
        "tags": [],
        "source": "local",
        "layout": TableLayout.SINGLE.value,
        "partition_by": [],
        "columns": [],
        "extra": {},
    }
    return access.insert_object(conn, "table", name, meta, signature([]), now())


def _to_meta(conn, obj) -> TableMeta:
    """catalog 行 → TableMeta（含文件清单；consistent 只读对账，不触发 scan）"""
    meta = loads(obj["meta"])
    rows = conn.execute(
        "SELECT rel_path, partition_path, size, mtime_ns FROM stkoe_data_files "
        "WHERE object_id=? ORDER BY rel_path", (obj["id"],)).fetchall()
    part_count = len({r["partition_path"] for r in rows if r["partition_path"]})
    return TableMeta(
        name=obj["name"],
        version=obj["version"],
        layout=TableLayout(meta.get("layout", "single")),
        display_name=meta.get("display_name", obj["name"]),
        description=meta.get("description", ""),
        tags=tuple(meta.get("tags", [])),
        source=meta.get("source", "local"),
        extra=meta.get("extra", {}),
        partition_by=tuple(meta.get("partition_by", [])),
        partition_count=part_count if rows else 0,
        files=tuple(FileMeta(rel_path=r["rel_path"], partition_path=r["partition_path"],
                             size=r["size"], mtime_ns=r["mtime_ns"]) for r in rows),
        columns=tuple(ColumnMeta.from_dict(c) for c in meta.get("columns", [])),
        consistent=bool(obj["signature"]) and obj["signature"] == signature(
            disk_files(_root(obj["name"]))) if _root(obj["name"]).exists() else True,
        created_at=obj["created_at"],
        updated_at=obj["updated_at"],
    )


def _with_conn(conn):
    """同步模式（conn=None）下取主连接"""
    return catalog().conn if conn is None else conn


# ---------- add ----------

def add(name: str, *, all: bool = False, background: bool | None = None,
        dbt_manifest: str | None = None
        ) -> TableScanReport | list[TableScanReport] | TaskHandle:
    """注册表：按目录内容生成登记 + 立即扫描同步。``all=True`` 批量发现注册。

    - 目录不存在 → 报错（add 是"发现资产"语义，不空注册）
    - 已注册 → TableExistsError（更新数据内容请用 scan）
    - ``dbt_manifest`` 非空 → 注册后合并 DBT manifest 同名模型元数据（表/列描述），
      不 bump version；与 ``all=True`` 互斥。
    """
    if all and dbt_manifest:
        raise ValueError("--dbt-manifest 与 --all 互斥（批量发现无同名语义）")
    def _run(conn, ctl):
        if all:
            root = get_root() / "tables"
            if not root.exists():
                return []
            out = []
            for d in sorted(x for x in root.iterdir() if x.is_dir()):
                if _object(_with_conn(conn), d.name) is None and any(d.rglob("*.parquet")):
                    out.append(_scan_impl(d.name, conn=conn))
            return out
        root = _root(name)
        if not root.exists():
            raise TableNotFoundError(f"table dir not found: {root}")
        with catalog().txn() if conn is None else conn_txn(conn):
            if _object(_with_conn(conn), name) is not None:
                raise TableExistsError(f"table already registered: {name} (use scan to refresh)")
        report = _scan_impl(name, conn=conn)
        if dbt_manifest:
            _apply_dbt(name, dbt_manifest)
        return report

    return defer("table_add", name, _run, background=background)


def _apply_dbt(name: str, manifest: str | None) -> tuple[str, int]:
    """合并 DBT manifest 同名模型元数据到表 meta（独立事务，不 bump version）。

    返回 (节点名, 命中列数)；找不到同名模型 / manifest 损坏抛对应错误。
    """
    with catalog().txn() as conn:
        obj = _object(conn, name)
        if obj is None:
            raise TableNotFoundError(f"table not registered: {name}")
        meta = loads(obj["meta"])
        return _apply_dbt_locked(conn, obj["id"], meta, name, manifest)


def _apply_dbt_locked(conn, obj_id: int, meta: dict, table_name: str,
                      manifest: str | None) -> tuple[dict, int]:
    """事务已持有时合并 dbt 元数据，返回 (新 meta, 应用列数) 并落库。"""
    try:
        node = find_node(load_manifest(manifest), table_name)
    except DbtManifestError as e:
        raise DbtManifestError(f"table {table_name}: {e}") from e
    if node is None:
        raise DbtNodeNotFoundError(f"table {table_name}: no matching model in dbt manifest "
                                   "(matched by alias/name)")
    meta, applied = apply_table_meta(meta, node, table_name)
    access.update_object_meta(conn, obj_id, meta, now_str=now())
    return meta, applied


# ---------- get / meta / list ----------

def get(name: str, *, columns: list[str] | None = None,
        where: pl.Expr | str | None = None,
        partition: str | list[str] | None = None,
        exclude_tool: bool = False, limit: int | None = None) -> pl.DataFrame:
    """读表数据：读前自动同步（快检）→ catalog 文件裁剪 → 读取（不读数据页）。

    返回 collect 后的 DataFrame；``limit`` 限制返回行数。
    """
    lf = get_lazy(name, columns=columns, where=where, partition=partition,
                  exclude_tool=exclude_tool)
    if limit is not None:
        lf = lf.limit(limit)
    return lf.collect()


def get_lazy(name: str, *, columns: list[str] | None = None,
             where: pl.Expr | str | None = None,
             partition: str | list[str] | None = None,
             exclude_tool: bool = False) -> pl.LazyFrame:
    """读表（lazy）：读前快检 → catalog 裁剪 → scan_parquet，不触发计算。

    ``exclude_tool=True`` 剔除 ignore_cols 工具字段。
    """
    _ensure_fresh(name)
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise TableNotFoundError(f"table not registered: {name}")
    files = prune_files(conn, obj["id"], partition, where)
    if not files:
        return pl.LazyFrame()
    lf = pl.scan_parquet([_root(name) / f["rel_path"] for f in files], hive_partitioning=True)
    if where is not None:
        lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
    if columns is not None:
        lf = lf.select(*columns)
    elif exclude_tool:
        tool = {*ignore_cols()}
        keep = [c.name for c in _to_meta(conn, obj).columns if c.name not in tool]
        lf = lf.select(*keep)
    return lf


def meta(name: str) -> TableMeta:
    """表元数据：版本/布局/分区/文件清单/列元数据（只读对账，不触发 scan）"""
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise TableNotFoundError(f"table not registered: {name}")
    return _to_meta(conn, obj)


def list() -> list[TableMeta]:
    """已注册表列表"""
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='table' "
                        "ORDER BY name").fetchall()
    return [_to_meta(conn, r) for r in rows]


def candidates() -> list[str]:
    """未登记但含 parquet 的表目录（「新建本地表」候选）"""
    root = get_root() / "tables"
    if not root.exists():
        return []
    conn = catalog().conn
    out = []
    for d in sorted(x for x in root.iterdir() if x.is_dir()):
        if _object(conn, d.name) is None and any(d.rglob("*.parquet")):
            out.append(d.name)
    return out


# ---------- set / col ----------

def set(name: str, *, display_name: str | None = None, description: str | None = None,
        tags: list[str] | None = None, new_name: str | None = None,
        background: bool | None = None, dbt_manifest: str | None = None,
        **extra) -> TableMeta | TaskHandle:
    """修改表级元数据（description/display_name/tags/extra/dbt）；``new_name`` 改变名。

    ``dbt_manifest`` 非空时先合并 DBT manifest 同名模型元数据（表/列描述），
    之后显式传参（--desc 等）优先于 dbt 结果。纯元数据修改不 bump version。
    """
    if new_name:
        return rename(name, new_name, background=background)
    def _run(conn, ctl):
        cx = _with_conn(conn)
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            meta = loads(obj["meta"])
            if dbt_manifest:
                meta, _ = _apply_dbt_locked(cx, obj["id"], meta, name, dbt_manifest)
            if display_name is not None:
                meta["display_name"] = display_name
            if description is not None:
                meta["description"] = description
            if tags is not None:
                meta["tags"] = tags
            meta.setdefault("extra", {}).update(extra)
            access.update_object_meta(cx, obj["id"], meta, now_str=now())
            obj = _object(cx, name)
        return _to_meta(cx, obj)
    return defer("table_set", name, _run, background=background)


def rename(old: str, new: str, *, background: bool | None = None) -> TableMeta | TaskHandle:
    """改名：目录 tables/old → tables/new，并同步 catalog / 依赖边 / dataset 引用。

    rel_path 相对表根，改名不改变签名/列统计 → 无需重 scan。
    """
    def _run(conn, ctl):
        cx = _with_conn(conn)
        src, dst = _root(old), _root(new)
        with conn_txn(cx):
            obj = _object(cx, old)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {old}")
            if _object(cx, new) is not None:
                raise TableExistsError(f"table already registered: {new}")
            if src.exists():
                if dst.exists():
                    raise FileExistsError(f"target dir already exists: {dst}")
                src.rename(dst)
            meta = loads(obj["meta"])
            if meta.get("display_name") in ("", old):
                meta["display_name"] = new
            access.update_object_meta(cx, obj["id"], meta, now_str=now())
            cx.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
        _rename_cascade(old, new)
        return _to_meta(cx, _object(cx, new))
    return defer("table_rename", f"{old}->{new}", _run, background=background)


def _rename_cascade(old: str, new: str) -> None:
    """表改名后同步依赖边与下游引用（dataset meta 里的表名/字段来源表）"""
    from .catalog.spec import DatasetMeta as _DS
    with catalog().txn() as conn:
        access.rename_dep(conn, "table", old, new)
        for d in access.dependents(conn, "table", new):
            obj = access.get_object(conn, d["obj_name"], d["obj_type"])
            if obj is None:
                continue
            meta = loads(obj["meta"])
            if meta.get("index_table") == old:
                meta["index_table"] = new
            meta["tables"] = [new if t == old else t for t in meta.get("tables", [])]
            for c in meta.get("columns", []):
                if c.get("source_table") == old:
                    c["source_table"] = new
            access.update_object_meta(conn, obj["id"], meta, now_str=now())


def col(name: str, column: str, *, display_name: str | None = None,
        description: str | None = None, unit: str | None = None,
        formula: str | None = None, tags: list[str] | None = None) -> TableMeta:
    """更新字段（列）元数据：display_name / description / unit / formula / tags。

    只改 catalog 列说明，不改数据文件。
    """
    with catalog().txn() as conn:
        obj = _object(conn, name)
        if obj is None:
            raise TableNotFoundError(f"table not registered: {name}")
        meta = loads(obj["meta"])
        cols = meta.get("columns", [])
        idx = next((i for i, c in enumerate(cols) if c["name"] == column), None)
        if idx is None:
            raise TableNotFoundError(f"column not found: {name}.{column}")
        c = dict(cols[idx])
        if display_name is not None:
            c["display_name"] = display_name
        if description is not None:
            c["description"] = description
        if unit is not None:
            c["unit"] = unit
        if formula is not None:
            c["formula"] = formula
        if tags is not None:
            c["tags"] = tags
        cols[idx] = c
        meta["columns"] = cols
        access.update_object_meta(conn, obj["id"], meta, now_str=now())
        obj = _object(conn, name)
    return _to_meta(catalog().conn, obj)


# ---------- del ----------

def del_(name: str, *, force: bool = False, background: bool | None = None) -> TaskHandle:
    """删除表注册。默认校验依赖：被 dataset/stat 引用时抛 DependencyError；
    ``force=True`` 级联删除依赖方（dataset 连同 stat/物化产物一并清理）。

    只删 catalog 登记（对象行/文件清单/列统计/依赖边），**绝不删除用户数据文件**
    （数据目录仍在 → 后续 scan/add 可重新发现）。
    """
    def _run(conn, ctl):
        cx = _with_conn(conn)
        with conn_txn(cx):
            obj = _object(cx, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            dependents = access.dependents(cx, "table", name)
            if dependents and not force:
                raise DependencyError("dependencies exist: " + ", ".join(
                    f"{d['obj_type']}:{d['obj_name']}" for d in dependents)
                    + " (use --force to cascade)")
            cx.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
        if force:
            _drop_dependents_cascade(name)
    return defer("table_del", name, _run, background=background)


def _drop_dependents_cascade(name: str) -> None:
    """force 删除表的下游资产（dataset→stat 一并级联清理）"""
    from . import dataset as dataset_mod
    with catalog().txn() as conn:
        ds = access.dependents(conn, "table", name)
    for d in ds:
        if d["obj_type"] == "dataset":
            dataset_mod.del_(d["obj_name"], force=True, background=False)


# ---------- scan 扫描更新 ----------

def scan(name: str | None = None, *, all: bool = False, resync: bool = False,
         cascade: bool = True, background: bool | None = None
         ) -> TableScanReport | list[TableScanReport] | TaskHandle:
    """扫描表并同步元数据（幂等：无差异不 bump version）；变更后自动触发下游更新。

    ``all=True`` 扫描根目录全部；``resync=True`` 强制全量重读 footer；
    ``cascade=False`` 关闭下游触发（REPL 默认后台执行返回 TaskHandle）。
    """
    if all:
        if name:
            raise ValueError("--all 与 name 互斥")
        def _all(conn, ctl):
            root = get_root() / "tables"
            if not root.exists():
                return []
            return [_scan_impl(d.name, conn=conn, resync=resync, cascade=cascade)
                    for d in sorted(root.iterdir()) if d.is_dir()]
        return defer("table_scan", "*", _all, background=background)
    return defer("table_scan", name,
                 lambda conn, ctl: _scan_impl(name, conn=conn, resync=resync, cascade=cascade),
                 background=background)


def scan_all(*, resync: bool = False, cascade: bool = True) -> list[TableScanReport]:
    """扫描全部已注册表"""
    return [_scan_impl(m.name, resync=resync, cascade=cascade) for m in list()]


def _scan_impl(name: str, *, resync: bool = False, cascade: bool = True,
               conn=None, ctl=None) -> TableScanReport:
    """核心：列目录 → 对账 → 有差异才扫 footer → 更新 catalog（幂等） → 触发下游"""
    root = _root(name)
    if not root.exists():
        raise TableNotFoundError(f"table dir not found: {root}")
    disk = disk_files(root)
    cx = _with_conn(conn)
    with catalog().txn() if conn is None else conn_txn(conn):
        obj = _object(cx, name)
        implicit = obj is None
        version_before = 0 if implicit else obj["version"]
        if implicit:
            obj = _register(cx, name)
        cat = access.get_data_files(cx, obj["id"])
        stats = access.get_stats(cx, obj["id"]) if cat else {}
        diffs = diff_files(disk, cat)
        layout, pkeys = detect_layout([f.rel_path for f in disk])
        changed = resync or bool(diffs)

        if not changed:
            part_set = {r["partition_path"] for r in cat.values()}
            partition_count = len(part_set) if disk else 0
            version_after = obj["version"]
        else:
            payload: list[tuple] = []
            for f in disk:
                old = cat.get(f.rel_path)
                if (old is not None and not resync and old["size"] == f.size
                        and old["mtime_ns"] == f.mtime_ns):
                    ftr = {"row_count": old["row_count"], "file_bytes": old["file_bytes"],
                           "schema": loads(old["schema"] or "{}"), "stats": stats.get(old["id"], {})}
                else:
                    ftr = footer(root / f.rel_path)
                payload.append((f, ftr, partition_of(f.rel_path)))

            items = [
                (part, f.rel_path, ftr["row_count"], ftr["file_bytes"], f.size, f.mtime_ns,
                 dumps(ftr["schema"]), ftr["stats"])
                for f, ftr, part in payload
            ]
            access.replace_data_files(cx, obj["id"], items)

            old_cols = {c["name"]: c for c in loads(obj["meta"]).get("columns", [])}
            new_cols = []
            for c in columns_union([(f.rel_path, ftr) for f, ftr, _ in payload],
                                   {*ignore_cols()}):
                prev = dict(old_cols.get(c.name, {}))
                prev.update({k: v for k, v in c.to_dict().items() if v is not None or k == "is_tool"})
                new_cols.append(prev)
            meta = loads(obj["meta"]) | {
                "layout": layout.value,
                "partition_by": pkeys,
                "columns": new_cols,
            }
            version_after = obj["version"] if implicit else version_before + 1
            access.update_object_meta(cx, obj["id"], meta, signature=signature(disk),
                                      now_str=now(), bump=not implicit)
            partition_count = len({p for _, _, p in payload}) if disk else 0

    triggered: tuple[str, ...] = ()
    if cascade and changed:
        triggered = tuple(_notify_downstream(name))
    return TableScanReport(
        name=name, version_before=version_before, version_after=version_after,
        layout=layout, partition_by=tuple(pkeys), partition_count=partition_count,
        diffs=tuple(diffs), changed=changed, implicit_registered=implicit,
        triggered=triggered)


def _notify_downstream(name: str) -> list[str]:
    """表变更后按 stkoe_depends 触发下游：dataset scan（重物化）→ stat scan（重算）"""
    from . import dataset as _dataset
    from . import stat as _stat
    out: list[str] = []
    conn = catalog().conn
    for d in access.dependents(conn, "table", name):
        try:
            if d["obj_type"] == "dataset":
                _dataset.scan(d["obj_name"])
            elif d["obj_type"] == "stat":
                _stat.scan(d["obj_name"])
            out.append(f"{d['obj_type']}:{d['obj_name']}")
        except Exception as e:
            out.append(f"{d['obj_type']}:{d['obj_name']}(err:{type(e).__name__})")
    return out


# ---------- 读前快检 / 数据键 ----------

def _ensure_fresh(name: str):
    """读前快检：stat 签名一致则继续；不一致自动 scan（未注册则隐式注册）"""
    root = _root(name)
    if not root.exists():
        return
    obj = _object(catalog().conn, name)
    disk_sig = signature(disk_files(root))
    if obj is None or disk_sig != (obj["signature"] or ""):
        # 快检路径的自动 sniff 不级联下游（避免读路径触发 dataset/stat 重算循环）
        _scan_impl(name, cascade=False)


def data_key(name: str) -> str:
    """当前数据标识（stat 缓存有效性判定）：目录磁盘签名（自动保鲜）"""
    _ensure_fresh(name)
    obj = _object(catalog().conn, name)
    return (obj["signature"] or "") if obj else ""


def field_graph(name: str) -> dict[str, list[tuple[str, str]]]:
    """字段血缘：{列: [(来源表, 来源字段)]}（dataset 列映射或表自身）"""
    m = meta(name)
    out = {}
    for c in m.columns:
        out[c.name] = [(c.source_table, c.source_field) if c.source_table else (name, c.name)]
    return out