"""table 模块：只读观察者 + 元数据同步（sniff）

用户用外部工具更新数据，框架只负责识别资产形态、同步 catalog 元数据与统计、
按 catalog 清单读取。框架绝不写/删用户数据文件。

通用能力（文件指纹/footer/布局/差异、Task 模型、谓词裁剪、catalog 行访问）
见 util / task / query / catalog.access。
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from . import catalog, get_root, ignore_cols
from .catalog import access
from .catalog.json import dumps, loads
from .catalog.spec import (
    ColumnMeta,
    SniffReport,
    TableLayout,
    TableMeta,
    TableStatus,
    TaskHandle,
)
from .query import prune_files, to_expr
from .task import conn_txn, run_task
from .util import (
    columns_union,
    detect_layout,
    diff_files,
    disk_files,
    footer,
    iter_parquets,
    now,
    partition_of,
    signature,
)


class TableNotFoundError(FileNotFoundError):
    pass


def _root(name: str) -> Path:
    return get_root() / "tables" / name


def _register(conn, name: str):
    """注册空表行（sniff 隐式发现 / create 复用）"""
    meta = {
        "display_name": name,
        "description": "",
        "tags": [],
        "source": "local",
        "as_index": False,
        "layout": TableLayout.SINGLE.value,
        "partition_by": [],
        "columns": [],
        "extra": {},
    }
    return access.insert_object(conn, "table", name, meta, signature([]), now())


def _describe_row(conn, obj) -> TableMeta:
    meta = loads(obj["meta"])
    agg = conn.execute(
        "SELECT COUNT(*) AS file_count, COALESCE(SUM(row_count),0) AS row_count, "
        "COALESCE(SUM(file_bytes),0) AS bytes, "
        "COUNT(DISTINCT partition_path) AS partition_count "
        "FROM stkoe_data_files WHERE object_id=?",
        (obj["id"],),
    ).fetchone()
    return TableMeta(
        name=obj["name"],
        version=obj["version"],
        layout=TableLayout(meta.get("layout", "single")),
        partition_by=tuple(meta.get("partition_by", [])),
        partition_count=agg["partition_count"] or 0,
        columns=tuple(ColumnMeta.from_dict(c) for c in meta.get("columns", [])),
        row_count=agg["row_count"] or None,
        file_count=agg["file_count"] or 0,
        bytes=agg["bytes"] or 0,
        as_index=meta.get("as_index", False),
        has_data=agg["file_count"] > 0,
        display_name=meta.get("display_name", obj["name"]),
        description=meta.get("description", ""),
        tags=tuple(meta.get("tags", [])),
        source=meta.get("source", "local"),
        extra=meta.get("extra", {}),
        created_at=obj["created_at"],
        updated_at=obj["updated_at"],
    )


# ---------- Query ----------

def list() -> list[TableMeta]:
    """列出全部已注册表"""
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='table' ORDER BY name").fetchall()
    return [_describe_row(conn, r) for r in rows]


def describe(name: str) -> TableMeta:
    """表元数据（读前自动快检）"""
    _ensure_fresh(name)
    obj = access.get_object(catalog().conn, name, "table")
    if obj is None:
        raise TableNotFoundError(f"table not registered: {name}")
    return _describe_row(catalog().conn, obj)


def status(name: str) -> TableStatus:
    """只读对账：catalog 清单 vs 磁盘差异，不修改状态"""
    disk = disk_files(_root(name))
    conn = catalog().conn
    obj = access.get_object(conn, name, "table")
    if obj is None:
        return TableStatus(name=name, registered=False, consistent=False,
                           signature_catalog=None, signature_disk=signature(disk))
    cat = access.get_data_files(conn, obj["id"])
    diffs = diff_files(disk, cat)
    sig_disk = signature(disk)
    return TableStatus(name=name, registered=True, consistent=obj["signature"] == sig_disk and not diffs,
                       signature_catalog=obj["signature"], signature_disk=sig_disk, diffs=tuple(diffs))


def schema(name: str) -> pl.Schema:
    """仅 schema（读 footer 元数据，不读数据）"""
    _ensure_fresh(name)
    files = iter_parquets(_root(name))
    if not files:
        return pl.Schema({})
    return pl.scan_parquet(files, hive_partitioning=True).collect_schema()


def partitions(name: str) -> list[str]:
    """列出分区路径（HIVE）"""
    obj = access.get_object(catalog().conn, name, "table")
    if obj is None:
        raise TableNotFoundError(f"table not registered: {name}")
    rows = catalog().conn.execute(
        "SELECT DISTINCT partition_path FROM stkoe_data_files "
        "WHERE object_id=? AND partition_path!='' ORDER BY partition_path",
        (obj["id"],),
    ).fetchall()
    return [r["partition_path"] for r in rows]


def select(name: str, *, columns: list[str] | None = None,
           where: pl.Expr | str | None = None,
           partition: str | list[str] | None = None,
           exclude_tool: bool = False) -> pl.LazyFrame:
    """读前自动快检 → catalog 裁剪 → scan_parquet lazy（不触发计算）

    ``exclude_tool=True`` 时剔除配置声明的工具字段（ignore_cols）。
    """
    _ensure_fresh(name)
    conn = catalog().conn
    obj = access.get_object(conn, name, "table")
    if obj is None:
        raise TableNotFoundError(f"table not registered: {name}")
    files = prune_files(conn, obj["id"], partition, where)
    if not files:
        return pl.LazyFrame()
    lf = pl.scan_parquet([_root(name) / f["rel_path"] for f in files], hive_partitioning=True)
    if where is not None:
        lf = lf.filter(to_expr(where))
    if columns is not None:
        lf = lf.select(*columns)
    elif exclude_tool:
        tool = set(ignore_cols())
        keep = [c.name for c in _describe_row(conn, obj).columns if c.name not in tool]
        lf = lf.select(*keep)
    return lf


# ---------- Task ----------

def sniff(name: str, *, resync: bool = False) -> SniffReport:
    """同步表元数据与统计：列目录 → 比对 → 有差异才读 footer 并 bump version（幂等）"""
    root = _root(name)
    if not root.exists():
        raise TableNotFoundError(f"table dir not found: {root}")
    disk = disk_files(root)
    with catalog().txn() as conn:
        obj = access.get_object(conn, name, "table")
        implicit = obj is None
        version_before = 0 if implicit else obj["version"]
        if implicit:
            obj = _register(conn, name)
        cat = access.get_data_files(conn, obj["id"])
        stats = access.get_stats(conn, obj["id"]) if cat else {}
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
            access.replace_data_files(conn, obj["id"], items)

            meta = loads(obj["meta"]) | {
                "layout": layout.value,
                "partition_by": pkeys,
                "columns": [c.to_dict() for c in
                            columns_union([(f.rel_path, ftr) for f, ftr, _ in payload], set(ignore_cols()))],
            }
            # 隐式注册：INSERT 已置 version=1，不再 bump；已注册表同步后 +1
            version_after = obj["version"] if implicit else version_before + 1
            access.update_object_meta(conn, obj["id"], meta, signature=signature(disk),
                                      now_str=now(), bump=not implicit)
            partition_count = len({p for _, _, p in payload}) if disk else 0

    return SniffReport(name=name, version_before=version_before, version_after=version_after,
                       layout=layout, partition_by=tuple(pkeys), partition_count=partition_count,
                       diffs=tuple(diffs), changed=changed, implicit_registered=implicit)


def sniff_all() -> list[SniffReport]:
    """扫描 tables/ 根目录，同步并注册所有表"""
    root = get_root() / "tables"
    if not root.exists():
        return []
    return [sniff(d.name) for d in sorted(root.iterdir()) if d.is_dir()]


def create(name: str, *, meta: dict | None = None) -> TaskHandle:
    """注册表（仅 catalog；数据目录由用户自建或 sniff 隐式发现）"""
    _root(name).mkdir(parents=True, exist_ok=True)

    def _run(conn, ctl):
        with conn_txn(conn):
            if access.get_object(conn, name, "table") is None:
                obj = _register(conn, name)
                cur = loads(obj["meta"])
                if meta:
                    cur["extra"] = meta
                access.update_object_meta(conn, obj["id"], cur)

    return run_task("table_create", name, _run)


def create_all() -> list[SniffReport]:
    """注册并同步 tables/ 下所有未注册且有 parquet 数据的目录（已注册/空目录跳过）"""
    root = get_root() / "tables"
    if not root.exists():
        return []
    registered = {r["name"] for r in catalog().conn.execute(
        "SELECT name FROM stkoe_objects WHERE type='table'").fetchall()}
    reports = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and d.name not in registered and iter_parquets(d):
            reports.append(sniff(d.name))
    return reports


def drop(name: str, *, with_data: bool = False) -> TaskHandle:
    """删注册 + 框架登记的元数据/统计；绝不删除用户数据文件（with_data 仅为 API 对称）"""

    def _run(conn, ctl):
        with conn_txn(conn):
            obj = access.get_object(conn, name, "table")
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))

    return run_task("table_drop", name, _run)


def rename(old: str, new: str) -> TaskHandle:
    """改名：目录 tables/old → tables/new，并同步 catalog 元数据

    数据文件 rel_path 相对表根目录，改名不改变 rel_path/签名/列统计，
    故无需重 sniff；仅同步 display_name（未自定义时跟随新名）与 updated_at。
    """
    src, dst = _root(old), _root(new)

    def _run(conn, ctl):
        with conn_txn(conn):
            obj = access.get_object(conn, old, "table")
            if obj is None:
                raise TableNotFoundError(f"table not registered: {old}")
            if access.get_object(conn, new, "table") is not None:
                raise ValueError(f"table already registered: {new}")
            if not src.exists():
                raise FileNotFoundError(f"table dir not found: {src}")
            if dst.exists():
                raise FileExistsError(f"target dir already exists: {dst}")
            src.rename(dst)
            meta = loads(obj["meta"])
            if meta.get("display_name") in ("", old):
                meta["display_name"] = new
            access.update_object_meta(conn, obj["id"], meta, now_str=now())
            conn.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))

    return run_task("table_rename", f"{old}->{new}", _run)


def update(name: str, *, display_name: str | None = None, description: str | None = None,
           tags: list[str] | None = None, bump: bool = False, **extra) -> TableMeta:
    """更新描述性元数据（不改数据文件；默认不 bump version）"""
    with catalog().txn() as conn:
        obj = access.get_object(conn, name, "table")
        if obj is None:
            raise TableNotFoundError(f"table not registered: {name}")
        meta = loads(obj["meta"])
        if display_name is not None:
            meta["display_name"] = display_name
        if description is not None:
            meta["description"] = description
        if tags is not None:
            meta["tags"] = tags
        meta["extra"].update(extra)
        access.update_object_meta(conn, obj["id"], meta, now_str=now(), bump=bump)
        obj = access.get_object(conn, name, "table")
    return _describe_row(catalog().conn, obj)


# ---------- 快检（M2.5） ----------

def _ensure_fresh(name: str):
    """读前快检：stat 签名一致则继续；不一致自动 sniff（隐式注册）"""
    root = _root(name)
    if not root.exists():
        return
    obj = access.get_object(catalog().conn, name, "table")
    disk_sig = signature(disk_files(root))
    if obj is None or disk_sig != (obj["signature"] or ""):
        sniff(name)
