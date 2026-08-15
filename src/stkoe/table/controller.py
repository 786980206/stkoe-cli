"""TABLE 模块：原始表资产的元数据管理与读取（TableController）

职责边界（性能第一）：
- 原始表元数据管理：列目录 stat 指纹 → catalog 清单对账 → 布局/分区/列识别 →
  signature 单调版本。读路径只读 catalog + stat 快检，不读数据页。
- 表数据文件是用户资产：任何操作不写/不删用户 parquet。
- 动词接口：add/get/delete/list/meta（与 v1 的 table 功能接口对齐）。

TableController 为 async 控制器；内部 SQLite/parquet footer 等阻塞 IO 经
``asyncio.to_thread`` 执行，避免阻塞事件循环。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import polars as pl

from ..jsonutil import dumps_str, loads
from ..settings import load_config
from .catalog import (
    Catalog,
    get_data_files,
    get_object,
    get_stats,
    insert_object,
    meta_of,
    replace_data_files,
    update_object_meta,
)
from .query import prune_files, to_expr
from .spec import (
    ColumnMeta,
    FileMeta,
    TableLayout,
    TableMeta,
    TableScanReport,
)
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

DEFAULT_IGNORE_COLS = ("optime",)

#: ``table set`` 可编辑的标准元数据字段（其余任意键进 extra）
META_FIELDS = ("display_name", "description", "source", "type")

#: ``table col`` 可编辑的列元数据字段
COL_META_FIELDS = ("display_name", "description", "unit", "formula", "tags")


class TableNotFoundError(FileNotFoundError):
    pass


class TableExistsError(ValueError):
    pass


class TableController:
    """表控制面：注册/读取/删除/元数据/列表（async 方法，内部阻塞 IO 走线程）"""

    def __init__(self, data_dir: Path | str | None = None,
                 ignore_cols: tuple[str, ...] = DEFAULT_IGNORE_COLS):
        if data_dir is None:
            data_dir = load_config().data_dir
        self.data_dir = Path(data_dir).expanduser()
        self.root = self.data_dir / "table"
        self.catalog = Catalog(self.data_dir / "catalog.db")
        self.ignore_cols = set(ignore_cols)

    # ---------- 内部转换 ----------

    def _root(self, name: str) -> Path:
        return self.root / name

    def _object(self, conn, name: str):
        return get_object(conn, name, "table")

    def _register(self, conn, name: str):
        """注册空表行（隐式发现 / add 复用）"""
        meta = {
            "display_name": name,
            "description": "",
            "tags": [],
            "source": "local",
            "type": "",
            "layout": TableLayout.SINGLE.value,
            "partition_by": [],
            "columns": [],
            "extra": {},
        }
        return insert_object(conn, "table", name, meta, signature([]), now())

    def _to_meta(self, conn, obj) -> TableMeta:
        """catalog 行 → TableMeta（含文件清单；consistent 只读对账，不触发 scan）"""
        meta = meta_of(obj)
        rows = conn.execute(
            "SELECT rel_path, partition_path, size, mtime_ns FROM stkoe_data_files "
            "WHERE object_id=? ORDER BY rel_path", (obj["id"],)).fetchall()
        part_count = len({r["partition_path"] for r in rows if r["partition_path"]})
        return TableMeta(
            name=obj["name"],
            version=obj["version"],
            layout=TableLayout(meta.get("layout", "single")),
            type=meta.get("type", ""),
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
                disk_files(self._root(obj["name"]))) if self._root(obj["name"]).exists() else True,
            created_at=obj["created_at"],
            updated_at=obj["updated_at"],
        )

    # ---------- add ----------

    async def add(self, name: str, *, all: bool = False,
                  meta: dict | None = None) -> TableScanReport | list[TableScanReport]:
        """注册表：按目录内容生成登记 + 立即扫描同步。``all=True`` 批量发现注册。

        ``meta`` 注册时即写入的元数据（键语义与 ``set`` 一致：display_name/description/
        source/tags 为标准字段，其余任意键进 extra）；仅单表注册生效。

        - 目录不存在 → 报错（add 是"发现资产"语义，不空注册）
        - 已注册 → TableExistsError（更新数据内容请用 scan）
        """
        return await asyncio.to_thread(self._add_sync, name, all, meta)

    def _add_sync(self, name: str, all: bool, meta: dict | None) -> TableScanReport | list[TableScanReport]:
        if all:
            if not self.root.exists():
                return []
            out = []
            for d in sorted(x for x in self.root.iterdir() if x.is_dir()):
                if self._object(self.catalog.new_conn(), d.name) is None and any(d.rglob("*.parquet")):
                    out.append(self._scan_impl(d.name))
            return out
        if not name:
            raise ValueError("add 需要表名（或 --all 批量发现）")
        root = self._root(name)
        if not root.exists():
            raise TableNotFoundError(f"table dir not found: {root}")
        with self.catalog.txn() as conn:
            if self._object(conn, name) is not None:
                raise TableExistsError(f"table already registered: {name} (use scan to refresh)")
        return self._scan_impl(name, meta=meta)

    # ---------- set（元数据更新） ----------

    async def set(self, name: str, **kw) -> TableMeta:
        """更新表元数据：display_name/description/source/tags 为标准字段，其余任意键进 extra；
        版本递增，返回更新后的 TableMeta（表未注册报错，不做隐式注册）"""
        return await asyncio.to_thread(self._set_sync, name, kw)

    def _set_sync(self, name: str, kw: dict) -> TableMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            meta = self._apply_meta_fields(dict(meta_of(obj)), kw)
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._to_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    @staticmethod
    def _apply_meta_fields(meta: dict, kw: dict) -> dict:
        """规范化元数据键值并合并进 meta：display_name/description/source/tags 为标准
        字段（tags 逗号分隔），其余任意键进 extra（``set`` 与 ``add`` 共用）"""
        for key, value in kw.items():
            if key == "tags":
                meta["tags"] = [t.strip() for t in str(value).split(",") if t.strip()]
            elif key in META_FIELDS:
                meta[key] = str(value)
            else:
                extra = dict(meta.get("extra") or {})
                extra[key] = value
                meta["extra"] = extra
        return meta

    # ---------- get / meta / list ----------

    async def get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None,
                  partition: str | list[str] | None = None,
                  exclude_tool: bool = False,
                  limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        """读表数据：读前自动同步（快检）→ catalog 文件裁剪 → 读取。

        返回 collect 后的 DataFrame；``limit`` 限制返回行数、``offset`` 跳过起始行；
        ``count_total=True`` 返回 ``(df, total)``，total 为过滤后（未加 limit/offset）的总行数。
        """
        return await asyncio.to_thread(
            self._get_sync, name, columns=columns, where=where,
            partition=partition, exclude_tool=exclude_tool, limit=limit,
            offset=offset, count_total=count_total)

    def _get_sync(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None,
                  partition: str | list[str] | None = None,
                  exclude_tool: bool = False,
                  limit: int | None = None,
                  offset: int | None = None,
                  count_total: bool = False) -> pl.DataFrame | tuple[pl.DataFrame, int]:
        lf = self._get_lazy(name, columns=columns, where=where,
                            partition=partition, exclude_tool=exclude_tool)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def _get_lazy(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None,
                  partition: str | list[str] | None = None,
                  exclude_tool: bool = False) -> pl.LazyFrame:
        """读表（lazy）：读前快检 → catalog 裁剪 → scan_parquet，不触发计算。

        ``exclude_tool=True`` 剔除 ignore_cols 工具字段。
        """
        self._ensure_fresh(name)
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            files = prune_files(conn, obj["id"], partition, where)
            if not files:
                return pl.LazyFrame()
            paths = [self._root(name) / f["rel_path"] for f in files]
            lf = pl.scan_parquet(paths, hive_partitioning=True)
            if where is not None:
                lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
            if columns is not None:
                lf = lf.select(*columns)
            elif exclude_tool:
                keep = [c.name for c in self._to_meta(conn, obj).columns if c.name not in self.ignore_cols]
                lf = lf.select(*keep)
            return lf
        finally:
            conn.close()

    async def meta(self, name: str) -> TableMeta:
        """表元数据：版本/布局/分区/文件清单/列元数据（只读对账，不触发 scan）"""
        return await asyncio.to_thread(self._meta_sync, name)

    def _meta_sync(self, name: str) -> TableMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            return self._to_meta(conn, obj)
        finally:
            conn.close()

    async def list(self, *, candidate: bool = False) -> list[TableMeta] | list[str]:
        """表列表：``candidate=True`` 返回未登记但含 parquet 的表目录（「新建本地表」候选）"""
        return await asyncio.to_thread(self._list_sync, candidate)

    def _list_sync(self, candidate: bool = False) -> list[TableMeta] | list[str]:
        conn = self.catalog.new_conn()
        try:
            if candidate:
                if not self.root.exists():
                    return []
                out = []
                for d in sorted(x for x in self.root.iterdir() if x.is_dir()):
                    if self._object(conn, d.name) is None and any(d.rglob("*.parquet")):
                        out.append(d.name)
                return out
            rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='table' "
                                "ORDER BY name").fetchall()
            return [self._to_meta(conn, r) for r in rows]
        finally:
            conn.close()

    # ---------- col（列元数据） ----------

    async def col(self, name: str, column: str, **kw) -> TableMeta:
        """更新列元数据：display_name/description/unit/formula/tags（tags 逗号分隔），
        版本递增，返回更新后的 TableMeta。只改 catalog 列说明，不改数据文件；
        表未注册 / 列不存在报错，不做隐式注册"""
        return await asyncio.to_thread(self._col_sync, name, column, kw)

    def _col_sync(self, name: str, column: str, kw: dict) -> TableMeta:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            meta = dict(meta_of(obj))
            cols = list(meta.get("columns", []))
            idx = next((i for i, c in enumerate(cols) if c.get("name") == column), None)
            if idx is None:
                raise TableNotFoundError(f"column not found: {name}.{column}")
            c = dict(cols[idx])
            for key, value in kw.items():
                if key not in COL_META_FIELDS:
                    raise ValueError(f"未知列元数据字段: {key}")
                if key == "tags":
                    c[key] = [t.strip() for t in str(value).split(",") if t.strip()]
                else:
                    c[key] = str(value)
            cols[idx] = c
            meta["columns"] = cols
            update_object_meta(conn, obj["id"], meta, now_str=now(), bump=True)
            conn.commit()
            return self._to_meta(conn, self._object(conn, name))
        finally:
            conn.close()

    # ---------- delete ----------

    async def delete(self, name: str, *, force: bool = False) -> dict:
        """删除表注册。默认校验依赖：被下游引用时抛 DependencyError；
        ``force=True`` 级联删除依赖方。

        只删 catalog 登记（对象行/文件清单/列统计），**绝不删除用户数据文件**
        （数据目录仍在 → 后续 add 可重新发现）。
        """
        return await asyncio.to_thread(self._delete_sync, name, force)

    def _delete_sync(self, name: str, force: bool) -> dict:
        conn = self.catalog.new_conn()
        try:
            obj = self._object(conn, name)
            if obj is None:
                raise TableNotFoundError(f"table not registered: {name}")
            dependents = conn.execute(
                "SELECT obj_type, obj_name FROM stkoe_depends WHERE dep_type='table' "
                "AND dep_name=?", (name,)).fetchall() if self._has_table("stkoe_depends", conn) else []
            if dependents and not force:
                raise DependencyError(dependents)
            conn.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
            conn.commit()
        finally:
            conn.close()
        return {"deleted": name}

    @staticmethod
    def _has_table(name: str, conn) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

    # ---------- 读前快检 ----------

    def _ensure_fresh(self, name: str):
        """读前快检：stat 签名一致则继续；不一致自动 scan（未注册则隐式注册）"""
        root = self._root(name)
        if not root.exists():
            return
        obj = self._object(self.catalog.new_conn(), name)
        disk_sig = signature(disk_files(root))
        if obj is None or disk_sig != (obj["signature"] or ""):
            self._scan_impl(name, cascade=False)

    def data_key(self, name: str) -> str:
        """当前数据标识：读前快检后返回 catalog 签名（未登记则 ''）"""
        self._ensure_fresh(name)
        root = self._root(name)
        if not root.exists():
            return ""
        obj = self._object(self.catalog.new_conn(), name)
        return obj["signature"] if obj is not None else signature(disk_files(root))

    # ---------- scan 扫描更新 ----------

    async def scan(self, name: str, *, all: bool = False,
                   resync: bool = False) -> TableScanReport | list[TableScanReport]:
        """扫描更新表登记（幂等）：无差异不 bump version。``all=True`` 批量重扫全部已注册表"""
        if all:
            return await asyncio.to_thread(self._scan_all_sync, resync)
        return await asyncio.to_thread(self._scan_impl, name)

    def _scan_all_sync(self, resync: bool = False) -> list[TableScanReport]:
        conn = self.catalog.new_conn()
        try:
            rows = conn.execute("SELECT name FROM stkoe_objects WHERE type='table' "
                                "ORDER BY name").fetchall()
        finally:
            conn.close()
        return [self._scan_impl(r["name"]) for r in rows]

    def _scan_impl(self, name: str, *, cascade: bool = True,
                   meta: dict | None = None) -> TableScanReport:
        """核心：列目录 → 对账 → 有差异才扫 footer → 更新 catalog（幂等）

        ``meta`` 仅首次（隐式注册）生效：add 时写入的初始元数据，语义与 ``set`` 一致。
        """
        root = self._root(name)
        if not root.exists():
            raise TableNotFoundError(f"table dir not found: {root}")
        disk = disk_files(root)
        with self.catalog.txn() as conn:
            obj = self._object(conn, name)
            implicit = obj is None
            version_before = 0 if implicit else obj["version"]
            if implicit:
                obj = self._register(conn, name)
            cat = get_data_files(conn, obj["id"])
            stats = get_stats(conn, obj["id"]) if cat else {}
            diffs = diff_files(disk, cat)
            layout, pkeys = detect_layout([f.rel_path for f in disk])
            changed = bool(diffs)

            if not changed:
                part_set = {r["partition_path"] for r in cat.values()}
                partition_count = len(part_set) if disk else 0
                version_after = obj["version"]
            else:
                payload: list[tuple] = []
                for f in disk:
                    old = cat.get(f.rel_path)
                    if old is not None and old["size"] == f.size and old["mtime_ns"] == f.mtime_ns:
                        ftr = {"row_count": old["row_count"], "file_bytes": old["file_bytes"],
                               "schema": loads(old["schema"] or "{}"),
                               "stats": stats.get(old["id"], {})}
                    else:
                        ftr = footer(root / f.rel_path)
                    payload.append((f, ftr, partition_of(f.rel_path)))

                items = [
                    (part, f.rel_path, ftr["row_count"], ftr["file_bytes"], f.size, f.mtime_ns,
                     dumps_str(ftr["schema"]), ftr["stats"])
                    for f, ftr, part in payload
                ]
                replace_data_files(conn, obj["id"], items)

                old_cols = {c["name"]: c for c in meta_of(obj).get("columns", [])}
                new_cols = []
                for c in columns_union([(f.rel_path, ftr) for f, ftr, _ in payload], self.ignore_cols):
                    prev = dict(old_cols.get(c.name, {}))
                    prev.update({k: v for k, v in c.to_dict().items() if v is not None or k == "is_tool"})
                    new_cols.append(prev)
                base = self._apply_meta_fields(dict(meta_of(obj)), meta or {}) if implicit else {}
                meta_out = dict(meta_of(obj)) | base | {
                    "layout": layout.value,
                    "partition_by": pkeys,
                    "columns": new_cols,
                }
                version_after = obj["version"] if implicit else version_before + 1
                update_object_meta(conn, obj["id"], meta_out, signature=signature(disk),
                                   now_str=now(), bump=not implicit)
                partition_count = len({p for _, _, p in payload}) if disk else 0

        return TableScanReport(
            name=name, version_before=version_before, version_after=version_after,
            layout=layout, partition_by=tuple(pkeys), partition_count=partition_count,
            diffs=tuple(diffs), changed=changed, implicit_registered=implicit)


class DependencyError(ValueError):
    """删除/重命名被依赖方时存在下游引用；``dependents`` 为结构化依赖列表"""

    def __init__(self, dependents: list, action: str = "delete"):
        self.dependents = [dict(d) for d in dependents]
        self.action = action
        msg = "dependencies exist: " + ", ".join(
            f"{d['obj_type']}:{d['obj_name']}" for d in self.dependents)
        super().__init__(msg + f" (use --force to {action})")


__all__ = ["TableController", "TableNotFoundError", "TableExistsError",
           "DependencyError", "DEFAULT_IGNORE_COLS"]
