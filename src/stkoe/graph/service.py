"""GraphService：V3.0 资产统一服务（登记/依赖/版本走 graph，物理数据走 graph.db 指纹 + polars）。

替代 V2.0 table/dataset 等 controller 的 SQLite catalog 登记层：
- **登记/元数据/依赖/版本** → graph 节点/边（graphqlite，graph.db）
- **物理指纹**（stkoe_data_files / stkoe_file_stats）→ graph.db 普通表（同文件同事务）
- **物理数据**（parquet 扫描/读取/prune）→ 复用 table/util.py / table/query.py 纯函数

assets：``table`` / ``index``（独立主体）／``panel``（原 dataset）。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import polars as pl

from ..jsonutil import dumps_str, loads
from ..settings import load_config
from ..table import util as T
from ..table.query import prune_files, to_expr
from ..table.controller import (
    DEFAULT_IGNORE_COLS,
    DependencyError,
    TableExistsError,
    TableNotFoundError,
)
from .controller import GraphController
from .events import DataChangeEvent
from .handlers import IndexHandler, PanelHandler, TableHandler
from .model import node_id
from .store import GraphStore


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class GraphService:
    """统一资产服务：table / index / panel。"""

    def __init__(self, data_dir: Path | str | None = None):
        if data_dir is None:
            data_dir = load_config().data_dir
        self.data_dir = Path(data_dir).expanduser()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = GraphStore(str(self.data_dir / "graph.db"))
        self.graph = GraphController(self.store)
        self.tables_root = self.data_dir / "tables"
        self.ignore_cols = set(DEFAULT_IGNORE_COLS)

    def close(self) -> None:
        self.store.close()

    # =====================================================================
    # 物理共用（table / index 相同：parquet 目录扫描 + 指纹 + 节点）
    # =====================================================================

    def _root(self, name: str) -> Path:
        return self.tables_root / name

    def _require_node(self, asset_type: str, name: str) -> dict:
        node = self.store.get_node(node_id(asset_type, name))
        if node is None:
            raise TableNotFoundError(f"{asset_type} not registered: {name}")
        return node

    @staticmethod
    def _norm_col_kw(kw: dict) -> dict:
        """列元数据参数规范化：tags 逗号拆分列表，文本字段字符串化（对齐 V2.0 table col）。"""
        out = {}
        for k, v in kw.items():
            if k == "tags":
                out[k] = [t.strip() for t in str(v).split(",") if t.strip()]
            elif k in ("display_name", "description", "unit", "formula"):
                out[k] = str(v)
            else:
                out[k] = v
        return out

    def _files(self, node_id_: str) -> list[dict]:
        """指纹 → 文件清单（FileMeta 形态）。"""
        return [{"rel_path": f["rel_path"], "partition": f["partition_path"],
                 "size": f["size"], "mtime_ns": f["mtime_ns"]}
                for f in self.store.fingerprint_get(node_id_).values()]

    def _meta_dict(self, asset_type: str, name: str) -> dict:
        """graph 节点 + 指纹 → V2.0 TableMeta 形态 dict。"""
        node = self._require_node(asset_type, name)
        files = self._files(node_id(asset_type, name))
        part_count = len({f["partition"] for f in files if f["partition"]})
        disk = T.disk_files(self._root(name))
        consistent = bool(node.get("signature")) and \
            node["signature"] == T.signature(disk) if self._root(name).exists() else True
        return {
            "name": name,
            "version": node.get("version", 0),
            "layout": node.get("layout", "single"),
            "type": node.get("type", ""),
            "display_name": node.get("display_name") or name,
            "description": node.get("description", ""),
            "tags": list(node.get("tags") or ()),
            "source": node.get("source", "local"),
            "extra": node.get("extra", {}),
            "partition_by": list(node.get("partition_by") or ()),
            "partition_count": part_count,
            "files": files,
            "columns": node.get("columns") or [],
            "consistent": consistent,
            "created_at": node.get("create_time", ""),
            "updated_at": node.get("update_time", ""),
        }

    def _scan_disk(self, asset_type: str, name: str, *, meta: dict | None = None,
                   extra_data: dict | None = None) -> dict:
        """核心：列目录 → 对账 → 有差异才扫 footer → 指纹 + 节点更新（幂等）。

        - 首次（隐式登记）：graph.add 建节点（版本 v1）；差异必然存在 → 写指纹 + 列
        - 非首次且变化：指纹替换 + 节点 patch + ``notify_change``（铸版本 + 下游置脏）
        - 无变化：不 bump 版本
        """
        root = self._root(name)
        if not root.exists():
            raise TableNotFoundError(f"table dir not found: {root}")
        disk = T.disk_files(root)
        nid = node_id(asset_type, name)
        node = self.store.get_node(nid)
        implicit = node is None
        version_before = 0 if implicit else node["version"]
        cat = self.store.fingerprint_get(nid) if node else {}
        stats = self.store.fingerprint_stats(nid) if cat else {}
        diffs = T.diff_files(disk, cat)
        layout, pkeys = T.detect_layout([f.rel_path for f in disk])
        changed = bool(diffs)
        version_after = version_before

        with self.store.txn():
            if implicit:
                data: dict[str, Any] = {"columns": [], "layout": layout.value,
                                        "partition_by": pkeys, "signature": ""}
                if extra_data:
                    data.update(extra_data)
                self.graph.add(asset_type, name, **data)
                node = self.store.get_node(nid)

            if changed:
                payload = []
                for f in disk:
                    old = cat.get(f.rel_path)
                    if old is not None and old["size"] == f.size and old["mtime_ns"] == f.mtime_ns:
                        ftr = {"row_count": old["row_count"], "file_bytes": old["file_bytes"],
                               "schema": loads(old["schema"] or "{}"),
                               "stats": stats.get(old["id"], {})}
                    else:
                        ftr = T.footer(root / f.rel_path)
                    payload.append((f, ftr, T.partition_of(f.rel_path)))

                items = [
                    (part, f.rel_path, ftr["row_count"], ftr["file_bytes"], f.size, f.mtime_ns,
                     dumps_str(ftr["schema"]), ftr["stats"])
                    for f, ftr, part in payload
                ]
                self.store.fingerprint_replace(nid, items)

                old_cols = {c["name"]: c for c in (node.get("columns") or [])}
                new_cols = []
                for c in T.columns_union([(f.rel_path, ftr) for f, ftr, _ in payload],
                                         self.ignore_cols):
                    prev = dict(old_cols.get(c.name, {}))
                    prev.update({k: v for k, v in c.to_dict().items() if v is not None or k == "is_tool"})
                    new_cols.append(prev)

                base = {}
                if implicit and meta:
                    for k, v in meta.items():
                        if k == "tags":
                            base["tags"] = [t.strip() for t in str(v).split(",") if t.strip()]
                        elif k in ("display_name", "description", "source"):
                            base[k] = str(v)
                        else:
                            extra = dict(node.get("extra") or {})
                            extra[k] = v
                            base["extra"] = extra
                patch = {**base,
                         "layout": layout.value,
                         "partition_by": pkeys,
                         "columns": new_cols,
                         "signature": T.signature(disk),
                         "update_time": _now_iso()}
                self.store.patch_node(nid, **patch)
                if not implicit:
                    # 物理数据变化 → 铸新版本 + 事件入日志 + 下游置脏
                    self.graph.notify_change(asset_type, name, event=DataChangeEvent(
                        action="upsert", field_scope=[c["name"] for c in new_cols]))
                version_after = node["version"] if implicit else self.store.get_node(nid)["version"]
                partition_count = len({p for _, _, p in payload}) if disk else 0
            else:
                partition_count = len({r["partition_path"] for r in cat.values()}) if disk else 0

        return {
            "name": name, "version_before": version_before, "version_after": version_after,
            "layout": layout.value, "partition_by": list(pkeys),
            "partition_count": partition_count,
            "diffs": [{"rel_path": d.rel_path, "kind": d.kind,
                       "catalog_size": d.catalog_size, "disk_size": d.disk_size,
                       "catalog_mtime_ns": d.catalog_mtime_ns,
                       "disk_mtime_ns": d.disk_mtime_ns} for d in diffs],
            "changed": changed,
            "implicit_registered": implicit,
        }

    def _ensure_fresh(self, asset_type: str, name: str) -> None:
        """读前快检：签名一致则继续；不一致自动 scan（未登记则隐式注册）。"""
        root = self._root(name)
        if not root.exists():
            return
        node = self.store.get_node(node_id(asset_type, name))
        disk_sig = T.signature(T.disk_files(root))
        if node is None or disk_sig != (node.get("signature") or ""):
            self._scan_disk(asset_type, name)

    def _read_lazy(self, asset_type: str, name: str, *, columns: list[str] | None = None,
                   where: pl.Expr | str | None = None, partition=None,
                   exclude_tool: bool = False) -> pl.LazyFrame:
        """读表 lazy：快检 → prune（分区/列统计裁剪）→ scan_parquet。"""
        self._ensure_fresh(asset_type, name)
        nid = node_id(asset_type, name)
        node = self._require_node(asset_type, name)
        files = prune_files(self.store.connection, nid, partition, where)
        if not files:
            return pl.LazyFrame()
        paths = [self._root(name) / f["rel_path"] for f in files]
        lf = pl.scan_parquet(paths, hive_partitioning=True)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if columns is not None:
            lf = lf.select(*columns)
        elif exclude_tool:
            keep = [c["name"] for c in (node.get("columns") or [])
                    if c.get("name") not in self.ignore_cols]
            lf = lf.select(*keep)
        return lf

    def _get_data(self, asset_type: str, name: str, *, columns=None, where=None,
                  partition=None, exclude_tool=False, limit=None, offset=None,
                  count_total=False):
        """读表数据：返回 (df, total)。"""
        lf = self._read_lazy(asset_type, name, columns=columns, where=where,
                             partition=partition, exclude_tool=exclude_tool)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        return df, (total if total is not None else df.height)

    # =====================================================================
    # table
    # =====================================================================

    def table_add(self, name: str, *, all: bool = False, meta: dict | None = None):
        """注册表（发现资产）：目录必须存在；已注册报 TableExistsError。"""
        if all:
            if not self.tables_root.exists():
                return []
            out = []
            for d in sorted(x for x in self.tables_root.iterdir() if x.is_dir()):
                if self.store.get_node(node_id("table", d.name)) is None \
                        and any(d.rglob("*.parquet")):
                    out.append(self._scan_disk("table", d.name))
            return out
        if not name:
            raise ValueError("add 需要表名（或 --all 批量发现）")
        root = self._root(name)
        if not root.exists():
            raise TableNotFoundError(f"table dir not found: {root}")
        if self.store.get_node(node_id("table", name)) is not None:
            raise TableExistsError(f"table already registered: {name} (use scan to refresh)")
        return self._scan_disk("table", name, meta=meta)

    def table_get(self, name: str, *, columns=None, where=None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        df, total = self._get_data("table", name, columns=columns, where=where,
                                   partition=partition, exclude_tool=exclude_tool,
                                   limit=limit, offset=offset, count_total=count_total)
        return (df, total) if count_total else df

    def table_meta(self, name: str) -> dict:
        return self._meta_dict("table", name)

    def table_list(self, *, candidate: bool = False) -> list:
        if candidate:
            if not self.tables_root.exists():
                return []
            out = []
            for d in sorted(x for x in self.tables_root.iterdir() if x.is_dir()):
                if self.store.get_node(node_id("table", d.name)) is None \
                        and any(d.rglob("*.parquet")):
                    out.append(d.name)
            return out
        return [self._meta_dict("table", n["name"]) for n in self.graph.list("table")]

    def table_set(self, name: str, **kw) -> dict:
        self._require_node("table", name)
        return self.graph.set("table", name, **kw)

    def table_col(self, name: str, column: str, **kw) -> dict:
        return self.graph.col("table", name, column, **self._norm_col_kw(kw))

    def table_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("table", name)
        self.graph.delete("table", name, force=force)
        self.store.fingerprint_clear(node_id("table", name))
        return {"deleted": name}

    def table_scan(self, name: str, *, all: bool = False) -> dict | list:
        if all:
            return [self._scan_disk("table", n["name"]) for n in self.graph.list("table")]
        return self._scan_disk("table", name)

    def table_data_key(self, name: str) -> str:
        """当前数据标识：快检后返回签名（未登记则 ''）。"""
        root = self._root(name)
        if not root.exists():
            return ""
        self._ensure_fresh("table", name)
        node = self.store.get_node(node_id("table", name))
        return node.get("signature", "") if node else T.signature(T.disk_files(root))

    # =====================================================================
    # index（独立主体：物理与 table 同，节点为 Index + symbol/datetime 列）
    # =====================================================================

    def index_add(self, name: str, *, symbol_col: str = "sym", datetime_col: str = "date",
                  materialize_partition: str = "yearly", meta: dict | None = None) -> dict:
        root = self._root(name)
        if not root.exists():
            raise TableNotFoundError(f"index dir not found: {root}")
        if self.store.get_node(node_id("index", name)) is not None:
            raise TableExistsError(f"index already registered: {name}")
        return self._scan_disk(
            "index", name, meta=meta,
            extra_data={"symbol_col": symbol_col, "datetime_col": datetime_col,
                        "materialize_partition": materialize_partition})

    def index_get(self, name: str, *, columns=None, where=None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        df, total = self._get_data("index", name, columns=columns, where=where,
                                   partition=partition, exclude_tool=exclude_tool,
                                   limit=limit, offset=offset, count_total=count_total)
        return (df, total) if count_total else df

    def index_meta(self, name: str) -> dict:
        return self._meta_dict("index", name)

    def index_list(self) -> list:
        return [self._meta_dict("index", n["name"]) for n in self.graph.list("index")]

    def index_set(self, name: str, **kw) -> dict:
        self._require_node("index", name)
        return self.graph.set("index", name, **kw)

    def index_col(self, name: str, column: str, **kw) -> dict:
        return self.graph.col("index", name, column, **self._norm_col_kw(kw))

    def index_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("index", name)
        self.graph.delete("index", name, force=force)
        self.store.fingerprint_clear(node_id("index", name))
        return {"deleted": name}

    def index_scan(self, name: str, *, all: bool = False) -> dict | list:
        if all:
            return [self._scan_disk("index", n["name"]) for n in self.graph.list("index")]
        return self._scan_disk("index", name)

    def index_data_key(self, name: str) -> str:
        root = self._root(name)
        if not root.exists():
            return ""
        self._ensure_fresh("index", name)
        node = self.store.get_node(node_id("index", name))
        return node.get("signature", "") if node else T.signature(T.disk_files(root))

    # =====================================================================
    # panel（原 dataset：graph 节点 + DEPENDS 边；get 实时 join）
    # =====================================================================

    def panel_add(self, name: str, index: str, tables: list[str] | None = None,
                  keys: list[str] | None = None, **kw) -> dict:
        """panel add：index 为已注册 Index 节点，tables 为已注册 table 节点。"""
        return PanelHandler.add(self.graph, name, index,
                                tables={t: "left_join" for t in (tables or [])},
                                keys=keys, **kw)

    def _panel_columns(self, node: dict) -> list[dict]:
        """派生列：index 列优先（keys 标 as_index），member 表列同名跳过。"""
        keys = set(node.get("keys") or [])
        cols: list[dict] = []
        seen: set[str] = set()
        index = node.get("index", "").split(":", 1)[1]
        idx = self._require_node("index", index)
        for c in idx.get("columns") or []:
            cc = dict(c)
            cc.update({"source_table": index, "source_field": cc["name"],
                       "as_index": cc["name"] in keys})
            cols.append(cc)
            seen.add(cc["name"])
        for t in (node.get("tables") or {}):
            tnode = self._require_node("table", t)
            for c in tnode.get("columns") or []:
                if c["name"] in seen:
                    continue
                cc = dict(c)
                cc.update({"source_table": t, "source_field": cc["name"],
                           "as_index": cc["name"] in keys})
                cols.append(cc)
                seen.add(cc["name"])
        return cols

    def panel_meta(self, name: str) -> dict:
        node = self._require_node("panel", name)
        meta = self.graph._meta(node)
        meta["columns"] = self._panel_columns(node)
        return meta

    def panel_list(self) -> list:
        return [self.panel_meta(n["name"]) for n in self.graph.list("panel")]

    def panel_set(self, name: str, **kw) -> dict:
        self._require_node("panel", name)
        return self.graph.set("panel", name, **kw)

    def panel_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("panel", name)
        self.graph.delete("panel", name, force=force)
        return {"deleted": name}

    def panel_get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        """实时 join 视图：index 为左表，member 表 left join on keys。"""
        node = self._require_node("panel", name)
        index = node.get("index", "").split(":", 1)[1]
        tables = list((node.get("tables") or {}).keys())
        keys = list(node.get("keys") or ())
        cols = self._panel_columns(node)
        by_src: dict[str, list[dict]] = {}
        for c in cols:
            by_src.setdefault(c["source_table"], []).append(c)

        def frame(t: str, asset_type: str) -> pl.LazyFrame:
            lf = self._read_lazy(asset_type, t)
            used = {c["source_field"] for c in by_src.get(t, [])}
            exprs = [pl.col(c["source_field"]).alias(c["name"]) for c in by_src.get(t, [])]
            exprs += [pl.col(k).alias(k) for k in keys if k not in used]
            return lf.select(*exprs)

        frames = [frame(index, "index")]
        frames += [frame(t, "table") for t in tables]
        joined = frames[0]
        for f in frames[1:]:
            joined = joined.join(f, on=keys, how="left")
        joined = joined.select(*[c["name"] for c in cols])

        if where is not None:
            joined = joined.filter(to_expr(where) if isinstance(where, str) else where)
        if columns is not None:
            joined = joined.select(*columns)
        elif exclude_tool:
            keep = [c["name"] for c in cols if c["name"] not in self.ignore_cols]
            joined = joined.select(*keep)
        total = None
        if count_total and (limit is not None or offset is not None):
            total = joined.select(pl.len()).collect().item()
        if limit is not None or offset is not None:
            joined = joined.slice(offset if offset is not None else 0, limit)
        df = joined.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df


__all__ = ["GraphService"]
