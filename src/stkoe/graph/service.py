"""GraphService：V3.0 资产统一服务（登记/依赖/版本走 graph，物理数据走 graph.db 指纹 + polars）。

替代 V2.0 table/dataset 等 controller 的 SQLite catalog 登记层：
- **登记/元数据/依赖/版本** → graph 节点/边（graphqlite，graph.db）
- **物理指纹**（stkoe_data_files / stkoe_file_stats）→ graph.db 普通表（同文件同事务）
- **物理数据**（parquet 扫描/读取/prune）→ 复用 table/util.py / table/query.py 纯函数

assets：``table`` / ``index``（独立主体）／``panel``（原 dataset）。
"""
from __future__ import annotations

import hashlib
import shutil
import time
from pathlib import Path
from typing import Any

import polars as pl

from ..factor.engine import get_engine as get_factor_engine
from ..factor.engine import parse_pipeline
from ..factor_test.spec import FactorTesterSpec
from ..factor_test.tester import prepare_factor_data
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
from .handlers import (
    FactorHandler,
    FeatureHandler,
    FieldsetHandler,
    IndexHandler,
    PanelHandler,
    SampleHandler,
    TableHandler,
    TesterHandler,
)
from .model import ColumnMeta, FieldMeta, node_id
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
        self.store = GraphStore(str(self._db_path(self.data_dir)))
        self.graph = GraphController(self.store)
        self.tables_root = self.data_dir / "table"
        self.indexs_root = self.data_dir / "index"  # index 资产独立物理目录
        self.ignore_cols = set(DEFAULT_IGNORE_COLS)

    @classmethod
    def _db_path(cls, data_dir: Path) -> Path:
        """资产库文件：统一 ``catalog.db``（新结构：图节点/边 + 物理指纹普通表）。

        兼容旧名 ``graph.db``：文件存在时回退读取（保护已入库数据，写入恒走 catalog.db）。
        """
        db = data_dir / "catalog.db"
        if not db.exists():
            old = data_dir / "graph.db"
            if old.exists():
                return old
        return db

    def close(self) -> None:
        self.store.close()

    # =====================================================================
    # 物理共用（table / index 相同：parquet 目录扫描 + 指纹 + 节点）
    # =====================================================================

    def _root(self, name: str) -> Path:
        return self.tables_root / name

    def _index_root(self, name: str) -> Path:
        return self.indexs_root / name

    def _asset_root(self, asset_type: str, name: str) -> Path:
        """按资产类型取物理目录：index 走 indexs/，其余走 tables/。"""
        if asset_type == "index":
            return self._index_root(name)
        return self._root(name)

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

    @staticmethod
    def _norm_cols(cols) -> list[dict]:
        """列元数据输出规范化：补齐 V2.0 ColumnMeta 全键（unit/formula/source_table/...）。"""
        return [ColumnMeta.from_dict(c).to_dict() for c in (cols or [])]

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
        root = self._asset_root(asset_type, name)
        disk = T.disk_files(root)
        consistent = bool(node.get("signature")) and \
            node["signature"] == T.signature(disk) if root.exists() else True
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
            "columns": self._norm_cols(node.get("columns")),
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
        root = self._asset_root(asset_type, name)
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
        root = self._asset_root(asset_type, name)
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
        paths = [self._asset_root(asset_type, name) / f["rel_path"] for f in files]
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

    def table_lazy(self, name: str, *, where=None, exclude_tool: bool = False) -> pl.LazyFrame:
        """table 读取 lazy 视图（stat 等下游消费）。"""
        return self._read_lazy("table", name, where=where, exclude_tool=exclude_tool)

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

    def table_update(self, name: str, *, all: bool = False) -> dict | list:
        """源头表更新：重扫对账（物理变化 → 版本递增 + 下游置脏）。

        源头（table/index）无上游，天然就绪；`--all` 批量重扫全部已登记表。
        """
        if all:
            return [self._scan_disk("table", n["name"]) for n in self.graph.list("table")]
        return self._scan_disk("table", name)

    def table_scan(self, name: str, *, all: bool = False) -> dict | list:
        """旧名别名（V3 语义改称 update）。"""
        return self.table_update(name, all=all)

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
        root = self._index_root(name)
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

    def index_list(self, *, candidate: bool = False) -> list:
        """index 清单；candidate=True 返回未登记为 index 但含 parquet 的表目录。"""
        if candidate:
            if not self.indexs_root.exists():
                return []
            out = []
            for d in sorted(x for x in self.indexs_root.iterdir() if x.is_dir()):
                if self.store.get_node(node_id("index", d.name)) is None \
                        and any(d.rglob("*.parquet")):
                    out.append(d.name)
            return out
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

    def index_update(self, name: str, *, all: bool = False) -> dict | list:
        """源头 index 更新：重扫对账（物理变化 → 版本递增 + 下游置脏）。"""
        if all:
            return [self._scan_disk("index", n["name"]) for n in self.graph.list("index")]
        return self._scan_disk("index", name)

    def index_scan(self, name: str, *, all: bool = False) -> dict | list:
        """旧名别名（V3 语义改称 update）。"""
        return self.index_update(name, all=all)

    def index_data_key(self, name: str) -> str:
        root = self._index_root(name)
        if not root.exists():
            return ""
        self._ensure_fresh("index", name)
        node = self.store.get_node(node_id("index", name))
        return node.get("signature", "") if node else T.signature(T.disk_files(root))

    # =====================================================================
    # panel（原 dataset：graph 节点 + DEPENDS 边；get 实时 join）
    # =====================================================================

    def panel_add(self, name: str, index: str, tables: list[str] | None = None,
                  **kw) -> dict:
        """panel add：index 为已注册 Index 节点，tables 为已注册 table 节点。

        keys 由 index 推断（symbol_col + datetime_col，去空去重；兜底 sym/date），
        不再接受显式 ``--keys``（旧参数被忽略）。
        """
        idx_node = self._require_node("index", index)
        keys = [c for c in (idx_node.get("symbol_col"), idx_node.get("datetime_col"))
                if c]
        keys = list(dict.fromkeys(keys)) or ["sym", "date"]
        kw.pop("keys", None)  # 忽略旧 --keys 参数，以 index 推断为准
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
        return self._norm_cols(cols)

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

    def panel_update(self, name: str) -> dict:
        """panel 更新：传导检查上游（index/成员表）就绪 → 实时 join 可构造 → 标记有效。

        panel 为实时 join 视图（无物化），update 语义 = 确认上游就绪并置 valid=True。
        """
        self.graph.assert_ready("panel", name)
        joined, _ = self._panel_lazy(name)
        rows = joined.select(pl.len()).collect().item()
        self.store.patch_node(node_id("panel", name), valid=True,
                              update_time=_now_iso())
        return {"name": name, "valid": True, "rows": rows,
                "version": self.store.get_node(node_id("panel", name))["version"]}

    def panel_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("panel", name)
        self.graph.delete("panel", name, force=force)
        return {"deleted": name}

    def _panel_lazy(self, name: str, where: pl.Expr | str | None = None) -> tuple[pl.LazyFrame, list[str]]:
        """panel 实时 join 视图（lazy）：index 为左表，member 表 left join on keys。"""
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
        return joined, keys

    def _collect_page(self, lf: pl.LazyFrame, *, columns=None, exclude_tool=False,
                      limit=None, offset=None, count_total=False):
        total = None
        if count_total and (limit is not None or offset is not None):
            total = lf.select(pl.len()).collect().item()
        if columns is not None:
            lf = lf.select(*columns)
        if limit is not None or offset is not None:
            lf = lf.slice(offset if offset is not None else 0, limit)
        df = lf.collect()
        if count_total:
            return df, (total if total is not None else df.height)
        return df

    def panel_get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        """实时 join 视图：index 为左表，member 表 left join on keys。"""
        joined, _ = self._panel_lazy(name, where)
        return self._collect_page(joined, columns=columns, exclude_tool=exclude_tool,
                                  limit=limit, offset=offset, count_total=count_total)

    def panel_lazy(self, name: str, *, where=None) -> pl.LazyFrame:
        """panel 实时 join 视图（lazy，stat 等下游消费）。"""
        return self._panel_lazy(name, where)[0]

    # =====================================================================
    # fieldset（衍生指标集：graph 登记；check/scan 用 panel 视图 + 公式引擎）
    # =====================================================================

    def _fieldset_meta_node(self, name: str) -> dict:
        node = self._require_node("fieldset", name)
        meta = self.graph._meta(node)
        meta["keys"] = self._panel_keys(node.get("dataset", "").split(":", 1)[1])
        return meta

    def _panel_keys(self, panel: str) -> list[str]:
        pnode = self._require_node("panel", panel)
        return list(pnode.get("keys") or ())

    def _fieldset_view_lf(self, name: str, *, fields_only: bool = False,
                          where: pl.Expr | str | None = None) -> tuple[pl.LazyFrame, list[str]]:
        """fieldset 视图：panel 全列 + 已校验衍生字段（fields_only 时仅 keys+字段）。"""
        node = self._require_node("fieldset", name)
        panel = node.get("dataset", "").split(":", 1)[1]
        keys = self._panel_keys(panel)
        base, _ = self._panel_lazy(panel, where)
        fields = [FieldMeta.from_dict(f) for f in (node.get("fields") or {}).values()
                  if f.get("validated")]
        engine = get_fieldset_engine(node.get("engine") or "polars")
        if fields_only:
            return engine.scan(base, keys, fields), keys
        derived = engine.scan(base, keys, fields)
        if derived.select(pl.len()).collect().item():
            base = base.join(derived, on=keys, how="left")
        return base, keys

    def fieldset_add(self, name: str, panel: str, *, engine: str = "polars", **kw) -> dict:
        return FieldsetHandler.add(self.graph, name, panel, engine=engine, **kw)

    def fieldset_add_field(self, name: str, field: str, formula: str, **kw) -> dict:
        if not formula:
            raise ValueError("fieldset add_field 需要 formula")
        return FieldsetHandler.add_field(self.graph, name, field, formula, **kw)

    def fieldset_set_field(self, name: str, field: str, **kw) -> dict:
        return FieldsetHandler.set_field(self.graph, name, field, **kw)

    def fieldset_delete_field(self, name: str, field: str) -> dict:
        return FieldsetHandler.delete_field(self.graph, name, field)

    def fieldset_meta_field(self, name: str, field: str) -> dict:
        return FieldsetHandler.meta_field(self.graph, name, field)

    def fieldset_meta(self, name: str) -> dict:
        return self._fieldset_meta_node(name)

    def fieldset_list(self) -> list:
        return [self._fieldset_meta_node(n["name"]) for n in self.graph.list("fieldset")]

    def fieldset_set(self, name: str, **kw) -> dict:
        self._require_node("fieldset", name)
        return self.graph.set("fieldset", name, **kw)

    def fieldset_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("fieldset", name)
        self.graph.delete("fieldset", name, force=force)
        return {"deleted": name}

    def fieldset_check(self, name: str, field: str) -> dict:
        """校验单个指标；通过后写回 validated=True（视图/物化只取已校验字段）。"""
        node = self._require_node("fieldset", name)
        fields = node.get("fields") or {}
        if field not in fields:
            raise TableNotFoundError(f"field not found: {field}")
        base, keys = self._panel_lazy(node.get("dataset", "").split(":", 1)[1])
        engine = get_fieldset_engine(node.get("engine") or "polars")
        ok, message = engine.check(base, FieldMeta.from_dict(fields[field]))
        if ok and not fields[field].get("validated"):
            new_fields = dict(fields)
            new_fields[field] = {**fields[field], "validated": True}
            self.graph.set("fieldset", name, definition=True, fields=new_fields)
        return {"fieldset": name, "field": field, "ok": ok, "message": message}

    def fieldset_get(self, name: str, *, fields_only: bool = False,
                     columns: list[str] | None = None, where=None,
                     limit=None, offset=None, count_total: bool = False):
        lf, _ = self._fieldset_view_lf(name, fields_only=fields_only, where=where)
        return self._collect_page(lf, columns=columns, limit=limit, offset=offset,
                                  count_total=count_total)

    def fieldset_update(self, name: str, *, resync: bool = False) -> dict:
        """fieldset 更新：传导检查上游（panel 链）就绪 → 校验已校验字段 → resolve 标记有效。

        物化语义 = 校验通过 + 节点有效（物理产物后续接入）；scan 为旧名别名。
        """
        self.graph.assert_ready("fieldset", name)
        node = self._require_node("fieldset", name)
        fields = [f for f in (node.get("fields") or {}).values() if f.get("validated")]
        base, keys = self._panel_lazy(node.get("dataset", "").split(":", 1)[1])
        engine = get_fieldset_engine(node.get("engine") or "polars")
        out = engine.scan(base, keys, [FieldMeta.from_dict(f) for f in fields])
        rows = out.select(pl.len()).collect().item()
        m = self.graph.resolve("fieldset", name)
        return {"name": name, "materialized": True, "valid": True, "rows": rows,
                "fields_count": len(fields), "version": m["version"]}

    def fieldset_scan(self, name: str, *, resync: bool = False) -> dict:
        """旧名别名（V3 语义改称 update）。"""
        return self.fieldset_update(name, resync=resync)

    def fieldset_test(self, name: str, formula: str):
        node = self._require_node("fieldset", name)
        base, _ = self._panel_lazy(node.get("dataset", "").split(":", 1)[1])
        engine = get_fieldset_engine(node.get("engine") or "polars")
        df = engine.test(base, formula)
        return {"ok": True, "rows": df.height, "columns": list(df.columns)}, df

    # =====================================================================
    # sample（样本池：graph 登记，依赖 fieldset；get/check 实时过滤）
    # =====================================================================

    def _sample_view_lf(self, name: str, *, where=None) -> pl.LazyFrame:
        node = self._require_node("sample", name)
        fset = node.get("fieldset", "").split(":", 1)[1]
        lf, _ = self._fieldset_view_lf(fset, where=where)
        engine = get_sample_engine(node.get("engine") or "polars")
        return engine.filter(lf, node.get("formula") or "")

    def sample_add(self, name: str, fieldset: str, *, formula: str = "",
                   engine: str = "polars", **kw) -> dict:
        return SampleHandler.add(self.graph, name, fieldset, formula=formula,
                                 engine=engine, **kw)

    def sample_get(self, name: str, *, columns=None, where=None, limit=None,
                   offset=None, count_total: bool = False):
        lf = self._sample_view_lf(name, where=where)
        return self._collect_page(lf, columns=columns, limit=limit, offset=offset,
                                  count_total=count_total)

    def _sample_keys(self, node: dict) -> list[str]:
        """sample 的索引列 = 其 fieldset 底层 panel 的 keys。"""
        fset = node.get("fieldset", "").split(":", 1)[1]
        fnode = self._require_node("fieldset", fset)
        return self._panel_keys(fnode.get("dataset", "").split(":", 1)[1])

    def sample_check(self, name: str) -> dict:
        node = self._require_node("sample", name)
        keys = self._sample_keys(node)
        try:
            lf = self._sample_view_lf(name)
            df = lf.collect()
        except Exception as e:
            return {"sample": name, "ok": False, "rows": 0, "columns": [], "message": str(e)}
        cols = set(df.columns)
        ok = all(k in cols for k in keys) and df.height > 0
        return {"sample": name, "ok": ok, "rows": df.height,
                "columns": list(df.columns),
                "message": "" if ok else "过滤后缺少索引列或行数为 0"}

    def sample_meta(self, name: str) -> dict:
        node = self._require_node("sample", name)
        return self.graph._meta(node)

    def sample_list(self) -> list:
        return [self.sample_meta(n["name"]) for n in self.graph.list("sample")]

    def sample_set(self, name: str, **kw) -> dict:
        self._require_node("sample", name)
        return self.graph.set("sample", name, **kw)

    def sample_update(self, name: str) -> dict:
        """sample 更新：传导检查上游（fieldset 链）就绪 → 过滤视图可构造 → 标记有效。

        sample 无物化，update 语义 = 确认上游就绪并置 valid=True。
        """
        self.graph.assert_ready("sample", name)
        self._sample_view_lf(name).select(pl.len()).collect()
        self.store.patch_node(node_id("sample", name), valid=True,
                              update_time=_now_iso())
        return {"name": name, "valid": True,
                "version": self.store.get_node(node_id("sample", name))["version"]}

    def sample_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("sample", name)
        self.graph.delete("sample", name, force=force)
        return {"deleted": name}

    # =====================================================================
    # feature（因子定义库：纯定义，graph 登记；test 在 sample 视图上求值）
    # =====================================================================

    def feature_add(self, name: str, formula: str, *, engine: str = "polars",
                    unit: str | None = None, **kw) -> dict:
        return FeatureHandler.add(self.graph, name, formula, engine=engine,
                                  unit=unit, **kw)

    def feature_meta(self, name: str) -> dict:
        return self.graph.meta("feature", name)

    def feature_list(self) -> list:
        return self.graph.list("feature")

    def feature_set(self, name: str, **kw) -> dict:
        self._require_node("feature", name)
        return self.graph.set("feature", name, **kw)

    def feature_update(self, name: str) -> dict:
        """feature 更新：纯定义资产（无上游），标记有效即可。"""
        self.graph.assert_ready("feature", name)
        self.store.patch_node(node_id("feature", name), valid=True,
                              update_time=_now_iso())
        return {"name": name, "valid": True,
                "version": self.store.get_node(node_id("feature", name))["version"]}

    def feature_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("feature", name)
        self.graph.delete("feature", name, force=force)
        return {"deleted": name}

    def feature_test(self, name: str, sample: str):
        node = self._require_node("feature", name)
        lf = self._sample_view_lf(sample)
        engine = get_feature_engine(node.get("engine") or "polars")
        try:
            df = engine.test(lf, node.get("formula") or "")
            total = lf.select(pl.len()).collect().item()
            valid = df.height == total
            return ({"feature": name, "sample": sample, "ok": True, "valid": valid,
                     "rows": df.height, "columns": list(df.columns),
                     "message": "" if valid else f"结果行数 {df.height} != 样本行数 {total}"},
                    df if df.height else None)
        except Exception as e:
            return ({"feature": name, "sample": sample, "ok": False, "valid": False,
                     "rows": 0, "columns": [], "message": f"公式执行失败: {e}"}, None)

    # =====================================================================
    # factor（最终因子：feature 公式 + sample 视图 + pipeline 算子链；物化落盘）
    # =====================================================================

    def _sample_view_cols(self, sample: str) -> list[dict]:
        """sample 视图列（name + data_type），供 factor/test meta 引用。"""
        lf = self._sample_view_lf(sample)
        return [{"name": c, "data_type": str(t)}
                for c, t in zip(lf.collect_schema().names(), lf.collect_schema().dtypes())]

    def _factor_keys(self, node: dict) -> list[str]:
        """factor 的 keys = 其 sample 的 keys（fieldset → panel）。"""
        sample = node.get("sample", "").split(":", 1)[1]
        snode = self._require_node("sample", sample)
        return self._sample_keys(snode)

    def _factor_meta_dict(self, name: str) -> dict:
        """V2.0 FactorMeta 形态 dict（含 keys/columns/materialized/curated/field）。"""
        node = self._require_node("factor", name)
        meta = self.graph._meta(node)
        extra = dict(meta.get("extra") or {})
        keys = self._factor_keys(node)
        sample = node.get("sample", "").split(":", 1)[1]
        materialized = bool(extra.get("materialized"))
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
            "curated": materialized and dep_hash == self._factor_hash(node),
            "columns": self._sample_view_cols(sample),
            "field": extra.get("field"),
            "extra": extra,
            "display_name": node.get("display_name") or name,
            "description": node.get("description", ""),
            "tags": list(node.get("tags") or ()),
            "source": node.get("source", "local"),
            "created_at": node.get("create_time", ""),
            "updated_at": node.get("update_time", ""),
        }

    def _factor_hash(self, node: dict) -> str:
        """物化一致性签名 = 上游 feature/sample 版本 + engine/pipeline/factor_col。"""
        feature = node.get("feature", "").split(":", 1)[1] if node.get("feature") else ""
        sample = node.get("sample", "").split(":", 1)[1] if node.get("sample") else ""
        parts = [
            f"feature:{feature}:{self._require_node('feature', feature).get('version', 0)}",
            f"sample:{sample}:{self._require_node('sample', sample).get('version', 0)}",
            f"engine:{node.get('engine', 'polars')}",
            f"pipeline:{node.get('pipeline', '')}",
            f"factor_col:{node.get('factor_col', '')}",
        ]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _factor_compute(self, node: dict, *, partition: str | None = None) -> pl.DataFrame:
        """实时计算最终因子：sample 视图求 feature 公式 → 拼索引+因子列 → 算子链。"""
        feature = node.get("feature", "").split(":", 1)[1]
        sample = node.get("sample", "").split(":", 1)[1]
        fnode = self._require_node("feature", feature)
        keys = self._factor_keys(node)
        lf = self._sample_view_lf(sample)
        engine = get_factor_engine(node.get("engine") or "polars")
        field = engine.field(lf, fnode.get("formula") or "")
        src_rows = lf.select(pl.len()).collect().item()
        if field.height != src_rows:
            raise ValueError(f"feature 公式非逐行计算: 结果 {field.height} 行 != 样本 {src_rows} 行")
        idx = lf.select(*[pl.col(k) for k in keys]).collect()
        factor_col = node.get("factor_col") or feature
        df = idx.hstack(field.rename({"field": factor_col}))
        return engine.transform(df, node.get("pipeline") or "nothing()")

    def _factor_view_lf(self, name: str, *, where=None,
                        partition: str | None = None) -> pl.LazyFrame:
        """读 factor（lazy）：物化且 curated → 读物化 parquet；否则实时计算。"""
        node = self._require_node("factor", name)
        fm = self._factor_meta_dict(name)
        if fm["materialized"] and fm["curated"]:
            root = self.data_dir / "factor" / name
            if root.exists():
                lf = pl.scan_parquet(root, hive_partitioning=True)
                if partition is not None:
                    lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
            else:
                lf = self._factor_compute(node).lazy()
        else:
            lf = self._factor_compute(node).lazy()
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def factor_add(self, name: str, feature: str, sample: str, *,
                   engine: str = "polars", pipeline: str = "nothing()",
                   factor_col: str | None = None, **kw) -> dict:
        """创建最终因子：校验 feature/sample 已注册 + pipeline/engine 合法。"""
        if not feature:
            raise ValueError("factor add 需要 --feature <feature 名>")
        if not sample:
            raise ValueError("factor add 需要 --sample <sample 名>")
        self._require_node("feature", feature)
        self._require_node("sample", sample)
        parse_pipeline(pipeline)
        get_factor_engine(engine)
        FactorHandler.add(self.graph, name, feature, sample, engine=engine,
                          pipeline=pipeline, factor_col=factor_col, **kw)
        return self._factor_meta_dict(name)

    def factor_get(self, name: str, *, where=None, partition: str | None = None,
                   limit=None, offset=None, count_total: bool = False):
        lf = self._factor_view_lf(name, where=where, partition=partition)
        return self._collect_page(lf, limit=limit, offset=offset, count_total=count_total)

    def factor_meta(self, name: str) -> dict:
        return self._factor_meta_dict(name)

    def factor_list(self) -> list:
        return [self._factor_meta_dict(n["name"]) for n in self.graph.list("factor")]

    def factor_set(self, name: str, **kw) -> dict:
        self._require_node("factor", name)
        return self.graph.set("factor", name, **kw)

    def factor_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("factor", name)
        self.graph.delete("factor", name, force=force)
        shutil.rmtree(self.data_dir / "factor" / name, ignore_errors=True)
        return {"deleted": name}

    def factor_check(self, name: str) -> dict:
        """校验因子：计算成功、含全部索引列、因子列恰好一列、行数 > 0。"""
        node = self._require_node("factor", name)
        keys = self._factor_keys(node)
        try:
            df = self._factor_compute(node)
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

    def factor_update(self, name: str | None = None, *, all: bool = False,
                      resync: bool = False) -> dict | list[dict]:
        """factor 更新：传导检查上游（sample/feature 全链）就绪 → 物化 factors/<name>/。

        幂等（依赖签名一致则跳过）；update 成功后节点置 valid=True。scan 为旧名别名。
        """
        if all:
            return [self._factor_scan_one(n["name"], resync=resync)
                    for n in self.graph.list("factor")]
        if not name:
            raise ValueError("factor update 需要因子名（或 --all）")
        self.graph.assert_ready("factor", name)
        return self._factor_scan_one(name, resync=resync)

    def factor_scan(self, name: str | None = None, *, all: bool = False,
                    resync: bool = False) -> dict | list[dict]:
        """旧名别名（V3 语义改称 update）。"""
        return self.factor_update(name, all=all, resync=resync)

    def _factor_scan_one(self, name: str, *, resync: bool = False) -> dict:
        node = self._require_node("factor", name)
        extra = dict(node.get("extra") or {})
        cur_hash = self._factor_hash(node)
        version_before = node.get("version", 0)
        # 幂等仅当节点有效：上游变化置脏（valid=False）后 update 必须强制重建
        if not resync and node.get("valid") \
                and extra.get("dependency_hash") == cur_hash \
                and extra.get("materialized"):
            return {"name": name, "version_before": version_before,
                    "version_after": version_before, "materialized": True,
                    "changed": False, "partition_by": list(extra.get("partition_by") or ())}
        df = self._factor_compute(node)
        out_dir = self.data_dir / "factor" / name
        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_dir / "data.parquet")
        feature = node.get("feature", "").split(":", 1)[1]
        fnode = self._require_node("feature", feature)
        self.graph.set("factor", name, materialized=True, materialized_at=_now_iso(),
                       dependency_hash=cur_hash, partition_by=[], partition_gran="",
                       field={"name": node.get("factor_col") or feature,
                              "formula": fnode.get("formula") or "",
                              "display_name": node.get("factor_col") or feature,
                              "description": "", "unit": None, "tags": []})
        self.store.patch_node(node_id("factor", name), valid=True)
        version_after = self.store.get_node(node_id("factor", name))["version"]
        return {"name": name, "version_before": version_before,
                "version_after": version_after, "materialized": True, "changed": True,
                "partition_by": [], "rebuilt_partitions": [""]}

    # =====================================================================
    # test（因子测试数据集：factor 关联 sample 视图 + 测试必需列；物化落盘）
    # =====================================================================

    def _test_spec(self, node: dict) -> FactorTesterSpec:
        return FactorTesterSpec.from_dict(node.get("spec") or {})

    def _test_meta_dict(self, name: str) -> dict:
        """V2.0 FactorTestMeta 形态 dict。"""
        node = self._require_node("tester", name)
        meta = self.graph._meta(node)
        extra = dict(meta.get("extra") or {})
        materialized = bool(extra.get("materialized"))
        dep_hash = extra.get("dependency_hash") or ""
        return {
            "name": name,
            "version": node.get("version", 0),
            "factor": node.get("factor", "").split(":", 1)[1] if node.get("factor") else "",
            "sample": node.get("sample", "").split(":", 1)[1] if node.get("sample") else "",
            "returns": node.get("returns", "r"),
            "groupby": node.get("groupby", "ic"),
            "marketcap": node.get("marketcap", "fv"),
            "spec": self._test_spec(node).to_dict(),
            "factor_col": node.get("factor_col", ""),
            "keys": list(node.get("keys") or ()),
            "materialized": materialized,
            "materialized_at": extra.get("materialized_at"),
            "curated": materialized and dep_hash == self._test_hash(node),
            "columns": list(extra.get("columns") or ()),
            "extra": extra,
            "display_name": node.get("display_name") or name,
            "description": node.get("description", ""),
            "tags": list(node.get("tags") or ()),
            "source": node.get("source", "local"),
            "created_at": node.get("create_time", ""),
            "updated_at": node.get("update_time", ""),
        }

    def _test_hash(self, node: dict) -> str:
        """物化一致性签名 = factor 的 hash + spec + 测试列名。"""
        factor = node.get("factor", "").split(":", 1)[1] if node.get("factor") else ""
        fnode = self._require_node("factor", factor)
        parts = [
            f"factor:{factor}:{self._factor_hash(fnode)}",
            f"returns:{node.get('returns', 'r')}",
            f"groupby:{node.get('groupby', 'ic')}",
            f"marketcap:{node.get('marketcap', 'fv')}",
            f"factor_col:{node.get('factor_col', '')}",
            f"spec:{dumps_str(self._test_spec(node).to_dict())}",
        ]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _test_build(self, node: dict) -> pl.DataFrame:
        """测试数据集：sample 视图（含测试必需列）+ factor 列 → prepare_factor_data。"""
        factor = node.get("factor", "").split(":", 1)[1]
        fnode = self._require_node("factor", factor)
        fm = self._factor_meta_dict(factor)
        sample = node.get("sample", "").split(":", 1)[1] if node.get("sample") else fm["sample"]
        view = self._sample_view_lf(sample).collect()
        returns = node.get("returns", "r")
        groupby = node.get("groupby", "ic")
        marketcap = node.get("marketcap", "fv")
        need = ["date", "sym", returns, groupby, marketcap]
        missing = [c for c in need if c not in view.columns]
        if missing:
            raise ValueError(f"sample 缺少测试必需列: {missing}（需要 date/sym 与 "
                             f"returns/groupby/marketcap）")
        fdf = self._factor_compute(fnode)
        keys = list(fm["keys"])
        base = (
            view.select(*[pl.col(c) for c in need])
            .with_columns(pl.lit(1, dtype=pl.Int32).alias("sample"))
            .join(fdf, on=keys, how="left")
            .rename({fm["factor_col"]: "factor", returns: "returns",
                     groupby: "group", marketcap: "marketcap"})
        )
        return prepare_factor_data(base, self._test_spec(node))

    def _test_view_lf(self, name: str, *, where=None) -> pl.LazyFrame:
        """读测试数据集（lazy）：物化且 curated → 读物化 parquet；否则实时构造。"""
        node = self._require_node("tester", name)
        tm = self._test_meta_dict(name)
        if tm["materialized"] and tm["curated"]:
            p = self.data_dir / "factor_test" / name / "data.parquet"
            if p.exists():
                lf = pl.scan_parquet(p)
            else:
                lf = self._test_build(node).lazy()
        else:
            lf = self._test_build(node).lazy()
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def test_data(self, name: str) -> pl.DataFrame:
        """测试数据集 DataFrame（stat 测试器用）：物化且 curated 读物化，否则实时构造。"""
        node = self._require_node("tester", name)
        tm = self._test_meta_dict(name)
        if tm["materialized"] and tm["curated"]:
            p = self.data_dir / "factor_test" / name / "data.parquet"
            if p.exists():
                return pl.read_parquet(p)
        return self._test_build(node)

    def test_add(self, name: str, factor: str, *, returns: str = "r",
                 groupby: str = "ic", marketcap: str = "fv",
                 factor_col: str | None = None, spec: dict | None = None,
                 **kw) -> dict:
        """创建测试数据集：依赖已注册 factor，校验 sample 视图含必需列。"""
        if not factor:
            raise ValueError("test add 需要 --factor <因子名>")
        fnode = self._require_node("factor", factor)
        fm = self._factor_meta_dict(factor)
        sample = fm["sample"]
        # 校验 sample 视图含测试必需列
        view_cols = {c["name"] for c in self._sample_view_cols(sample)}
        need = ["date", "sym", returns, groupby, marketcap]
        missing = [c for c in need if c not in view_cols]
        if missing:
            raise ValueError(f"sample 缺少测试必需列 {missing}，不能创建测试数据集（需要 {need}）")
        spec_d = FactorTesterSpec.from_dict(spec or {}).to_dict() if spec else \
            {"quantiles": 5, "periods": [1, 5, 10],
             "date_range": ["2023-01-01", "2026-01-01"], "rolling_window": 252}
        TesterHandler.add(
            self.graph, name, factor,
            returns=returns, groupby=groupby, marketcap=marketcap,
            factor_col=factor_col or fm["factor_col"] or factor,
            spec=spec_d, sample=node_id("sample", sample),
            keys=list(fm["keys"]), **kw)
        return self._test_meta_dict(name)

    def test_get(self, name: str, *, where=None, limit=None, offset=None,
                 count_total: bool = False):
        lf = self._test_view_lf(name, where=where)
        return self._collect_page(lf, limit=limit, offset=offset, count_total=count_total)

    def test_meta(self, name: str) -> dict:
        return self._test_meta_dict(name)

    def test_list(self) -> list:
        return [self._test_meta_dict(n["name"]) for n in self.graph.list("tester")]

    def test_set(self, name: str, **kw) -> dict:
        self._require_node("tester", name)
        return self.graph.set("tester", name, **kw)

    def test_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("tester", name)
        self.graph.delete("tester", name, force=force)
        shutil.rmtree(self.data_dir / "factor_test" / name, ignore_errors=True)
        return {"deleted": name}

    def test_check(self, name: str) -> dict:
        """校验测试数据集：构造成功、含必需列、行数 > 0。"""
        node = self._require_node("tester", name)
        keys = list(node.get("keys") or ())
        try:
            df = self._test_build(node)
        except Exception as e:
            return {"test": name, "ok": False, "rows": 0, "columns": list(keys),
                    "message": f"测试数据集构造失败: {e}"}
        need = ["date", "sym", "sample", "returns", "group", "marketcap",
                "factor", "factor_quantile"]
        missing = [c for c in need if c not in df.columns]
        if missing:
            return {"test": name, "ok": False, "rows": df.height,
                    "columns": list(df.columns), "message": f"结果集缺少必需列: {missing}"}
        if df.height == 0:
            return {"test": name, "ok": False, "rows": 0,
                    "columns": list(df.columns), "message": "结果行数为 0"}
        return {"test": name, "ok": True, "rows": df.height,
                "columns": list(df.columns), "message": f"有效（{df.height} 行）"}

    def test_update(self, name: str | None = None, *, all: bool = False,
                    resync: bool = False) -> dict | list[dict]:
        """test 更新：传导检查上游（factor 全链）就绪 → 物化 factor_tests/<name>/。

        幂等；update 成功后节点置 valid=True。scan 为旧名别名。
        """
        if all:
            return [self._test_scan_one(n["name"], resync=resync)
                    for n in self.graph.list("tester")]
        if not name:
            raise ValueError("test update 需要测试集名（或 --all）")
        self.graph.assert_ready("tester", name)
        return self._test_scan_one(name, resync=resync)

    def test_scan(self, name: str | None = None, *, all: bool = False,
                  resync: bool = False) -> dict | list[dict]:
        """旧名别名（V3 语义改称 update）。"""
        return self.test_update(name, all=all, resync=resync)

    def _test_scan_one(self, name: str, *, resync: bool = False) -> dict:
        node = self._require_node("tester", name)
        extra = dict(node.get("extra") or {})
        cur_hash = self._test_hash(node)
        spec = self._test_spec(node)
        version_before = node.get("version", 0)
        # 幂等仅当节点有效：上游变化置脏（valid=False）后 update 必须强制重建
        if not resync and node.get("valid") \
                and extra.get("dependency_hash") == cur_hash \
                and extra.get("materialized"):
            return {"name": name, "version_before": version_before,
                    "version_after": version_before, "materialized": True,
                    "changed": False, "rows": 0, "quantiles": spec.quantiles,
                    "periods": list(spec.periods)}
        df = self._test_build(node)
        out_dir = self.data_dir / "factor_test" / name
        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_dir / "data.parquet")
        cols = [{"name": c, "display_name": c, "data_type": str(t)}
                for c, t in zip(df.columns, df.dtypes)]
        self.graph.set("tester", name, materialized=True, materialized_at=_now_iso(),
                       dependency_hash=cur_hash, columns=cols)
        self.store.patch_node(node_id("tester", name), valid=True)
        version_after = self.store.get_node(node_id("tester", name))["version"]
        return {"name": name, "version_before": version_before,
                "version_after": version_after, "materialized": True, "changed": True,
                "rows": df.height, "quantiles": spec.quantiles,
                "periods": list(spec.periods)}


def get_fieldset_engine(name: str):
    from ..fieldset.engine import get_engine

    return get_engine(name)


def get_sample_engine(name: str):
    from ..sample.engine import get_engine

    return get_engine(name)


def get_feature_engine(name: str):
    from ..feature.engine import get_engine

    return get_engine(name)


__all__ = ["GraphService"]
