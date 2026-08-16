"""GraphService：V3.0 资产图服务（登记/依赖/版本/事件走 graph，物理指纹 graph.db 普通表）。

**分层**：本类只承载**图交互与共享基础设施**——
- 图能力：节点/边/版本/事件（``_scan_disk``/``_change_events``/``_resolve_col_meta``/
  ``_upstream_scope``/``update_cascade`` …）与通用读取（``_read_lazy``/``_get_data``/
  ``_collect_page``）；
- 对外 API：table/index/panel/fieldset/sample/feature/factor/tester 各资产的公共
  方法**仅薄委托**到对应资产模块的 ``ops.py``（业务实现全部在资产模块内，如
  ``table/ops.py`` / ``panel/ops.py``；示例：``panel_add`` 实现见 ``panel/ops.py``），
  保持 Execute/CLI/测试既有调用不变；跨模块共享的视图/计算能力（``_panel_lazy`` 等）
  也经本类的薄委托调用，便于统一入口与测试 monkeypatch。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ..jsonutil import dumps_str, loads
from ..settings import load_config
from ..storage import (columns_union, detect_layout, diff_files, disk_files,
                       footer, partition_of, prune_files, scan, signature, to_expr)
from ..table.errors import DEFAULT_IGNORE_COLS
from .controller import GraphController
from .errors import AssetNotFoundError, CycleError
from .events import DataChangeEvent
from .model import ColumnMeta, column_node_id, node_id, split_node_id
from .store import GraphStore
from .version import now_iso
from ..table import ops as table_ops
from ..index import ops as index_ops
from ..panel import ops as panel_ops
from ..fieldset import ops as fieldset_ops
from ..sample import ops as sample_ops
from ..feature import ops as feature_ops
from ..factor import ops as factor_ops
from ..factor_tester import ops as tester_ops


_ASSET_TYPES = ("table", "index", "panel", "fieldset", "sample", "feature",
                "factor", "tester")

#: 变化文件行数超过该值时不再读列 distinct（symbol_scope 回退分区值/全集）——
#: 大文件全量重写场景读全列代价远大于按标的裁剪的收益；真实日更增量文件
#: （每天一个文件、几千到几万行）远低于该阈值，不受影响
_SYMBOL_SCAN_LIMIT = 500_000


class GraphService:
    """统一资产服务：图交互 + 各资产公共 API 薄委托（业务实现见各资产模块 ops.py）。"""

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

    @classmethod
    def open_graph_store(cls, data_dir: Path | str | None = None) -> GraphStore | None:
        """只读打开资产图库（§13：dispatch graph 命令复用同一打开逻辑/命名回退）。

        缺省 data_dir 取配置；库文件（catalog.db / 旧 graph.db）不存在返回 None
        （命令方输出空图），不做建目录/建库副作用。
        """
        if data_dir is None:
            data_dir = load_config().data_dir
        root = Path(data_dir).expanduser()
        db = cls._db_path(root)
        return GraphStore(str(db)) if db.exists() else None

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
            raise AssetNotFoundError(f"{asset_type} not registered: {name}")
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
        disk = disk_files(root)
        consistent = bool(node.get("signature")) and \
            node["signature"] == signature(disk) if root.exists() else True
        out = {
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
        # index 专属键（symbol/datetime 列 + 物化分区粒度）并入 meta 顶层
        for k in ("symbol_col", "datetime_col", "materialize_partition"):
            if node.get(k):
                out[k] = node[k]
        return out

    def _scan_disk(self, asset_type: str, name: str, *, meta: dict | None = None,
                   extra_data: dict | None = None,
                   col_meta: dict[str, dict] | None = None) -> dict:
        """核心：列目录 → 对账 → 有差异才扫 footer → 指纹 + 节点更新（幂等）。

        - 首次（隐式登记）：graph.add 建节点（版本 v1）；差异必然存在 → 写指纹 + 列
        - 非首次且变化：指纹替换 + 节点 patch + ``notify_change``（铸版本 + 下游置脏）
        - 无变化：不 bump 版本
        - ``col_meta``：列级元数据覆盖（dbt manifest 等），按列名合并进登记列
          （只由 add 传入；update 走本方法时不传，已有列说明保持不变）
        """
        root = self._asset_root(asset_type, name)
        if not root.exists():
            raise AssetNotFoundError(f"{asset_type} dir not found: {root}")
        disk = disk_files(root)
        nid = node_id(asset_type, name)
        node = self.store.get_node(nid)
        implicit = node is None
        version_before = 0 if implicit else node["version"]
        cat = self.store.fingerprint_get(nid) if node else {}
        stats = self.store.fingerprint_stats(nid) if cat else {}
        diffs = diff_files(disk, cat)
        layout, pkeys = detect_layout([f.rel_path for f in disk])
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
                        ftr = footer(root / f.rel_path)
                    payload.append((f, ftr, partition_of(f.rel_path)))

                items = [
                    (part, f.rel_path, ftr["row_count"], ftr["file_bytes"], f.size, f.mtime_ns,
                     dumps_str(ftr["schema"]), ftr["stats"])
                    for f, ftr, part in payload
                ]
                self.store.fingerprint_replace(nid, items)

                old_cols = {c["name"]: c for c in (node.get("columns") or [])}
                new_cols = []
                for c in columns_union([(f.rel_path, ftr) for f, ftr, _ in payload],
                                         self.ignore_cols):
                    prev = dict(old_cols.get(c.name, {}))
                    prev.update({k: v for k, v in c.to_dict().items() if v is not None or k == "is_tool"})
                    if col_meta and c.name in col_meta:
                        prev.update(col_meta[c.name])
                    new_cols.append(prev)

                base = {}
                if implicit and meta:
                    for k, v in meta.items():
                        if k == "tags":
                            if isinstance(v, str):
                                base["tags"] = [t.strip() for t in v.split(",") if t.strip()]
                            else:
                                base["tags"] = [str(t).strip() for t in v
                                                if str(t).strip()]
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
                         "signature": signature(disk),
                         "update_time": now_iso()}
                self.store.patch_node(nid, **patch)
                # 列节点图对账：源头列（table/index）随登记/重扫同步
                self.graph.sync_columns(asset_type, name, new_cols)
                if not implicit:
                    # 物理数据变化 → 范围化事件入日志（upsert/delete）+ 下游置脏
                    for ev in self._change_events(asset_type, name, diffs, cat):
                        self.graph.notify_change(asset_type, name, event=ev)
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

    def _change_events(self, asset_type: str, name: str, diffs: list,
                       cat: dict[str, dict]) -> list[DataChangeEvent]:
        """物理变化 → 范围化事件（V3.0 设计语义，P0-1 + symbol_scope）。

        - added/changed 文件 → ``action="upsert"``；removed 文件 → ``action="delete"``；
        - ``datetime_scope``：hive 分区路径含 ``<datetime_col>=<v>`` 时直接用分区值；
          其余从变化文件 footer 的 datetime 列 min/max 提取（只读元数据不读数据页）；
          removed 文件已不在磁盘，用 catalog 指纹的 ``partition_path`` 提取分区值，
          取不到则范围 None（全集，保守）；
        - ``symbol_scope``：资产登记了 ``symbol_col``（index）时提取——hive 分区键
          ``<symbol_col>=<v>`` 直取分区值；否则读变化文件该列的 distinct 值
          （读数据页，P2）；removed 文件取不到分区值回退 None（全集）；未登记
          symbol_col 的资产（table 等）恒 None；
        - ``field_scope``：None（文件全部列）。
        """
        node = self.store.get_node(node_id(asset_type, name)) or {}
        datetime_col = node.get("datetime_col", "date")
        symbol_col = node.get("symbol_col") or ""
        root = self._asset_root(asset_type, name)

        def partition_values(rel: str, part_path: str, key: str) -> list[str]:
            vals = []
            for seg in (part_path or partition_of(rel)).split("/"):
                if not seg:
                    continue
                k, _, v = seg.partition("=")
                if k == key and v:
                    vals.append(v)
            return vals

        def scope_for(rel: str, part_path: str) -> list[str] | None:
            scope = partition_values(rel, part_path, datetime_col)
            if (root / rel).exists():  # added/changed：footer min/max
                try:
                    st = footer(root / rel)["stats"].get(datetime_col)
                    if st:
                        lo, hi = st[1], st[2]
                        if lo is not None and hi is not None:
                            scope.extend([lo, hi])
                except Exception:
                    pass
            return list(dict.fromkeys(s for s in scope if s is not None)) or None

        def symbols_for(rel: str, part_path: str) -> list[str] | None:
            """变化文件的标的集合：symbol 分区键直取；小文件读该列 distinct（数据页）。

            大文件（全量覆盖重写场景，行数超过 ``_SYMBOL_SCAN_LIMIT``）不再读列
            distinct——读 2000 万行的列开销远大于裁剪收益（此时几乎所有标的都
            变了，回退为分区值/全集仍正确，只是增量不按标的裁剪）。
            """
            if not symbol_col:
                return None
            vals = partition_values(rel, part_path, symbol_col)
            f = root / rel
            if f.exists():
                try:
                    ftr = footer(f)
                    if ftr.get("row_count", 0) <= _SYMBOL_SCAN_LIMIT:
                        lf = scan(f, exclude=())
                        if symbol_col in lf.collect_schema().names():
                            vals.extend(lf.select(
                                pl.col(symbol_col).cast(pl.String).unique())
                                .collect().to_series().to_list())
                except Exception:
                    pass
            return list(dict.fromkeys(v for v in vals if v is not None)) or None

        upsert_scope: list[str] = []
        delete_scope: list[str] = []
        upsert_symbols: list[str] = []
        delete_symbols: list[str] = []
        for d in diffs:
            part = (cat.get(d.rel_path) or {}).get("partition_path") or ""
            sc = scope_for(d.rel_path, part)
            syms = symbols_for(d.rel_path, part)
            if d.kind == "removed":
                if sc is not None:
                    delete_scope.extend(sc)
                if syms is not None:
                    delete_symbols.extend(syms)
            else:
                if sc is not None:
                    upsert_scope.extend(sc)
                if syms is not None:
                    upsert_symbols.extend(syms)
        upsert_scope = list(dict.fromkeys(upsert_scope))
        delete_scope = list(dict.fromkeys(delete_scope))
        had_upsert = any(d.kind != "removed" for d in diffs)
        had_delete = any(d.kind == "removed" for d in diffs)

        def interval(vals: list[str]) -> list[str] | None:
            vals = [v for v in vals if v is not None]
            return [min(vals), max(vals)] if vals else None

        events = []
        # 有增改文件 → 至少一个 upsert 事件（范围取不到时为全集 None，保证下游置脏）；
        # datetime_scope 统一为 [min, max] 区间（字符串/ISO 字典序可比），
        # symbol_scope 为变化标的并集（None=全集不裁剪），供增量物化过滤
        if upsert_scope or had_upsert:
            events.append(DataChangeEvent(
                action="upsert",
                symbol_scope=list(dict.fromkeys(upsert_symbols)) or None,
                datetime_scope=interval(upsert_scope), field_scope=None))
        if delete_scope or had_delete:
            events.append(DataChangeEvent(
                action="delete",
                symbol_scope=list(dict.fromkeys(delete_symbols)) or None,
                datetime_scope=interval(delete_scope), field_scope=None))
        return events

    def _ensure_fresh(self, asset_type: str, name: str) -> None:
        """读前快检：签名一致则继续；不一致自动 scan（未登记则隐式注册）。"""
        root = self._asset_root(asset_type, name)
        if not root.exists():
            return
        node = self.store.get_node(node_id(asset_type, name))
        disk_sig = signature(disk_files(root))
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
        lf = scan(paths)
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

    def _manifest_meta(self, name: str) -> tuple[dict, dict]:
        """dbt manifest 元数据：返回 (资产级 meta, 列级 col_meta)。

        未配置 ``dbt-manifest-file`` 或 manifest 中无匹配节点 → 空；
        配置了但文件缺失/解析失败 → 抛错（配置错误显式暴露）。
        """
        from ..dbt import asset_meta, column_meta, find_node, manifest_path

        p = manifest_path()
        if p is None:
            return {}, {}
        node = find_node(p, name)
        if node is None:
            return {}, {}
        return asset_meta(node), column_meta(node)

    # =====================================================================
    # table（业务实现见 table/ops.py——本层仅薄委托，保持 Execute/CLI/测试 API）
    # =====================================================================

    def table_add(self, name: str, *, all: bool = False, meta: dict | None = None):
        return table_ops.table_add(self, name, all=all, meta=meta)

    def table_get(self, name: str, *, columns=None, where=None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        return table_ops.table_get(self, name, columns=columns, where=where,
                                   partition=partition, exclude_tool=exclude_tool,
                                   limit=limit, offset=offset, count_total=count_total)

    def table_lazy(self, name: str, *, where=None, exclude_tool: bool = False) -> pl.LazyFrame:
        """table 读取 lazy 视图（stat 等下游消费）。"""
        return table_ops.table_lazy(self, name, where=where, exclude_tool=exclude_tool)

    def table_meta(self, name: str) -> dict:
        return table_ops.table_meta(self, name)

    def table_list(self, *, candidate: bool = False) -> list:
        return table_ops.table_list(self, candidate=candidate)

    def table_set(self, name: str, **kw) -> dict:
        return table_ops.table_set(self, name, **kw)

    def table_col(self, name: str, column: str, **kw) -> dict:
        return table_ops.table_col(self, name, column, **kw)

    def table_delete(self, name: str, *, force: bool = False) -> dict:
        return table_ops.table_delete(self, name, force=force)

    def table_update(self, name: str, *, all: bool = False) -> dict | list:
        return table_ops.table_update(self, name, all=all)

    def table_data_key(self, name: str) -> str:
        return table_ops.table_data_key(self, name)

    # =====================================================================
    # index（业务实现见 index/ops.py——本层仅薄委托）
    # =====================================================================

    def index_add(self, name: str, *, all: bool = False, symbol_col: str = "sym",
                  datetime_col: str = "date", materialize_partition: str = "yearly",
                  meta: dict | None = None) -> dict | list:
        return index_ops.index_add(self, name, all=all, symbol_col=symbol_col,
                                   datetime_col=datetime_col,
                                   materialize_partition=materialize_partition,
                                   meta=meta)

    def index_get(self, name: str, *, columns=None, where=None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        return index_ops.index_get(self, name, columns=columns, where=where,
                                   partition=partition, exclude_tool=exclude_tool,
                                   limit=limit, offset=offset, count_total=count_total)

    def index_meta(self, name: str) -> dict:
        return index_ops.index_meta(self, name)

    def index_list(self, *, candidate: bool = False) -> list:
        return index_ops.index_list(self, candidate=candidate)

    def index_set(self, name: str, **kw) -> dict:
        return index_ops.index_set(self, name, **kw)

    def index_col(self, name: str, column: str, **kw) -> dict:
        return index_ops.index_col(self, name, column, **kw)

    def index_delete(self, name: str, *, force: bool = False) -> dict:
        return index_ops.index_delete(self, name, force=force)

    def index_update(self, name: str, *, all: bool = False) -> dict | list:
        return index_ops.index_update(self, name, all=all)

    def index_data_key(self, name: str) -> str:
        return index_ops.index_data_key(self, name)

    # =====================================================================
    # panel（业务实现见 panel/ops.py——本层仅薄委托；_panel_lazy 等视图/计算
    # 能力也在 panel/ops.py，fieldset/sample/factor/tester 模块经 svc 委托调用）
    # =====================================================================

    def panel_add(self, name: str, index: str,
                  tables: dict[str, str] | list | tuple | None = None, **kw) -> dict:
        return panel_ops.panel_add(self, name, index, tables=tables, **kw)

    def panel_meta(self, name: str) -> dict:
        return panel_ops.panel_meta(self, name)

    def panel_list(self) -> list:
        return panel_ops.panel_list(self)

    def panel_set(self, name: str, **kw) -> dict:
        return panel_ops.panel_set(self, name, **kw)

    def panel_update(self, name: str) -> dict:
        return panel_ops.panel_update(self, name)

    def panel_delete(self, name: str, *, force: bool = False) -> dict:
        return panel_ops.panel_delete(self, name, force=force)

    def panel_get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        return panel_ops.panel_get(self, name, columns=columns, where=where,
                                   partition=partition, exclude_tool=exclude_tool,
                                   limit=limit, offset=offset, count_total=count_total)

    def panel_lazy(self, name: str, *, where=None) -> pl.LazyFrame:
        """panel 实时 join 视图（lazy，stat 等下游消费）。"""
        return panel_ops.panel_lazy(self, name, where=where)

    # ---- 跨模块共享视图/计算能力（fieldset/sample/factor/tester 经 svc 调用）----

    def _panel_lazy(self, name: str, where: pl.Expr | str | None = None,
                    live: bool = False) -> tuple[pl.LazyFrame, list[str]]:
        """panel 视图（lazy）：物化且 curated 读物化，否则实时 join（实现见 panel/ops.py）。"""
        return panel_ops._panel_lazy(self, name, where=where, live=live)

    def _resolve_col_meta(self, asset_id: str, col: str) -> dict:
        """列元数据**引用解析**：沿 DERIVES 递归到定义点列节点，返回其完整 meta。

        定义点（保存完整元数据）：源头列（table/index）、fieldset 自建字段
        （带 formula）、factor 因子列、feature 公式定义；链路中间层
        （panel/sample/factor keys/tester 透传列）不重复存储——改源头列说明，
        下游 meta 自动反映。结构性覆盖（as_index/window_size）沿路径叠加。
        """
        cid = column_node_id(asset_id, col)
        node = self.store.get_node(cid)
        if node is None:
            return ColumnMeta.from_dict({"name": col}).to_dict()
        path = [node]
        seen = {cid}
        cur_id = cid
        while True:
            nxt_id = None
            for d in self.store.deps_of(cur_id, rel_type="DERIVES"):
                if d["target"] not in seen \
                        and self.store.get_node(d["target"]) is not None:
                    nxt_id = d["target"]
                    break
            if nxt_id is None:
                break
            nxt = self.store.get_node(nxt_id)
            path.append(nxt)
            seen.add(nxt_id)
            # 定义点终止：源头资产（table/index/feature）或带 formula 的字段/因子列
            if nxt.get("asset_type") in ("table", "index", "feature") \
                    or nxt.get("formula"):
                break
            cur_id = nxt_id
        src = path[-1]
        meta: dict = {"name": col}
        for k in ("display_name", "description", "data_type", "unit", "formula",
                  "tags", "validated"):
            v = src.get(k)
            if v is not None and v != "":
                meta[k] = v
        if not meta.get("display_name"):
            meta["display_name"] = col
        # 结构映射（source_table/source_field）从 DERIVES 第一跳推导——列节点的
        # 直接上游即其来源（panel.x ← index.x → source_table="index"）；源头
        # 列节点不存该信息（对源头无意义）
        first = self.store.deps_of(cid, rel_type="DERIVES")
        if first:
            tgt = self.store.get_node(first[0]["target"])
            if tgt is not None:
                src_asset = (tgt.get("asset") or "").split(":", 1)[-1]
                if src_asset:
                    meta["source_table"] = src_asset
                if tgt.get("name"):
                    meta["source_field"] = tgt["name"]
        meta["as_index"] = any(bool(p.get("as_index")) for p in path)
        meta["window_size"] = max(
            (int(p.get("window_size") or 0) for p in path), default=0)
        return ColumnMeta.from_dict(meta).to_dict()

    def _require_materialized(self, asset_type: str, name: str, meta: dict) -> Path:
        """物化型资产 get 门控（第 3 态）：本应物化但未物化（或已过期）→ 报错提示先 update。

        已物化（materialized 且 curated）→ 返回物化目录；调用方读物化 parquet。
        """
        dirs = {"panel": "panel", "fieldset": "fieldset",
                "factor": "factor", "tester": "factor_tester"}
        root = self.data_dir / dirs[asset_type] / name
        if not meta.get("materialized") or not meta.get("curated"):
            raise ValueError(
                f"{asset_type} {name} 未物化（或物化已过期）: "
                f"请先执行 {asset_type} update {name} 物化后再读取")
        return root

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

    # =====================================================================
    # fieldset（业务实现见 fieldset/ops.py——本层仅薄委托）
    # =====================================================================

    def fieldset_add(self, name: str, panel: str, *, engine: str = "polars", **kw) -> dict:
        return fieldset_ops.fieldset_add(self, name, panel, engine=engine, **kw)

    def fieldset_add_field(self, name: str, field: str, formula: str, **kw) -> dict:
        return fieldset_ops.fieldset_add_field(self, name, field, formula, **kw)

    def fieldset_set_field(self, name: str, field: str, **kw) -> dict:
        return fieldset_ops.fieldset_set_field(self, name, field, **kw)

    def fieldset_delete_field(self, name: str, field: str) -> dict:
        return fieldset_ops.fieldset_delete_field(self, name, field)

    def fieldset_meta_field(self, name: str, field: str) -> dict:
        return fieldset_ops.fieldset_meta_field(self, name, field)

    def fieldset_meta(self, name: str) -> dict:
        return fieldset_ops.fieldset_meta(self, name)

    def fieldset_list(self) -> list:
        return fieldset_ops.fieldset_list(self)

    def fieldset_set(self, name: str, **kw) -> dict:
        return fieldset_ops.fieldset_set(self, name, **kw)

    def fieldset_delete(self, name: str, *, force: bool = False) -> dict:
        return fieldset_ops.fieldset_delete(self, name, force=force)

    def fieldset_check(self, name: str, field: str) -> dict:
        return fieldset_ops.fieldset_check(self, name, field)

    def fieldset_get(self, name: str, *, fields_only: bool = False,
                     columns: list[str] | None = None, where=None,
                     limit=None, offset=None, count_total: bool = False):
        return fieldset_ops.fieldset_get(self, name, fields_only=fields_only,
                                         columns=columns, where=where,
                                         limit=limit, offset=offset,
                                         count_total=count_total)

    def fieldset_update(self, name: str, *, resync: bool = False) -> dict:
        return fieldset_ops.fieldset_update(self, name, resync=resync)

    def fieldset_test(self, name: str, formula: str):
        return fieldset_ops.fieldset_test(self, name, formula)

    # ---- 跨模块共享视图能力（sample/factor 经 svc 调用）----

    def _panel_keys(self, panel: str) -> list[str]:
        return fieldset_ops._panel_keys(self, panel)

    def _fieldset_view_lf(self, name: str, *, fields_only: bool = False,
                          where: pl.Expr | str | None = None) -> tuple[pl.LazyFrame, list[str]]:
        """fieldset 视图（实现见 fieldset/ops.py）。"""
        return fieldset_ops._fieldset_view_lf(self, name, fields_only=fields_only,
                                              where=where)

    def _fieldset_view_col_names(self, fieldset: str) -> list[str]:
        """fieldset 视图列名（实现见 fieldset/ops.py）。"""
        return fieldset_ops._fieldset_view_col_names(self, fieldset)

    # =====================================================================
    # sample（业务实现见 sample/ops.py——本层仅薄委托）
    # =====================================================================

    def sample_add(self, name: str, fieldset: str, index: str, **kw) -> dict:
        return sample_ops.sample_add(self, name, fieldset, index, **kw)

    def sample_get(self, name: str, *, columns=None, where=None, limit=None,
                   offset=None, count_total: bool = False):
        return sample_ops.sample_get(self, name, columns=columns, where=where,
                                     limit=limit, offset=offset, count_total=count_total)

    def sample_check(self, name: str) -> dict:
        return sample_ops.sample_check(self, name)

    def sample_meta(self, name: str) -> dict:
        return sample_ops.sample_meta(self, name)

    def sample_list(self) -> list:
        return sample_ops.sample_list(self)

    def sample_set(self, name: str, **kw) -> dict:
        return sample_ops.sample_set(self, name, **kw)

    def sample_update(self, name: str) -> dict:
        return sample_ops.sample_update(self, name)

    def sample_delete(self, name: str, *, force: bool = False) -> dict:
        return sample_ops.sample_delete(self, name, force=force)

    # ---- 跨模块共享视图能力（factor/tester 经 svc 调用）----

    def _sample_view_lf(self, name: str, *, where=None) -> pl.LazyFrame:
        """sample 视图（实现见 sample/ops.py）。"""
        return sample_ops._sample_view_lf(self, name, where=where)

    def _sample_view_cols(self, sample: str) -> list[dict]:
        """sample 视图列元数据（实现见 sample/ops.py）。"""
        return sample_ops._sample_view_cols(self, sample)

    def _sample_keys(self, node: dict) -> list[str]:
        """sample 的索引列（实现见 sample/ops.py）。"""
        return sample_ops._sample_keys(self, node)

    # =====================================================================
    # feature（业务实现见 feature/ops.py——本层仅薄委托）
    # =====================================================================

    def feature_add(self, name: str, formula: str, *, engine: str = "polars",
                    unit: str | None = None, **kw) -> dict:
        return feature_ops.feature_add(self, name, formula, engine=engine,
                                       unit=unit, **kw)

    def feature_meta(self, name: str) -> dict:
        return feature_ops.feature_meta(self, name)

    def feature_list(self) -> list:
        return feature_ops.feature_list(self)

    def feature_set(self, name: str, **kw) -> dict:
        return feature_ops.feature_set(self, name, **kw)

    def feature_update(self, name: str) -> dict:
        return feature_ops.feature_update(self, name)

    def feature_delete(self, name: str, *, force: bool = False) -> dict:
        return feature_ops.feature_delete(self, name, force=force)

    def feature_test(self, name: str, sample: str):
        return feature_ops.feature_test(self, name, sample)

    # =====================================================================
    # 共享图能力（沿链找 index / 积累事件范围，各资产 update 与 meta 复用）
    # =====================================================================

    def _index_node(self, node: dict) -> dict | None:
        """沿血缘链找该资产依赖的 index 节点（Cypher 变长上游遍历，一次拿全链；
        取最接近的 index，不找 table）。"""
        nid = node.get("id") or node_id(node["type"], node["name"])
        for d in self.store.upstream(nid):
            if d["type"] == "index":
                return self.store.get_node(d["id"])
        return None

    def _index_name(self, node: dict) -> str:
        idx = self._index_node(node)
        return idx.get("name", "") if idx else ""

    def _upstream_scope(self, node: dict) -> tuple[list[str], list[str] | None] | None:
        """最近上游（**直接依赖**）积累事件的范围：``([lo, hi], symbols)``。

        datetime 取 [min, max] 区间（增量过滤用）；symbols 为未消费事件变化标的的
        并集（None=全集不裁剪）。沿链收集（不找最上游 table/index）：上游 update
        时已把消费的合并事件写入自身 version_list（resolve 语义），``_accumulated``
        按出边 required_version 水位取未消费事件；无事件 / 无明确范围 → None
        （全量重算）。
        """
        acc = self.graph._accumulated(node)
        scope: list[str] = []
        symbols: list[str] = []
        for ev in (acc.get("upsert"), acc.get("delete")):
            if ev:
                if ev.datetime_scope:
                    scope.extend(ev.datetime_scope)
                if ev.symbol_scope:
                    symbols.extend(ev.symbol_scope)
        scope = [s for s in scope if s is not None]
        if not scope:
            return None
        symbols = list(dict.fromkeys(symbols))
        return [min(scope), max(scope)], (symbols or None)

    # =====================================================================
    # factor（业务实现见 factor/ops.py——本层仅薄委托）
    # =====================================================================

    def factor_add(self, name: str, feature: str, sample: str, *,
                   engine: str = "polars", pipeline: str = "nothing()",
                   factor_col: str | None = None, **kw) -> dict:
        return factor_ops.factor_add(self, name, feature, sample, engine=engine,
                                     pipeline=pipeline, factor_col=factor_col, **kw)

    def factor_get(self, name: str, *, where=None, partition: str | None = None,
                   limit=None, offset=None, count_total: bool = False):
        return factor_ops.factor_get(self, name, where=where, partition=partition,
                                     limit=limit, offset=offset, count_total=count_total)

    def factor_meta(self, name: str) -> dict:
        return factor_ops.factor_meta(self, name)

    def factor_list(self) -> list:
        return factor_ops.factor_list(self)

    def factor_set(self, name: str, **kw) -> dict:
        return factor_ops.factor_set(self, name, **kw)

    def factor_delete(self, name: str, *, force: bool = False) -> dict:
        return factor_ops.factor_delete(self, name, force=force)

    def factor_check(self, name: str) -> dict:
        return factor_ops.factor_check(self, name)

    def factor_update(self, name: str | None = None, *, all: bool = False,
                      resync: bool = False) -> dict | list[dict]:
        return factor_ops.factor_update(self, name, all=all, resync=resync)

    # ---- 跨模块共享能力（tester 经 svc 调用）----

    def _factor_meta_dict(self, name: str) -> dict:
        """factor 元数据（实现见 factor/ops.py）。"""
        return factor_ops._factor_meta_dict(self, name)

    def _factor_hash(self, node: dict) -> str:
        """factor 物化签名（实现见 factor/ops.py）。"""
        return factor_ops._factor_hash(self, node)

    def _factor_compute(self, node: dict, *, partition: str | None = None,
                        dt_range: tuple[str, str] | None = None,
                        symbols: list[str] | None = None,
                        view_df: pl.DataFrame | None = None) -> pl.DataFrame:
        """实时计算因子（实现见 factor/ops.py）。"""
        return factor_ops._factor_compute(self, node, partition=partition,
                                          dt_range=dt_range, symbols=symbols,
                                          view_df=view_df)

    # =====================================================================
    # tester（业务实现见 factor_tester/ops.py——本层仅薄委托）
    # =====================================================================

    def tester_add(self, name: str, factor: str, *, returns: str = "r",
                   groupby: str = "ic", marketcap: str = "fv",
                   factor_col: str | None = None, spec: dict | None = None,
                   **kw) -> dict:
        return tester_ops.tester_add(self, name, factor, returns=returns,
                                     groupby=groupby, marketcap=marketcap,
                                     factor_col=factor_col, spec=spec, **kw)

    def tester_get(self, name: str, *, where=None, limit=None, offset=None,
                   count_total: bool = False):
        return tester_ops.tester_get(self, name, where=where, limit=limit,
                                     offset=offset, count_total=count_total)

    def tester_meta(self, name: str) -> dict:
        return tester_ops.tester_meta(self, name)

    def tester_list(self) -> list:
        return tester_ops.tester_list(self)

    def tester_set(self, name: str, **kw) -> dict:
        return tester_ops.tester_set(self, name, **kw)

    def tester_delete(self, name: str, *, force: bool = False) -> dict:
        return tester_ops.tester_delete(self, name, force=force)

    def tester_check(self, name: str) -> dict:
        return tester_ops.tester_check(self, name)

    def tester_data(self, name: str) -> pl.DataFrame:
        """测试数据集 DataFrame（stat 测试器用）。"""
        return tester_ops.tester_data(self, name)

    def tester_update(self, name: str | None = None, *, all: bool = False,
                      resync: bool = False) -> dict | list[dict]:
        return tester_ops.tester_update(self, name, all=all, resync=resync)
    # ---------- 沿链级联 update ----------

    def update_cascade(self, asset_type: str | None = None, name: str | None = None,
                       *, all: bool = False) -> dict:
        """沿链级联 update：按拓扑序更新目标节点及其全部下游链。

        - ``--node <type:name>``：更新该资产 + 其下游闭包（BFS 收集，含自身）；
          ``--all``：按拓扑序更新图中全部资产节点；
        - 每个节点都走各自 ``*_update``（内含 ``assert_ready`` 上游传导就绪
          检查），拓扑序保证任一节点更新时其上游都已就绪；某节点上游未就绪
          → ``DependencyError`` 中止（已更新的节点保持已更新，未更新的不动）；
        - 返回 ``{"node", "scope", "updated": [{"node", "version_before",
          "version_after", "result"}...]}``；``result`` 为对应 ``*_update`` 的
          返回值（各资产返回形态不一），``version_*`` 是统一的可比口径
          （版本未推进 = 该节点本次无真实变更）。
        """
        if all:
            nids = [node_id(t, n["name"])
                    for t in _ASSET_TYPES for n in self.graph.list(t)]
            scope = "all"
            center = "*"
        else:
            if not asset_type or not name:
                raise ValueError("update_cascade 需要 --node <type:name>（或 --all）")
            nid = node_id(asset_type, name)
            self._require_node(asset_type, name)
            nids = [nid] + [d["id"] for d in self.store.downstream(nid)]
            scope = "downstream"
            center = nid
        # 拓扑序：依赖方先于被依赖方（闭包内 DAG；guard 兜底防意外成环死循环）
        order: list[str] = []
        pending = set(nids)
        guard = 0
        while pending and guard < len(pending) * len(pending) + 10:
            guard += 1
            progressed = False
            for nid_ in list(pending):
                if any(d["target"] in pending for d in self.store.deps_of(nid_)):
                    continue  # 上游还在集合内，等其先更新
                order.append(nid_)
                pending.remove(nid_)
                progressed = True
            if not progressed:
                raise CycleError(
                    f"血缘图存在环或无法拓扑排序，级联 update 中止: {sorted(pending)}")
        updated = []
        for nid_ in order:
            t, n = split_node_id(nid_)
            before = self.store.get_node(nid_).get("version", 0)
            result = getattr(self, f"{t}_update")(n)
            after = self.store.get_node(nid_).get("version", 0)
            updated.append({"node": nid_, "version_before": before,
                            "version_after": after, "result": result})
        return {"node": center, "scope": scope, "updated": updated}


__all__ = ["GraphService"]
