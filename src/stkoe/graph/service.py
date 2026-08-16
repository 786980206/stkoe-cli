"""GraphService：V3.0 资产统一服务（登记/依赖/版本走 graph，物理数据走 graph.db 指纹 + polars）。

替代 V2.0 table/dataset 等 controller 的 SQLite catalog 登记层：
- **登记/元数据/依赖/版本** → graph 节点/边（graphqlite，graph.db）
- **物理指纹**（stkoe_data_files / stkoe_file_stats）→ graph.db 普通表（同文件同事务）
- **物理数据**（parquet 扫描/读取/prune）→ 复用 table/util.py / table/query.py 纯函数

assets：``table`` / ``index``（独立主体）／``panel``。
"""
from __future__ import annotations

import hashlib
import re
import shutil
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from ..factor.engine import get_engine as get_factor_engine
from ..factor.engine import parse_pipeline
from ..factor_tester.spec import FactorTesterSpec
from ..factor_tester.tester import prepare_factor_data
from ..jsonutil import dumps_str, loads
from ..settings import load_config
from ..table.errors import DEFAULT_IGNORE_COLS, TableExistsError
from ..table import util as T
from ..table.query import prune_files, to_expr
from .controller import GraphController
from .errors import AssetNotFoundError, CycleError
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
from .model import ColumnMeta, FieldMeta, node_id, split_node_id
from .store import GraphStore


_ASSET_TYPES = ("table", "index", "panel", "fieldset", "sample", "feature",
                "factor", "tester")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _formula_refs(formula: str, candidates: set[str]) -> list[str]:
    """公式引用列：提取公式中的标识符，与候选列名（panel/sample 视图列）求交。

    只保留候选集合内的名字，避免把函数名/字面量误当列引用；保序去重。
    """
    return list(dict.fromkeys(m for m in _IDENT_RE.findall(formula or "")
                              if m in candidates))


def _expand_scope(scope, back: int = 0, forward: int = 0):
    """按窗口展开 datetime 区间：``[lo, hi] → [lo-back, hi+forward]``。

    滚动窗口语义（回看 w）：t 时刻输出用到 [t-w+1, t] 的输入 → 输入在 [lo, hi]
    变化时输出受影响范围是 **[lo, hi+w-1]**（向前延伸）；前向收益类窗口（如
    test 的 d{no}）则相反向后延伸 lo。非 ISO 日期/解析失败 → 原样返回。
    """
    if not scope or (not back and not forward):
        return scope
    try:
        lo = (date.fromisoformat(scope[0]) - timedelta(days=back)).isoformat()
        hi = (date.fromisoformat(scope[1]) + timedelta(days=forward)).isoformat()
    except (ValueError, TypeError):
        return scope
    return [lo, hi]


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
        disk = T.disk_files(root)
        consistent = bool(node.get("signature")) and \
            node["signature"] == T.signature(disk) if root.exists() else True
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
                         "signature": T.signature(disk),
                         "update_time": _now_iso()}
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
            for seg in (part_path or T.partition_of(rel)).split("/"):
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
                    st = T.footer(root / rel)["stats"].get(datetime_col)
                    if st:
                        lo, hi = st[1], st[2]
                        if lo is not None and hi is not None:
                            scope.extend([lo, hi])
                except Exception:
                    pass
            return list(dict.fromkeys(s for s in scope if s is not None)) or None

        def symbols_for(rel: str, part_path: str) -> list[str] | None:
            """变化文件的标的集合：symbol 分区键直取；否则读该列 distinct（数据页）。"""
            if not symbol_col:
                return None
            vals = partition_values(rel, part_path, symbol_col)
            f = root / rel
            if f.exists():
                try:
                    lf = pl.scan_parquet(f)
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

    def table_add(self, name: str, *, all: bool = False, meta: dict | None = None):
        """注册表（发现资产）：目录必须存在；已注册报 TableExistsError。

        配置了 ``dbt-manifest-file`` 时先应用 manifest 元数据（description/列说明等），
        参数显式指定的值（``--display_name/--description/...``）覆盖 manifest。
        """
        if all:
            if not self.tables_root.exists():
                return []
            out = []
            for d in sorted(x for x in self.tables_root.iterdir() if x.is_dir()):
                if self.store.get_node(node_id("table", d.name)) is None \
                        and any(d.rglob("*.parquet")):
                    m, cols = self._manifest_meta(d.name)
                    out.append(self._scan_disk(
                        "table", d.name, meta={**m, **(meta or {})}, col_meta=cols))
            return out
        if not name:
            raise ValueError("add 需要表名（或 --all 批量发现）")
        root = self._root(name)
        if not root.exists():
            raise AssetNotFoundError(f"table dir not found: {root}")
        if self.store.get_node(node_id("table", name)) is not None:
            raise TableExistsError(f"table already registered: {name} (use scan to refresh)")
        m, cols = self._manifest_meta(name)
        return self._scan_disk("table", name, meta={**m, **(meta or {})}, col_meta=cols)

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

    def _check_index_unique(self, name: str, *, symbol_col: str | None = None,
                            datetime_col: str | None = None) -> None:
        """校验 index 物理数据的 ``(symbol_col, datetime_col)`` 组合唯一（V3.0 设计
        ``IndexHandler.add``：index 是时间×标的的索引，不允许重复键）。"""
        node = self.store.get_node(node_id("index", name)) or {}
        sym = symbol_col or node.get("symbol_col") or "sym"
        dt = datetime_col or node.get("datetime_col") or "date"
        lf = pl.scan_parquet(self._index_root(name), hive_partitioning=True)
        total = lf.select(pl.len()).collect().item()
        uniq = lf.select(sym, dt).unique().select(pl.len()).collect().item()
        if uniq != total:
            raise ValueError(
                f"index {name} 的 ({sym}, {dt}) 组合不唯一: {total} 行 / {uniq} 组唯一"
                f"（index 要求 symbol+datetime 键唯一）")

    def index_add(self, name: str, *, all: bool = False, symbol_col: str = "sym",
                  datetime_col: str = "date", materialize_partition: str = "yearly",
                  meta: dict | None = None) -> dict | list:
        """注册 index（发现资产）：目录必须存在；已注册报 TableExistsError。

        ``--all`` 批量发现：扫描 ``index/`` 下未登记且含 parquet 的目录（同 table add --all）。
        单表登记前校验 ``(symbol, datetime)`` 组合唯一（V3.0 设计）。
        配置了 ``dbt-manifest-file`` 时先应用 manifest 元数据（参数显式指定覆盖）。
        """
        if all:
            if not self.indexs_root.exists():
                return []
            out = []
            for d in sorted(x for x in self.indexs_root.iterdir() if x.is_dir()):
                if self.store.get_node(node_id("index", d.name)) is None \
                        and any(d.rglob("*.parquet")):
                    m, cols = self._manifest_meta(d.name)
                    out.append(self._scan_disk(
                        "index", d.name, meta={**m, **(meta or {})}, col_meta=cols,
                        extra_data={"symbol_col": symbol_col, "datetime_col": datetime_col,
                                    "materialize_partition": materialize_partition}))
            return out
        if not name:
            raise ValueError("add 需要 index 名（或 --all 批量发现）")
        root = self._index_root(name)
        if not root.exists():
            raise AssetNotFoundError(f"index dir not found: {root}")
        if self.store.get_node(node_id("index", name)) is not None:
            raise TableExistsError(f"index already registered: {name}")
        self._check_index_unique(name, symbol_col=symbol_col, datetime_col=datetime_col)
        m, cols = self._manifest_meta(name)
        return self._scan_disk(
            "index", name, meta={**m, **(meta or {})}, col_meta=cols,
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
        """源头 index 更新：重扫对账（物理变化 → 版本递增 + 下游置脏）+ 唯一性校验。"""
        if all:
            out = [self._scan_disk("index", n["name"]) for n in self.graph.list("index")]
            for n in self.graph.list("index"):
                self._check_index_unique(n["name"])
            return out
        r = self._scan_disk("index", name)
        self._check_index_unique(name)
        return r

    def index_data_key(self, name: str) -> str:
        root = self._index_root(name)
        if not root.exists():
            return ""
        self._ensure_fresh("index", name)
        node = self.store.get_node(node_id("index", name))
        return node.get("signature", "") if node else T.signature(T.disk_files(root))

    # =====================================================================
    # panel（graph 节点 + DEPENDS 边；get 实时 join）
    # =====================================================================

    def panel_add(self, name: str, index: str,
                  tables: dict[str, str] | list | tuple | None = None, **kw) -> dict:
        """panel add：index 为已注册 Index 节点，tables 为已注册 table 节点。

        keys 由 index 推断（symbol_col + datetime_col，去空去重；兜底 sym/date），
        不再接受显式 ``--keys``（旧参数被忽略）。
        tables 支持 {表名: join}、[(表名, join)]、["表名:join" | "表名"] 混合；
        join 缺省 asof（可选 left），见 PanelHandler.add。
        列级血缘：panel 列 DERIVES → index/成员表列（DEPENDS 边 detail.columns）。
        """
        idx_node = self._require_node("index", index)
        keys = [c for c in (idx_node.get("symbol_col"), idx_node.get("datetime_col"))
                if c]
        keys = list(dict.fromkeys(keys)) or ["sym", "date"]
        kw.pop("keys", None)  # 忽略旧 --keys 参数，以 index 推断为准
        col_maps = {"index": {c["name"]: c["name"]
                              for c in (idx_node.get("columns") or [])}}
        for t in self._table_names(tables):
            tnode = self._require_node("table", t)
            col_maps[t] = {c["name"]: c["name"] for c in (tnode.get("columns") or [])
                           if c["name"] not in col_maps["index"]}
        return PanelHandler.add(self.graph, name, index,
                                tables=tables, keys=keys, column_maps=col_maps, **kw)

    @staticmethod
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

    def _panel_hash(self, node: dict) -> str:
        """panel 物化签名 = 上游 index/table 版本 + tables(join) + keys。"""
        index = node.get("index", "").split(":", 1)[1]
        parts = [f"index:{index}:{self._require_node('index', index).get('version', 0)}"]
        for t, j in (node.get("tables") or {}).items():
            parts.append(f"table:{t}:{self._require_node('table', t).get('version', 0)}:{j}")
        parts.append(f"keys:{','.join(node.get('keys') or ())}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def panel_meta(self, name: str) -> dict:
        node = self._require_node("panel", name)
        meta = self.graph._meta(node)
        extra = dict(meta.get("extra") or {})
        materialized = bool(node.get("materialized") or extra.get("materialized"))
        dep_hash = extra.get("dependency_hash") or ""
        meta["columns"] = self._panel_columns(node)
        meta["keys"] = list(node.get("keys") or ())
        meta["materialized"] = materialized
        meta["materialized_at"] = extra.get("materialized_at")
        meta["curated"] = materialized and dep_hash == self._panel_hash(node)
        keys = list(node.get("keys") or ())
        meta["partition_by"], meta["partition_gran"] = self._partition_plan(
            node, dt_col=keys[-1] if keys else "")
        meta["extra"] = extra
        return meta

    def panel_list(self) -> list:
        return [self.panel_meta(n["name"]) for n in self.graph.list("panel")]

    def panel_set(self, name: str, **kw) -> dict:
        self._require_node("panel", name)
        return self.graph.set("panel", name, **kw)

    def panel_update(self, name: str) -> dict:
        """panel 更新：传导检查上游（index/成员表）就绪 → join 视图物化落盘
        ``panel/<name>/``（分区布局**镜像 index**：分区 index → hive 目录，flat → 单文件）
        + 铸版本（积累事件）+ 边水位对齐。

        增量：源头积累事件有明确 datetime 区间且已有物化 → 只重算该区间（分区场景只
        替换受影响分区文件，flat 场景删区间+合并）；首次 / 无区间 → 全量物化。
        """
        self.graph.assert_ready("panel", name)
        node = self._require_node("panel", name)
        out_dir = self.data_dir / "panel" / name
        keys = list(node.get("keys") or ())
        dt = keys[-1] if keys else ""
        pkeys, gran = self._partition_plan(node, dt_col=dt)
        out_path = out_dir / ("data.parquet" if not pkeys else "")
        scope = self._upstream_scope(node)
        if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
            (lo, hi), syms = scope
            dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
            sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
            where = dt_expr & sym_expr if sym_expr is not None else dt_expr
            inc, _ = self._panel_lazy(name, where=where, live=True)
            df_inc = inc.collect()
            if pkeys:
                # 分区级增量：删区间涉及的桶并保留桶内区间外旧行 → 合并写回
                old = pl.read_parquet(out_dir, hive_partitioning=True)
                self._rewrite_buckets(old, df_inc, dt_expr, pkeys, out_dir, gran, dt,
                                      sym_expr=sym_expr)
            else:
                old = pl.read_parquet(out_path)
                keep = old.filter(~(dt_expr & sym_expr)) if sym_expr is not None \
                    else old.filter(~dt_expr)
                df = pl.concat([keep, df_inc], how="vertical_relaxed"
                               ).unique(subset=keys, keep="last").sort(keys)
                df.write_parquet(out_path)
            rows = df_inc.height
        else:
            joined, _ = self._panel_lazy(name, live=True)
            rows = joined.select(pl.len()).collect().item()
            self._write_partitioned(joined.collect(), out_dir, pkeys, gran=gran, dt_col=dt)
        m = self.graph.resolve("panel", name, extra={
            "dependency_hash": self._panel_hash(node),
            "materialized_at": _now_iso(),
        })
        return {"name": name, "valid": True, "materialized": True, "rows": rows,
                "version": m["version"]}

    def panel_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("panel", name)
        self.graph.delete("panel", name, force=force)
        shutil.rmtree(self.data_dir / "panel" / name, ignore_errors=True)
        return {"deleted": name}

    def _panel_lazy(self, name: str, where: pl.Expr | str | None = None,
                    live: bool = False) -> tuple[pl.LazyFrame, list[str]]:
        """panel 视图（lazy）：**物化且 curated 读物化 parquet**（下游沿链取上游物化），
        否则实时 join（index 为左表，member 表按各自 join 类型 on keys）。

        ``live=True``：强制实时 join（panel 自身重建时不能用旧物化）。
        """
        node = self._require_node("panel", name)
        keys = list(node.get("keys") or ())
        if not live:
            fm = self.panel_meta(name)
            root = self.data_dir / "panel" / name
            if fm["curated"] and (root / "data.parquet").exists() \
                    or (fm["curated"] and any(root.glob("*=*"))):
                lf = self._scan_materialized(root)
                if where is not None:
                    lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
                return lf, keys
        index = node.get("index", "").split(":", 1)[1]
        table_map = dict(node.get("tables") or {})  # {表名: join 类型}
        tables = list(table_map.keys())
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
        for i, t in enumerate(tables):
            f = frames[i + 1]
            if (table_map.get(t) or "asof_join") == "left_join":
                joined = joined.join(f, on=keys, how="left")
            else:
                joined = _asof_join(joined, f, keys)
        joined = joined.select(*[c["name"] for c in cols])
        if where is not None:
            joined = joined.filter(to_expr(where) if isinstance(where, str) else where)
        return joined, keys

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

    def panel_get(self, name: str, *, columns: list[str] | None = None,
                  where: pl.Expr | str | None = None, partition=None,
                  exclude_tool: bool = False, limit=None, offset=None,
                  count_total: bool = False):
        """panel 读取（第 1/3 态）：**已物化（curated）→ 读物化 parquet；
        未物化 → 报错提示先 update**（不再静默回退实时 join）。"""
        meta = self.panel_meta(name)
        root = self._require_materialized("panel", name, meta)
        lf = self._scan_materialized(root)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return self._collect_page(lf, columns=columns, exclude_tool=exclude_tool,
                                  limit=limit, offset=offset, count_total=count_total)

    def panel_lazy(self, name: str, *, where=None) -> pl.LazyFrame:
        """panel 实时 join 视图（lazy，stat 等下游消费）。"""
        return self._panel_lazy(name, where)[0]

    # =====================================================================
    # fieldset（衍生指标集：graph 登记；check/update 用 panel 视图 + 公式引擎）
    # =====================================================================

    def _fieldset_hash(self, node: dict) -> str:
        """fieldset 物化签名 = panel 版本 + 已校验字段公式/窗口 + engine。"""
        panel = node.get("panel", "").split(":", 1)[1]
        parts = [f"panel:{panel}:{self._require_node('panel', panel).get('version', 0)}"]
        for fname, f in (node.get("fields") or {}).items():
            if f.get("validated"):
                parts.append(f"{fname}:{f.get('formula', '')}:{f.get('window_size', 0)}")
        parts.append(f"engine:{node.get('engine', 'polars')}")
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _fieldset_meta_node(self, name: str) -> dict:
        node = self._require_node("fieldset", name)
        meta = self.graph._meta(node)
        extra = dict(meta.get("extra") or {})
        keys = self._panel_keys(node.get("panel", "").split(":", 1)[1])
        materialized = bool(node.get("materialized") or extra.get("materialized"))
        dep_hash = extra.get("dependency_hash") or ""
        meta["keys"] = keys
        meta["materialized"] = materialized
        meta["materialized_at"] = extra.get("materialized_at")
        meta["curated"] = materialized and dep_hash == self._fieldset_hash(node)
        meta["extra"] = extra
        return meta

    def _panel_keys(self, panel: str) -> list[str]:
        pnode = self._require_node("panel", panel)
        return list(pnode.get("keys") or ())

    def _fieldset_view_lf(self, name: str, *, fields_only: bool = False,
                          where: pl.Expr | str | None = None) -> tuple[pl.LazyFrame, list[str]]:
        """fieldset 视图：panel 全列 + 已校验衍生字段（fields_only 时仅 keys+字段）。

        物化且 curated → 衍生字段读物化 parquet（fields_only 直接返回；
        否则与 panel 视图 join）。
        """
        node = self._require_node("fieldset", name)
        panel = node.get("panel", "").split(":", 1)[1]
        keys = self._panel_keys(panel)
        fm = self._fieldset_meta_node(name)
        root = self.data_dir / "fieldset" / name
        if fm["curated"] and (root / "data.parquet").exists() \
                or (fm["curated"] and any(root.glob("*=*"))):
            lf = self._scan_materialized(root)
            if where is not None:
                lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
            if fields_only:
                return lf, keys
            base, _ = self._panel_lazy(panel)
            return base.join(lf, on=keys, how="left"), keys
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
        """fieldset add：面板 keys 透传列级血缘（fieldset keys DERIVES → panel keys）。"""
        pnode = self._require_node("panel", panel)
        keys = list(pnode.get("keys") or ())
        col_maps = {"panel": {k: k for k in keys}}
        return FieldsetHandler.add(self.graph, name, panel, engine=engine,
                                   column_maps=col_maps, **kw)

    def fieldset_add_field(self, name: str, field: str, formula: str, **kw) -> dict:
        if not formula:
            raise ValueError("fieldset add_field 需要 formula")
        r = FieldsetHandler.add_field(self.graph, name, field, formula, **kw)
        # 列级血缘：字段列 DERIVES → 公式引用的 panel 列（或同集字段）
        self._sync_fieldset_field_derives(name, field, formula)
        return r

    def fieldset_set_field(self, name: str, field: str, **kw) -> dict:
        node = self._require_node("fieldset", name)
        old_formula = ((node.get("fields") or {}).get(field) or {}).get("formula")
        r = FieldsetHandler.set_field(self.graph, name, field, **kw)
        if "formula" in kw and kw["formula"] != old_formula:
            self.graph.clear_derives("fieldset", name, field)
            self._sync_fieldset_field_derives(name, field, kw["formula"])
        return r

    def fieldset_delete_field(self, name: str, field: str) -> dict:
        # 字段列节点由 set(fields=...) 对账清理（无 DERIVES 引用的孤立节点删除）
        return FieldsetHandler.delete_field(self.graph, name, field)

    def _fieldset_ref_cols(self, name: str) -> tuple[set[str], set[str]]:
        """fieldset 公式可引用列：panel 视图列 ∪ 本 fieldset 已定义字段名。"""
        node = self._require_node("fieldset", name)
        panel = node.get("panel", "").split(":", 1)[-1]
        pnode = self._require_node("panel", panel)
        pcols = {c["name"] for c in self._panel_columns(pnode)}
        ffields = set((node.get("fields") or {}).keys())
        return pcols, ffields

    def _sync_fieldset_field_derives(self, name: str, field: str,
                                     formula: str) -> list[str]:
        """字段列级血缘：字段列 DERIVES → 公式引用的 panel 列 / 同集字段列。

        同时把引用列写回字段 meta 的 ``required_fields``（派生信息，不额外置脏）。
        """
        node = self._require_node("fieldset", name)
        panel = node.get("panel", "").split(":", 1)[-1]
        pcols, ffields = self._fieldset_ref_cols(name)
        refs = _formula_refs(formula, pcols | ffields)
        to_panel = [r for r in refs if r in pcols]
        if to_panel:
            self.graph.sync_derives("fieldset", name, "panel", panel,
                                    {field: to_panel})
        to_fields = [r for r in refs if r in ffields]
        if to_fields:
            self.graph.sync_derives("fieldset", name, "fieldset", name,
                                    {field: to_fields})
        fields = dict(node.get("fields") or {})
        cur = fields.get(field)
        if cur is not None and list(cur.get("required_fields") or ()) != refs:
            fields[field] = {**cur, "required_fields": refs}
            # required_fields 是 formula 的派生信息（与 validated 同属状态更新）：
            # add_field/set_field 已按定义变化置脏过自身与下游，此处不重复置脏
            self.graph.set("fieldset", name, definition=True, fields=fields,
                           self_invalidate=False)
        return refs

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
            raise AssetNotFoundError(f"field not found: {field}")
        base, keys = self._panel_lazy(node.get("panel", "").split(":", 1)[1])
        engine = get_fieldset_engine(node.get("engine") or "polars")
        ok, message = engine.check(base, FieldMeta.from_dict(fields[field]))
        if ok and not fields[field].get("validated"):
            new_fields = dict(fields)
            new_fields[field] = {**fields[field], "validated": True}
            # validated 写回是状态更新（非定义变化）→ 不使自身失效
            self.graph.set("fieldset", name, definition=True,
                           fields=new_fields, self_invalidate=False)
        return {"fieldset": name, "field": field, "ok": ok, "message": message}

    def fieldset_get(self, name: str, *, fields_only: bool = False,
                     columns: list[str] | None = None, where=None,
                     limit=None, offset=None, count_total: bool = False):
        """fieldset 读取（第 1/3 态）：已物化 → 物化字段（+ panel 合并）；未物化 → 报错。"""
        node = self._require_node("fieldset", name)
        meta = self.fieldset_meta(name)
        root = self._require_materialized("fieldset", name, meta)
        lf = self._scan_materialized(root)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        if not fields_only:
            panel = node.get("panel", "").split(":", 1)[1]
            keys = self._panel_keys(panel)
            base, _ = self._panel_lazy(panel)  # 上游 panel 物化或实时（内部视图）
            lf = base.join(lf, on=keys, how="left")
        return self._collect_page(lf, columns=columns, limit=limit, offset=offset,
                                  count_total=count_total)

    def fieldset_update(self, name: str, *, resync: bool = False) -> dict:
        """fieldset 更新：传导检查上游（panel 链）就绪 → 衍生字段物化落盘
        ``fieldset/<name>/data.parquet``（keys + 已校验字段）+ 铸版本 + 水位对齐。

        增量：源头积累事件有明确 datetime 区间且已有物化 → 只重算该区间字段并合并写回；
        首次 / 无区间 / ``--resync`` → 全量。
        """
        self.graph.assert_ready("fieldset", name)
        node = self._require_node("fieldset", name)
        panel = node.get("panel", "").split(":", 1)[1]
        keys = self._panel_keys(panel)
        fields = [FieldMeta.from_dict(f) for f in (node.get("fields") or {}).values()
                  if f.get("validated")]
        engine = get_fieldset_engine(node.get("engine") or "polars")
        out_dir = self.data_dir / "fieldset" / name
        dt = keys[-1] if keys else ""
        pkeys, gran = self._partition_plan(node, dt_col=dt)
        out_path = out_dir / ("data.parquet" if not pkeys else "")
        scope = None if resync else self._upstream_scope(node)
        # 滚动窗口（fieldset 字段 window_size）：输入 [lo, hi] 变化 → 输出受影响
        # [lo, hi+w-1]，增量重算区间与自身事件范围都按最大回看宽度向前展开
        win = max((f.window_size for f in fields), default=0)
        if scope and win > 1:
            (lo, hi), syms = scope
            scope = (_expand_scope([lo, hi], forward=win - 1), syms)
        if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
            (lo, hi), syms = scope
            dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
            sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
            where = dt_expr & sym_expr if sym_expr is not None else dt_expr
            base, _ = self._panel_lazy(panel, where=where)
            df_inc = engine.scan(base, keys, fields).collect()
            if pkeys:
                old = pl.read_parquet(out_dir, hive_partitioning=True)
                self._rewrite_buckets(old, df_inc, dt_expr, pkeys, out_dir, gran, dt,
                                      sym_expr=sym_expr)
            else:
                old = pl.read_parquet(out_path)
                keep = old.filter(~(dt_expr & sym_expr)) if sym_expr is not None \
                    else old.filter(~dt_expr)
                df = pl.concat([keep, df_inc], how="vertical_relaxed"
                               ).unique(subset=keys, keep="last").sort(keys)
                df.write_parquet(out_path)
            rows = df_inc.height
        else:
            base, _ = self._panel_lazy(panel)
            out = engine.scan(base, keys, fields)
            rows = out.select(pl.len()).collect().item()
            self._write_partitioned(out.collect(), out_dir, pkeys, gran=gran, dt_col=dt)
        m = self.graph.resolve("fieldset", name, extra={
            "dependency_hash": self._fieldset_hash(node),
            "materialized_at": _now_iso(),
        }, own_event=DataChangeEvent(
            action="upsert",
            field_scope=[f.name for f in fields],  # 记录自身重算的字段，而非上游列
            datetime_scope=scope[0] if scope else None,  # 窗口展开后的范围，供下游增量
            symbol_scope=scope[1] if scope else None,    # 变化标的集合，供下游增量
        ))
        return {"name": name, "materialized": True, "valid": True, "rows": rows,
                "fields_count": len(fields), "version": m["version"]}

    def fieldset_test(self, name: str, formula: str):
        node = self._require_node("fieldset", name)
        base, _ = self._panel_lazy(node.get("panel", "").split(":", 1)[1])
        engine = get_fieldset_engine(node.get("engine") or "polars")
        df = engine.test(base, formula)
        return {"ok": True, "rows": df.height, "columns": list(df.columns)}, df

    # =====================================================================
    # sample（样本池：graph 登记，依赖 fieldset + index；get/check 实时过滤）
    # =====================================================================

    def _sample_index_keys(self, node: dict) -> tuple[str, str]:
        """sample 的筛选 index 键：symbol_col + datetime_col（缺省 sym/date）。"""
        idx = node.get("index", "").split(":", 1)[-1]
        inode = self._require_node("index", idx)
        return (inode.get("symbol_col") or "sym",
                inode.get("datetime_col") or "date")

    def _sample_view_lf(self, name: str, *, where=None) -> pl.LazyFrame:
        """sample 视图：fieldset 视图 ∩ 指定 index 的键集合（semi join）。

        只保留 (symbol, datetime) 键存在于该 index 数据中的行；index 键列名与
        视图 keys 不同名时按位置映射（symbol → keys[0]，datetime → keys[-1]）。
        """
        node = self._require_node("sample", name)
        fset = node.get("fieldset", "").split(":", 1)[-1]
        lf, keys = self._fieldset_view_lf(fset, where=where)
        sym, dt = self._sample_index_keys(node)
        key_sym = sym if sym in keys else (keys[0] if keys else sym)
        key_dt = dt if dt in keys else (keys[-1] if len(keys) > 1 else dt)
        idx_lf = pl.scan_parquet(self._index_root(node.get("index", "").split(":", 1)[-1]),
                                 hive_partitioning=True)
        idx_lf = idx_lf.select([sym, dt]).unique()
        if sym != key_sym or dt != key_dt:
            idx_lf = idx_lf.rename({sym: key_sym, dt: key_dt})
        return lf.join(idx_lf, on=[key_sym, key_dt], how="semi")

    def sample_add(self, name: str, fieldset: str, index: str, **kw) -> dict:
        """sample add：列级血缘——视图列透传（sample 列 DERIVES → fieldset 列）
        + 筛选 index 键映射（sample keys DERIVES → index symbol/datetime 列）。"""
        col_maps = {"fieldset": {c: c for c in self._fieldset_view_col_names(fieldset)}}
        idx_node = self._require_node("index", index)
        sym = idx_node.get("symbol_col") or "sym"
        dt = idx_node.get("datetime_col") or "date"
        # 视图 keys（panel keys）与 index 键列按位置映射（symbol → keys[0]、datetime → keys[-1]）
        fnode = self._require_node("fieldset", fieldset)
        keys = self._panel_keys(fnode.get("panel", "").split(":", 1)[-1])
        key_map: dict[str, str] = {}
        if keys:
            key_map[keys[0]] = sym
        if len(keys) > 1:
            key_map[keys[-1]] = dt
        if key_map:
            col_maps["index"] = key_map
        return SampleHandler.add(self.graph, name, fieldset, index,
                                 column_maps=col_maps, **kw)

    def _fieldset_view_col_names(self, fieldset: str) -> list[str]:
        """fieldset 视图列名：其 panel 全列 + 已校验字段（仅元数据，不读数据）。"""
        fnode = self._require_node("fieldset", fieldset)
        pnode = self._require_node("panel", fnode.get("panel", "").split(":", 1)[-1])
        names = [c["name"] for c in self._panel_columns(pnode)]
        names += [f for f, fd in (fnode.get("fields") or {}).items()
                  if fd.get("validated")]
        return names

    def sample_get(self, name: str, *, columns=None, where=None, limit=None,
                   offset=None, count_total: bool = False):
        lf = self._sample_view_lf(name, where=where)
        return self._collect_page(lf, columns=columns, limit=limit, offset=offset,
                                  count_total=count_total)

    def _sample_keys(self, node: dict) -> list[str]:
        """sample 的索引列 = 其 fieldset 底层 panel 的 keys。"""
        fset = node.get("fieldset", "").split(":", 1)[-1]
        fnode = self._require_node("fieldset", fset)
        return self._panel_keys(fnode.get("panel", "").split(":", 1)[-1])

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
        """sample 元数据（V2.0 形态 dict，§10）：含 keys/columns（完整列元数据）。"""
        node = self._require_node("sample", name)
        return {
            "name": name,
            "version": node.get("version", 0),
            "fieldset": node.get("fieldset", "").split(":", 1)[-1]
            if node.get("fieldset") else "",
            "index": node.get("index", "").split(":", 1)[-1]
            if node.get("index") else "",
            "keys": self._sample_keys(node),
            "valid": bool(node.get("valid")),
            "materialized": False,  # sample 无物化，恒实时构造
            "columns": self._sample_view_cols(name),
            "display_name": node.get("display_name") or name,
            "description": node.get("description", ""),
            "tags": list(node.get("tags") or ()),
            "source": node.get("source", "local"),
            "created_at": node.get("create_time", ""),
            "updated_at": node.get("update_time", ""),
        }

    def sample_list(self) -> list:
        return [self.sample_meta(n["name"]) for n in self.graph.list("sample")]

    def sample_set(self, name: str, **kw) -> dict:
        self._require_node("sample", name)
        # 定义键规范化：set --index/--fieldset 存 node_id 形态（与 add 一致）
        if "index" in kw:
            kw["index"] = node_id("index", kw["index"])
        if "fieldset" in kw:
            kw["fieldset"] = node_id("fieldset", kw["fieldset"])
        return self.graph.set("sample", name, **kw)

    def sample_update(self, name: str) -> dict:
        """sample 更新：传导检查上游（fieldset 链 + 筛选 index）就绪 → 视图可构造 → 铸版本。

        sample 无物化；update = 确认上游就绪并铸版本（消费的积累事件入 version_list，
        无新事件不空 bump），出边水位对齐。
        """
        self.graph.assert_ready("sample", name)
        self._sample_view_lf(name).select(pl.len()).collect()
        m = self.graph.resolve("sample", name, mark_materialized=False)
        return {"name": name, "valid": True,
                "version": m["version"]}

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
        if "window_size" in kw:
            kw["window_size"] = int(kw["window_size"] or 0)
        return self.graph.set("feature", name, **kw)

    def feature_update(self, name: str) -> dict:
        """feature 更新：纯定义资产（无上游），标记有效并铸版本（无事件不空 bump）。"""
        self.graph.assert_ready("feature", name)
        m = self.graph.resolve("feature", name, mark_materialized=False)
        return {"name": name, "valid": True,
                "version": m["version"]}

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
        """sample 视图列（**完整列元数据**，§10）：panel 列继承 ColumnMeta 全键，
        fieldset 衍生字段继承 FieldMeta，未知列回退 name+data_type。"""
        node = self._require_node("sample", sample)
        lf = self._sample_view_lf(sample)
        schema = lf.collect_schema()
        fset = node.get("fieldset", "").split(":", 1)[-1]
        fnode = self._require_node("fieldset", fset)
        panel = fnode.get("panel", "").split(":", 1)[-1]
        panel_cols = {c["name"]: c for c in
                      self._panel_columns(self._require_node("panel", panel))}
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
        materialized = bool(node.get("materialized") or extra.get("materialized"))
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

    def _index_partition_keys(self, node: dict) -> list[str]:
        """下游物化的分区键 = 其 index 的实际分区键（镜像，最上游依赖 index）。"""
        idx = self._index_node(node)
        return list(idx.get("partition_by") or ()) if idx else []

    def _partition_plan(self, node: dict, dt_col: str = "") -> tuple[list[str], str]:
        """下游物化分区方案 = 继承 index 的 ``materialize_partition`` 时间桶。

        - yearly/monthly/daily（默认 yearly）：**无论 index 物理是否分区**，下游都按
          时间粒度分桶落盘（``part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]``，见
          ``_write_partitioned``）；``dt_col`` 为时间键（keys 末列）；
        - gran 未知 / 无 index / 无时间键 → 单文件（``([], "")``）。
        """
        idx = self._index_node(node)
        if idx is None or not dt_col:
            return [], ""
        gran = (idx.get("materialize_partition") or "yearly").strip().lower()
        if gran in ("yearly", "monthly", "daily"):
            return ["part"], gran
        return [], ""

    @staticmethod
    def _scan_materialized(root: Path, partition: str | None = None) -> pl.LazyFrame:
        """读物化 parquet（hive 分区还原），**剔除内部分区桶列 part**——
        保持对外列集合与实时视图一致（part 仅供物化增量删桶，不对外暴露）。
        ``partition`` 按 part 桶前缀过滤（如 ``--partition 2024`` 取 2024 年桶）。"""
        lf = pl.scan_parquet(root, hive_partitioning=True)
        if partition is not None:
            lf = lf.filter(pl.col("part").cast(pl.String).str.starts_with(partition))
        return lf.select(pl.all().exclude("part"))

    def _rewrite_buckets(self, old: pl.DataFrame, df_inc: pl.DataFrame,
                         dt_expr: pl.Expr, pkeys: list[str], out_dir: Path,
                         gran: str, dt_col: str,
                         *, sym_expr: pl.Expr | None = None) -> pl.DataFrame:
        """分区级增量写回：删受影响桶后，把**桶内区间外旧行**与增量行合并写回。

        时间桶粒度（yearly/monthly/daily）粗于增量区间（天级）——直接删桶会丢掉
        桶内未变化的行；且增量新日期可能与旧数据**同桶**（affected 取两边的并集）。
        故：受影响桶 = 旧数据命中「区间（×标的）」行的桶 ∪ 增量数据所在桶；保留
        受影响桶内 ``~(dt_expr [& sym_expr])`` 旧行，与增量合并后整体重写这些桶。
        ``sym_expr`` 给出时（事件带 symbol_scope）命中判定收窄到变化标的，
        未变化的标的行不重算。
        """
        key = pkeys[0]
        cut = {"yearly": 4, "monthly": 7, "daily": 10}.get(gran, 4)
        part_expr = pl.col(dt_col).cast(pl.String).str.slice(0, cut).alias(key)
        inc_parts = df_inc.with_columns(part_expr)[key].unique().to_list()
        hit = dt_expr & sym_expr if sym_expr is not None else dt_expr
        affected = sorted(set(old.filter(hit)[key].unique().to_list())
                          | set(inc_parts))
        keep = old.filter(~hit).filter(pl.col(key).is_in(affected)) \
            if affected else None
        for v in affected:
            shutil.rmtree(out_dir / f"{key}={v}", ignore_errors=True)
        if keep is not None:
            # 增量行补同款 part 列（与 keep 列数对齐；_write_partitioned 会再覆盖同值）
            merged = pl.concat(
                [keep, df_inc.with_columns(part_expr)], how="vertical_relaxed")
        else:
            merged = df_inc
        self._write_partitioned(merged, out_dir, pkeys, gran=gran, dt_col=dt_col)
        return merged

    @staticmethod
    def _write_partitioned(df: pl.DataFrame, out_dir: Path, partition_keys: list[str],
                           gran: str = "", dt_col: str = "") -> None:
        """物化落盘：按分区键写 hive 目录 ``key=value/``；无分区键 → 单文件。

        ``partition_keys=["part"]``（时间桶）：按 ``materialize_partition`` 粒度从
        ``dt_col`` 提取桶值（yearly→YYYY、monthly→YYYY-MM、daily→YYYY-MM-DD，
        String/ISO 前缀切片）生成 ``part`` 列后写 ``part=<v>/data.parquet``。
        分区文件内**保留分区列**（读取 hive_partitioning=True 时用文件列，类型/列序不变）。
        """
        out_dir.mkdir(parents=True, exist_ok=True)
        if not partition_keys:
            df.write_parquet(out_dir / "data.parquet")
            return
        key = partition_keys[0]
        if key == "part":
            cut = {"yearly": 4, "monthly": 7, "daily": 10}.get(gran, 4)
            df = df.with_columns(
                pl.col(dt_col).cast(pl.String).str.slice(0, cut).alias("part"))
        for val in df[key].unique().to_list():
            sub = out_dir / f"{key}={val}"
            sub.mkdir(parents=True, exist_ok=True)
            df.filter(pl.col(key) == pl.lit(val)).write_parquet(sub / "data.parquet")

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

    def _factor_compute(self, node: dict, *, partition: str | None = None,
                        dt_range: tuple[str, str] | None = None,
                        symbols: list[str] | None = None) -> pl.DataFrame:
        """实时计算最终因子：sample 视图求 feature 公式 → 拼索引+因子列 → 算子链。

        ``dt_range=(lo, hi)`` / ``symbols`` 给出时只计算「时间区间 × 标的集合」
        内的行（增量物化用，字符串/ISO 可比）。
        """
        feature = node.get("feature", "").split(":", 1)[1]
        sample = node.get("sample", "").split(":", 1)[1]
        fnode = self._require_node("feature", feature)
        keys = self._factor_keys(node)
        lf = self._sample_view_lf(sample)
        if dt_range:
            dt = keys[-1]
            lo, hi = dt_range
            lf = lf.filter(pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi)))
        if symbols:
            lf = lf.filter(pl.col(keys[0]).is_in(symbols))
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
        """读 factor（lazy，第 1/3 态）：已物化（curated）→ 读物化 parquet；
        未物化 → 报错提示先 update（不再实时计算回退）。"""
        node = self._require_node("factor", name)
        fm = self._factor_meta_dict(name)
        root = self._require_materialized("factor", name, fm)
        lf = self._scan_materialized(root, partition=partition)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def factor_add(self, name: str, feature: str, sample: str, *,
                   engine: str = "polars", pipeline: str = "nothing()",
                   factor_col: str | None = None, **kw) -> dict:
        """创建最终因子：校验 feature/sample 已注册 + pipeline/engine 合法。

        列级血缘：factor keys 透传 sample keys；factor_col DERIVES → feature
        公式引用的 sample 视图列（DEPENDS 边 detail.columns）。
        """
        if not feature:
            raise ValueError("factor add 需要 --feature <feature 名>")
        if not sample:
            raise ValueError("factor add 需要 --sample <sample 名>")
        fnode = self._require_node("feature", feature)
        snode = self._require_node("sample", sample)
        parse_pipeline(pipeline)
        get_factor_engine(engine)
        keys = self._sample_keys(snode)
        fcol = factor_col or feature
        col_maps = {"sample": {k: k for k in keys}}
        view = set(self._fieldset_view_col_names(
            snode.get("fieldset", "").split(":", 1)[-1]))
        refs = _formula_refs(fnode.get("formula") or "", view)
        if refs:
            col_maps["sample"][fcol] = refs
        FactorHandler.add(self.graph, name, feature, sample, engine=engine,
                          pipeline=pipeline, factor_col=factor_col,
                          column_maps=col_maps, **kw)
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

    def _factor_scan_one(self, name: str, *, resync: bool = False) -> dict:
        node = self._require_node("factor", name)
        extra = dict(node.get("extra") or {})
        cur_hash = self._factor_hash(node)
        version_before = node.get("version", 0)
        # 幂等仅当节点有效：上游变化置脏（valid=False）后 update 必须强制重建
        if not resync and node.get("valid") \
                and extra.get("dependency_hash") == cur_hash \
                and (node.get("materialized") or extra.get("materialized")):
            return {"name": name, "version_before": version_before,
                    "version_after": version_before, "materialized": True,
                    "changed": False, "partition_by": list(extra.get("partition_by") or ())}
        out_dir = self.data_dir / "factor" / name
        keys = self._factor_keys(node)
        dt = keys[-1] if keys else ""
        pkeys, gran = self._partition_plan(node, dt_col=dt)
        out_path = out_dir / ("data.parquet" if not pkeys else "")
        feature = node.get("feature", "").split(":", 1)[1]
        fnode = self._require_node("feature", feature)
        # 增量物化：最近上游积累事件有明确 datetime 区间且已有物化 → 只重算该区间
        # （分区场景只替换受影响分区文件，flat 场景删区间+合并）
        # feature 滚动窗口：输入 [lo, hi] 变化 → factor 输出受影响 [lo, hi+w-1]
        scope = None if resync else self._upstream_scope(node)
        fwin = int(fnode.get("window_size") or 0)
        if scope and fwin > 1:
            (lo, hi), syms = scope
            scope = (_expand_scope([lo, hi], forward=fwin - 1), syms)
        if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
            (lo, hi), syms = scope
            dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
            sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
            inc = self._factor_compute(node, dt_range=(lo, hi), symbols=syms)
            if pkeys:
                old = pl.read_parquet(out_dir, hive_partitioning=True)
                df = self._rewrite_buckets(old, inc, dt_expr, pkeys, out_dir, gran, dt,
                                           sym_expr=sym_expr)
            else:
                old = pl.read_parquet(out_path)
                keep = old.filter(~(dt_expr & sym_expr)) if sym_expr is not None \
                    else old.filter(~dt_expr)
                df = pl.concat([keep, inc], how="vertical_relaxed"
                               ).unique(subset=keys, keep="last").sort(keys)
                df.write_parquet(out_path)
        else:
            df = self._factor_compute(node)
            self._write_partitioned(df, out_dir, pkeys, gran=gran, dt_col=dt)
        # 物化成功 → resolve 收口：铸版本并记录**消费的合并事件**（own_event 带
        # 窗口展开后的 datetime_scope，供下游 test 沿链增量）+ 出边水位对齐
        m = self.graph.resolve("factor", name, extra={
            "dependency_hash": cur_hash, "partition_by": pkeys,
            "partition_gran": gran, "materialized_at": _now_iso(),
            "field": {"name": node.get("factor_col") or feature,
                      "formula": fnode.get("formula") or "",
                      "display_name": node.get("factor_col") or feature,
                      "description": "", "unit": None, "tags": [],
                      "window_size": fwin},
        }, own_event=DataChangeEvent(
            action="upsert",
            field_scope=[node.get("factor_col") or feature],
            datetime_scope=scope[0] if scope else None,
            symbol_scope=scope[1] if scope else None,
        ))
        version_after = m["version"]
        return {"name": name, "version_before": version_before,
                "version_after": version_after, "materialized": True, "changed": True,
                "partition_by": list(pkeys), "rebuilt_partitions": [""]}

    # =====================================================================
    # test（因子测试数据集：factor 关联 sample 视图 + 测试必需列；物化落盘）
    # =====================================================================

    def _tester_spec(self, node: dict) -> FactorTesterSpec:
        return FactorTesterSpec.from_dict(node.get("spec") or {})

    def _tester_meta_dict(self, name: str) -> dict:
        """V2.0 FactorTestMeta 形态 dict。"""
        node = self._require_node("tester", name)
        meta = self.graph._meta(node)
        extra = dict(meta.get("extra") or {})
        materialized = bool(node.get("materialized") or extra.get("materialized"))
        dep_hash = extra.get("dependency_hash") or ""
        return {
            "name": name,
            "version": node.get("version", 0),
            "factor": node.get("factor", "").split(":", 1)[1] if node.get("factor") else "",
            "sample": node.get("sample", "").split(":", 1)[1] if node.get("sample") else "",
            "returns": node.get("returns", "r"),
            "groupby": node.get("groupby", "ic"),
            "marketcap": node.get("marketcap", "fv"),
            "spec": self._tester_spec(node).to_dict(),
            "factor_col": node.get("factor_col", ""),
            "keys": list(node.get("keys") or ()),
            "materialized": materialized,
            "materialized_at": extra.get("materialized_at"),
            "curated": materialized and dep_hash == self._tester_hash(node),
            "columns": list(extra.get("columns") or ()),
            "extra": extra,
            "display_name": node.get("display_name") or name,
            "description": node.get("description", ""),
            "tags": list(node.get("tags") or ()),
            "source": node.get("source", "local"),
            "created_at": node.get("create_time", ""),
            "updated_at": node.get("update_time", ""),
        }

    def _tester_hash(self, node: dict) -> str:
        """物化一致性签名 = factor 的 hash + spec + 测试列名。"""
        factor = node.get("factor", "").split(":", 1)[1] if node.get("factor") else ""
        fnode = self._require_node("factor", factor)
        parts = [
            f"factor:{factor}:{self._factor_hash(fnode)}",
            f"returns:{node.get('returns', 'r')}",
            f"groupby:{node.get('groupby', 'ic')}",
            f"marketcap:{node.get('marketcap', 'fv')}",
            f"factor_col:{node.get('factor_col', '')}",
            f"spec:{dumps_str(self._tester_spec(node).to_dict())}",
        ]
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    def _tester_build(self, node: dict, *, dt_range: tuple[str, str] | None = None,
                      symbols: list[str] | None = None) -> pl.DataFrame:
        """测试数据集：sample 视图（含测试必需列）+ factor 列 → prepare_factor_data。

        ``dt_range=(lo, hi)`` / ``symbols`` 给出时只构造「时间区间 × 标的集合」
        内的行（增量物化用）。
        """
        factor = node.get("factor", "").split(":", 1)[1]
        fnode = self._require_node("factor", factor)
        fm = self._factor_meta_dict(factor)
        sample = node.get("sample", "").split(":", 1)[1] if node.get("sample") else fm["sample"]
        view = self._sample_view_lf(sample)
        keys = list(fm["keys"])
        if dt_range:
            dt = keys[-1]
            lo, hi = dt_range
            view = view.filter(pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi)))
        if symbols:
            view = view.filter(pl.col(keys[0]).is_in(symbols))
        view = view.collect()
        returns = node.get("returns", "r")
        groupby = node.get("groupby", "ic")
        marketcap = node.get("marketcap", "fv")
        need = ["date", "sym", returns, groupby, marketcap]
        missing = [c for c in need if c not in view.columns]
        if missing:
            raise ValueError(f"sample 缺少测试必需列: {missing}（需要 date/sym 与 "
                             f"returns/groupby/marketcap）")
        fdf = self._factor_compute(fnode, dt_range=dt_range, symbols=symbols)
        base = (
            view.select(*[pl.col(c) for c in need])
            .with_columns(pl.lit(1, dtype=pl.Int32).alias("sample"))
            .join(fdf, on=keys, how="left")
            .rename({fm["factor_col"]: "factor", returns: "returns",
                     groupby: "group", marketcap: "marketcap"})
        )
        return prepare_factor_data(base, self._tester_spec(node))

    def _tester_view_lf(self, name: str, *, where=None) -> pl.LazyFrame:
        """读测试数据集（lazy，第 1/3 态）：已物化（curated）→ 读物化 parquet；
        未物化 → 报错提示先 update。"""
        node = self._require_node("tester", name)
        tm = self._tester_meta_dict(name)
        root = self._require_materialized("tester", name, tm)
        lf = self._scan_materialized(root)
        if where is not None:
            lf = lf.filter(to_expr(where) if isinstance(where, str) else where)
        return lf

    def tester_data(self, name: str) -> pl.DataFrame:
        """测试数据集 DataFrame（stat 测试器用，第 1/3 态）：已物化 → 读物化；
        未物化 → 报错提示先 update（不再实时构造回退）。"""
        node = self._require_node("tester", name)
        tm = self._tester_meta_dict(name)
        root = self._require_materialized("tester", name, tm)
        return self._scan_materialized(root).collect()

    def tester_add(self, name: str, factor: str, *, returns: str = "r",
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
        keys = list(fm["keys"])
        fcol = factor_col or fm["factor_col"] or factor
        # 列级血缘（factor 依赖）：keys 透传 + factor/factor_quantile → factor.factor_col
        factor_map = {k: k for k in keys}
        factor_map.update({"factor": fcol, "factor_quantile": fcol})
        TesterHandler.add(
            self.graph, name, factor,
            returns=returns, groupby=groupby, marketcap=marketcap,
            factor_col=factor_col or fm["factor_col"] or factor,
            spec=spec_d, sample=node_id("sample", sample),
            keys=keys, column_maps={"factor": factor_map}, **kw)
        # 列级血缘（跨依赖引用）：returns/group/marketcap/d{no} ← sample 视图列
        sample_map = {"returns": returns, "group": groupby, "marketcap": marketcap}
        sample_map.update({f"d{no}": returns for no in spec_d.get("periods", [])})
        self.graph.sync_derives("tester", name, "sample", sample, sample_map)
        return self._tester_meta_dict(name)

    def tester_get(self, name: str, *, where=None, limit=None, offset=None,
                 count_total: bool = False):
        lf = self._tester_view_lf(name, where=where)
        return self._collect_page(lf, limit=limit, offset=offset, count_total=count_total)

    def tester_meta(self, name: str) -> dict:
        return self._tester_meta_dict(name)

    def tester_list(self) -> list:
        return [self._tester_meta_dict(n["name"]) for n in self.graph.list("tester")]

    def tester_set(self, name: str, **kw) -> dict:
        self._require_node("tester", name)
        return self.graph.set("tester", name, **kw)

    def tester_delete(self, name: str, *, force: bool = False) -> dict:
        self._require_node("tester", name)
        self.graph.delete("tester", name, force=force)
        shutil.rmtree(self.data_dir / "factor_tester" / name, ignore_errors=True)
        return {"deleted": name}

    def tester_check(self, name: str) -> dict:
        """校验测试数据集：构造成功、含必需列、行数 > 0。"""
        node = self._require_node("tester", name)
        keys = list(node.get("keys") or ())
        try:
            df = self._tester_build(node)
        except Exception as e:
            return {"tester": name, "ok": False, "rows": 0, "columns": list(keys),
                    "message": f"测试数据集构造失败: {e}"}
        need = ["date", "sym", "sample", "returns", "group", "marketcap",
                "factor", "factor_quantile"]
        missing = [c for c in need if c not in df.columns]
        if missing:
            return {"tester": name, "ok": False, "rows": df.height,
                    "columns": list(df.columns), "message": f"结果集缺少必需列: {missing}"}
        if df.height == 0:
            return {"tester": name, "ok": False, "rows": 0,
                    "columns": list(df.columns), "message": "结果行数为 0"}
        return {"tester": name, "ok": True, "rows": df.height,
                "columns": list(df.columns), "message": f"有效（{df.height} 行）"}

    def tester_update(self, name: str | None = None, *, all: bool = False,
                    resync: bool = False) -> dict | list[dict]:
        """tester 更新：传导检查上游（factor 全链）就绪 → 物化 factor_tester/<name>/。

        幂等；update 成功后节点置 valid=True。
        """
        if all:
            return [self._tester_scan_one(n["name"], resync=resync)
                    for n in self.graph.list("tester")]
        if not name:
            raise ValueError("test update 需要测试集名（或 --all）")
        self.graph.assert_ready("tester", name)
        return self._tester_scan_one(name, resync=resync)

    def _tester_scan_one(self, name: str, *, resync: bool = False) -> dict:
        node = self._require_node("tester", name)
        extra = dict(node.get("extra") or {})
        cur_hash = self._tester_hash(node)
        spec = self._tester_spec(node)
        version_before = node.get("version", 0)
        # 幂等仅当节点有效：上游变化置脏（valid=False）后 update 必须强制重建
        if not resync and node.get("valid") \
                and extra.get("dependency_hash") == cur_hash \
                and (node.get("materialized") or extra.get("materialized")):
            return {"name": name, "version_before": version_before,
                    "version_after": version_before, "materialized": True,
                    "changed": False, "rows": 0, "quantiles": spec.quantiles,
                    "periods": list(spec.periods)}
        out_dir = self.data_dir / "factor_tester" / name
        keys = list(self._factor_meta_dict(
            node.get("factor", "").split(":", 1)[1])["keys"])
        dt = keys[-1] if keys else ""
        pkeys, gran = self._partition_plan(node, dt_col=dt)
        out_path = out_dir / ("data.parquet" if not pkeys else "")
        # 增量物化：最近上游积累事件有明确 datetime 区间且已有物化 → 只重算该区间
        # （分区场景只替换受影响分区文件，flat 场景删区间+合并）
        scope = None if resync else self._upstream_scope(node)
        # d{no} 为前向累计收益（t 时刻输出用到 t..t+no-1 的 returns）：输入在
        # [lo, hi] 变化 → 输出受影响 [hi-no+1, hi]，重算区间按最大 period 向后展开
        max_no = max(spec.periods, default=0)
        if scope and max_no > 1:
            (lo, hi), syms = scope
            scope = (_expand_scope([lo, hi], back=max_no - 1), syms)
        if scope and ((pkeys and out_dir.exists()) or (not pkeys and out_path.exists())):
            (lo, hi), syms = scope
            dt_expr = pl.col(dt).cast(pl.String).is_between(pl.lit(lo), pl.lit(hi))
            sym_expr = pl.col(keys[0]).is_in(syms) if syms else None
            inc = self._tester_build(node, dt_range=(lo, hi), symbols=syms)
            if pkeys:
                old = pl.read_parquet(out_dir, hive_partitioning=True)
                df = self._rewrite_buckets(old, inc, dt_expr, pkeys, out_dir, gran, dt,
                                           sym_expr=sym_expr)
            else:
                old = pl.read_parquet(out_path)
                keep = old.filter(~(dt_expr & sym_expr)) if sym_expr is not None \
                    else old.filter(~dt_expr)
                df = pl.concat([keep, inc], how="vertical_relaxed"
                               ).unique(subset=keys, keep="last").sort(keys)
                df.write_parquet(out_path)
        else:
            df = self._tester_build(node)
            self._write_partitioned(df, out_dir, pkeys, gran=gran, dt_col=dt)
        cols = [{"name": c, "display_name": c, "data_type": str(t)}
                for c, t in zip(df.columns, df.dtypes)]
        # 物化成功 → resolve 收口：铸版本并记录消费的合并事件（带 datetime_scope，
        # 供下游沿链增量）+ 出边 required_version 对齐 + valid/materialized
        m = self.graph.resolve("tester", name, extra={
            "dependency_hash": cur_hash, "materialized_at": _now_iso(),
            "columns": cols,
        })
        version_after = m["version"]
        return {"name": name, "version_before": version_before,
                "version_after": version_after, "materialized": True, "changed": True,
                "rows": df.height, "quantiles": spec.quantiles,
                "periods": list(spec.periods)}

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


def get_fieldset_engine(name: str):
    from ..fieldset.engine import get_engine

    return get_engine(name)


def get_feature_engine(name: str):
    from ..feature.engine import get_engine

    return get_engine(name)


__all__ = ["GraphService"]
