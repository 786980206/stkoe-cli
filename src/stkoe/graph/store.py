"""GraphStore：graphqlite 存储层封装（节点/边 CRUD + 血缘遍历）。

graphqlite = SQLite 扩展（Cypher 查询）。本层约定：
- 节点 label = 资产类型，``id`` 属性 = ``"<type>:<name>"``；``type`` 恒由 label 推导；
- 属性值原生存储：标量（int/str/bool）与复杂值（dict/list）均以原生形态经
  ``$param`` 写入；``RETURN n`` 整节点读取返回忠实存储值；
- **非 ASCII 参数**：graphqlite 的 ``connection.cypher`` 用 ``json.dumps(ensure_ascii=True)``
  序列化参数，会把中文等字符损坏（实测 ``改名`` → ``u6539u540d``）。本层自带
  ``_cypher``（ensure_ascii=False）规避；
- 多语句写入用 ``txn()`` 包裹原生 SQL ``BEGIN/COMMIT/ROLLBACK`` 保证原子性
  （实测 Cypher 内不支持事务语句，但 SQL 级事务可整体回滚 cypher 写入）。
- **物理指纹表**：graph.db 同时承载普通 SQL 表（``stkoe_data_files`` /
  ``stkoe_file_stats``，物理 parquet 文件清单/列统计）——与图节点同文件、同事务
  （实测普通表 INSERT 与 cypher 写入可同事务回滚），替代 V2.0 独立 catalog.db
  的登记表（对象登记/依赖已由图节点/边承载）。
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import graphqlite

from .errors import EdgeNotFoundError
from .model import split_node_id

_PROP_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# 物理指纹普通表（V2.0 catalog.db 的 stkoe_data_files / stkoe_file_stats 迁移至此；
# object_id 以图节点 id 承载，如 "table:index"）
_FP_SCHEMA = """
CREATE TABLE IF NOT EXISTS stkoe_data_files (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id      TEXT NOT NULL,
    partition_path TEXT NOT NULL DEFAULT '',
    rel_path       TEXT NOT NULL,
    row_count      INTEGER,
    file_bytes     INTEGER,
    size           INTEGER,
    mtime_ns       INTEGER,
    schema         TEXT,
    UNIQUE (object_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_df_obj ON stkoe_data_files(object_id);
CREATE TABLE IF NOT EXISTS stkoe_file_stats (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    data_file_id  INTEGER NOT NULL REFERENCES stkoe_data_files(id) ON DELETE CASCADE,
    col           TEXT NOT NULL,
    dtype         TEXT,
    min           TEXT,
    max           TEXT,
    null_count    INTEGER,
    UNIQUE (data_file_id, col)
);
CREATE INDEX IF NOT EXISTS idx_fs_col ON stkoe_file_stats(col);
"""


def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _safe_key(key: str) -> str:
    """属性键白名单：只允许合法 Cypher 标识符，防止注入。"""
    if not _PROP_NAME.match(key):
        raise ValueError(f"非法属性键: {key!r}")
    return key


class GraphStore:
    """graphqlite 图存储：节点/边/遍历原语。"""

    def __init__(self, db_path: str = ":memory:"):
        # check_same_thread=False：任务版 handler 在 run 线程建连接、to_thread 线程顺序使用
        self._conn: sqlite3.Connection = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row  # 物理指纹查询按列名访问
        # WAL + synchronous=NORMAL：减少 fsync（对齐 task/store.py 与旧 catalog 的提速结论）；
        # busy_timeout 显式给足，多连接并发（多任务并行）时等锁而非立刻报 locked
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        graphqlite.load(self._conn)
        self._conn.executescript(_FP_SCHEMA)  # 物理指纹普通表（幂等）
        self._txn_depth = 0

    # ---------- 连接 ----------

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """执行普通 SQL（物理指纹表读写；与图同连接/同事务）。

        Python 3.13 默认 ``isolation_level=''``（legacy 模式）：txn() 之外的
        DML 会隐式开启事务且不自动提交，连接关闭时被回滚（曾致 delete 资产后
        指纹残留）。此处对 txn() 外的写语句立即 commit；txn() 内的交给
        txn() 统一提交。
        """
        cur = self._conn.execute(sql, params)
        if self._txn_depth == 0 and sql.lstrip()[:6].upper() in (
                "INSERT", "UPDATE", "DELETE", "REPLAC"):
            self._conn.commit()
        return cur

    # ---------- 物理指纹（stkoe_data_files / stkoe_file_stats） ----------

    def fingerprint_get(self, object_id: str) -> dict[str, dict]:
        """object_id（图节点 id）的全部 stkoe_data_files，rel_path -> 行 dict。"""
        rows = self.execute(
            "SELECT * FROM stkoe_data_files WHERE object_id=? ORDER BY rel_path",
            (object_id,)).fetchall()
        return {r["rel_path"]: dict(r) for r in rows}

    def fingerprint_stats(self, object_id: str) -> dict[int, dict[str, tuple]]:
        """object_id 的列统计：file_id -> {col: (dtype, min, max, null_count)}。"""
        rows = self.execute(
            "SELECT df.id AS file_id, fs.col, fs.dtype, fs.min, fs.max, fs.null_count "
            "FROM stkoe_data_files df JOIN stkoe_file_stats fs ON fs.data_file_id = df.id "
            "WHERE df.object_id=?", (object_id,)).fetchall()
        out: dict[int, dict[str, tuple]] = {}
        for r in rows:
            out.setdefault(r["file_id"], {})[r["col"]] = (r["dtype"], r["min"], r["max"], r["null_count"])
        return out

    def fingerprint_replace(self, object_id: str, items: list[tuple]) -> None:
        """整表替换指纹：items = [(partition_path, rel_path, row_count, file_bytes,
        size, mtime_ns, schema_json, stats)]，stats = {col: (dtype, min, max, null)}。"""
        self.execute("DELETE FROM stkoe_data_files WHERE object_id=?", (object_id,))
        for (part, rel, row_count, file_bytes, size, mtime_ns, schema_json, stats) in items:
            cur = self.execute(
                "INSERT INTO stkoe_data_files (object_id, partition_path, rel_path, row_count, "
                "file_bytes, size, mtime_ns, schema) VALUES (?,?,?,?,?,?,?,?)",
                (object_id, part, rel, row_count, file_bytes, size, mtime_ns, schema_json))
            fid = cur.lastrowid
            for col, (dtype, lo, hi, nulls) in stats.items():
                self.execute(
                    "INSERT INTO stkoe_file_stats (data_file_id, col, dtype, min, max, null_count) "
                    "VALUES (?,?,?,?,?,?)",
                    (fid, col, dtype, lo, hi, nulls))

    def fingerprint_clear(self, object_id: str) -> None:
        """清空 object_id 的指纹（删除资产登记时调用）。"""
        self.execute("DELETE FROM stkoe_data_files WHERE object_id=?", (object_id,))

    def _cypher(self, query: str, params: dict | None = None) -> list[dict]:
        """执行 Cypher 并解析结果（参数 ensure_ascii=False，中文安全）。"""
        try:
            if params:
                params_json = json.dumps(params, ensure_ascii=False)
                cursor = self._conn.execute("SELECT cypher(?, ?)", (query, params_json))
            else:
                cursor = self._conn.execute("SELECT cypher(?)", (query,))
        except sqlite3.Error as e:
            err_str = str(e)
            try:
                err_data = json.loads(err_str)
                if isinstance(err_data, dict) and "error" in err_data:
                    raise sqlite3.Error(err_data["error"]) from None
            except (json.JSONDecodeError, TypeError):
                pass
            raise

        row = cursor.fetchone()
        if row is None or row[0] is None:
            return []
        result_str = row[0]
        try:
            data = json.loads(result_str)
        except json.JSONDecodeError:
            if result_str.startswith("Error") or result_str.startswith('{"error"'):
                raise sqlite3.Error(result_str)
            return [{"result": result_str}]
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data
        if isinstance(data, dict):
            return [data]
        return [{"result": result_str}]

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "GraphStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @contextmanager
    def txn(self) -> Iterator[None]:
        """SQL 级事务上下文：最外层 BEGIN/COMMIT/ROLLBACK，支持嵌套。"""
        if self._txn_depth > 0:
            self._txn_depth += 1
            try:
                yield
            finally:
                self._txn_depth -= 1
            return
        self._txn_depth = 1
        self._conn.execute("BEGIN")
        try:
            yield
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        finally:
            self._txn_depth = 0

    # ---------- 节点 CRUD ----------

    def create_node(self, node_id: str, label: str, props: dict[str, Any]) -> None:
        """创建节点（已存在则覆盖属性；属性键受白名单约束）。"""
        self._cypher(f"CREATE (n:{label} {{id: $id}})", {"id": node_id})
        for k, v in props.items():
            self._cypher(
                f"MATCH (n {{id: $id}}) SET n.{_safe_key(k)} = $v",
                {"id": node_id, "v": v},
            )

    def has_node(self, node_id: str) -> bool:
        r = self._cypher("MATCH (n {id: $id}) RETURN count(n) AS c", {"id": node_id})
        return bool(r[0].get("c", 0)) if r else False

    @staticmethod
    def _normalize(node: dict | None) -> dict | None:
        if not node:
            return None
        props = dict(node.get("properties") or {})
        labels = node.get("labels") or []
        if labels:
            props["type"] = labels[0].lower()  # type 恒由 label 推导
        return props

    def get_node(self, node_id: str) -> dict | None:
        """返回节点属性 dict（含 ``type`` 归一），不存在返回 None。"""
        r = self._cypher("MATCH (n {id: $id}) RETURN n", {"id": node_id})
        if not r or "n" not in r[0]:
            return None
        return self._normalize(r[0].get("n"))

    def patch_node(self, node_id: str, **props: Any) -> None:
        """就地更新节点属性（仅更新给定键）。"""
        for k, v in props.items():
            self._cypher(
                f"MATCH (n {{id: $id}}) SET n.{_safe_key(k)} = $v",
                {"id": node_id, "v": v},
            )

    def delete_node(self, node_id: str, detach: bool = True) -> None:
        """删除节点（detach=True 连带删除所有边，用于 force 路径）。"""
        if detach:
            self._cypher("MATCH (n {id: $id}) DETACH DELETE n", {"id": node_id})
        else:
            self._cypher("MATCH (n {id: $id}) DELETE n", {"id": node_id})

    def list_nodes(self, label: str | None = None) -> list[dict]:
        """按 label 列出全部节点属性（不含边）。"""
        if label:
            r = self._cypher(f"MATCH (n:{label}) RETURN n")
        else:
            r = self._cypher("MATCH (n) RETURN n")
        out = []
        for row in r:
            node = self._normalize(row.get("n"))
            if node is not None:
                out.append(node)
        return out

    def stale_nodes(self) -> list[dict]:
        """全部 ``valid=false`` 的节点（待重算清单；列节点恒有效不参与）。"""
        return [n for n in self.list_nodes() if not n.get("valid", True)]

    # ---------- 列节点（列级血缘：Column 节点 + DERIVES 边） ----------

    def columns_of(self, asset_id: str) -> list[dict]:
        """某资产的列节点清单（属性 dict，含归一 ``type``）。"""
        r = self._cypher("MATCH (c:Column {asset: $id}) RETURN c", {"id": asset_id})
        out = []
        for row in r:
            node = self._normalize(row.get("c"))
            if node is not None:
                out.append(node)
        return out

    def delete_columns_of(self, asset_id: str) -> None:
        """删除某资产的全部列节点（连带 DERIVES 边）——资产删除时级联。"""
        self._cypher("MATCH (c:Column {asset: $id}) DETACH DELETE c", {"id": asset_id})

    def delete_derives_from(self, column_id: str) -> None:
        """删除某列节点的全部 DERIVES 出边（重派生前清旧映射）。"""
        self._cypher("MATCH (c {id: $id})-[r:DERIVES]->() DELETE r", {"id": column_id})

    # ---------- 边 CRUD ----------

    @staticmethod
    def _edge_props(r: Any) -> dict | None:
        if r is None:
            return None
        if isinstance(r, dict) and isinstance(r.get("properties"), dict):
            return r["properties"]
        if isinstance(r, dict):
            return r
        return None

    def create_edge(self, src_id: str, tgt_id: str, rel_type: str, props: dict) -> None:
        """建边（MERGE 幂等：同类型边已存在则更新属性）。"""
        self._cypher(
            f"MATCH (a {{id: $src}}), (b {{id: $tgt}}) MERGE (a)-[r:{rel_type}]->(b)",
            {"src": src_id, "tgt": tgt_id},
        )
        for k, v in props.items():
            self._cypher(
                f"MATCH (a {{id: $src}})-[r:{rel_type}]->(b {{id: $tgt}}) "
                f"SET r.{_safe_key(k)} = $v",
                {"src": src_id, "tgt": tgt_id, "v": v},
            )

    def get_edge(self, src_id: str, tgt_id: str, rel_type: str = "DEPENDS") -> dict | None:
        r = self._cypher(
            f"MATCH (a {{id: $src}})-[r:{rel_type}]->(b {{id: $tgt}}) RETURN r",
            {"src": src_id, "tgt": tgt_id},
        )
        if not r:
            return None
        return self._edge_props(r[0].get("r"))

    def patch_edge(self, src_id: str, tgt_id: str, rel_type: str, **props: Any) -> None:
        for k, v in props.items():
            self._cypher(
                f"MATCH (a {{id: $src}})-[r:{rel_type}]->(b {{id: $tgt}}) "
                f"SET r.{_safe_key(k)} = $v",
                {"src": src_id, "tgt": tgt_id, "v": v},
            )

    def delete_edge(self, src_id: str, tgt_id: str, rel_type: str = "DEPENDS") -> None:
        self._cypher(
            f"MATCH (a {{id: $src}})-[r:{rel_type}]->(b {{id: $tgt}}) DELETE r",
            {"src": src_id, "tgt": tgt_id},
        )

    def _edge_rows(self, r: list[dict]) -> list[dict]:
        out = []
        for row in r:
            edge = self._edge_props(row.get("r"))
            if edge is None:
                continue
            out.append({"source": row.get("source"), "target": row.get("target"), **edge})
        return out

    def deps_of(self, node_id: str, rel_type: str = "DEPENDS") -> list[dict]:
        """出边：节点 → 其依赖的上游（含边属性）。"""
        r = self._cypher(
            f"MATCH (a {{id: $id}})-[r:{rel_type}]->(b) "
            f"RETURN a.id AS source, b.id AS target, r",
            {"id": node_id},
        )
        return self._edge_rows(r)

    def dependents(self, node_id: str, rel_type: str = "DEPENDS") -> list[dict]:
        """入边：依赖该节点的下游（含边属性）。"""
        r = self._cypher(
            f"MATCH (a)-[r:{rel_type}]->(b {{id: $id}}) "
            f"RETURN a.id AS source, b.id AS target, r",
            {"id": node_id},
        )
        return self._edge_rows(r)

    def has_incoming(self, node_id: str) -> bool:
        return bool(self.dependents(node_id))

    # ---------- 血缘遍历 ----------

    def _walk(self, start: str, outgoing: bool, depth: int | None,
              rel: str = "DEPENDS") -> list[dict]:
        """血缘遍历：**逐层批量 Cypher**（每层一次 ``MATCH ... WHERE a.id IN $ids``）。

        - 比变长路径 ``length(p)`` 可靠（graphqlite 对多跳路径的 length 返回 1）；
        - 比逐节点查询高效（一次查询一层，批量 IN 拿下一层 + 边属性）；
        - 返回 [{id, type, name, depth, required_version}]，按深度升序（BFS 序）；
        - ``rel`` 可指定 ``DERIVES``（列级血缘：列 → 其来源列）。
        """
        seen: set[str] = set()
        out: list[dict] = []
        level = [start]
        d = 0
        max_depth = depth if depth is not None else 1 << 30
        while level and d < max_depth:
            d += 1
            rel_pat = f"-[r:{rel}]->" if outgoing else f"<-[r:{rel}]-"
            q = (f"MATCH (a){rel_pat}(n) WHERE a.id IN $ids "
                 f"RETURN DISTINCT n.id AS id, r.required_version AS rv")
            rows = self._cypher(q, {"ids": level})
            nxt: list[str] = []
            for row in rows:
                nid = row.get("id")
                if nid is None or nid in seen:
                    continue
                seen.add(nid)
                t, name = split_node_id(nid)
                out.append({"id": nid, "type": t, "name": name, "depth": d,
                            "required_version": int(row.get("rv") or 0)})
                nxt.append(nid)
            level = nxt
        return out

    def upstream(self, node_id: str, depth: int | None = None) -> list[dict]:
        """上游血缘：传递依赖（出边方向）。"""
        return self._walk(node_id, outgoing=True, depth=depth)

    def downstream(self, node_id: str, depth: int | None = None) -> list[dict]:
        """下游血缘：传递影响（入边方向）。"""
        return self._walk(node_id, outgoing=False, depth=depth)

    def column_upstream(self, column_id: str, depth: int | None = None) -> list[dict]:
        """列的上游来源：``(列) -[:DERIVES]-> (来源列)`` 传递闭包。"""
        return self._walk(column_id, outgoing=True, depth=depth, rel="DERIVES")

    def column_downstream(self, column_id: str, depth: int | None = None) -> list[dict]:
        """列的下游派生：``(派生列) -[:DERIVES]-> (列)`` 传递闭包。"""
        return self._walk(column_id, outgoing=False, depth=depth, rel="DERIVES")

    # ---------- 统计 ----------

    def stats(self) -> dict:
        """图统计：资产节点/DEPENDS 边（血缘图口径）+ 列节点/DERIVES 边。"""
        total = self._cypher("MATCH (n) RETURN count(n) AS c")
        cols = self._cypher("MATCH (n:Column) RETURN count(n) AS c")
        edges = self._cypher("MATCH ()-[r]->() RETURN count(r) AS c")
        derives = self._cypher("MATCH ()-[r:DERIVES]->() RETURN count(r) AS c")
        return {
            "node_count": int(total[0]["c"] or 0) - int(cols[0]["c"] or 0)
            if total and cols else 0,
            "edge_count": int(edges[0]["c"] or 0) - int(derives[0]["c"] or 0)
            if edges and derives else 0,
            "column_count": int(cols[0]["c"] or 0) if cols else 0,
            "derives_count": int(derives[0]["c"] or 0) if derives else 0,
        }


__all__ = ["GraphStore"]
