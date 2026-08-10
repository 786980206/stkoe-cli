"""SQLite catalog：表对象、文件清单、列统计、事务与行访问

单库：``<data_dir>/catalog.db``（WAL 模式）。catalog 表统一 ``stkoe_`` 前缀。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from ..jsonutil import dumps_str, loads

_SCHEMA = """
CREATE TABLE IF NOT EXISTS stkoe_objects (
    id         INTEGER PRIMARY KEY,
    type       TEXT NOT NULL,
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL DEFAULT 1,
    signature  TEXT,
    meta       TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (type, name)
);
CREATE TABLE IF NOT EXISTS stkoe_data_files (
    id             INTEGER PRIMARY KEY,
    object_id      INTEGER NOT NULL REFERENCES stkoe_objects(id) ON DELETE CASCADE,
    partition_path TEXT NOT NULL DEFAULT '',
    rel_path       TEXT NOT NULL,
    row_count      INTEGER,
    file_bytes     INTEGER,
    size           INTEGER,
    mtime_ns       INTEGER,
    schema         TEXT,
    UNIQUE (object_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_stkoe_data_files_obj  ON stkoe_data_files(object_id);
CREATE INDEX IF NOT EXISTS idx_stkoe_data_files_part ON stkoe_data_files(object_id, partition_path);
CREATE TABLE IF NOT EXISTS stkoe_file_stats (
    id            INTEGER PRIMARY KEY,
    data_file_id  INTEGER NOT NULL REFERENCES stkoe_data_files(id) ON DELETE CASCADE,
    col           TEXT NOT NULL,
    dtype         TEXT,
    min           TEXT,
    max           TEXT,
    null_count    INTEGER,
    UNIQUE (data_file_id, col)
);
CREATE INDEX IF NOT EXISTS idx_stkoe_file_stats_col ON stkoe_file_stats(col);
"""


class Catalog:
    """单文件 SQLite 目录：表对象、文件清单、列统计"""

    def __init__(self, path: Path):
        self.path = Path(path)

    def new_conn(self) -> sqlite3.Connection:
        """新建独立连接（SQLite WAL 支持多连接读写分离）"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.commit()
        return conn

    @contextmanager
    def txn(self):
        """事务上下文：成功提交，异常回滚"""
        conn = self.new_conn()
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


# ---------- stkoe_objects ----------

def get_object(conn, name: str, obj_type: str = "table"):
    """按名称+类型查 stkoe_objects 行（未命中返回 None）"""
    return conn.execute(
        "SELECT * FROM stkoe_objects WHERE name=? AND type=?", (name, obj_type)
    ).fetchone()


def insert_object(conn, obj_type: str, name: str, meta: dict, signature: str, now_str: str):
    """插入 stkoe_objects 行并返回该行（version 从 1 起）"""
    cur = conn.execute(
        "INSERT INTO stkoe_objects (type, name, version, signature, meta, created_at, updated_at) "
        "VALUES (?,?,1,?,?,?,?)",
        (obj_type, name, signature, dumps_str(meta), now_str, now_str),
    )
    return conn.execute("SELECT * FROM stkoe_objects WHERE id=?", (cur.lastrowid,)).fetchone()


def update_object_meta(conn, object_id: int, meta: dict, signature: str | None = None,
                       now_str: str | None = None, bump: bool = False):
    """更新 stkoe_objects 的 meta（可选签名/时间戳/版本递增）"""
    sets, args = ["meta=?"], [dumps_str(meta)]
    if signature is not None:
        sets.append("signature=?")
        args.append(signature)
    if now_str is not None:
        sets.append("updated_at=?")
        args.append(now_str)
    if bump:
        sets.append("version=version+1")
    args.append(object_id)
    conn.execute(f"UPDATE stkoe_objects SET {', '.join(sets)} WHERE id=?", args)


# ---------- stkoe_data_files / stkoe_file_stats ----------

def get_data_files(conn, object_id: int) -> dict[str, dict]:
    """object_id 的全部 stkoe_data_files，rel_path -> 行 dict（含 id/partition_path/size/mtime_ns/schema）"""
    rows = conn.execute(
        "SELECT * FROM stkoe_data_files WHERE object_id=?", (object_id,)
    ).fetchall()
    return {r["rel_path"]: dict(r) for r in rows}


def get_stats(conn, object_id: int) -> dict[int, dict[str, tuple]]:
    """object_id 的全部 stkoe_file_stats，file_id -> {col: (dtype, min, max, null_count)}"""
    rows = conn.execute(
        "SELECT df.id AS file_id, fs.col, fs.dtype, fs.min, fs.max, fs.null_count "
        "FROM stkoe_data_files df JOIN stkoe_file_stats fs ON fs.data_file_id = df.id "
        "WHERE df.object_id=?",
        (object_id,),
    ).fetchall()
    out: dict[int, dict[str, tuple]] = {}
    for r in rows:
        out.setdefault(r["file_id"], {})[r["col"]] = (r["dtype"], r["min"], r["max"], r["null_count"])
    return out


def replace_data_files(conn, object_id: int, items: list[tuple]) -> None:
    """整表替换 stkoe_data_files/stkoe_file_stats（items: (partition_path, rel_path, row_count,
    file_bytes, size, mtime_ns, schema_json, stats))，stats 为 {col: (dtype, min, max, null)}"""
    conn.execute("DELETE FROM stkoe_data_files WHERE object_id=?", (object_id,))
    for (part, rel, row_count, file_bytes, size, mtime_ns, schema_json, stats) in items:
        cur = conn.execute(
            "INSERT INTO stkoe_data_files (object_id, partition_path, rel_path, row_count, file_bytes, "
            "size, mtime_ns, schema) VALUES (?,?,?,?,?,?,?,?)",
            (object_id, part, rel, row_count, file_bytes, size, mtime_ns, schema_json),
        )
        fid = cur.lastrowid
        for col, (dtype, lo, hi, nulls) in stats.items():
            conn.execute(
                "INSERT INTO stkoe_file_stats (data_file_id, col, dtype, min, max, null_count) "
                "VALUES (?,?,?,?,?,?)",
                (fid, col, dtype, lo, hi, nulls),
            )


# ---------- meta JSON 便捷 ----------

def meta_of(obj) -> dict:
    return loads(obj["meta"] or "{}")


__all__ = ["Catalog", "get_object", "insert_object", "update_object_meta",
           "get_data_files", "get_stats", "replace_data_files", "meta_of"]
