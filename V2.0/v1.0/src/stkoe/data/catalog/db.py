"""SQLite catalog：连接、DDL、事务"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path

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
CREATE TABLE IF NOT EXISTS stkoe_tasks (
    task_id     TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    object_ref  TEXT,
    status      TEXT NOT NULL,
    progress    REAL,
    stage       TEXT,
    error       TEXT,
    result_ref  TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS stkoe_task_logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES stkoe_tasks(task_id) ON DELETE CASCADE,
    seq     INTEGER NOT NULL,
    ts      TEXT NOT NULL,
    level   TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stkoe_task_logs_task ON stkoe_task_logs(task_id, seq);
CREATE TABLE IF NOT EXISTS stkoe_depends (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    obj_type   TEXT NOT NULL,   -- 依赖方类型（dataset/stat）
    obj_name   TEXT NOT NULL,   -- 依赖方名称
    dep_type   TEXT NOT NULL,   -- 被依赖方类型（table/dataset）
    dep_name   TEXT NOT NULL,   -- 被依赖方名称
    detail     TEXT,            -- 附加信息（JSON：join keys / group_cols）
    created_at TEXT NOT NULL,
    UNIQUE (obj_type, obj_name, dep_type, dep_name)
);
CREATE INDEX IF NOT EXISTS idx_stkoe_depends_dep ON stkoe_depends(dep_type, dep_name);
"""


class Catalog:
    """单文件 SQLite 目录：表对象、文件清单、列统计、任务、任务日志"""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = self.new_conn()

    def new_conn(self) -> sqlite3.Connection:
        """新建独立连接（后台任务用；SQLite WAL 支持多连接读写分离）"""
        conn = sqlite3.connect(str(self.path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        self._migrate_objects_unique(conn)
        conn.commit()
        return conn

    def _migrate_objects_unique(self, conn: sqlite3.Connection) -> None:
        """v0.4 迁移：stkoe_objects.name 全局 UNIQUE → (type, name) 复合 UNIQUE
        （stat 对象与 dataset 同名，须按类型区分身份）
        """
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='stkoe_objects'").fetchone()
        if row is None or "UNIQUE (type, name)" in row["sql"]:
            return
        # 旧表：重建为复合唯一（数据拷贝，id 保持不变以保留外键引用）
        conn.executescript("""
            CREATE TABLE stkoe_objects_new (
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
            INSERT INTO stkoe_objects_new (id, type, name, version, signature, meta, created_at, updated_at)
                SELECT id, type, name, version, signature, meta, created_at, updated_at FROM stkoe_objects;
            DROP TABLE stkoe_objects;
            ALTER TABLE stkoe_objects_new RENAME TO stkoe_objects;
        """)

    @contextmanager
    def txn(self):
        """事务上下文：成功提交，异常回滚（主连接专用）"""
        try:
            yield self.conn
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def close(self):
        self.conn.close()
