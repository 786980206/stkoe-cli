"""catalog 行访问：stkoe_objects/stkoe_data_files/stkoe_file_stats/stkoe_depends 通用读写
（与对象类型无关）

供 table/dataset/stat/field 模块复用同一套 catalog 事务语义。
"""
from ..util import now
from . import json as _json


def get_object(conn, name: str, obj_type: str = "table"):
    """按名称+类型查 stkoe_objects 行（未命中返回 None）"""
    return conn.execute(
        "SELECT * FROM stkoe_objects WHERE name=? AND type=?", (name, obj_type)
    ).fetchone()


def get_object_by_id(conn, object_id: int):
    return conn.execute("SELECT * FROM stkoe_objects WHERE id=?", (object_id,)).fetchone()


def insert_object(conn, obj_type: str, name: str, meta: dict, signature: str, now_str: str):
    """插入 stkoe_objects 行并返回该行（version 从 1 起）"""
    cur = conn.execute(
        "INSERT INTO stkoe_objects (type, name, version, signature, meta, created_at, updated_at) "
        "VALUES (?,?,1,?,?,?,?)",
        (obj_type, name, signature, _json.dumps(meta), now_str, now_str),
    )
    return conn.execute("SELECT * FROM stkoe_objects WHERE id=?", (cur.lastrowid,)).fetchone()


def update_object_meta(conn, object_id: int, meta: dict, signature: str | None = None,
                       now_str: str | None = None, bump: bool = False):
    """更新 stkoe_objects 的 meta（可选签名/时间戳/版本递增）"""
    sets, args = ["meta=?"], [_json.dumps(meta)]
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


# ---------- stkoe_depends（资源依赖图，供触发/级联用） ----------

def add_dep(conn, obj_type: str, obj_name: str, dep_type: str, dep_name: str,
            detail: dict | None = None) -> None:
    """登记依赖边：obj → dep（幂等：同边已存在则忽略）"""
    conn.execute(
        "INSERT OR IGNORE INTO stkoe_depends "
        "(obj_type, obj_name, dep_type, dep_name, detail, created_at) VALUES (?,?,?,?,?,?)",
        (obj_type, obj_name, dep_type, dep_name,
         _json.dumps(detail) if detail else None, now()),
    )


def set_deps(conn, obj_type: str, obj_name: str, deps: list[tuple[str, str, dict | None]]) -> None:
    """整表替换 obj 的全部出边（deps: [(dep_type, dep_name, detail)]）"""
    clear_deps(conn, obj_type, obj_name)
    for dep_type, dep_name, detail in deps:
        add_dep(conn, obj_type, obj_name, dep_type, dep_name, detail)


def clear_deps(conn, obj_type: str, obj_name: str) -> None:
    """删除 obj 的全部出边（drop 时）"""
    conn.execute("DELETE FROM stkoe_depends WHERE obj_type=? AND obj_name=?", (obj_type, obj_name))


def deps_of(conn, obj_type: str, obj_name: str) -> list[dict]:
    """obj 依赖的资源列表：{dep_type, dep_name, detail}"""
    rows = conn.execute(
        "SELECT dep_type, dep_name, detail FROM stkoe_depends "
        "WHERE obj_type=? AND obj_name=? ORDER BY dep_type, dep_name", (obj_type, obj_name)).fetchall()
    return [{"dep_type": r["dep_type"], "dep_name": r["dep_name"],
             "detail": _json.loads(r["detail"]) if r["detail"] else {}} for r in rows]


def dependents(conn, dep_type: str, dep_name: str) -> list[dict]:
    """依赖 dep 的所有资源：{obj_type, obj_name, detail}"""
    rows = conn.execute(
        "SELECT obj_type, obj_name, detail FROM stkoe_depends "
        "WHERE dep_type=? AND dep_name=? ORDER BY obj_type, obj_name", (dep_type, dep_name)).fetchall()
    return [{"obj_type": r["obj_type"], "obj_name": r["obj_name"],
             "detail": _json.loads(r["detail"]) if r["detail"] else {}} for r in rows]


def rename_dep(conn, dep_type: str, dep_name: str, new_name: str) -> None:
    """被依赖方改名：同步依赖它的全部边（table/dataset rename 时）"""
    conn.execute("UPDATE stkoe_depends SET dep_name=? WHERE dep_type=? AND dep_name=?",
                 (new_name, dep_type, dep_name))


def rename_obj(conn, obj_type: str, obj_name: str, new_name: str) -> None:
    """依赖方改名：同步它自己的全部出边（dataset rename 时）"""
    conn.execute("UPDATE stkoe_depends SET obj_name=? WHERE obj_type=? AND obj_name=?",
                 (new_name, obj_type, obj_name))
