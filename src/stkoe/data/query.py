"""查询内核：谓词解析 + 基于 stkoe_file_stats 的文件级裁剪（与对象类型无关）

供 table.select / dataset.select 等复用。裁剪正确性不变式：无统计列必含，
有统计列按 min/max 与查询范围是否相交判定。
"""
import re

import polars as pl

NUMERIC_DTYPES = {
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
}

_RANGE_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*(>=|<=|>|<|==)\s*(.+?)\s*$")
_RANGE2_RE = re.compile(r"^\s*(.+?)\s*<=\s*([A-Za-z_]\w*)\s*<=\s*(.+?)\s*$")


def parse_pred(s: str) -> tuple[str, str | None, str | None] | None:
    """解析单列范围谓词 -> (col, lo, hi)，lo/hi 为原始值字符串"""
    m = _RANGE2_RE.match(s)
    if m:
        return m.group(2), m.group(1).strip(), m.group(3).strip()
    m = _RANGE_RE.match(s)
    if m:
        col, op, val = m.group(1), m.group(2), m.group(3)
        if op == "==":
            return col, val, val
        if op == ">=":
            return col, val, None
        if op == "<=":
            return col, None, val
        if op == ">":
            return col, val, None
        if op == "<":
            return col, None, val
    return None


def to_expr(where) -> pl.Expr:
    """where 字符串 -> pl.Expr；pl.Expr 原样返回"""
    if isinstance(where, pl.Expr):
        return where
    pred = parse_pred(where)
    if pred is None:
        raise ValueError(f"unsupported where predicate: {where!r}")
    col, lo, hi = pred
    if lo is not None and hi is not None and lo == hi:
        return pl.col(col) == lit(lo)
    exprs = []
    if lo is not None:
        exprs.append(pl.col(col) >= lit(lo))
    if hi is not None:
        exprs.append(pl.col(col) <= lit(hi))
    return exprs[0] if len(exprs) == 1 else exprs[0] & exprs[1]


_OPS = {
    "eq": lambda c, v: c == lit(v),
    "neq": lambda c, v: c != lit(v),
    "gt": lambda c, v: c > lit(v),
    "gte": lambda c, v: c >= lit(v),
    "lt": lambda c, v: c < lit(v),
    "lte": lambda c, v: c <= lit(v),
}


def to_filters_expr(filters) -> pl.Expr | None:
    """结构化过滤条件列表 -> AND 组合的 polars Expr（与 where 语义叠加）。

    每项为 ``{field, op, value}``，op ∈ eq/neq/gt/gte/lt/lte；值按列类型自动转换
    缺失则忽略；未知列忽略（不因界面残留字段失败）。
    """
    if not filters:
        return None
    exprs = []
    for f in filters:
        if not isinstance(f, dict) or not f.get("field"):
            continue
        op = (f.get("op") or "eq").strip().lower()
        fn = _OPS.get(op)
        if fn is None:
            raise ValueError(f"unsupported filter op: {op}")
        value = f.get("value")
        if value is None:
            continue
        try:
            exprs.append(fn(pl.col(f["field"]), str(value)))
        except Exception:
            continue
    if not exprs:
        return None
    out = exprs[0]
    for e in exprs[1:]:
        out = out & e
    return out


def apply_sort(lf: pl.LazyFrame, sort) -> pl.LazyFrame:
    """应用排序（null 排最后）：[{field, dir}] 逐项追加"""
    if not sort:
        return lf
    by, desc = [], []
    for item in sort:
        if not isinstance(item, dict) or not item.get("field"):
            continue
        by.append(item["field"])
        desc.append(bool(item.get("desc")))
    if not by:
        return lf
    return lf.sort(by, descending=desc, nulls_last=True)


def lit(v: str) -> pl.Expr:
    if re.fullmatch(r"-?\d+", v):
        return pl.lit(int(v))
    if re.fullmatch(r"-?\d+\.\d+([eE][-+]?\d+)?", v):
        return pl.lit(float(v))
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return pl.lit(v).str.strptime(pl.Date, "%Y-%m-%d")
    return pl.lit(v)


def is_numeric_col(conn, object_id: int, col: str) -> bool:
    row = conn.execute(
        "SELECT dtype FROM stkoe_file_stats fs JOIN stkoe_data_files df ON fs.data_file_id=df.id "
        "WHERE df.object_id=? AND fs.col=? LIMIT 1",
        (object_id, col),
    ).fetchone()
    return row is not None and row["dtype"] in NUMERIC_DTYPES


def prune_files(conn, object_id: int, partition=None, where=None) -> list:
    """裁剪内核：一条 SQL 出候选文件（分区精确/前缀 + 列统计范围）"""
    sql = "SELECT id, rel_path, partition_path FROM stkoe_data_files WHERE object_id=?"
    args: list = [object_id]

    if partition is not None:
        parts = [partition] if isinstance(partition, str) else [p for p in partition]
        cond = " OR ".join("(partition_path=? OR partition_path LIKE ?)" for _ in parts)
        sql += f" AND ({cond})"
        for p in parts:
            args += [p, f"{p}=%"]

    if isinstance(where, str):
        pred = parse_pred(where)
        if pred is not None:
            col, lo, hi = pred
            # 文件与查询范围相交才算候选：有统计的列按范围判断，无统计的列必包含（正确性）
            if is_numeric_col(conn, object_id, col):
                cmp_sql = []
                if lo is not None:
                    cmp_sql.append("CAST(fs.max AS NUMERIC) >= ?")
                    args.append(float(lo))
                if hi is not None:
                    cmp_sql.append("CAST(fs.min AS NUMERIC) <= ?")
                    args.append(float(hi))
            else:
                cmp_sql = []
                if lo is not None:
                    cmp_sql.append("fs.max >= ?")
                    args.append(lo)
                if hi is not None:
                    cmp_sql.append("fs.min <= ?")
                    args.append(hi)
            if cmp_sql:
                cmp = " AND ".join(cmp_sql)
                sql += (
                    f" AND (NOT EXISTS (SELECT 1 FROM stkoe_file_stats fs "
                    f"WHERE fs.data_file_id=stkoe_data_files.id AND fs.col=?)"
                    f" OR EXISTS (SELECT 1 FROM stkoe_file_stats fs WHERE fs.data_file_id=stkoe_data_files.id"
                    f" AND fs.col=? AND {cmp}))"
                )
                args += [col, col]
    return conn.execute(sql, args).fetchall()
