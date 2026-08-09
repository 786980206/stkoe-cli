"""field 指标管理（v0.4.1 迁移：catalog 注册，替换遗留 YAML 实现）

指标是 dataset 之上的派生定义（formula 为表达式存根，不做物化计算）；
注册存 catalog type='field'，产物不落盘（fields/ 目录仅作存档保留）。
接口对齐 table/dataset/stat：create（add）/meta/list/del/rename。
"""
import datetime
from dataclasses import dataclass

from .catalog import access
from . import catalog, logger


class FieldError(ValueError):
    pass


class FieldExistsError(FieldError):
    pass


class FieldNotFoundError(FieldError):
    pass


@dataclass(frozen=True)
class FieldMeta:
    """指标元数据"""
    name: str
    version: int
    dataset: str
    formula: str | None = None
    display_name: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    materialized: bool = False
    created_at: str | None = None
    updated_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name, "version": self.version, "dataset": self.dataset,
            "formula": self.formula, "display_name": self.display_name,
            "description": self.description, "tags": list(self.tags),
            "materialized": self.materialized,
            "created_at": self.created_at, "updated_at": self.updated_at,
        }


def _object(conn, name: str):
    return access.get_object(conn, name, "field")


def _meta(conn, obj) -> FieldMeta:
    m = __import__("json").loads(obj["meta"])
    return FieldMeta(
        name=obj["name"], version=obj["version"], dataset=m.get("dataset", ""),
        formula=m.get("formula"), display_name=m.get("display_name", obj["name"]),
        description=m.get("description", ""), tags=tuple(m.get("tags", [])),
        materialized=bool(m.get("materialized", False)),
        created_at=obj["created_at"], updated_at=obj["updated_at"],
    )


def create(name: str, dataset: str, formula: str | None = None, **meta_input) -> FieldMeta:
    """新建指标：绑定 dataset（必须已注册）+ 公式存根，注册 catalog。

    ``**meta_input`` 可覆盖 display_name/description/tags 等。
    """
    from .dataset import describe as describe_dataset
    describe_dataset(dataset)  # 未注册抛 DatasetNotFoundError
    conn = catalog().conn
    if _object(conn, name) is not None:
        raise FieldExistsError(f"field already registered: {name}")
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "dataset": dataset, "formula": formula,
        "display_name": name, "description": "", "tags": [],
        "materialized": False,
        "create_time": now, "update_time": now,
    }
    meta.update(meta_input)
    obj = access.insert_object(conn, "field", name, meta, "", now)
    # 指标依赖 dataset（供血缘/级联）
    access.add_dep(conn, "field", name, "dataset", dataset, {"formula": formula})
    logger.debug(f"field [{name}] created on dataset {dataset}")
    return _meta(conn, obj)


def meta(name: str) -> FieldMeta:
    conn = catalog().conn
    obj = _object(conn, name)
    if obj is None:
        raise FieldNotFoundError(f"field not registered: {name}")
    return _meta(conn, obj)


def list() -> list[FieldMeta]:
    conn = catalog().conn
    rows = conn.execute("SELECT * FROM stkoe_objects WHERE type='field' "
                        "ORDER BY name").fetchall()
    return [_meta(conn, r) for r in rows]


def rename(old: str, new: str) -> FieldMeta:
    """改名（catalog + 依赖边）"""
    conn = catalog().conn
    with catalog().txn() as cx:
        obj = _object(cx, old)
        if obj is None:
            raise FieldNotFoundError(f"field not registered: {old}")
        if _object(cx, new) is not None:
            raise FieldExistsError(f"field already registered: {new}")
        cx.execute("UPDATE stkoe_objects SET name=? WHERE id=?", (new, obj["id"]))
        access.rename_obj(cx, "field", old, new)
        access.rename_dep(cx, "field", old, new)
    return _meta(conn, _object(conn, new))


def del_(name: str) -> None:
    """删除指标注册（fields/<name>/ 历史 YAML 存档目录保留）"""
    conn = catalog().conn
    with catalog().txn() as cx:
        obj = _object(cx, name)
        if obj is None:
            raise FieldNotFoundError(f"field not registered: {name}")
        cx.execute("DELETE FROM stkoe_objects WHERE id=?", (obj["id"],))
        access.clear_deps(cx, "field", name)


def describe(name: str) -> FieldMeta:
    """兼容别名：旧 field 使用 describe(name) 读取"""
    return meta(name)