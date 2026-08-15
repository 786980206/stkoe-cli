"""GraphController：图的增删改查 + 依赖约束 + 版本事件响应。

本层把 graphqlite 原语组织成资产语义操作：
- 节点 CRUD（add/get/meta/set/col/list/delete），删除带「无下游」约束（force 绕过）；
- DEPENDS 边管理（link/unlink），required_version 默认取被依赖方当前版本；
- 事件响应：notify_change（上游变化 → 铸版本 + 下游置脏）→
  resolve/resolve_all（积累事件合并 → storage 物化钩子 → 版本递增 + 边水位对齐）；
- 血缘查询：upstream / downstream / lineage。

物理数据（parquet 读取/物化）暂未接入：``storage`` 钩子默认 no-op 成功，
后续替换为真实实现即可（见 ``NullStorage``）。
"""
from __future__ import annotations

from typing import Any

from .errors import (
    AssetExistsError,
    AssetNotFoundError,
    CycleError,
    DependencyError,
)
from .events import accumulate, event_from_kwargs, merge_events
from .model import (
    ASSET_TYPES,
    TYPE_TO_LABEL,
    AssetMeta,
    DataChangeEvent,
    node_id,
)
from .store import GraphStore, _now_iso
from .version import new_version

# 各资产类型的「定义键」：改动这些键会令下游失效（需要重算）
DEFINITION_KEYS: dict[str, frozenset[str]] = {
    "table": frozenset({"type", "columns"}),
    "index": frozenset({"type", "columns", "symbol_col", "datetime_col", "materialize_partition"}),
    "panel": frozenset({"index", "tables", "keys"}),
    "fieldset": frozenset({"dataset", "fields"}),
    "sample": frozenset({"fieldset", "engine", "formula"}),
    "feature": frozenset({"engine", "formula"}),
    "factor": frozenset({"feature", "sample", "engine", "pipeline", "factor_col"}),
    "tester": frozenset({"factor", "returns", "groupby", "marketcap", "factor_col", "spec"}),
    "model": frozenset(),
    "stat": frozenset(),
}

_META_KEYS = frozenset({"display_name", "description", "tags", "source"})


class NullStorage:
    """物理存储钩子（占位）：物化/校验均为记录性 no-op，流程可整体跑通。"""

    def materialize(self, node: AssetMeta, accumulated: dict) -> bool:
        return True

    def check(self, node: AssetMeta) -> dict:
        return {"ok": True, "message": "storage not wired (no-op)"}


class GraphController:
    """资产图控制器。"""

    def __init__(self, store: GraphStore | None = None, storage: Any = None):
        self._store = store or GraphStore()
        self._storage = storage or NullStorage()

    @property
    def store(self) -> GraphStore:
        return self._store

    # ---------- 工具 ----------

    def _require(self, asset_type: str, name: str) -> dict:
        if asset_type not in ASSET_TYPES:
            raise AssetNotFoundError(f"未知资产类型: {asset_type}")
        props = self._store.get_node(node_id(asset_type, name))
        if props is None:
            raise AssetNotFoundError(f"{asset_type} not registered: {name}")
        return props

    def _meta(self, props: dict) -> dict:
        """节点属性 → 对外 AssetMeta dict（去掉内部 id）。"""
        props = {k: v for k, v in props.items() if k != "id"}
        return AssetMeta.from_dict(props).to_dict()

    def _bump(self, props: dict, event: DataChangeEvent) -> dict:
        """铸新版本（高精度时间戳）+ 事件入 version_list（返回增量属性）。"""
        version = new_version()
        version_list = dict(props.get("version_list") or {})
        version_list[str(version)] = event.to_dict()
        return {
            "version": version,
            "version_list": version_list,
            "update_time": _now_iso(),
        }

    def _mark_stale(self, node_id: str) -> None:
        """把节点标记为失效（valid=False, materialized=False）。"""
        self._store.patch_node(node_id, valid=False, materialized=False)

    # ---------- 节点 CRUD ----------

    def add(
        self,
        asset_type: str,
        name: str,
        *,
        display_name: str | None = None,
        description: str = "",
        tags: list | tuple | None = None,
        source: str = "local",
        extra: dict | None = None,
        deps: list[tuple[str, str, dict]] | None = None,
        **data: Any,
    ) -> dict:
        """创建资产节点（deps = [(dep_type, dep_name, detail)]，同事务建边）。

        派生资产（panel/fieldset/sample/factor/tester）创建后 valid=False；
        table/index（数据源头）valid=True。
        """
        if asset_type not in ASSET_TYPES:
            raise AssetNotFoundError(f"未知资产类型: {asset_type}")
        nid = node_id(asset_type, name)
        if self._store.has_node(nid):
            raise AssetExistsError(f"{asset_type} already exists: {name}")

        # 校验并解析依赖边（被依赖方必须存在）
        edges = []
        for dep_type, dep_name, detail in deps or []:
            dep_id = node_id(dep_type, dep_name)
            dep = self._store.get_node(dep_id)
            if dep is None:
                raise AssetNotFoundError(f"依赖不存在: {dep_type}:{dep_name}")
            edges.append((dep_id, dep, dict(detail or {})))

        now = _now_iso()
        data = dict(data)
        data.pop("type", None)  # type 恒由 label 推导，不接受 data 覆盖
        props: dict[str, Any] = {
            "name": name,
            "display_name": display_name if display_name is not None else name,
            "description": description,
            "tags": list(tags or ()),
            "source": source,
            "version": new_version(),  # 初始版本同样是高精度时间戳
            "version_list": {},
            "materialized": False,
            "valid": asset_type in ("table", "index"),
            "create_time": now,
            "update_time": now,
            "extra": dict(extra or {}),
            **data,
        }
        with self._store.txn():
            self._store.create_node(nid, TYPE_TO_LABEL[asset_type], props)
            for dep_id, dep, detail in edges:
                self._store.create_edge(
                    nid, dep_id, "DEPENDS",
                    {"required_version": int(dep.get("version", 1)),
                     "detail": detail, "create_time": now},
                )
        return self._meta(self._store.get_node(nid))

    def get(self, asset_type: str, name: str) -> dict:
        return self._meta(self._require(asset_type, name))

    def meta(self, asset_type: str, name: str) -> dict:
        return self._meta(self._require(asset_type, name))

    def list(self, asset_type: str | None = None) -> list[dict]:
        label = TYPE_TO_LABEL[asset_type] if asset_type else None
        return [self._meta(p) for p in self._store.list_nodes(label)]

    def set(self, asset_type: str, name: str, *, definition: bool = False, **kwargs: Any) -> dict:
        """更新节点属性。

        - 定义键（见 DEFINITION_KEYS）变更 → 下游置脏（valid=False）；
        - 未识别键一律进 extra；每次 set 版本递增并记事件。
        """
        props = self._require(asset_type, name)
        nid = node_id(asset_type, name)

        def_keys = DEFINITION_KEYS.get(asset_type, frozenset())
        data_keys = set(kwargs) & def_keys
        meta_keys = set(kwargs) & _META_KEYS
        extra_keys = set(kwargs) - def_keys - _META_KEYS

        changed = set(kwargs)
        bumps = self._bump(props, DataChangeEvent(
            action="upsert",
            field_scope=sorted(changed) if changed else None,
        ))

        patch: dict[str, Any] = {}
        for k in meta_keys:
            v = kwargs[k]
            if k == "tags":
                patch[k] = [x.strip() for x in v.split(",")] if isinstance(v, str) \
                    else list(v or ())
            else:
                patch[k] = v
        for k in data_keys:
            patch[k] = kwargs[k]
        if extra_keys:
            extra = dict(props.get("extra") or {})
            for k in extra_keys:
                extra[k] = kwargs[k]
            patch["extra"] = extra
        patch.update(bumps)

        with self._store.txn():
            self._store.patch_node(nid, **patch)
            # 定义键变更或显式 definition=True → 下游失效
            if definition or (data_keys & def_keys):
                self._propagate_stale(nid)
        return self._meta(self._store.get_node(nid))

    def col(
        self,
        asset_type: str,
        name: str,
        column: str,
        *,
        field: bool = False,
        **kwargs: Any,
    ) -> dict:
        """修改列/字段元数据（table/index 的 columns；fieldset 的 fields）。"""
        props = self._require(asset_type, name)
        nid = node_id(asset_type, name)

        if field:
            fields = dict(props.get("fields") or {})
            if column not in fields:
                raise AssetNotFoundError(f"field not found: {column}")
            fields[column] = {**fields[column], **kwargs}
            patch = {"fields": fields}
        else:
            cols = list(props.get("columns") or [])
            idx = next((i for i, c in enumerate(cols) if c.get("name") == column), None)
            if idx is None:
                raise AssetNotFoundError(f"column not found: {column}")
            cols[idx] = {**cols[idx], **kwargs}
            patch = {"columns": cols}

        bumps = self._bump(props, DataChangeEvent(
            action="upsert", field_scope=[column],
        ))
        patch.update(bumps)
        with self._store.txn():
            self._store.patch_node(nid, **patch)
            if asset_type in ("table", "index"):
                self._propagate_stale(nid)
        return self._meta(self._store.get_node(nid))

    def delete(self, asset_type: str, name: str, *, force: bool = False) -> dict:
        """删除节点：无下游依赖才可删（force 绕过并连带清除其所有边）。"""
        props = self._require(asset_type, name)
        nid = node_id(asset_type, name)
        downstream = self._store.dependents(nid)
        if downstream and not force:
            raise DependencyError([
                {"type": d["source"].split(":", 1)[0],
                 "name": d["source"].split(":", 1)[1]} for d in downstream
            ])
        with self._store.txn():
            self._store.delete_node(nid, detach=True)
        return {"deleted": name}

    # ---------- 边管理 ----------

    def link(
        self,
        src_type: str,
        src_name: str,
        tgt_type: str,
        tgt_name: str,
        *,
        detail: dict | None = None,
        required_version: int | None = None,
    ) -> dict:
        """建依赖边：``(依赖方) -[:DEPENDS]-> (被依赖方)``。"""
        src = self._require(src_type, src_name)
        tgt = self._require(tgt_type, tgt_name)
        src_id = node_id(src_type, src_name)
        tgt_id = node_id(tgt_type, tgt_name)
        rv = required_version if required_version is not None else int(tgt.get("version", 1))
        self._store.create_edge(
            src_id, tgt_id, "DEPENDS",
            {"required_version": rv, "detail": dict(detail or {}), "create_time": _now_iso()},
        )
        return self._edge(src_id, tgt_id)

    def unlink(self, src_type: str, src_name: str, tgt_type: str, tgt_name: str) -> dict:
        src_id = node_id(src_type, src_name)
        tgt_id = node_id(tgt_type, tgt_name)
        self._store.delete_edge(src_id, tgt_id)
        return {"unlinked": f"{src_type}:{src_name} -> {tgt_type}:{tgt_name}"}

    def _edge(self, src_id: str, tgt_id: str) -> dict:
        e = self._store.get_edge(src_id, tgt_id)
        return {"source": src_id, "target": tgt_id, **e} if e else {}

    def deps_of(self, asset_type: str, name: str) -> list[dict]:
        """出边：该资产的直接上游依赖。"""
        nid = node_id(asset_type, name)
        self._require(asset_type, name)
        return self._store.deps_of(nid)

    def dependents(self, asset_type: str, name: str) -> list[dict]:
        """入边：直接依赖该资产的下游。"""
        nid = node_id(asset_type, name)
        self._require(asset_type, name)
        return self._store.dependents(nid)

    # ---------- 血缘查询 ----------

    def upstream(self, asset_type: str, name: str, depth: int | None = None) -> list[dict]:
        self._require(asset_type, name)
        return self._store.upstream(node_id(asset_type, name), depth=depth)

    def downstream(self, asset_type: str, name: str, depth: int | None = None) -> list[dict]:
        self._require(asset_type, name)
        return self._store.downstream(node_id(asset_type, name), depth=depth)

    def lineage(self, asset_type: str, name: str, depth: int | None = None) -> dict:
        """完整上下游血缘。"""
        node = self.meta(asset_type, name)
        return {
            "node": node,
            "upstream": self.upstream(asset_type, name, depth=depth),
            "downstream": self.downstream(asset_type, name, depth=depth),
        }

    def stale(self) -> list[dict]:
        """全部失效（待重算）节点。"""
        return [self._meta(p) for p in self._store.stale_nodes()]

    def stats(self) -> dict:
        return self._store.stats()

    # ---------- 事件响应 ----------

    def _propagate_stale(self, nid: str) -> list[str]:
        """BFS 下游置脏：返回受影响节点 id 列表。"""
        affected = []
        for d in self._store.downstream(nid):
            self._mark_stale(d["id"])
            affected.append(d["id"])
        return affected

    def notify_change(
        self,
        asset_type: str,
        name: str,
        *,
        event: DataChangeEvent | None = None,
        **kw: Any,
    ) -> dict:
        """上游数据变化登记：铸新版本 + 事件入日志 + 下游置脏。

        返回 {"node": meta, "affected": [下游 id...]}。
        """
        props = self._require(asset_type, name)
        nid = node_id(asset_type, name)
        ev = event or event_from_kwargs(**kw)
        bumps = self._bump(props, ev)
        with self._store.txn():
            self._store.patch_node(nid, **bumps)
            affected = self._propagate_stale(nid)
        return {"node": self._meta(self._store.get_node(nid)), "affected": affected}

    def _accumulated(self, node_props: dict) -> dict:
        """汇总该节点所有出边上积累的更新事件（跨依赖并集）。

        对每个出边：取被依赖方 version_list 中 ``version > required_version``
        的事件合并；再把所有边的合并结果按 action 再次合并（并集）。
        """
        nid = node_props["id"] if "id" in node_props else node_id(
            node_props.get("type", ""), node_props.get("name", ""))
        all_events: list[DataChangeEvent] = []
        for e in self._store.deps_of(nid):
            dep = self._store.get_node(e["target"])
            if dep is None:
                continue
            rv = int(e.get("required_version", 0))
            acc = accumulate(dep.get("version_list") or {}, rv)
            if acc["upsert"] is not None:
                all_events.append(acc["upsert"])
            if acc["delete"] is not None:
                all_events.append(acc["delete"])
        return merge_events(all_events)

    def accumulated(self, asset_type: str, name: str) -> dict:
        """公开接口：该资产积累的更新事件（on_change 输出形态）。"""
        return self._accumulated(self._require(asset_type, name))

    def resolve(self, asset_type: str, name: str) -> dict:
        """重算单节点：积累事件 → storage 物化 → 版本递增 + 出边水位对齐。

        - 有积累事件时：铸新版本并把合并事件写入 version_list（下游据此感知变更）；
          无积累事件（如定义变更后的首次校验）只置 valid/materialized，不空 bump；
        - 出边 required_version 对齐为被依赖方当前版本。
        """
        props = self._require(asset_type, name)
        nid = node_id(asset_type, name)
        accumulated = self._accumulated(props)

        # storage 物化钩子（本阶段 no-op）
        node = AssetMeta.from_dict(self._meta(props))
        self._storage.materialize(node, accumulated)

        patches: dict[str, Any] = {"valid": True, "materialized": True,
                                   "update_time": _now_iso()}
        if accumulated["upsert"] is not None or accumulated["delete"] is not None:
            patches.update(self._bump(
                props, accumulated["upsert"] or accumulated["delete"]))
        with self._store.txn():
            self._store.patch_node(nid, **patches)
            for e in self._store.deps_of(nid):
                dep = self._store.get_node(e["target"])
                if dep is not None:
                    self._store.patch_edge(
                        nid, e["target"], "DEPENDS",
                        required_version=int(dep.get("version", 1)),
                    )
        return self._meta(self._store.get_node(nid))

    def resolve_all(self) -> dict:
        """拓扑序重算全部失效节点（先依赖后依赖方）；成环时中止报错。

        返回 {"resolved": [meta...], "remaining": [meta...]}。
        """
        resolved: list[dict] = []
        pending = {n["id"]: n for n in self._store.stale_nodes()}
        guard = 0
        while pending and guard < len(pending) * len(pending) + 10:
            guard += 1
            progressed = False
            for nid in list(pending):
                props = pending[nid]
                t, n = props["id"].split(":", 1)
                deps = self._store.deps_of(nid)
                if any(d["target"] in pending for d in deps):
                    continue  # 依赖方还未重算
                resolved.append(self.resolve(t, n))
                pending.pop(nid)
                progressed = True
            if not progressed:
                break
        if pending:
            remaining = [self._meta(p) for p in pending.values()]
            raise CycleError(
                f"血缘图存在环或无法拓扑排序，未能重算: "
                f"{[p['id'] for p in pending.values()]}")
        return {"resolved": resolved, "remaining": []}


__all__ = ["GraphController", "NullStorage", "DEFINITION_KEYS"]
