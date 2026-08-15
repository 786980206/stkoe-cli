"""graph 模块全流程测试：节点/边 CRUD、依赖约束、版本事件、血缘传播、handler。

覆盖 graph-design.md §3/§4 的约定：
- 节点 CRUD 与「无下游才可删除」约束（force 绕过）；
- 版本/version_list 与 DataChangeEvent 积累合并；
- notify_change → 下游置脏 → resolve/resolve_all 拓扑重算 + 边水位对齐；
- v3.0-def.py 形态的 handler 全链路。
"""
from __future__ import annotations

import json
import os

import pytest

from stkoe.graph import (
    GraphController,
    GraphHandler,
    GraphStore,
    AssetExistsError,
    AssetNotFoundError,
    CycleError,
    DataChangeEvent,
    DependencyError,
    FactorHandler,
    FeatureHandler,
    FieldsetHandler,
    IndexHandler,
    PanelHandler,
    SampleHandler,
    TableHandler,
    accumulate,
    merge_events,
)
from stkoe.graph.events import events_after


@pytest.fixture
def ctrl():
    return GraphController(GraphStore(":memory:"))


@pytest.fixture
def lineage(ctrl):
    """标准血缘链：index/m1(table) → panel → fieldset → sample → factor（+ feature）。

    构建后先 ``resolve_all`` 一次把派生节点全部置为有效（settled 状态）。
    """
    IndexHandler.add(ctrl, "index", symbol_col="sym", datetime_col="date",
                     columns=[{"name": "sym", "data_type": "String"},
                              {"name": "date", "data_type": "Date"}])
    TableHandler.add(ctrl, "m1", columns=[
        {"name": "sym", "data_type": "String"},
        {"name": "date", "data_type": "Date"},
        {"name": "price", "data_type": "Float"}])
    PanelHandler.add(ctrl, "ds1", "index", tables={"m1": "left_join"},
                     keys=["sym", "date"])
    FieldsetHandler.add(ctrl, "fs1", "ds1")
    FieldsetHandler.add_field(ctrl, "fs1", "ma5", "price.rolling_mean(5)")
    SampleHandler.add(ctrl, "sp1", "fs1", formula="(date >= '2024-01-01')")
    FeatureHandler.add(ctrl, "ma5f", "price.rolling_mean(5)")
    FactorHandler.add(ctrl, "fac1", "ma5f", "sp1", pipeline="nothing()")
    ctrl.resolve_all()  # settle：派生节点全部 valid/materialized，无积累事件不空 bump
    return ctrl


# =====================================================================
# 事件合并 / 积累（events.py 单元）
# =====================================================================

class TestEvents:
    def test_merge_union_intersect(self):
        evs = [
            DataChangeEvent(action="upsert", symbol_scope=["a", "b"],
                            datetime_scope=["d1", "d2"], field_scope=["x", "y"]),
            DataChangeEvent(action="upsert", symbol_scope=["b", "c"],
                            datetime_scope=["d2", "d3"], field_scope=["y", "z"]),
            DataChangeEvent(action="delete", symbol_scope=["a"],
                            datetime_scope=["d1"], field_scope=None),
        ]
        out = merge_events(evs)
        u = out["upsert"]
        assert sorted(u.symbol_scope) == ["a", "b", "c"]
        assert sorted(u.datetime_scope) == ["d1", "d2", "d3"]
        assert sorted(u.field_scope) == ["y"]  # 交集
        d = out["delete"]
        assert d.symbol_scope == ["a"]
        assert d.field_scope is None  # None 与任何交集仍为 None

    def test_merge_none_is_universal(self):
        out = merge_events([
            DataChangeEvent(action="upsert", symbol_scope=None),
            DataChangeEvent(action="upsert", symbol_scope=["a"]),
        ])
        assert out["upsert"].symbol_scope is None  # 全集吞并

    def test_events_after(self):
        vl = {"1": {"action": "upsert", "symbol_scope": ["a"]},
              "2": {"action": "delete", "symbol_scope": ["b"]},
              "3": {"action": "upsert", "symbol_scope": ["c"]}}
        evs = events_after(vl, 1)
        assert [e.symbol_scope for e in evs] == [["b"], ["c"]]

    def test_accumulate_shape(self):
        vl = {"1": {"action": "upsert", "symbol_scope": ["a"]},
              "2": {"action": "delete", "symbol_scope": ["b"]}}
        out = accumulate(vl, 0)
        assert out["upsert"].symbol_scope == ["a"]
        assert out["delete"].symbol_scope == ["b"]


# =====================================================================
# 存储层（store.py）
# =====================================================================

class TestStore:
    def test_node_roundtrip(self):
        st = GraphStore(":memory:")
        st.create_node("table:t1", "Table", {"name": "t1", "version": 1,
                                             "version_list": {"1": {"action": "upsert"}},
                                             "tags": ["a", "b"], "valid": True})
        assert st.has_node("table:t1")
        node = st.get_node("table:t1")
        assert node["type"] == "table"
        assert node["version"] == 1
        assert node["version_list"] == {"1": {"action": "upsert"}}
        assert node["tags"] == ["a", "b"]
        assert node["valid"] is True

    def test_edge_roundtrip(self):
        st = GraphStore(":memory:")
        st.create_node("table:a", "Table", {"name": "a"})
        st.create_node("panel:b", "Panel", {"name": "b"})
        st.create_edge("panel:b", "table:a", "DEPENDS",
                       {"required_version": 1, "detail": {"role": "member"}})
        e = st.get_edge("panel:b", "table:a")
        assert e["required_version"] == 1
        assert e["detail"] == {"role": "member"}
        assert st.deps_of("panel:b")[0]["target"] == "table:a"
        assert st.dependents("table:a")[0]["source"] == "panel:b"
        assert st.has_incoming("table:a")
        assert not st.has_incoming("panel:b")

    def test_traversal_depth_and_cycle_guard(self):
        st = GraphStore(":memory:")
        for nid in ["table:a", "panel:b", "fieldset:c", "factor:d"]:
            st.create_node(nid, nid.split(":")[0].capitalize(), {"name": nid})
        st.create_edge("panel:b", "table:a", "DEPENDS", {"required_version": 1})
        st.create_edge("fieldset:c", "panel:b", "DEPENDS", {"required_version": 1})
        st.create_edge("factor:d", "fieldset:c", "DEPENDS", {"required_version": 1})
        assert [n["id"] for n in st.downstream("table:a")] == \
            ["panel:b", "fieldset:c", "factor:d"]
        assert [n["id"] for n in st.downstream("table:a", depth=1)] == ["panel:b"]
        assert [n["id"] for n in st.upstream("factor:d")] == \
            ["fieldset:c", "panel:b", "table:a"]
        assert st.stats() == {"node_count": 4, "edge_count": 3}

    def test_txn_rollback(self):
        st = GraphStore(":memory:")
        st.create_node("table:a", "Table", {"name": "a"})
        with pytest.raises(ValueError):
            with st.txn():
                st.create_node("panel:b", "Panel", {"name": "b"})
                raise ValueError("boom")
        assert not st.has_node("panel:b")
        assert st.has_node("table:a")

    def test_delete_node_detach(self):
        st = GraphStore(":memory:")
        st.create_node("table:a", "Table", {"name": "a"})
        st.create_node("panel:b", "Panel", {"name": "b"})
        st.create_edge("panel:b", "table:a", "DEPENDS", {})
        st.delete_node("table:a", detach=True)
        assert not st.has_node("table:a")
        assert st.dependents("table:a") == []
        assert st.stats()["edge_count"] == 0

    def test_persistence_across_reopen(self):
        """文件型图库：连接重开数据仍在。

        临时目录用 os.makedirs（本环境 tempfile.mkdtemp 目录有 ACL 限制，
        工作区子目录写入也被沙箱限制；系统临时目录 + makedirs 可写）。
        """
        import shutil

        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_persist")
        os.makedirs(base, exist_ok=True)
        try:
            db = os.path.join(base, "g.db")
            st = GraphStore(db)
            st.create_node("table:a", "Table", {"name": "a", "version": 3})
            st.close()
            st2 = GraphStore(db)
            node = st2.get_node("table:a")
            assert node["name"] == "a"
            assert node["version"] == 3
            st2.close()
        finally:
            shutil.rmtree(base, ignore_errors=True)


# =====================================================================
# 控制器 CRUD 与依赖约束
# =====================================================================

class TestControllerCrud:
    def test_add_get_meta_list(self, ctrl):
        meta = TableHandler.add(ctrl, "t1", display_name="表1",
                                description="d", tags=["a", "b"],
                                columns=[{"name": "x"}], extra={"k": 1})
        assert meta["type"] == "table"
        assert meta["name"] == "t1"
        assert isinstance(meta["version"], int) and meta["version"] > 0  # 时间戳版本
        assert meta["valid"] is True
        assert meta["extra"] == {"k": 1}
        assert meta["columns"] == [{"name": "x"}]
        got = ctrl.get("table", "t1")
        assert got["name"] == "t1"
        assert [m["name"] for m in ctrl.list("table")] == ["t1"]

    def test_add_duplicate(self, ctrl):
        TableHandler.add(ctrl, "t1")
        with pytest.raises(AssetExistsError):
            TableHandler.add(ctrl, "t1")

    def test_unknown_type(self, ctrl):
        with pytest.raises(AssetNotFoundError):
            ctrl.add("nope", "x")

    def test_missing_node(self, ctrl):
        with pytest.raises(AssetNotFoundError):
            ctrl.get("table", "nope")

    def test_add_dep_missing(self, ctrl):
        with pytest.raises(AssetNotFoundError):
            PanelHandler.add(ctrl, "ds1", "nope")

    def test_set_meta_extra_and_version(self, ctrl):
        TableHandler.add(ctrl, "t1")
        v0 = TableHandler.meta(ctrl, "t1")["version"]
        m = TableHandler.set(ctrl, "t1", display_name="改名", source="manual",
                             unknown_key="x")
        assert m["display_name"] == "改名"
        assert m["source"] == "manual"
        assert m["extra"]["unknown_key"] == "x"
        assert m["version"] > v0  # 时间戳版本单调递增
        assert str(m["version"]) in m["version_list"]

    def test_set_definition_invalidates_downstream(self, lineage):
        # 链：index → ds1 → fs1 → sp1 → fac1；改 panel keys 应令全部下游失效
        v0 = PanelHandler.meta(lineage, "ds1")["version"]
        m = PanelHandler.set(lineage, "ds1", definition=True, keys=["sym"])
        assert m["version"] > v0
        stale = {n["name"] for n in lineage.stale()}
        assert {"fs1", "sp1", "fac1"} <= stale

    def test_col_columns_and_fields(self, ctrl):
        TableHandler.add(ctrl, "t1", columns=[{"name": "x", "data_type": "String"}])
        v0 = TableHandler.meta(ctrl, "t1")["version"]
        m = TableHandler.col(ctrl, "t1", "x", display_name="列X")
        assert m["columns"][0]["display_name"] == "列X"
        assert m["version"] > v0
        with pytest.raises(AssetNotFoundError):
            TableHandler.col(ctrl, "t1", "nope", display_name="?")

    def test_col_fieldset_field(self, lineage):
        f = FieldsetHandler.set_field(lineage, "fs1", "ma5", display_name="MA5")
        assert f["fields"]["ma5"]["display_name"] == "MA5"
        assert f["fields"]["ma5"]["validated"] is False
        # 改公式 → validated 复位
        f2 = FieldsetHandler.set_field(lineage, "fs1", "ma5", formula="x*2")
        assert f2["fields"]["ma5"]["validated"] is False

    def test_delete_blocked_by_downstream(self, lineage):
        # 链：index → ds1 → fs1 → sp1 → fac1；中间节点全部被下游挡住
        with pytest.raises(DependencyError):
            PanelHandler.delete(lineage, "ds1")
        with pytest.raises(DependencyError):
            FieldsetHandler.delete(lineage, "fs1")   # fs1 有下游 sp1
        with pytest.raises(DependencyError):
            SampleHandler.delete(lineage, "sp1")     # sp1 有下游 fac1
        # fac1 是叶子 → 可删
        FactorHandler.delete(lineage, "fac1")
        assert "fac1" not in [n["name"] for n in lineage.list("factor")]

    def test_delete_ok_when_leaf(self, lineage):
        # fac1 是叶子：删除后 sp1 失去下游
        FactorHandler.delete(lineage, "fac1")
        assert [d["id"] for d in lineage.downstream("sample", "sp1")] == []
        # 逐层删叶子：sp1 → fs1
        SampleHandler.delete(lineage, "sp1")
        FieldsetHandler.delete(lineage, "fs1")
        with pytest.raises(AssetNotFoundError):
            lineage.get("fieldset", "fs1")
        # ds1 仍有下游（fs1 已删 → 无下游，可删）
        assert [d["id"] for d in lineage.downstream("panel", "ds1")] == []

    def test_delete_force(self, lineage):
        m = PanelHandler.delete(lineage, "ds1", force=True)
        assert m["deleted"] == "ds1"
        with pytest.raises(AssetNotFoundError):
            lineage.get("panel", "ds1")
        # force 后下游节点仍在但失去该边
        assert [d["id"] for d in lineage.downstream("table", "m1")] == []

    def test_link_unlink(self, ctrl):
        TableHandler.add(ctrl, "t1")
        FeatureHandler.add(ctrl, "f1", "x*2")
        tgt_v = TableHandler.meta(ctrl, "t1")["version"]
        e = ctrl.link("feature", "f1", "table", "t1", detail={"role": "src"})
        assert e["required_version"] == tgt_v  # 默认水位 = 被依赖方当前版本
        assert ctrl.deps_of("feature", "f1")[0]["target"] == "table:t1"
        ctrl.unlink("feature", "f1", "table", "t1")
        assert ctrl.deps_of("feature", "f1") == []


# =====================================================================
# 事件响应（血缘传播）
# =====================================================================

class TestEventFlow:
    def test_notify_change_versions_and_stale(self, lineage):
        v0 = lineage.get("index", "index")["version"]
        r = IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["000001.SZ"], datetime_scope=["2024-01-02"]))
        idx = lineage.get("index", "index")
        assert idx["version"] > v0  # 时间戳版本单调递增
        assert idx["version_list"][str(idx["version"])]["symbol_scope"] == ["000001.SZ"]
        affected = {a.split(":", 1)[1] for a in r["affected"]}
        assert affected == {"ds1", "fs1", "sp1", "fac1"}
        # 下游全部失效
        for name in ("ds1", "fs1", "sp1", "fac1"):
            assert lineage.get("panel" if name == "ds1" else
                               "fieldset" if name == "fs1" else
                               "sample" if name == "sp1" else "factor",
                               name)["valid"] is False

    def test_accumulated_after_change(self, lineage):
        """积累事件沿链传播：index 变化 → ds1 → fs1 → sp1 逐级消费。"""
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"], datetime_scope=["d1"]))
        # ds1 未重算前，fs1 的直接依赖（ds1）version_list 无新事件
        assert FieldsetHandler.on_change(lineage, "fs1")["upsert"] is None
        # ds1 重算（消费 index 事件并记入自身 version_list）后 fs1 可见
        PanelHandler.update(lineage, "ds1")
        acc = FieldsetHandler.on_change(lineage, "fs1")
        assert acc["upsert"] is not None
        assert acc["upsert"].symbol_scope == ["a"]
        assert acc["delete"] is None
        # 链式：fs1 重算后 sp1 才能看到
        assert lineage.accumulated("sample", "sp1")["upsert"] is None
        FieldsetHandler.materialize(lineage, "fs1")
        acc2 = lineage.accumulated("sample", "sp1")
        assert acc2["upsert"].symbol_scope == ["a"]

    def test_resolve_aligns_watermark(self, lineage):
        v0 = PanelHandler.meta(lineage, "ds1")["version"]
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"]))
        idx_v = lineage.get("index", "index")["version"]
        m = PanelHandler.update(lineage, "ds1")
        assert m["valid"] is True and m["materialized"] is True
        assert m["version"] > v0
        edge = lineage.deps_of("panel", "ds1")[0]
        assert edge["required_version"] == idx_v  # 水位对齐 index 当前版本
        # index 的 version_list 事件进入 ds1 的 version_list
        assert m["version_list"][str(m["version"])]["symbol_scope"] == ["a"]

    def test_resolve_all_topo_order(self, lineage):
        v0 = lineage.get("factor", "fac1")["version"]
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"]))
        res = lineage.resolve_all()
        names = [n["name"] for n in res["resolved"]]
        assert names == ["ds1", "fs1", "sp1", "fac1"]  # 拓扑序（ma5f 已有效）
        assert lineage.stale() == []
        assert lineage.get("factor", "fac1")["version"] > v0

    def test_multiple_changes_accumulate_once(self, lineage):
        """两次 notify 后一次 resolve：事件合并为一次重算。"""
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"]))
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["b"]))
        acc = lineage.accumulated("panel", "ds1")
        assert sorted(acc["upsert"].symbol_scope) == ["a", "b"]
        before = len(PanelHandler.meta(lineage, "ds1")["version_list"])
        m = PanelHandler.update(lineage, "ds1")
        assert len(m["version_list"]) == before + 1  # 一次重算只 bump 一次
        assert sorted(m["version_list"][str(m["version"])]["symbol_scope"]) == ["a", "b"]

    def test_cycle_detected(self, ctrl):
        # 直接用 store 造环：a→b→a，且两者均失效
        st = ctrl.store
        st.create_node("table:a", "Table", {"name": "a", "valid": False})
        st.create_node("panel:b", "Panel", {"name": "b", "valid": False})
        st.create_edge("table:a", "panel:b", "DEPENDS", {"required_version": 1})
        st.create_edge("panel:b", "table:a", "DEPENDS", {"required_version": 1})
        with pytest.raises(CycleError):
            ctrl.resolve_all()

    def test_stale_and_stats(self, lineage):
        assert lineage.stats() == {"node_count": 7, "edge_count": 6}
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(action="upsert"))
        assert len(lineage.stale()) == 4


# =====================================================================
# Handler 全链路（v3.0-def.py 形态）
# =====================================================================

class TestHandlers:
    def test_full_chain_meta(self, lineage):
        p = PanelHandler.meta(lineage, "ds1")
        assert p["index"] == "index:index"
        assert p["tables"] == {"m1": "left_join"}
        assert p["keys"] == ["sym", "date"]
        f = FieldsetHandler.meta(lineage, "fs1")
        assert f["dataset"] == "panel:ds1"
        assert f["fields"]["ma5"]["formula"] == "price.rolling_mean(5)"
        assert f["fields"]["ma5"]["validated"] is False
        s = SampleHandler.meta(lineage, "sp1")
        assert s["fieldset"] == "fieldset:fs1"  # sample 基于 fieldset 衍生
        assert s["formula"] == "(date >= '2024-01-01')"
        fa = FactorHandler.meta(lineage, "fac1")
        assert fa["feature"] == "feature:ma5f"
        assert fa["sample"] == "sample:sp1"
        assert fa["factor_col"] == "ma5f"

    def test_fieldset_add_delete_field(self, ctrl):
        IndexHandler.add(ctrl, "index", columns=[{"name": "sym"}])
        TableHandler.add(ctrl, "m1", columns=[{"name": "price"}])
        PanelHandler.add(ctrl, "ds1", "index", tables={"m1": "left_join"},
                         keys=["sym"])
        FieldsetHandler.add(ctrl, "fs1", "ds1")
        FieldsetHandler.add_field(ctrl, "fs1", "f1", "x*2", display_name="F1")
        m = FieldsetHandler.meta_field(ctrl, "fs1", "f1")
        assert m["display_name"] == "F1"
        with pytest.raises(ValueError):
            FieldsetHandler.add_field(ctrl, "fs1", "f1", "x")  # 重复
        FieldsetHandler.delete_field(ctrl, "fs1", "f1")
        assert "f1" not in FieldsetHandler.meta(ctrl, "fs1")["fields"]

    def test_feature_formula_required(self, ctrl):
        with pytest.raises(ValueError):
            FeatureHandler.add(ctrl, "f1", "")

    def test_index_defaults(self, ctrl):
        m = IndexHandler.add(ctrl, "idx", columns=[{"name": "sym"}, {"name": "date"}])
        assert m["symbol_col"] == "sym"
        assert m["datetime_col"] == "date"
        assert m["materialize_partition"] == "yearly"

    def test_factor_links_two_deps(self, lineage):
        deps = lineage.deps_of("factor", "fac1")
        targets = sorted(d["target"] for d in deps)
        assert targets == ["feature:ma5f", "sample:sp1"]
        details = {d["target"]: d["detail"] for d in deps}
        assert details["feature:ma5f"] == {"role": "feature"}
        assert details["sample:sp1"] == {"role": "sample"}

    def test_tester_handler(self, lineage):
        from stkoe.graph import TesterHandler
        t = TesterHandler.add(lineage, "tt1", "fac1", returns="r", groupby="ic",
                              marketcap="fv", spec={"quantiles": 5, "periods": [1, 5]})
        assert t["factor"] == "factor:fac1"
        assert t["spec"]["periods"] == [1, 5]
        deps = lineage.deps_of("tester", "tt1")
        assert deps[0]["target"] == "factor:fac1"
        assert deps[0]["detail"] == {"role": "factor"}

    def test_graph_handler_lineage(self, lineage):
        g = GraphHandler.get(lineage, "panel", "ds1")
        assert [u["id"] for u in g["upstream"]] == ["index:index", "table:m1"]
        assert {d["name"] for d in g["downstream"]} == {"fs1", "sp1", "fac1"}
        assert g["node"]["name"] == "ds1"
        # 链式：fs1 的下游 = {sp1, fac1}
        g2 = GraphHandler.get(lineage, "fieldset", "fs1")
        assert {d["name"] for d in g2["downstream"]} == {"sp1", "fac1"}
        assert [u["id"] for u in g2["upstream"]] == ["panel:ds1", "index:index", "table:m1"]

    def test_graph_handler_stale_and_scan(self, lineage):
        IndexHandler.notify_change(lineage, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"]))
        assert len(GraphHandler.stale(lineage)) == 4
        assert GraphHandler.scan(lineage) == {"node_count": 7, "edge_count": 6}

    def test_delete_leaf_then_parent(self, lineage):
        # 逐层删叶子（新链顺序：fac1 → sp1 → fs1 → ma5f → ds1 → m1 → index）
        FactorHandler.delete(lineage, "fac1")
        SampleHandler.delete(lineage, "sp1")
        FieldsetHandler.delete(lineage, "fs1")
        FeatureHandler.delete(lineage, "ma5f")
        PanelHandler.delete(lineage, "ds1")
        TableHandler.delete(lineage, "m1")
        IndexHandler.delete(lineage, "index")
        assert lineage.stats() == {"node_count": 0, "edge_count": 0}


class TestStorageHook:
    def test_custom_storage_hook_called(self, lineage):
        calls = []

        class SpyStorage:
            def materialize(self, node, accumulated):
                calls.append((node.name, accumulated))
                return True

            def check(self, node):
                return {"ok": True}

        ctrl2 = GraphController(GraphStore(":memory:"), storage=SpyStorage())
        IndexHandler.add(ctrl2, "index", columns=[{"name": "sym"}])
        PanelHandler.add(ctrl2, "p1", "index")
        IndexHandler.notify_change(ctrl2, "index", event=DataChangeEvent(
            action="upsert", symbol_scope=["a"]))
        PanelHandler.update(ctrl2, "p1")
        assert calls and calls[0][0] == "p1"
        assert calls[0][1]["upsert"].symbol_scope == ["a"]


# =====================================================================
# Execute 通道（graph source：后端经 gRPC Execute 返回 JSON，前端直接渲染）
# =====================================================================

def _make_graph_db(base_dir: str) -> GraphController:
    """在 base_dir 建 graph.db（index/m1 → panel → fieldset → sample → factor）。"""
    os.makedirs(base_dir, exist_ok=True)
    ctrl = GraphController(GraphStore(os.path.join(base_dir, "graph.db")))
    IndexHandler.add(ctrl, "index", symbol_col="sym", datetime_col="date",
                     columns=[{"name": "sym"}, {"name": "date"}])
    TableHandler.add(ctrl, "m1", columns=[{"name": "sym"}, {"name": "price"}])
    PanelHandler.add(ctrl, "ds1", "index", tables={"m1": "left_join"}, keys=["sym"])
    FieldsetHandler.add(ctrl, "fs1", "ds1")
    SampleHandler.add(ctrl, "sp1", "fs1", formula="x > 0")
    FeatureHandler.add(ctrl, "ma5f", "price.rolling_mean(5)")
    FactorHandler.add(ctrl, "fac1", "ma5f", "sp1")
    ctrl.resolve_all()
    ctrl.store.close()
    return ctrl


class TestGraphDispatch:
    """dispatch 层直测（不经 gRPC，data_dir 透传）。"""

    def test_lineage_full(self):
        from stkoe.grpc.dispatch import dispatch

        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_dispatch")
        _make_graph_db(base)
        try:
            results = dispatch("graph", "lineage", [], data_dir=base)
            payload = json.loads(results[0].data)
            assert payload["graph"]["node_count"] == 7
            assert payload["graph"]["edge_count"] == 6
            ids = {n["data"]["id"] for n in payload["elements"]["nodes"]}
            assert ids == {"index:index", "table:m1", "panel:ds1", "fieldset:fs1",
                           "sample:sp1", "feature:ma5f", "factor:fac1"}
        finally:
            import shutil
            shutil.rmtree(base, ignore_errors=True)

    def test_lineage_subgraph_with_depth(self):
        from stkoe.grpc.dispatch import dispatch

        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_dispatch")
        _make_graph_db(base)
        try:
            results = dispatch("graph", "lineage",
                               ["--node", "fieldset:fs1", "--depth", "1"],
                               data_dir=base)
            payload = json.loads(results[0].data)
            ids = {n["data"]["id"] for n in payload["elements"]["nodes"]}
            assert ids == {"fieldset:fs1", "panel:ds1", "sample:sp1"}
        finally:
            import shutil
            shutil.rmtree(base, ignore_errors=True)

    def test_nodes_and_stats(self):
        from stkoe.grpc.dispatch import dispatch

        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_dispatch")
        _make_graph_db(base)
        try:
            nodes = json.loads(dispatch("graph", "nodes", [], data_dir=base)[0].data)
            assert len(nodes) == 7
            by_type = [n for n in nodes if n["type"] == "panel"]
            assert by_type[0]["name"] == "ds1"
            stats = json.loads(dispatch("graph", "stats", [], data_dir=base)[0].data)
            assert stats == {"node_count": 7, "edge_count": 6}
        finally:
            import shutil
            shutil.rmtree(base, ignore_errors=True)

    def test_empty_graph_when_db_missing(self):
        from stkoe.grpc.dispatch import dispatch

        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_empty")
        os.makedirs(base, exist_ok=True)
        try:
            payload = json.loads(dispatch("graph", "lineage", [], data_dir=base)[0].data)
            assert payload["graph"]["node_count"] == 0
            assert payload["elements"]["nodes"] == []
            assert dispatch("graph", "nodes", [], data_dir=base)[0].data == "[]"
        finally:
            import shutil
            shutil.rmtree(base, ignore_errors=True)


class TestGraphGrpcExecute:
    """gRPC Execute 端到端：e:graph ... → DataHeader(0) + JsonData。"""

    @pytest.fixture()
    def srv(self):
        import socket

        from stkoe.grpc.server import StkoeServer

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        base = os.path.join(os.environ.get("TEMP", "."), "gql_test_grpc")
        _make_graph_db(base)
        srv = StkoeServer(port=port, data_dir=base).start()
        yield srv
        srv.stop()
        import shutil
        shutil.rmtree(base, ignore_errors=True)

    @pytest.fixture()
    def client(self, srv):
        import grpc

        from stkoe.grpc import stkoe_pb2_grpc

        ch = grpc.insecure_channel(f"127.0.0.1:{srv.port}")
        yield stkoe_pb2_grpc.StkoeServiceStub(ch)
        ch.close()

    @staticmethod
    def _collect(responses):
        header, datas = None, []
        for r in responses:
            if r.WhichOneof("type") == "header":
                header = r.header
            else:
                datas.append(r)
        return header, datas

    def test_execute_graph_lineage(self, client):
        from stkoe.grpc import stkoe_pb2

        header, datas = self._collect(client.Execute(
            stkoe_pb2.ExecuteRequest(source="graph", action="lineage")))
        assert header.code == 0
        assert len(datas) == 1
        assert datas[0].WhichOneof("type") == "json"
        payload = json.loads(datas[0].json.data)
        assert payload["graph"]["node_count"] == 7

    def test_execute_graph_lineage_node(self, client):
        from stkoe.grpc import stkoe_pb2

        header, datas = self._collect(client.Execute(
            stkoe_pb2.ExecuteRequest(
                source="graph", action="lineage",
                args=["--node", "panel:ds1", "--depth", "2"])))
        assert header.code == 0
        payload = json.loads(datas[0].json.data)
        assert payload["graph"]["center"] == "panel:ds1"
        # ds1 + 上游(index/m1) + 下游 2 层(fs1/sp1) = 5；fac1 在第 3 层不包含
        assert payload["graph"]["node_count"] == 5

    def test_execute_graph_nodes_stats(self, client):
        from stkoe.grpc import stkoe_pb2

        header, datas = self._collect(client.Execute(
            stkoe_pb2.ExecuteRequest(source="graph", action="nodes",
                                     args=["--type", "panel"])))
        assert header.code == 0
        nodes = json.loads(datas[0].json.data)
        assert [n["name"] for n in nodes] == ["ds1"]
        header, datas = self._collect(client.Execute(
            stkoe_pb2.ExecuteRequest(source="graph", action="stats")))
        assert json.loads(datas[0].json.data) == {"node_count": 7, "edge_count": 6}

    def test_execute_graph_bad_depth(self, client):
        from stkoe.grpc import stkoe_pb2

        header, datas = self._collect(client.Execute(
            stkoe_pb2.ExecuteRequest(source="graph", action="lineage",
                                     args=["--depth", "0"])))
        assert header.code != 0  # 业务错误：深度必须为正
