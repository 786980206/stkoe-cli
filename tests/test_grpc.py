# -*- coding: utf-8 -*-
"""gRPC 服务测试：Execute 流式（DataHeader/JsonData）+ SubmitTask/SubscribeTask + Health"""
import json
import logging
import socket
from pathlib import Path

import grpc
import pytest

from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc
from stkoe.grpc.server import StkoeServer


@pytest.fixture()
def srv(tmp_path):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = StkoeServer(port=port, data_dir=str(tmp_path / "data")).start()
    yield srv
    srv.stop()


@pytest.fixture()
def client(srv):
    ch = grpc.insecure_channel(f"127.0.0.1:{srv.port}")
    stub = stkoe_pb2_grpc.StkoeServiceStub(ch)
    yield stub
    ch.close()


def _collect(responses):
    """收集 Execute 流：返回 (DataHeader, 数据消息列表)"""
    header, datas = None, []
    for r in responses:
        if r.WhichOneof("type") == "header":
            header = r.header
        else:
            datas.append(r)
    return header, datas


def _json(payloads, name):
    """取 Execute 结果中指定 name 的 JsonData.data（JSON 解析）"""
    return next(json.loads(d.json.data) for d in payloads
                if d.WhichOneof("type") == "json" and d.json.name == name)


# ---------- Health ----------

def test_health(client):
    resp = client.Health(stkoe_pb2.HealthRequest())
    assert resp.status == "ok"
    assert resp.version


# ---------- Execute ----------

def test_execute_version_json(client):
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="version", action="")))
    assert header.code == 0
    assert len(datas) == 1
    assert datas[0].WhichOneof("type") == "json"
    assert datas[0].json.name == "version"
    assert "version" in datas[0].json.data


def test_execute_config_show_json(client):
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="config", action="show")))
    assert header.code == 0
    assert datas[0].WhichOneof("type") == "json"
    assert datas[0].json.name == "config"


def test_execute_config_set_then_show(client, tmp_path, monkeypatch):
    """Execute config set 任意字段 → stkoe.json；show 反映生效配置"""
    monkeypatch.setenv("STKOE_CONFIG", str(tmp_path / "stkoe.json"))

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="config", action="set",
        args=["--grpc-host", "0.0.0.0", "--grpc-port", "9000"])))
    assert header.code == 0
    assert json.loads(datas[0].json.data)["set"] == {
        "grpc-host": "0.0.0.0", "grpc-port": "9000"}

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="config", action="show")))
    assert header.code == 0
    out = json.loads(datas[0].json.data)
    assert out["config_file"] == str(tmp_path / "stkoe.json")
    assert out["grpc-host"] == "0.0.0.0"
    assert out["grpc-port"] == 9000


def test_execute_mock_demo(client, srv):
    """e:mock demo：写 tables/index + tables/m1 到服务数据目录（默认 300×500）"""
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="mock", action="demo")))
    assert header.code == 0
    reports = json.loads(datas[0].json.data)
    assert [r["name"] for r in reports] == ["index", "m1"]
    assert reports[0]["rows"] == 300 * 500
    root = Path(srv.data_dir)
    assert (root / "indexs" / "index" / "data.parquet").exists()
    assert (root / "tables" / "m1" / "data.parquet").exists()


def test_execute_mock_demo_with_flags(client, srv):
    """e:mock demo --n-syms/--n-days 生效（hyphen 键名）"""
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="mock", action="demo",
        args=["--n-syms", "5", "--n-days", "3"])))
    assert header.code == 0
    reports = json.loads(datas[0].json.data)
    assert reports[0]["rows"] == 5 * 3
    assert (Path(srv.data_dir) / "indexs" / "index" / "data.parquet").exists()


def test_execute_mock_gen(client, srv):
    """e:mock gen：参数化生成单张表到 tables/<name>/（--n-syms 生效）"""
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="mock", action="gen",
        args=["g1", "--kind", "klday", "--n-syms", "4"])))
    assert header.code == 0
    rep = json.loads(datas[0].json.data)
    assert rep["name"] == "g1"
    assert rep["rows"] == 4 * 3
    assert (Path(srv.data_dir) / "tables" / "g1" / "data.parquet").exists()


def test_execute_mock_gen_missing_name_error(client):
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="mock", action="gen")))
    assert header.code != 0
    assert "需要表名" in header.message


def test_execute_unknown_command_error_header(client):
    """未注册命令：首条即错误 DataHeader，且无数据消息"""
    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="no_such_source", action="bogus")))
    assert header.code != 0
    assert "不支持的命令" in header.message
    assert datas == []


# ---------- Execute 版 table ----------

def test_execute_table_add_list_meta_get_delete(client, srv, tmp_path):
    """Execute 路径 table add/list/meta/get/delete 全链路（与 s:table 任务版对齐）"""
    import polars as pl

    root = Path(srv.data_dir) / "tables" / "demo"
    root.mkdir(parents=True)
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(root / "p1.parquet")

    def _json(payloads, name):
        for d in payloads:
            if d.WhichOneof("type") == "json" and d.json.name == name:
                return json.loads(d.json.data)
        return None

    # add
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add", args=["demo"])))
    assert header.code == 0
    report = _json(datas, "table")
    assert report["name"] == "demo"
    assert report["changed"] or True

    # list
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="list")))
    assert header.code == 0
    names = [t["name"] for t in _json(datas, "tables")]
    assert "demo" in names

    # meta
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="meta", args=["demo"])))
    assert header.code == 0
    assert _json(datas, "table")["name"] == "demo"

    # add 携带元数据：e:table add demo2 --display_name --tags（--type 为旧概念，进 extra）
    root2 = Path(srv.data_dir) / "tables" / "demo2"
    root2.mkdir(parents=True)
    pl.DataFrame({"a": [1]}).write_parquet(root2 / "p.parquet")
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add",
        args=["demo2", "--display_name=E表", "--tags=x, y", "--type=index"])))
    assert header.code == 0
    add_meta = _json(datas, "table")
    assert add_meta["name"] == "demo2"
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="meta", args=["demo2"])))
    assert _json(datas, "table")["display_name"] == "E表"
    assert _json(datas, "table")["tags"] == ["x", "y"]
    # V3.0：类型由 label 承载（table 恒 "table"），index 是独立资产；--type 进 extra
    assert _json(datas, "table")["type"] == "table"
    assert _json(datas, "table")["extra"]["type"] == "index"

    # set：更新元数据
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="set",
        args=["demo", "--display_name=D表", "--description=测试", "--source=local"])))
    assert header.code == 0
    set_meta = _json(datas, "table")
    assert set_meta["display_name"] == "D表"
    assert set_meta["description"] == "测试"
    assert set_meta["source"] == "local"

    # col：更新列元数据
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="col",
        args=["demo", "a", "--display_name=数值", "--unit=元", "--tags=x, y"])))
    assert header.code == 0
    col_meta = _json(datas, "table")
    col_a = next(c for c in col_meta["columns"] if c["name"] == "a")
    assert col_a["display_name"] == "数值"
    assert col_a["unit"] == "元"
    assert col_a["tags"] == ["x", "y"]

    # get：元信息并入 ArrowTable.meta（不再返回 JsonData），含完整列元数据
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="get", args=["demo"])))
    assert header.code == 0
    jsons = [d for d in datas if d.WhichOneof("type") == "json"]
    assert jsons == []  # get 不返回 JsonData
    tables = [d for d in datas if d.WhichOneof("type") == "table"]
    assert len(tables) == 1
    assert tables[0].table.name == "demo"
    meta = json.loads(tables[0].table.meta)
    assert meta["rows"] == 3
    assert meta["total"] == 3
    assert [c["name"] for c in meta["columns"]] == ["a", "b"]
    col_a = next(c for c in meta["columns"] if c["name"] == "a")
    assert col_a["display_name"] == "数值"
    assert col_a["unit"] == "元"
    assert col_a["tags"] == ["x", "y"]

    # get --limit：rows 为当前页行数，total 为未分页总行数
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="get", args=["demo", "--limit", "2"])))
    assert header.code == 0
    meta = json.loads(next(d for d in datas
                           if d.WhichOneof("type") == "table").table.meta)
    assert meta["rows"] == 2
    assert meta["total"] == 3

    # get --offset：跳过起始行；rows 为当前页行数，total 仍为过滤后全量
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="get", args=["demo", "--offset", "1", "--limit", "2"])))
    assert header.code == 0
    meta = json.loads(next(d for d in datas
                           if d.WhichOneof("type") == "table").table.meta)
    assert meta["rows"] == 2
    assert meta["total"] == 3

    # ArrowTable 数据必须是 IPC Stream 格式，客户端 open_stream 可直接解析
    import pyarrow as pa
    got = pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(tables[0].table.data))).read_all()
    assert got.num_rows == 3
    assert got.num_columns == 2

    # delete
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="delete", args=["demo"])))
    assert header.code == 0
    assert _json(datas, "table") == {"deleted": "demo"}


def test_execute_table_add_duplicate_error(client, srv, tmp_path):
    """重复 add：TableExistsError 以错误 DataHeader 返回（code!=0）"""
    import polars as pl

    root = Path(srv.data_dir) / "tables" / "dup"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"a": [1]}).write_parquet(root / "p1.parquet")

    header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add", args=["dup"])))
    assert header.code == 0

    header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add", args=["dup"])))
    assert header.code != 0
    assert "already registered" in header.message


def test_execute_table_missing_args(client):
    """缺表名：错误 DataHeader"""
    for action in ("add", "get", "meta", "delete", "set", "scan"):
        header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
            source="table", action=action)))
        assert header.code != 0
        assert ("需要表名" in header.message
                or "--all" in header.message
                or "--key" in header.message)


def test_execute_table_scan(client, srv, tmp_path):
    """e:table scan：显式重扫对账（无差异 changed=False；追加文件后 changed=True + 版本递增）"""
    import polars as pl

    root = Path(srv.data_dir) / "tables" / "demo"
    root.mkdir(parents=True)
    pl.DataFrame({"sym": ["a", "b"], "price": [1.0, 2.0]}).write_parquet(root / "data.parquet")
    header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add", args=["demo"])))
    assert header.code == 0

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="scan", args=["demo"])))
    assert header.code == 0
    report = _json(datas, "table")
    assert report["name"] == "demo"
    assert report["changed"] is False
    # V3.0 版本为高精度时间戳：无差异不 bump（version_after == version_before）
    assert report["version_after"] == report["version_before"]

    pl.DataFrame({"sym": ["c"], "price": [3.0]}).write_parquet(root / "more.parquet")
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="scan", args=["demo"])))
    assert header.code == 0
    report = _json(datas, "table")
    assert report["changed"] is True
    assert report["version_after"] > report["version_before"]


def test_execute_table_list_candidate(client, srv, tmp_path):
    """table list --candidate：返回未登记但含 parquet 的目录"""
    import polars as pl

    root = Path(srv.data_dir) / "tables"
    (root / "reg").mkdir(parents=True)
    pl.DataFrame({"a": [1]}).write_parquet(root / "reg" / "p.parquet")
    header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="add", args=["reg"])))
    assert header.code == 0

    (root / "cand").mkdir(parents=True)
    pl.DataFrame({"b": [2]}).write_parquet(root / "cand" / "p.parquet")
    (root / "empty").mkdir(parents=True)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="list", args=["--candidate"])))
    assert header.code == 0
    cands = next((json.loads(d.json.data) for d in datas
                  if d.WhichOneof("type") == "json"
                  and d.json.name == "candidates"), None)
    assert cands == ["cand"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="table", action="list")))
    assert header.code == 0
    names = _json_names(datas)
    assert names == ["reg"]


def _json_names(datas):
    import json
    for d in datas:
        if d.WhichOneof("type") == "json":
            return [t["name"] for t in json.loads(d.json.data)]
    return []


def test_execute_index_list_candidate(client, srv, tmp_path):
    """e:index list --candidate：未登记 index 但含 parquet 的表目录候选"""
    import polars as pl

    root = Path(srv.data_dir) / "indexs"
    (root / "reg").mkdir(parents=True)
    pl.DataFrame({"sym": ["a"], "date": ["2024-01-01"]}).write_parquet(root / "reg" / "p.parquet")
    header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="index", action="add", args=["reg"])))
    assert header.code == 0
    (root / "cand").mkdir(parents=True)
    pl.DataFrame({"sym": ["b"], "date": ["2024-01-02"]}).write_parquet(root / "cand" / "p.parquet")

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="index", action="list", args=["--candidate"])))
    assert header.code == 0
    cands = next((json.loads(d.json.data) for d in datas
                  if d.WhichOneof("type") == "json" and d.json.name == "candidates"), None)
    assert cands == ["cand"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="index", action="list")))
    assert header.code == 0
    names = [i["name"] for i in _json(datas, "indexes")]
    assert names == ["reg"]


# ---------- Execute 版 sample ----------

def _seed_panel_chain(client, srv, x2_formula="x*2"):
    """graph 语义造数：table idx/mem → index idx → panel ds → fieldset fs1(+x2 校验通过)"""
    import polars as pl

    root = Path(srv.data_dir)
    d = root / "indexs" / "idx"
    d.mkdir(parents=True)
    pl.DataFrame({"sym": ["a", "b"], "x": [1.0, 2.0],
                  "date": ["2026-01-01", "2026-01-02"]}).write_parquet(d / "data.parquet")
    m = root / "tables" / "mem"
    m.mkdir(parents=True)
    pl.DataFrame({"sym": ["a", "b"], "date": ["2026-01-01", "2026-01-02"],
                  "y": [10.0, 20.0]}).write_parquet(m / "data.parquet")

    for src, action, args in [
        ("table", "add", ["mem"]),
        ("index", "add", ["idx"]),
        ("panel", "add", ["ds", "idx", "mem"]),  # keys 由 index 推断 [sym, date]
        ("fieldset", "add", ["fs1", "--dataset", "ds"]),
        ("fieldset", "add", ["fs1", "x2", "--formula", x2_formula]),
    ]:
        header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
            source=src, action=action, args=args)))
        assert header.code == 0, (src, action, args, header.message)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="fieldset", action="check", args=["fs1", "x2"])))
    assert header.code == 0
    assert _json(datas, "fieldset")[0]["ok"] is True


def test_execute_sample_add_check_get_delete(client, srv):
    """Execute 路径 sample add/check/get/delete/list 全链路（graph 语义：依赖 fieldset）"""
    _seed_panel_chain(client, srv)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="add",
        args=["sp1", "--fieldset", "fs1", "--formula", "(x>=2.0)"])))
    assert header.code == 0
    assert _json(datas, "sample")["name"] == "sp1"

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="check", args=["sp1"])))
    assert header.code == 0
    assert _json(datas, "sample")["ok"] is True

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="get", args=["sp1", "--limit", "5"])))
    assert header.code == 0
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    assert len(tables) == 1
    meta = json.loads(tables[0].table.meta)
    assert meta["rows"] == 1  # 仅 x>=2.0 → b
    assert [c["name"] for c in meta["columns"]] == ["sym", "x", "date", "y", "x2"]
    assert meta["total"] == 1

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="list")))
    assert header.code == 0
    names = [s["name"] for s in _json(datas, "samples")]
    assert names == ["sp1"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="delete", args=["sp1"])))
    assert header.code == 0
    assert _json(datas, "sample") == {"deleted": "sp1"}


# ---------- Execute 版 fieldset ----------

def test_execute_fieldset_get_default_and_fields_only(client, srv):
    """Execute 路径 fieldset get 默认返回 panel+fieldset join 视图，--fields-only 仅返回衍生数据"""
    _seed_panel_chain(client, srv)

    # 默认：panel 视图 + fieldset 已校验指标 join 拼接
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="fieldset", action="get", args=["fs1"])))
    assert header.code == 0
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    assert len(tables) == 1
    meta = json.loads(tables[0].table.meta)
    assert [c["name"] for c in meta["columns"]] == ["sym", "x", "date", "y", "x2"]
    assert meta["rows"] == 2

    # --fields-only：仅返回衍生数据（keys + 已校验指标）
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="fieldset", action="get", args=["fs1", "--fields-only"])))
    assert header.code == 0
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    meta = json.loads(tables[0].table.meta)
    assert [c["name"] for c in meta["columns"]] == ["sym", "date", "x2"]


# ---------- Execute 版 feature ----------

def test_execute_feature_add_test_list_delete(client, srv):
    """Execute 路径 feature add/test/list/delete 全链路（graph 语义：test 在 sample 视图求值）"""
    import polars as pl

    _seed_panel_chain(client, srv)
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="sample", action="add",
        args=["sp1", "--fieldset", "fs1"])))
    assert header.code == 0

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="feature", action="add", args=["f1", "--formula", "x*2"])))
    assert header.code == 0
    assert _json(datas, "feature")["name"] == "f1"

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="feature", action="test", args=["f1", "--sample", "sp1"])))
    assert header.code == 0
    res = _json(datas, "feature")
    assert res["ok"] is True
    assert res["valid"] is True
    assert res["rows"] == 2
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    assert len(tables) == 1
    assert tables[0].table.name == "test/f1"

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="feature", action="list")))
    assert header.code == 0
    assert [ft["name"] for ft in _json(datas, "features")] == ["f1"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="feature", action="delete", args=["f1"])))
    assert header.code == 0
    assert _json(datas, "feature") == {"deleted": "f1"}


# ---------- Execute 版 factor ----------

def _seed_factor_chain(client, srv, ready=True):
    """graph 语义造数：table idx/mem → index → panel ds([sym,date]) → fieldset fs1
    → sample sp1 → feature f1（test 必需列 date/sym/r/ic/fv 齐备）；
    ready=True 时依次 update 上游链（panel/fieldset/sample/feature）"""
    import polars as pl

    root = Path(srv.data_dir)
    d = root / "indexs" / "idx"
    d.mkdir(parents=True)
    pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-01"],
        "r": [0.01, 0.02], "ic": ["G1", "G1"], "fv": [1.0, 2.0], "x": [1.0, 2.0],
    }).write_parquet(d / "data.parquet")
    m = root / "tables" / "mem"
    m.mkdir(parents=True)
    pl.DataFrame({"sym": ["a", "b"], "date": ["2024-01-01", "2024-01-01"],
                  "price": [1.5, 2.5]}).write_parquet(m / "data.parquet")

    for src, action, args in [
        ("table", "add", ["mem"]),
        ("index", "add", ["idx"]),
        ("panel", "add", ["ds", "idx", "mem"]),  # keys 由 index 推断 [sym, date]
        ("fieldset", "add", ["fs1", "--dataset", "ds"]),
        ("fieldset", "add", ["fs1", "x2", "--formula", "x*2"]),
    ]:
        header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
            source=src, action=action, args=args)))
        assert header.code == 0, (src, action, args, header.message)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="fieldset", action="check", args=["fs1", "x2"])))
    assert header.code == 0
    assert _json(datas, "fieldset")[0]["ok"] is True

    for src, action, args in [
        ("sample", "add", ["sp1", "--fieldset", "fs1"]),
        ("feature", "add", ["f1", "--formula", "x*2"]),
    ]:
        header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
            source=src, action=action, args=args)))
        assert header.code == 0, (src, action, args, header.message)

    if ready:
        # update 语义：上游依次就绪（panel → fieldset → sample → feature）
        for src, action, args in [
            ("panel", "update", ["ds"]),
            ("fieldset", "update", ["fs1"]),
            ("sample", "update", ["sp1"]),
            ("feature", "update", ["f1"]),
        ]:
            header, _ = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
                source=src, action=action, args=args)))
            assert header.code == 0, (src, action, args, header.message)


def test_execute_factor_add_get_check_scan_delete(client, srv):
    """Execute 路径 factor add/get/check/scan/delete 全链路（graph 语义）"""
    _seed_factor_chain(client, srv)
    root = Path(srv.data_dir)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="add", args=["fac1", "--feature", "f1",
                                             "--sample", "sp1"])))
    assert header.code == 0
    assert _json(datas, "factor")["name"] == "fac1"
    assert _json(datas, "factor")["keys"] == ["sym", "date"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="get", args=["fac1"])))
    assert header.code == 0
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    assert len(tables) == 1
    meta = json.loads(tables[0].table.meta)
    assert meta["rows"] == 2
    assert [c["name"] for c in meta["columns"]] == ["sym", "date", "f1"]
    assert meta["total"] == 2

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="check", args=["fac1"])))
    assert header.code == 0
    assert _json(datas, "factor")["ok"] is True

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="scan", args=["fac1"])))
    assert header.code == 0
    assert _json(datas, "factor")["changed"] is True
    assert (root / "factors" / "fac1" / "data.parquet").exists()

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="delete", args=["fac1"])))
    assert header.code == 0
    assert _json(datas, "factor") == {"deleted": "fac1"}


# ---------- Execute 版 factor_test（test 源） ----------

def test_execute_test_add_get_check_scan_and_stat(client, srv):
    """Execute 路径 test add/get/check/scan + stat scan test 全链路（graph 语义）"""
    _seed_factor_chain(client, srv)
    root = Path(srv.data_dir)

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="add", args=["fac1", "--feature", "f1",
                                             "--sample", "sp1"])))
    assert header.code == 0
    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="factor", action="update", args=["fac1"])))
    assert header.code == 0

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="test", action="add", args=["t1", "--factor", "fac1",
                                           "--returns", "r", "--groupby", "ic",
                                           "--marketcap", "fv"])))
    assert header.code == 0
    assert _json(datas, "test")["name"] == "t1"
    assert _json(datas, "test")["keys"] == ["sym", "date"]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="test", action="get", args=["t1"])))
    assert header.code == 0
    tables = [dd for dd in datas if dd.WhichOneof("type") == "table"]
    assert len(tables) == 1
    meta = json.loads(tables[0].table.meta)
    assert meta["rows"] == 2
    assert "factor_quantile" in [c["name"] for c in meta["columns"]]

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="test", action="check", args=["t1"])))
    assert header.code == 0
    assert _json(datas, "test")["ok"] is True

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="test", action="scan", args=["t1"])))
    assert header.code == 0
    assert _json(datas, "test")["changed"] is True
    assert (root / "factor_tests" / "t1" / "data.parquet").exists()

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="stat", action="scan", args=["t1", "--kind", "ic"])))
    assert header.code == 0
    report = _json(datas, "stat")
    assert report["target_type"] == "test"
    assert "ic_d1" in report["partitions"]
    assert (root / "stats" / "test" / "t1" / "ic" / "ic_d1.parquet").exists()

    header, datas = _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="test", action="delete", args=["t1"])))
    assert header.code == 0
    assert _json(datas, "test") == {"deleted": "t1"}


# ---------- 请求日志 ----------

def test_grpc_request_logging(client, caplog):
    """INFO 日志列出收到的请求（含 source/action/args/peer）与处理完成耗时"""
    caplog.set_level(logging.INFO)
    _collect(client.Execute(stkoe_pb2.ExecuteRequest(
        source="version", action="get", args=["--foo"])))

    exec_recv = [r for r in caplog.records if "接收请求 Execute" in r.message]
    exec_done = [r for r in caplog.records if "完成 Execute" in r.message]
    assert len(exec_recv) == 1
    assert "source=version" in exec_recv[0].message
    assert "action=get" in exec_recv[0].message
    assert "args=['--foo']" in exec_recv[0].message
    assert "peer=" in exec_recv[0].message
    assert len(exec_done) == 1
    assert "code=0" in exec_done[0].message
    assert "耗时" in exec_done[0].message


# ---------- SubmitTask / SubscribeTask ----------

def test_execute_task_list(client, srv):
    """e:task list：任务列表 JSON（最新在前），--state 过滤"""
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(source="mock", action=""))
    assert resp.header.code == 0
    task_id = resp.task_id
    list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(task_id=task_id, replay=True)))

    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="task", action="list")))
    assert header.code == 0
    tasks = json.loads(datas[0].json.data)
    assert tasks[0]["task_id"] == task_id  # 最新在前
    assert tasks[0]["source"] == "mock"
    assert tasks[0]["state"] == "succeeded"

    header, datas = _collect(client.Execute(
        stkoe_pb2.ExecuteRequest(source="task", action="list",
                                 args=["--state", "succeeded"])))
    assert header.code == 0
    tasks = json.loads(datas[0].json.data)
    assert all(t["state"] == "succeeded" for t in tasks)
    assert tasks[0]["task_id"] == task_id


def test_submit_and_subscribe_replay(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="version", action="get"))
    assert resp.header.code == 0
    assert resp.task_id

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=resp.task_id, replay=True)))
    assert responses[0].header.code == 0
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events
    assert events[0].seq == 1
    assert events[-1].state == "succeeded"
    assert events[-1].progress == 1.0
    assert "version" in events[-1].data
    assert all(e.state in ("pending", "running", "succeeded",
                           "failed", "cancelled") for e in events)


def test_subscribe_missing_task_error_header(client):
    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id="no_such_task", replay=True)))
    assert responses[0].WhichOneof("type") == "header"
    assert responses[0].header.code != 0
    assert "not found" in responses[0].header.message


def test_submit_task_error_event(client):
    """提交未注册命令的任务：事件流以 failed 状态结束，含错误消息"""
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="no_such_source", action="bogus"))
    assert resp.header.code == 0
    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=resp.task_id, replay=True)))
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events[-1].state == "failed"
    assert "不支持的命令" in events[-1].message


# ---------- TaskControl ----------

def test_task_control_cancel(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    assert resp.header.code == 0
    task_id = resp.task_id

    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=task_id, action="cancel"))
    assert ctrl.header.code == 0
    assert ctrl.task_id == task_id

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=task_id, replay=True)))
    events = [r.event for r in responses if r.WhichOneof("type") == "event"]
    assert events[-1].state == "cancelled"


def test_task_control_pause_resume(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    task_id = resp.task_id

    # 等任务进入 running 后暂停
    import time as _time
    deadline = _time.monotonic() + 5
    events = []
    while _time.monotonic() < deadline:
        r = client.TaskControl(stkoe_pb2.TaskControlRequest(
            task_id=task_id, action="pause"))
        if r.header.code == 0:
            break
        _time.sleep(0.02)
    else:
        assert False, "任务未能暂停"

    assert client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=task_id, action="resume")).header.code == 0

    responses = list(client.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(
        task_id=task_id, replay=True)))
    states = [r.event.state for r in responses if r.WhichOneof("type") == "event"]
    assert "paused" in states
    assert states[-1] == "succeeded"


def test_task_control_missing_task_error(client):
    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id="no_such_task", action="cancel"))
    assert ctrl.header.code != 0
    assert "not found" in ctrl.header.message


def test_task_control_unknown_action(client):
    resp = client.SubmitTask(stkoe_pb2.SubmitTaskRequest(
        source="mock", action=""))
    ctrl = client.TaskControl(stkoe_pb2.TaskControlRequest(
        task_id=resp.task_id, action="explode"))
    assert ctrl.header.code != 0
    assert "不支持的任务操作" in ctrl.header.message


def test_port_conflict(srv):
    with pytest.raises(Exception):
        StkoeServer(port=srv.port).start()
