"""catalog 事务 / sniff 差异 / 布局识别测试"""
import os

import polars as pl
import pytest

import stkoe.data as data
from stkoe.data import catalog
from stkoe.data.table import TableNotFoundError
from stkoe.data.catalog.spec import TableLayout

from conftest import make_df, write_hive, write_single


def test_sniff_registers_single(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]))
    r = data.sniff("t1")
    assert r.implicit_registered and r.changed
    assert r.layout == TableLayout.SINGLE
    assert r.version_before == 0 and r.version_after == 1
    assert r.partition_count == 1

    m = data.describe("t1")
    assert m.version == 1
    assert m.file_count == 1 and m.row_count == 2
    assert [c.name for c in m.columns] == ["date", "sym", "r"]


def test_sniff_idempotent_no_bump(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
    r = data.sniff("t1")
    assert not r.changed
    assert r.version_after == 1


def test_sniff_detect_new_file_bumps(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    data.sniff("t1")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    r = data.sniff("t1")
    assert r.changed
    assert [x.kind for x in r.diffs] == ["added"]
    assert r.version_before == 1 and r.version_after == 2
    assert data.describe("t1").file_count == 2


def test_sniff_detect_removed_file(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    data.sniff("t1")
    (d / "a.parquet").unlink()
    r = data.sniff("t1")
    assert [x.kind for x in r.diffs] == ["removed"]
    assert r.version_after == 2
    assert data.describe("t1").file_count == 1


def test_sniff_detect_inplace_modify(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "a.parquet"
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(p)
    data.sniff("t1")
    old_mtime = p.stat().st_mtime_ns
    make_df([("2020-01-01", "a", 9.9)]).write_parquet(p)
    assert p.stat().st_mtime_ns != old_mtime or p.stat().st_size != (d / "a.parquet").stat().st_size
    r = data.sniff("t1")
    assert [x.kind for x in r.diffs] == ["changed"]
    assert r.version_after == 2


def test_sniff_hive_layout(root):
    df = make_df([("2020-01-01", "a", 1.0), ("2020-06-01", "b", 2.0), ("2021-01-01", "c", 3.0)])
    write_hive(root, "t1", df, partition_by="year")
    r = data.sniff("t1")
    assert r.layout == TableLayout.HIVE
    assert r.partition_by == ("year",)
    assert r.partition_count == 2

    m = data.describe("t1")
    assert m.partition_count == 2 and m.row_count == 3
    assert data.partitions("t1") == ["year=2020", "year=2021"]


def test_sniff_flat_layout(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    r = data.sniff("t1")
    assert r.layout == TableLayout.FLAT
    assert r.partition_count == 1


def test_read_path_auto_sniff(root):
    """未注册目录：describe/select 读前自动快检 → 隐式注册"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    m = data.describe("t1")
    assert m.name == "t1" and m.version == 1
    lf = data.select("t1")
    assert lf.collect().height == 1


def test_read_path_detect_manual_change(root):
    """手工改文件后 select 自动 sniff 到新数据"""
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "a.parquet"
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(p)
    data.sniff("t1")
    v1 = data.describe("t1").version
    make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]).write_parquet(p)
    lf = data.select("t1")
    assert lf.collect().height == 2
    assert data.describe("t1").version == v1 + 1


def test_status_readonly(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
    s = data.status("t1")
    assert s.registered and s.consistent and s.diffs == ()

    d = root / "tables" / "t1"
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    s = data.status("t1")
    assert not s.consistent
    assert [x.kind for x in s.diffs] == ["added"]
    # status 不动手：catalog 版本不变（describe 才会触发读前 sniff）
    v = catalog().conn.execute("SELECT version FROM stkoe_objects WHERE name='t1'").fetchone()
    assert v["version"] == 1


def test_create_drop_rename_update(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")

    h = data.create("t2")
    assert h.status == "succeeded"
    assert data.describe("t2").version == 1

    m = data.update("t1", display_name="新名字", tags=["x"], bump=True)
    assert m.display_name == "新名字" and m.version == 2

    # rename 一并移动目录并同步元数据
    h = data.rename("t1", "t1b")
    assert h.status == "succeeded"
    assert (root / "tables" / "t1b").exists()
    assert not (root / "tables" / "t1").exists()
    assert data.describe("t1b").file_count == 1
    assert data.describe("t1b").display_name == "新名字"  # 自定义 display_name 保留
    with pytest.raises(TableNotFoundError):
        data.describe("t1")

    h = data.drop("t2")
    assert h.status == "succeeded"
    # drop 后注册消失（数据目录还在 → 后续 read 会隐式重新注册，属设计行为）
    assert catalog().conn.execute("SELECT id FROM stkoe_objects WHERE name='t2'").fetchone() is None
    assert (root / "tables" / "t2").exists()


def test_rename_follows_display_name_and_stays_consistent(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t1")
    assert data.describe("t1").display_name == "t1"

    h = data.rename("t1", "t1b")
    assert h.status == "succeeded"
    m = data.describe("t1b")
    # display_name 未自定义 → 跟随新名；目录改名不改 rel_path/签名 → catalog 仍一致
    assert m.display_name == "t1b" and m.file_count == 1
    assert data.status("t1b").consistent

    # 冲突校验：目标已注册或已存在目录 → 失败
    write_single(root, "t2", make_df([("2020-01-01", "a", 1.0)]))
    data.sniff("t2")
    h = data.rename("t1b", "t2")
    assert h.status == "failed" and "registered" in (h.error or "")
    assert (root / "tables" / "t1b").exists()


def test_create_all(root):
    """--all 只注册未注册且有数据的表；已注册/空目录跳过"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "t2", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "t3", make_df([("2020-01-01", "a", 1.0)]))
    (root / "tables" / "empty").mkdir(parents=True, exist_ok=True)
    data.sniff("t1")
    data.create("t3")

    reports = data.create_all()
    assert [r.name for r in reports] == ["t2"]
    assert all(r.implicit_registered and r.changed for r in reports)
    assert data.describe("t2").file_count == 1
    # 已注册（t1/t3）与空目录（empty）未被触碰
    assert [m.name for m in data.list()] == ["t1", "t2", "t3"]


def test_list(root):
    write_single(root, "a", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "b", make_df([("2020-01-01", "b", 1.0)]))
    data.sniff("a")
    data.sniff("b")
    assert [m.name for m in data.list()] == ["a", "b"]


def test_sniff_all(root):
    write_single(root, "a", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "b", make_df([("2020-01-01", "b", 1.0)]))
    reports = data.sniff_all()
    assert {r.name for r in reports} == {"a", "b"}
    assert all(r.implicit_registered for r in reports)
