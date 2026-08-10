"""table 模块测试：scan 差异/幂等/布局识别、meta/consistent、set/rename/del"""
import polars as pl
import pytest

import stkoe.data as data
from stkoe.data.table import DependencyError, TableExistsError, TableNotFoundError
from stkoe.data.catalog.spec import TableLayout

from conftest import make_df, write_hive, write_single


def test_scan_registers_single(root):
    """隐式注册：scan 发现未注册目录自动注册，INSERT version=1 不再 bump"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]))
    r = data.table.scan("t1")
    assert r.implicit_registered and r.changed
    assert r.layout == TableLayout.SINGLE
    assert r.version_before == 0 and r.version_after == 1
    assert r.partition_count == 1

    m = data.table.meta("t1")
    assert m.version == 1
    assert len(m.files) == 1
    assert [c.name for c in m.columns] == ["date", "sym", "r"]
    assert all(c.data_type for c in m.columns)


def test_scan_idempotent_no_bump(root):
    """无差异重复 scan 不 bump version"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    r = data.table.scan("t1")
    assert not r.changed
    assert r.version_after == 1
    assert data.table.meta("t1").version == 1


def test_scan_detect_new_file_bumps(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    data.table.scan("t1")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    r = data.table.scan("t1")
    assert r.changed
    assert [x.kind for x in r.diffs] == ["added"]
    assert r.version_before == 1 and r.version_after == 2
    assert data.table.meta("t1").consistent


def test_scan_detect_removed_file(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    data.table.scan("t1")
    (d / "a.parquet").unlink()
    r = data.table.scan("t1")
    assert [x.kind for x in r.diffs] == ["removed"]
    assert r.version_after == 2
    assert len(data.table.meta("t1").files) == 1


def test_scan_detect_inplace_modify(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "a.parquet"
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(p)
    data.table.scan("t1")
    make_df([("2020-01-01", "a", 9.9)]).write_parquet(p)
    r = data.table.scan("t1")
    assert [x.kind for x in r.diffs] == ["changed"]
    assert r.version_after == 2


def test_scan_hive_layout(root):
    df = make_df([("2020-01-01", "a", 1.0), ("2020-06-01", "b", 2.0), ("2021-01-01", "c", 3.0)])
    write_hive(root, "t1", df, partition_by="year")
    r = data.table.scan("t1")
    assert r.layout == TableLayout.HIVE
    assert r.partition_by == ("year",)
    assert r.partition_count == 2

    m = data.table.meta("t1")
    assert m.partition_count == 2
    assert len(m.files) == 2
    assert all(f.partition_path for f in m.files)


def test_scan_flat_layout(root):
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    r = data.table.scan("t1")
    assert r.layout == TableLayout.FLAT
    assert r.partition_count == 1


def test_read_path_auto_sniff(root):
    """未注册目录：get 读前快检自动隐式注册并读到数据"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    df = data.table.get("t1")
    assert df.height == 1
    m = data.table.meta("t1")
    assert m.name == "t1" and m.version == 1


def test_read_path_detect_manual_change(root):
    """手工改文件后 get 自动 scan 到新数据并 bump"""
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "a.parquet"
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(p)
    data.table.scan("t1")
    v1 = data.table.meta("t1").version
    make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]).write_parquet(p)
    df = data.table.get("t1")
    assert df.height == 2
    assert data.table.meta("t1").version == v1 + 1


def test_meta_consistent_readonly(root):
    """meta 只读对账：磁盘新增文件 → consistent=False 且不 bump；scan 后恢复"""
    d = root / "tables" / "t1"
    d.mkdir(parents=True, exist_ok=True)
    make_df([("2020-01-01", "a", 1.0)]).write_parquet(d / "a.parquet")
    data.table.scan("t1")
    assert data.table.meta("t1").consistent

    make_df([("2020-02-01", "b", 2.0)]).write_parquet(d / "b.parquet")
    m = data.table.meta("t1")
    assert not m.consistent and m.version == 1  # meta 不动手

    r = data.table.scan("t1")
    assert r.changed and r.version_after == 2
    assert data.table.meta("t1").consistent


def test_add_and_list(root):
    """add 注册已有数据目录；--all 批量发现未注册表"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "t2", make_df([("2020-01-01", "a", 1.0)]))
    r = data.table.add("t1")
    assert r.name == "t1" and r.implicit_registered and r.changed
    with pytest.raises(TableExistsError):
        data.table.add("t1")
    with pytest.raises(TableNotFoundError):
        data.table.add("nope")
    reports = data.table.add(None, all=True)
    assert [x.name for x in reports] == ["t2"]
    assert [m.name for m in data.table.list()] == ["t1", "t2"]


def test_scan_all(root):
    write_single(root, "a", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "b", make_df([("2020-01-01", "b", 1.0)]))
    reports = data.table.scan(None, all=True)
    assert {r.name for r in reports} == {"a", "b"}
    assert all(r.implicit_registered for r in reports)


def test_set_metadata_no_bump(root):
    """set 改 display_name/tags 是纯元数据修改，不 bump version"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    m = data.table.set("t1", display_name="新名字", tags=["x"])
    assert m.display_name == "新名字" and m.version == 1
    assert data.table.meta("t1").display_name == "新名字"


def test_col_metadata(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    m = data.table.col("t1", "r", display_name="收益", unit="pct")
    c = next(c for c in m.columns if c.name == "r")
    assert c.display_name == "收益" and c.unit == "pct"


def test_rename_moves_dir_and_catalog(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    data.table.set("t1", display_name="旧名")
    m = data.table.rename("t1", "t1b")
    assert m.name == "t1b"
    assert (root / "tables" / "t1b").exists()
    assert not (root / "tables" / "t1").exists()
    assert data.table.meta("t1b").display_name == "旧名"  # 自定义 display_name 保留
    with pytest.raises(TableNotFoundError):
        data.table.meta("t1")

    # 冲突：目标已注册 → TableExistsError
    write_single(root, "t2", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t2")
    with pytest.raises(TableExistsError):
        data.table.rename("t1b", "t2")
    # 目录存在但未注册 → FileExistsError
    (root / "tables" / "t3").mkdir(parents=True)
    with pytest.raises(FileExistsError):
        data.table.rename("t1b", "t3")


def test_del_removes_registration_keeps_data(root):
    """del 只删 catalog 登记，绝不删用户数据文件"""
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    data.table.del_("t1")
    with pytest.raises(TableNotFoundError):
        data.table.meta("t1")
    assert (root / "tables" / "t1" / "t1.parquet").exists()
    with pytest.raises(TableNotFoundError):
        data.table.del_("nope")


def test_del_dependency_guard(root):
    """被 dataset 引用时 del 默认报错，force 级联清理"""
    write_single(root, "idx", make_df([("2020-01-01", "a", 1.0)]))
    write_single(root, "mem", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("idx")
    data.table.scan("mem")
    data.dataset.add("ds", "idx", "mem", background=False)
    with pytest.raises(DependencyError):
        data.table.del_("idx")
    data.table.del_("idx", force=True)
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.describe("ds")


def test_data_key_changes_with_content(root):
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0)]))
    data.table.scan("t1")
    k1 = data.table.data_key("t1")
    assert k1
    write_single(root, "t1", make_df([("2020-01-01", "a", 1.0), ("2020-01-02", "b", 2.0)]))
    assert data.table.data_key("t1") != k1
