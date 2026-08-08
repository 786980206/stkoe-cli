"""dataset 模块测试：scan_spec 规格/join 键校验、add 物化、scan 增量、分区、rename/del"""
import datetime
import time

import polars as pl
import pytest

import stkoe.data as data
from stkoe.data.table import DependencyError

from conftest import write_single


def _index_df(n=2, start="2020-01-01"):
    """索引表：仅 date, sym（join 键由 index 表列定义）"""
    return pl.DataFrame({
        "date": [start] * n,
        "sym": [f"s{i}" for i in range(n)],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


def _mem_df(n=2, start="2020-01-01"):
    return pl.DataFrame({
        "date": [start] * n,
        "sym": [f"s{i}" for i in range(n)],
        "r": [0.1 * (i + 1) for i in range(n)],
        "extra": [i + 1 for i in range(n)],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))


def _setup_pair(root, n=2):
    write_single(root, "idx", _index_df(n))
    write_single(root, "mem", _mem_df(n=n))
    data.table.scan("idx")
    data.table.scan("mem")


def _wait_materialized(name: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            dm = data.dataset.meta(name)
        except data.dataset.DatasetNotFoundError:
            time.sleep(0.05)
            continue
        if dm.materialized:
            return dm
        time.sleep(0.05)
    raise TimeoutError(f"materialize timeout: {name}")


# ---------- scan_spec：join 规格推导与校验 ----------

def test_scan_spec_derives_keys(root):
    _setup_pair(root)
    r = data.dataset.scan_spec("idx", "mem")
    assert r["ok"]
    assert r["keys"] == ["date", "sym"]
    assert r["columns"][0].as_index
    names = [c.name for c in r["columns"]]
    assert {"date", "sym", "extra"} <= set(names)
    for c in r["columns"]:
        assert c.source_table and c.source_field


def test_scan_spec_keys_from_index(root):
    """join 键取 index 表全部列，而非公共列；成员缺键必须报错"""
    write_single(root, "idx2", pl.DataFrame({"date": ["2020-01-01"], "sym": ["s0"], "x": [1]})
                 .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    write_single(root, "mem", _mem_df(n=1))
    data.table.scan("idx2")
    data.table.scan("mem")
    r = data.dataset.scan_spec("idx2", "mem")
    assert not r["ok"] and "x" in r["message"]


def test_scan_spec_explicit_key_missing(root):
    """显式键必须是 index 列，且每个成员表都要有"""
    _setup_pair(root)
    d = root / "tables" / "mem2"
    d.mkdir(parents=True)
    pl.DataFrame({"date": ["2020-01-01"], "other": [1]}) \
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")) \
        .write_parquet(d / "mem2.parquet")
    data.table.scan("mem2")
    r = data.dataset.scan_spec("idx", "mem2", keys=["date", "sym"])
    assert not r["ok"] and "sym" in r["message"]
    r2 = data.dataset.scan_spec("idx", "mem", keys=["foo"])
    assert not r2["ok"] and "not in index" in r2["message"]
    with pytest.raises(ValueError, match="missing join keys"):
        data.dataset.add("ds", "idx", "mem2", keys=["date", "sym"])


def test_scan_spec_no_common(root):
    """成员表缺 index 键 → 明确报错（不让 join 键静默退化）"""
    _setup_pair(root)
    d = root / "tables" / "mem3"
    d.mkdir(parents=True)
    pl.DataFrame({"foo": [1, 2]}).write_parquet(d / "mem3.parquet")
    data.table.scan("mem3")
    r = data.dataset.scan_spec("idx", "mem3")
    assert not r["ok"] and "missing join keys" in r["message"]


# ---------- add / 物化 ----------

def test_add_materializes_flat(root):
    """物化产物直接放 datasets/<name>/（无 .materialized 嵌套层级）"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    assert (root / "datasets" / "ds" / "data.parquet").exists()
    assert not (root / "datasets" / "ds" / ".materialized").exists()
    assert data.dataset.get("ds").height == 2


def test_add_meta_flat(root):
    _setup_pair(root)
    dm = data.dataset.add("ds", "idx", "mem", background=False)
    assert dm.materialized and dm.version == 1
    assert dm.partition_gran == "" and dm.partition_by == ()
    assert dm.keys == ("date", "sym")
    assert dm.curated
    out = data.dataset.get("ds")
    assert out.height == 2
    assert {"date", "sym", "r", "extra"} <= set(out.columns)


def test_add_background_then_materialized(root):
    _setup_pair(root)
    h = data.dataset.add("ds", "idx", "mem", background=True)
    assert h.status in ("submitted", "running")
    dm = _wait_materialized("ds")
    assert dm.materialized and dm.curated
    st = data.stat.get("ds")
    assert {"field", "count"} <= set(st.columns)
    assert len(st) == 4  # date/sym/r/extra
    row = st.filter(pl.col("field") == "r").to_dicts()[0]
    assert row["count"] == 2 and row["mean"] == pytest.approx(0.15)


def test_add_no_materialize_realtime(root):
    """只注册不物化；get 读前自动物化（透明）"""
    _setup_pair(root)
    dm = data.dataset.add("ds", "idx", "mem", materialize=False, background=False)
    assert dm.materialized is False
    assert not (root / "datasets" / "ds" / "data.parquet").exists()
    out = data.dataset.get("ds")
    assert out.height == 2 and {"extra", "r"} <= set(out.columns)
    assert data.dataset.meta("ds").materialized


def test_add_existing_requires_force(root):
    """已存在时非 force 直接报错（明确提示）；force 覆盖重建并清空旧物化产物"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    v1 = data.dataset.meta("ds").version
    with pytest.raises(data.dataset.DatasetExistsError):
        data.dataset.add("ds", "idx", "mem", background=False)
    dm = data.dataset.meta("ds")
    assert dm.version == v1 and dm.index_table == "idx" and dm.tables == ("mem",)

    m2 = pl.DataFrame({
        "date": ["2020-01-01", "2020-01-01"],
        "sym": ["s0", "s1"],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    write_single(root, "mem2", m2)
    data.table.scan("mem2")
    dm = data.dataset.add("ds", "idx", "mem2", background=False, force=True)
    assert dm.index_table == "idx" and dm.tables == ("mem2",)
    out = data.dataset.get("ds")
    assert out.columns == ["date", "sym"] and out.height == 2


# ---------- scan：增量重物化 ----------

def test_scan_idempotent(root):
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    v1 = data.dataset.meta("ds").version
    r = data.dataset.scan("ds")
    assert not r.changed and r.version_after == v1
    assert r.version_before == v1


def test_scan_incremental_identity_partition(root):
    """镜像 index HIVE 分区：改一个分区只重建该分区，版本 +1"""
    d = root / "tables" / "idx"
    d.mkdir(parents=True)
    df = pl.DataFrame({
        "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 2)],
        "sym": ["s0", "s0", "s1"],
    })
    df.write_parquet(d / "data", partition_by=["date"])
    data.table.scan("idx")
    memdf = pl.DataFrame({
        "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 2)],
        "sym": ["s0", "s0", "s1"],
        "extra": [1, 2, 3],
    })
    write_single(root, "mem", memdf)
    data.table.scan("mem")

    data.dataset.add("ds", "idx", "mem", background=False)
    dm = data.dataset.meta("ds")
    assert dm.partition_gran == "identity"
    assert sorted(p.name for p in (root / "datasets" / "ds").glob("part=*")) == \
        ["part=2020-01-01", "part=2020-01-02"]
    v1 = dm.version

    part = data.dataset.get("ds", partition="2020-01-02")
    assert set(str(x) for x in part["date"].to_list()) == {"2020-01-02"}

    # 追加一个同分区文件（内连接只保留有 mem 匹配的行，2020-01-02 保持 2 行）
    extra = pl.DataFrame({"date": [datetime.date(2020, 1, 2)], "sym": ["s2"]})
    extra.write_parquet(d / "data" / "date=2020-01-02" / "data-extra.parquet")
    r = data.dataset.scan("ds")
    assert r.changed
    assert r.rebuilt_partitions == ("2020-01-02",)
    assert r.version_before == v1 and r.version_after == v1 + 1
    assert data.dataset.get("ds", partition="2020-01-02").height == 2


def test_scan_incremental_flat(root):
    """flat 全量：改源文件后 scan 重物化并 bump"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    v1 = data.dataset.meta("ds").version
    write_single(root, "idx", _index_df(n=3))  # 追加一行
    write_single(root, "mem", _mem_df(n=3))    # 同步补齐 join 键
    r = data.dataset.scan("ds")
    assert r.changed
    assert r.rebuilt_partitions == ("",)
    assert r.version_after == v1 + 1
    assert data.dataset.get("ds").height == 3


def test_scan_auto_year_partition(root):
    """数据量≥1M 且含时间键 → 自动 year 分区"""
    d = root / "tables" / "big"
    d.mkdir(parents=True)
    n = 1_000_000
    dates = pl.date_range(datetime.date(2020, 1, 1), datetime.date(2022, 12, 31), "1d", eager=True) \
        .sample(n, with_replacement=True, seed=42)
    idx = pl.DataFrame({"date": dates, "sym": pl.Series([f"s{i % 200}" for i in range(n)])})
    idx.write_parquet(d / "big.parquet")
    data.table.scan("big")
    memb = idx.with_columns(pl.Series("cnt", range(n), dtype=pl.Int64))
    write_single(root, "memb", memb)
    data.table.scan("memb")

    data.dataset.add("ds", "big", "memb", background=False)
    dm = data.dataset.meta("ds")
    assert dm.partition_gran == "year" and dm.partition_by == ("date",)
    parts = sorted(p.name for p in (root / "datasets" / "ds").glob("part=*"))
    assert parts == ["part=2020", "part=2021", "part=2022"]
    y = data.dataset.get("ds", partition="2020")
    assert y.height > 0
    assert set(str(x)[:4] for x in y["date"].to_list()) == {"2020"}


def test_scan_all(root):
    _setup_pair(root)
    data.dataset.add("ds1", "idx", "mem", background=False)
    data.dataset.add("ds2", "idx", "mem", background=False)
    reports = data.dataset.scan(None, all=True)
    assert {r.name for r in reports} == {"ds1", "ds2"}
    assert all(not r.changed for r in reports)


# ---------- rename / del ----------

def test_rename_del(root):
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    dm = data.dataset.rename("ds", "ds2")
    assert dm.name == "ds2"
    assert data.dataset.meta("ds2").name == "ds2"
    assert (root / "datasets" / "ds2" / "data.parquet").exists()
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.meta("ds")

    data.dataset.del_("ds2")
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.meta("ds2")
    assert not (root / "datasets" / "ds2").exists()
    # 用户数据表不受影响
    assert data.table.meta("idx").version >= 1
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.del_("nope")


def test_del_keeps_materialized_without_data(root):
    """del_ with_data=False 保留物化产物（仅供显式选择）"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    data.dataset.del_("ds", with_data=False)
    assert (root / "datasets" / "ds" / "data.parquet").exists()


def test_del_dependency_guard(root):
    """被 stat 引用时 del 默认报错；force 级联删除 stat"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    data.stat.add("ds", background=False)
    with pytest.raises(DependencyError):
        data.dataset.del_("ds")
    data.dataset.del_("ds", force=True)
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.meta("ds")


def test_data_key_changes_when_sources_change(root):
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    k1 = data.dataset.data_key("ds")
    assert k1
    write_single(root, "mem", _mem_df(n=3))
    data.table.scan("mem")
    assert data.dataset.data_key("ds") != k1
