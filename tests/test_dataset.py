"""dataset 模块测试：scan_spec 规格/join 键校验、add 物化、scan 增量、分区、rename/del"""
import datetime
import time

import polars as pl
import pytest

import stkoe.data as data
from stkoe.data.table import DependencyError

from conftest import make_df, write_single


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


def test_join_keys_tz_mismatch(root):
    """join 键 datetime 时区元数据不一致（naive vs UTC）必须归一后 join"""
    idx = pl.DataFrame({
        "date": ["2020-01-01"] * 2,
        "sym": ["s0", "s1"],
        "ts": ["2020-01-01 00:00:00", "2020-01-01 00:00:01"],
    }).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("ts").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S"),
    )
    mem_naive = pl.DataFrame({
        "date": ["2020-01-01"] * 2,
        "sym": ["s0", "s1"],
        "ts": ["2020-01-01 00:00:00", "2020-01-01 00:00:01"],
        "r": [1.0, 2.0],
    }).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("ts").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S"),
    )
    mem_utc = mem_naive.with_columns(pl.col("ts").dt.convert_time_zone("UTC"))
    write_single(root, "idx", idx)
    write_single(root, "memA", mem_naive)
    write_single(root, "memB", mem_utc)
    data.table.scan("idx")
    data.table.scan("memA")
    data.table.scan("memB")
    data.dataset.add("ds", "idx", "memA", "memB", keys=["date", "sym", "ts"])
    lf = data.dataset.get_lazy("ds")
    assert lf.collect().height == 2  # join 成功（此前 raise PolarsError）


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


def test_validate_fast_and_full(root):
    """validate：fast 只查索引字段存在；full 额外检查唯一性；结果写入 meta"""
    rows = [("2020-01-01", "a", 0.1), ("2020-01-02", "b", 0.5)]
    write_single(root, "idx", make_df(rows))
    write_single(root, "mem", make_df(rows))
    data.table.scan("idx")
    data.table.scan("mem")
    data.dataset.add("ds_v", "idx", "mem", background=False)

    out = data.dataset.validate("ds_v", mode="fast")
    assert out["valid"] is True
    assert {t["name"] for t in out["tables"]} == {"idx", "mem"}

    out_full = data.dataset.validate("ds_v", mode="full")
    assert out_full["valid"] is True
    # 元数据持久化
    m = data.dataset.meta("ds_v")
    assert m.validation["valid"] is True

    # 破坏唯一性：给 idx 插重复键，full 应报 invalid
    dup = pl.DataFrame({"date": ["2020-01-01", "2020-01-01"],
                        "sym": ["a", "a"], "r": [0.1, 0.1]}).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    write_single(root, "idx", dup)
    data.table.scan("idx")
    out2 = data.dataset.validate("ds_v", mode="full")
    assert out2["valid"] is False
    idx_row = next(t for t in out2["tables"] if t["name"] == "idx")
    assert idx_row["index_unique"] is False


def test_candidates_finds_unregistered(root):
    """table.candidates：未登记但含 parquet 的目录"""
    d = root / "tables" / "orphan_tbl"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"a": [1, 2]}).write_parquet(d / "p0.parquet")
    cands = data.table.candidates()
    assert "orphan_tbl" in cands
    assert "idx" not in cands  # 未登记表不算候选


def test_del_force_cascades_fields(root):
    """dataset del --force 级联删除绑定 field（注册 + 产物目录）"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    data.field.create("fx", "ds", formula="def calc(data):\n    return data.with_columns((pl.col('r') * 2).alias('fx'))")
    with pytest.raises(DependencyError):
        data.dataset.del_("ds")
    data.dataset.del_("ds", force=True)
    with pytest.raises(data.field.FieldNotFoundError):
        data.field.meta("fx")
    assert not (root / "fields" / "fx").exists()


def test_rename_repoints_fields(root):
    """dataset rename：field meta 的 dataset 指向新名"""
    _setup_pair(root)
    data.dataset.add("ds", "idx", "mem", background=False)
    data.field.create("fy", "ds", formula="def calc(data):\n    return data.with_columns((pl.col('r') * 2).alias('fy'))")
    data.dataset.rename("ds", "ds2", background=False)
    assert data.field.meta("fy").dataset == "ds2"
    # 依赖边同步
    conn = data.catalog().conn
    rows = conn.execute(
        "SELECT dep_name FROM stkoe_depends WHERE obj_type='field' AND obj_name='fy'").fetchall()
    assert [r["dep_name"] for r in rows] == ["ds2"]


# ---------- ignore_cols：dataset 创建/物化必须排除工具列 ----------

def test_scan_spec_excludes_tool_cols(root):
    """scan_spec 的 keys 缺省与列映射须排除工具列（optime 等 ignore_cols）"""
    write_single(root, "it", pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01"], dtype=pl.Date),
        "sym": ["a"],
        "optime": ["2020-01-01 08:00:00"],
    }))
    write_single(root, "mt", pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01"], dtype=pl.Date),
        "sym": ["a"],
        "r": [0.1],
        "optime": ["2020-01-01 08:00:00"],
    }))
    data.table.scan("it")
    data.table.scan("mt")
    r = data.dataset.scan_spec("it", "mt")
    assert r["ok"]
    assert r["keys"] == ["date", "sym"]  # optime 不参与 join 键
    names = [c.name for c in r["columns"]]
    assert "optime" not in names
    assert {"date", "sym", "r"} <= set(names)


def test_dataset_materialize_excludes_tool_cols(root):
    """dataset 物化产物与 select 结果不得含工具列（optime）"""
    write_single(root, "it", pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01"], dtype=pl.Date),
        "sym": ["a"],
        "optime": ["2020-01-01 08:00:00"],
    }))
    write_single(root, "mt", pl.DataFrame({
        "date": pl.Series("date", ["2020-01-01"], dtype=pl.Date),
        "sym": ["a"],
        "r": [0.1],
        "optime": ["2020-01-01 08:00:00"],
    }))
    data.table.scan("it")
    data.table.scan("mt")
    data.dataset.add("ds_tool", "it", "mt", background=False)
    dm = _wait_materialized("ds_tool")
    assert not any(c.is_tool for c in dm.columns)
    assert "optime" not in {c.name for c in dm.columns}
    # 物化产物 parquet 不含 optime
    out = data.dataset.get("ds_tool")
    assert "optime" not in out.columns
    # 实时 join（未物化路径）同样排除
    data.dataset.add("ds_tool2", "it", "mt", background=False, materialize=False)
    out2 = data.dataset.get("ds_tool2")
    assert "optime" not in out2.columns
    assert "r" in out2.columns
