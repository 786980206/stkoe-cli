"""dataset 模块测试：scan/create/物化/sniff 增量/select/stats/rename/drop"""
import datetime
import time

import polars as pl
import pytest

import stkoe.data as data

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
    data.sniff("idx")
    data.sniff("mem")


def _wait_materialized(name: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = data.dataset.status(name)
        if not s.materializing:
            return s
        time.sleep(0.05)
    raise TimeoutError(f"materialize timeout: {name}")


def test_scan_derives_keys(root):
    _setup_pair(root)
    r = data.dataset.scan("idx", "mem")
    assert r["ok"]
    assert r["keys"] == ["date", "sym"]
    assert r["columns"][0].as_index
    names = [c.name for c in r["columns"]]
    assert {"date", "sym", "extra"} <= set(names)
    for c in r["columns"]:
        assert c.source_table and c.source_field


def test_scan_keys_from_index(root):
    """join 键取 index 表全部列，而非公共列"""
    # 成员表含 date,sym,extra；index 额外列 x 也作为键 → 成员缺 x 必须报错
    write_single(root, "idx2", pl.DataFrame({"date": ["2020-01-01"], "sym": ["s0"], "x": [1]})
                 .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")))
    write_single(root, "mem", _mem_df(n=1))
    data.sniff("idx2")
    data.sniff("mem")
    r = data.dataset.scan("idx2", "mem")
    assert not r["ok"] and "x" in r["message"]


def test_scan_explicit_key_missing(root):
    """显式键必须是 index 列，且每个成员表都要有"""
    _setup_pair(root)
    d = root / "tables" / "mem2"
    d.mkdir(parents=True)
    pl.DataFrame({"date": ["2020-01-01"], "other": [1]}) \
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")) \
        .write_parquet(d / "mem2.parquet")
    data.sniff("mem2")
    r = data.dataset.scan("idx", "mem2", keys=["date", "sym"])
    assert not r["ok"] and "sym" in r["message"]
    r2 = data.dataset.scan("idx", "mem", keys=["foo"])
    assert not r2["ok"] and "not in index" in r2["message"]
    with pytest.raises(ValueError):
        data.dataset.create("ds", "idx", "mem2", keys=["date", "sym"])


def test_scan_no_common(root):
    """成员表缺 index 键 → 明确报错（不让 join 键静默退化）"""
    _setup_pair(root)
    d = root / "tables" / "mem3"
    d.mkdir(parents=True)
    pl.DataFrame({"foo": [1, 2]}).write_parquet(d / "mem3.parquet")
    data.sniff("mem3")
    r = data.dataset.scan("idx", "mem3")
    assert not r["ok"] and "missing join keys" in r["message"]


def test_materialized_under_datasets_dir(root):
    """物化产物直接放 datasets/<name>/（无 .materialized 嵌套层级），统计数据在 stats/ 隔离"""
    _setup_pair(root)
    data.dataset.create("ds", "idx", "mem", background=False)
    assert (root / "datasets" / "ds" / "data.parquet").exists()
    assert not (root / "datasets" / "ds" / ".materialized").exists()
    assert data.dataset.select("ds").collect().height == 2


def test_create_materialize_flat(root):
    _setup_pair(root)
    h = data.dataset.create("ds", "idx", "mem", background=False)
    assert h.status == "succeeded"
    dm = data.dataset.describe("ds")
    assert dm.materialized and dm.version == 1
    assert dm.partition_gran == "" and dm.partition_by == ()
    s = data.dataset.status("ds")
    assert s.registered and s.consistent and not s.pending_partitions
    out = data.dataset.select("ds").collect()
    assert out.height == 2
    assert {"date", "sym", "r", "extra"} <= set(out.columns)


def test_create_background_and_stats(root):
    _setup_pair(root)
    h = data.dataset.create("ds", "idx", "mem", background=True)
    assert h.status in ("submitted", "running")
    s = _wait_materialized("ds")
    assert s.materialized and s.consistent
    st = data.stat.select("ds")
    assert {"field", "count"} <= set(st.columns)
    assert len(st) == 4  # date/sym/r/extra
    row = st.filter(pl.col("field") == "r").to_dicts()[0]
    assert row["count"] == 2 and row["mean"] == pytest.approx(0.15)


def test_create_no_materialize_realtime(root):
    _setup_pair(root)
    h = data.dataset.create("ds", "idx", "mem", materialize=False, background=False)
    assert h.status == "succeeded"
    assert not data.dataset.describe("ds").materialized
    out = data.dataset.select("ds").collect()
    assert out.height == 2 and {"extra", "r"} <= set(out.columns)


def test_sniff_idempotent(root):
    _setup_pair(root)
    data.dataset.create("ds", "idx", "mem", background=False)
    v1 = data.dataset.describe("ds").version
    r = data.dataset.sniff("ds")
    assert not r.changed and r.version_after == v1
    assert r.version_before == v1


def test_incremental_identity_partition(root):
    """镜像 index HIVE 分区：改一个分区只重建该分区，版本 +1"""
    d = root / "tables" / "idx"
    d.mkdir(parents=True)
    df = pl.DataFrame({
        "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 2)],
        "sym": ["s0", "s0", "s1"],
    })
    df.write_parquet(d / "data", partition_by=["date"])
    data.sniff("idx")
    memdf = pl.DataFrame({
        "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 2)],
        "sym": ["s0", "s0", "s1"],
        "extra": [1, 2, 3],
    })
    write_single(root, "mem", memdf)
    data.sniff("mem")

    data.dataset.create("ds", "idx", "mem", background=False)
    dm = data.dataset.describe("ds")
    assert dm.partition_gran == "identity"
    assert data.dataset.partitions("ds") == ["part=2020-01-01", "part=2020-01-02"]
    v1 = dm.version

    part = data.dataset.select("ds", partition="2020-01-02").collect()
    assert set(str(x) for x in part["date"].to_list()) == {"2020-01-02"}

    # 追加一个同分区文件（内连接只保留有 mem 匹配的行，2020-01-02 保持 2 行）
    extra = pl.DataFrame({"date": [datetime.date(2020, 1, 2)], "sym": ["s2"]})
    extra.write_parquet(d / "data" / "date=2020-01-02" / "data-extra.parquet")
    r = data.dataset.sniff("ds")
    assert r.changed
    assert r.rebuilt_partitions == ("2020-01-02",)
    assert r.version_before == v1 and r.version_after == v1 + 1
    s = data.dataset.status("ds")
    assert s.consistent and not s.pending_partitions
    assert data.dataset.select("ds", partition="2020-01-02").collect().height == 2


def test_incremental_flat(root):
    """flat 全量：改源文件后 sniff 重物化并 bump"""
    _setup_pair(root)
    data.dataset.create("ds", "idx", "mem", background=False)
    v1 = data.dataset.describe("ds").version
    write_single(root, "idx", _index_df(n=3))  # 追加一行
    write_single(root, "mem", _mem_df(n=3))    # 同步补齐 join 键
    r = data.dataset.sniff("ds")
    assert r.changed
    assert r.rebuilt_partitions == ("",)
    assert r.version_after == v1 + 1
    assert data.dataset.select("ds").collect().height == 3


def test_derived_year_partition(root):
    """数据量≥1M 且含时间键 → 自动 year 分区"""
    d = root / "tables" / "big"
    d.mkdir(parents=True)
    n = 1_100_000
    dates = pl.date_range(datetime.date(2020, 1, 1), datetime.date(2022, 12, 31), "1d", eager=True) \
        .sample(n, with_replacement=True)
    # index 表：date, sym
    idx = pl.DataFrame({"date": dates, "sym": pl.Series([f"s{i % 200}" for i in range(n)])})
    idx.write_parquet(d / "big.parquet")
    data.sniff("big")
    # 成员表：date, sym + 数据列
    memb = idx.with_columns(pl.Series("cnt", range(n), dtype=pl.Int64))
    write_single(root, "memb", memb)
    data.sniff("memb")

    data.dataset.create("ds", "big", "memb", background=False)
    dm = data.dataset.describe("ds")
    assert dm.partition_gran == "year" and dm.partition_by == ("date",)
    parts = data.dataset.partitions("ds")
    assert parts == ["part=2020", "part=2021", "part=2022"]
    y = data.dataset.select("ds", partition="2020").collect()
    assert y.height > 0
    assert set(str(x)[:4] for x in y["date"].to_list()) == {"2020"}


def test_rename_drop(root):
    _setup_pair(root)
    data.dataset.create("ds", "idx", "mem", background=False)
    h = data.dataset.rename("ds", "ds2")
    assert h.status == "succeeded"
    assert data.dataset.describe("ds2").name == "ds2"
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.describe("ds")
    data.dataset.drop("ds2", with_data=True)
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.dataset.describe("ds2")


def test_create_force_redefines(root):
    """已存在时非 force 报错提示；force 覆盖重建并清空旧物化产物"""
    _setup_pair(root)
    data.dataset.create("ds", "idx", "mem", background=False)
    v1 = data.dataset.describe("ds").version
    assert data.dataset.select("ds").collect().columns == ["date", "sym", "r", "extra"]

    # 非 force 再建：明确报错，提示用 force
    with pytest.raises(data.dataset.DatasetExistsError):
        data.dataset.create("ds", "idx", "mem", background=False)
    dm = data.dataset.describe("ds")
    assert dm.version == v1 and dm.index_table == "idx" and dm.tables == ("mem",)

    # force：覆盖为新定义（仅 date,sym）
    m2 = pl.DataFrame({
        "date": ["2020-01-01", "2020-01-01"],
        "sym": ["s0", "s1"],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    write_single(root, "mem2", m2)
    data.sniff("mem2")
    h = data.dataset.create("ds", "idx", "mem2", background=False, force=True)
    assert h.status == "succeeded"
    dm = data.dataset.describe("ds")
    assert dm.index_table == "idx" and dm.tables == ("mem2",)
    out = data.dataset.select("ds").collect()
    assert out.columns == ["date", "sym"] and out.height == 2
