"""stat 模块测试：add 预计算、get 缓存语义/refresh、--all、scan 幂等、drop/rename 级联"""
import polars as pl
import pytest

import stkoe.data as data

from conftest import write_single


def _index_df(n=2, start="2020-01-01"):
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


def _make_ds(root, name="ds"):
    _setup_pair(root)
    data.dataset.add(name, "idx", "mem", background=False)
    return name


def _stat_deps(conn, name):
    rows = conn.execute(
        "SELECT dep_type, dep_name, detail FROM stkoe_depends WHERE obj_type='stat' AND obj_name=?",
        (name,)).fetchall()
    return [{"dep_type": r["dep_type"], "dep_name": r["dep_name"],
             "detail": r["detail"]} for r in rows]


def test_add_materializes_to_stats_dir(root):
    """stat add：产物落 stats/<name>/group=all/stats.parquet（与 datasets/ 隔离），
    catalog 注册 type='stat'，依赖登记 stat → dataset"""
    ds = _make_ds(root)
    sm = data.stat.add(ds, background=False)
    assert sm.name == ds
    stat_dir = root / "stats" / ds
    assert (stat_dir / "group=all" / "stats.parquet").exists()
    sm = data.stat.meta(ds)
    assert sm.name == ds and sm.target_type == "dataset" and sm.target_name == ds
    assert sm.groups == ("all",)
    deps = _stat_deps(data.catalog().conn, ds)
    assert len(deps) == 1 and deps[0]["dep_type"] == "dataset" and deps[0]["dep_name"] == ds


def test_add_all_groups(root):
    """--all：预计算 'all' + 逐列分组，group=<col>/stats.parquet 各落一份"""
    ds = _make_ds(root)
    sm = data.stat.add(ds, all_=True, background=False)
    assert sm.groups
    expect = {"all", "date", "sym", "r", "extra"}
    assert set(sm.groups) == expect
    for g in sm.groups:
        assert (root / "stats" / ds / f"group={g}" / "stats.parquet").exists()
    # 分组统计行数：4 字段 × 2 sym
    g = data.stat.get(ds, group_col="sym")
    assert "sym" in g.columns
    assert g.height == len(g["field"].unique()) * 2


def test_get_default_reads_cache(root):
    """默认读缓存：篡改缓存文件后 get 直接返回篡改值（证明未重算）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    wrong = pl.DataFrame({"group": ["x"], "field": ["fake"], "data_type": ["x"], "count": [0]})
    wrong.write_parquet(root / "stats" / ds / "group=all" / "stats.parquet")
    st = data.stat.get(ds)
    assert st["field"].to_list() == ["fake"]


def test_get_refresh_recomputes(root):
    """refresh=True 强制重算：即使缓存看似有效也重新计算并覆盖"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    wrong = pl.DataFrame({"group": ["x"], "field": ["fake"], "data_type": ["x"], "count": [0]})
    wrong.write_parquet(root / "stats" / ds / "group=all" / "stats.parquet")
    st = data.stat.get(ds, refresh=True)
    assert "fake" not in st["field"].to_list()
    assert {"date", "sym", "r", "extra"} <= set(st["field"].to_list())
    st2 = data.stat.get(ds)
    assert st.equals(st2)


def test_get_lazy_computes_and_registers(root):
    """未预计算：首次 get 惰性计算并落盘 + 注册 stat 对象"""
    ds = _make_ds(root)
    assert not (root / "stats" / ds).exists()
    st = data.stat.get(ds)
    assert st.height == 4
    assert (root / "stats" / ds / "group=all" / "stats.parquet").exists()
    assert data.stat.meta(ds).groups == ("all",)


def test_get_all_dict(root):
    """--all：返回 {group: df} 字典，含 'all' + 逐列分组"""
    ds = _make_ds(root)
    out = data.stat.get(ds, all_=True)
    assert set(out) == {"all", "date", "sym", "r", "extra"}
    assert out["all"].height == 4
    assert out["sym"].height == 4 * 2


def test_get_group_col(root):
    """group_col 指定单列分组"""
    ds = _make_ds(root)
    st = data.stat.get(ds, group_col="date")
    assert "date" in st.columns
    assert st.height == 4  # 单日全字段


def test_get_unknown_group_col(root):
    """分组列不在目标列中 → 明确报错"""
    ds = _make_ds(root)
    with pytest.raises(ValueError, match="not in dataset columns"):
        data.stat.get(ds, group_col="nope")


def test_stat_requires_target(root):
    """目标不存在 → stat 报错"""
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.get("ghost")


def test_scan_invalidates_after_dataset_change(root):
    """源表变更：dataset scan 重物化后 stat data_key 变化，get 自动重算"""
    _setup_pair(root, n=2)
    data.dataset.add("ds", "idx", "mem", background=False)
    data.stat.add("ds", background=False)

    def cnt():
        return data.stat.get("ds").filter(pl.col("field") == "r")["count"].to_list()[0]

    assert cnt() == 2
    _setup_pair(root, n=3)  # 源表追加数据；table.scan 按依赖图级联重物化 dataset → stat
    assert cnt() == 3
    sm = data.stat.meta("ds")
    assert sm.groups == ("all",) and sm.stale_groups == ()  # 级联已同步，统计保持最新


def test_scan_recomputes_stale_groups(root):
    """stat scan：仅重算 data_key 失配的分组（幂等）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    r = data.stat.scan(ds)
    assert r["recomputed"] == [] and r["fresh"] == ["all"]
    # 篡改 data_key 使其过期
    conn = data.catalog().conn
    with data.catalog().txn() as c:
        row = c.execute("SELECT meta FROM stkoe_objects WHERE type='stat' AND name=?",
                        (ds,)).fetchone()
        meta = __import__("json").loads(row["meta"])
        meta["groups"]["all"]["data_key"] = "stale-key"
        c.execute("UPDATE stkoe_objects SET meta=? WHERE type='stat' AND name=?",
                  (__import__("json").dumps(meta), ds))
    r = data.stat.scan(ds)
    assert r["recomputed"] == ["all"]
    assert r["fresh"] == []
    assert data.stat.meta(ds).stale_groups == ()


def test_del_removes_registration_and_data(root):
    """stat del：删除注册 + 依赖边 + stats/<name>/ 目录"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    data.stat.del_(ds)
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.meta(ds)
    assert not (root / "stats" / ds).exists()
    assert _stat_deps(data.catalog().conn, ds) == []
    # dataset 本身不受影响
    assert data.dataset.meta(ds).materialized


def test_dataset_del_cascades_stat(root):
    """dataset del --force → 关联 stat 一并删除（依赖图一致）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    data.dataset.del_(ds, force=True)
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.meta(ds)
    assert not (root / "stats" / ds).exists()


def test_stat_rename(root):
    """stat rename：目录 + catalog + 依赖边同步（依赖目标仍是 dataset）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    sm = data.stat.rename(ds, "ds2")
    assert sm.name == "ds2"
    assert (root / "stats" / "ds2" / "group=all" / "stats.parquet").exists()
    assert not (root / "stats" / ds).exists()
    deps = _stat_deps(data.catalog().conn, "ds2")
    assert deps and deps[0]["dep_type"] == "dataset" and deps[0]["dep_name"] == ds


def test_dataset_rename_cascades_stat(root):
    """dataset rename → 关联 stat 一并改名（依赖图一致）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    data.dataset.rename(ds, "ds2")
    assert data.stat.meta("ds2").name == "ds2"
    assert (root / "stats" / "ds2").exists()
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.meta(ds)


def test_table_target_stat(root):
    """stat 可直接把 table 当目标（不依赖 dataset）"""
    write_single(root, "t1", _mem_df(n=2))
    data.table.scan("t1")
    data.stat.add("t1", background=False)
    sm = data.stat.meta("t1")
    assert sm.target_type == "table" and sm.target_name == "t1"
    st = data.stat.get("t1")
    assert st["field"].to_list() == ["date", "sym", "r", "extra"]


def test_version_bumps_on_content_change(root):
    """data_key 变化才 bump version（与 dataset 幂等语义一致）"""
    ds = _make_ds(root)
    data.stat.add(ds, background=False)
    v1 = data.stat.meta(ds).version
    data.stat.add(ds, background=False)  # 无变更：幂等不 bump
    assert data.stat.meta(ds).version == v1
    _setup_pair(root, n=3)
    data.dataset.scan(ds)  # 源变更 → 物化 bump → stat data_key 变化
    data.stat.add(ds, background=False)
    assert data.stat.meta(ds).version > v1