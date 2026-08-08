"""stat 模块测试：create/select 缓存/sniff/--all/依赖登记/drop/rename 级联"""
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
    data.sniff("idx")
    data.sniff("mem")


def _make_ds(root, name="ds"):
    _setup_pair(root)
    h = data.dataset.create(name, "idx", "mem", background=False)
    assert h.status == "succeeded"
    return name


def _stat_deps(conn, name):
    rows = conn.execute(
        "SELECT dep_type, dep_name, detail FROM stkoe_depends WHERE obj_type='stat' AND obj_name=?",
        (name,)).fetchall()
    return [{"dep_type": r["dep_type"], "dep_name": r["dep_name"],
             "detail": r["detail"]} for r in rows]


def test_create_materializes_to_stats_dir(root):
    """stat create：产物落 stats/<name>/group=all/stats.parquet（与 datasets/ 隔离），
    catalog 注册 type='stat'，依赖登记 stat → dataset"""
    ds = _make_ds(root)
    h = data.stat.create(ds, background=False)
    assert h.status == "succeeded"
    stat_dir = root / "stats" / ds
    assert (stat_dir / "group=all" / "stats.parquet").exists()
    # 统计不再写 datasets/<name>/ 下
    assert not (root / "datasets" / ds / ".stat").exists()
    sm = data.stat.describe(ds)
    assert sm.name == ds and sm.dataset == ds and sm.groups == ("all",)
    # 依赖边 stat → dataset
    deps = _stat_deps(data.catalog().conn, ds)
    assert len(deps) == 1 and deps[0]["dep_type"] == "dataset" and deps[0]["dep_name"] == ds


def test_create_all_groups(root):
    """--all：预计算 'all' + 逐索引列分组，group=<col>/stats.parquet 各落一份"""
    ds = _make_ds(root)
    h = data.stat.create(ds, all_=True, background=False)
    assert h.status == "succeeded"
    sm = data.stat.describe(ds)
    assert set(sm.groups) == {"all", "date", "sym"}
    for g in sm.groups:
        assert (root / "stats" / ds / f"group={g}" / "stats.parquet").exists()
    # 分组统计行数：4 字段 × 2 sym
    g = data.stat.select(ds, group_col="sym")
    assert "sym" in g.columns
    assert g.height == len(g["field"].unique()) * 2


def test_select_default_reads_cache(root):
    """默认读缓存：篡改缓存文件后 select 直接返回篡改值（证明未重算）"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    wrong = pl.DataFrame({"group": ["x"], "field": ["fake"], "data_type": ["x"], "count": [0]})
    wrong.write_parquet(root / "stats" / ds / "group=all" / "stats.parquet")
    st = data.stat.select(ds)
    assert st["field"].to_list() == ["fake"]


def test_select_refresh_recomputes(root):
    """refresh=True 强制重算：即使缓存看似有效也重新计算并覆盖"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    wrong = pl.DataFrame({"group": ["x"], "field": ["fake"], "data_type": ["x"], "count": [0]})
    wrong.write_parquet(root / "stats" / ds / "group=all" / "stats.parquet")
    st = data.stat.select(ds, refresh=True)
    assert "fake" not in st["field"].to_list()
    assert {"date", "sym", "r", "extra"} <= set(st["field"].to_list())
    st2 = data.stat.select(ds)
    assert st.equals(st2)


def test_select_lazy_computes_and_registers(root):
    """未预计算：首次 select 惰性计算并落盘 + 注册 stat 对象"""
    ds = _make_ds(root)
    assert not (root / "stats" / ds).exists()
    st = data.stat.select(ds)
    assert st.height == 4
    assert (root / "stats" / ds / "group=all" / "stats.parquet").exists()
    assert data.stat.describe(ds).groups == ("all",)


def test_select_all_dict(root):
    """--all：返回 {group: df} 字典，含 'all' + 逐索引列"""
    ds = _make_ds(root)
    out = data.stat.select(ds, all_=True)
    assert set(out) == {"all", "date", "sym"}
    assert out["all"].height == 4
    assert out["sym"].height == 4 * 2


def test_select_group_col(root):
    """group_col 指定单列分组"""
    ds = _make_ds(root)
    st = data.stat.select(ds, group_col="date")
    assert "date" in st.columns
    assert st.height == 4  # 单日全字段


def test_select_unknown_group_col(root):
    """分组列不在 dataset 列中 → 明确报错"""
    ds = _make_ds(root)
    with pytest.raises(ValueError, match="not in dataset columns"):
        data.stat.select(ds, group_col="nope")


def test_stat_requires_dataset(root):
    """dataset 不存在 → stat 报错"""
    with pytest.raises(data.dataset.DatasetNotFoundError):
        data.stat.select("ghost")


def test_sniff_invalidates_after_dataset_change(root):
    """源表变更：dataset sniff 重物化后 stat 缓存自动失效，stat sniff 重算"""
    _setup_pair(root, n=2)
    data.dataset.create("ds", "idx", "mem", background=False)
    data.stat.create("ds", background=False)

    def cnt():
        return data.stat.select("ds").filter(pl.col("field") == "r")["count"].to_list()[0]

    assert cnt() == 2
    _setup_pair(root, n=3)  # 源表追加数据
    r = data.dataset.sniff("ds")
    assert r.changed
    # stat 缓存 key 对齐 dependency_hash → 已失效，自动重算
    assert cnt() == 3
    s = data.stat.status("ds")
    assert s.consistent and not s.stale_groups


def test_sniff_recomputes_stale_groups(root):
    """stat sniff：仅重算 data_key 失配的分组"""
    ds = _make_ds(root, name="ds")
    data.stat.create(ds, background=False)
    # 篡改 'all' 分组的 data_key 使其过期
    conn = data.catalog().conn
    with data.catalog().txn() as c:
        row = c.execute("SELECT meta FROM stkoe_objects WHERE type='stat' AND name=?", (ds,)).fetchone()
        meta = __import__("json").loads(row["meta"])
        meta["groups"]["all"]["data_key"] = "stale-key"
        c.execute("UPDATE stkoe_objects SET meta=? WHERE type='stat' AND name=?", (__import__("json").dumps(meta), ds))
    r = data.stat.sniff(ds)
    assert set(r["recomputed"]) == {"all"}
    assert r["fresh"] == []
    assert data.stat.status(ds).consistent


def test_status_unregistered(root):
    """未注册 stat → status 标记 registered=False"""
    s = data.stat.status("ghost")
    assert not s.registered and s.consistent


def test_drop_removes_registration_and_data(root):
    """stat drop：删除注册 + 依赖边 + stats/<name>/ 目录"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    h = data.stat.drop(ds)
    assert h.status == "succeeded"
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.describe(ds)
    assert not (root / "stats" / ds).exists()
    assert _stat_deps(data.catalog().conn, ds) == []
    # dataset 本身不受影响
    assert data.dataset.describe(ds).materialized


def test_dataset_drop_cascades_stat(root):
    """dataset drop → 关联 stat 一并删除（依赖图一致）"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    data.dataset.drop(ds, with_data=True)
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.describe(ds)
    assert not (root / "stats" / ds).exists()


def test_stat_rename(root):
    """stat rename：目录 + catalog + 依赖边同步（依赖目标仍是 dataset）"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    h = data.stat.rename(ds, "ds2")
    assert h.status == "succeeded"
    assert data.stat.describe("ds2").name == "ds2"
    assert (root / "stats" / "ds2" / "group=all" / "stats.parquet").exists()
    assert not (root / "stats" / ds).exists()
    deps = _stat_deps(data.catalog().conn, "ds2")
    assert deps and deps[0]["dep_type"] == "dataset" and deps[0]["dep_name"] == ds


def test_dataset_rename_cascades_stat(root):
    """dataset rename → 关联 stat 一并改名（依赖图一致）"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    h = data.dataset.rename(ds, "ds2")
    assert h.status == "succeeded"
    assert data.stat.describe("ds2").name == "ds2"
    assert (root / "stats" / "ds2").exists()
    with pytest.raises(data.stat.StatNotFoundError):
        data.stat.describe(ds)


def test_stat_version_bumps_on_content_change(root):
    """data_key 变化才 bump version（与 dataset 幂等语义一致）"""
    ds = _make_ds(root)
    data.stat.create(ds, background=False)
    v1 = data.stat.describe(ds).version
    data.stat.create(ds, background=False)  # 无变更：幂等不 bump
    assert data.stat.describe(ds).version == v1
    _setup_pair(root, n=3)
    data.dataset.sniff(ds)  # 源变更 → 物化 bump → stat data_key 变化
    data.stat.create(ds, background=False)
    assert data.stat.describe(ds).version > v1
