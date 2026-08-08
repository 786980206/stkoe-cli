"""CLI：stkoe 命令入口（typer）"""
import sys

import orjson
import typer

from . import table
from . import mock as mock_mod
from . import task as task_mod
from . import dataset as dataset_mod
from . import stat as stat_mod
from .settings import StkoeConfig, config_path, load_config, resolve_data_path, save_config

app = typer.Typer(name="stkoe", help="DataCenter 数据管理框架", no_args_is_help=True)
table_app = typer.Typer(help="table 子命令：只读观察 + sniff 同步", no_args_is_help=True)
config_app = typer.Typer(help="配置子命令：查看/修改 stkoe.json", no_args_is_help=True)
mock_app = typer.Typer(help="mock 子命令：生成演示数据", no_args_is_help=True)
task_app = typer.Typer(help="任务子命令：进度/日志/暂停/取消", no_args_is_help=True)
dataset_app = typer.Typer(help="dataset 子命令：索引表+多表 join 逻辑数据集", no_args_is_help=True)
stat_app = typer.Typer(help="stat 子命令：dataset 统计物化（stats/ 目录，经 stkoe_depends 关联）", no_args_is_help=True)
app.add_typer(table_app, name="table")
app.add_typer(config_app, name="config")
app.add_typer(mock_app, name="mock")
app.add_typer(task_app, name="task")
app.add_typer(dataset_app, name="dataset")
app.add_typer(stat_app, name="stat")


def _print_json(obj):
    sys.stdout.buffer.write(orjson.dumps(obj) + b"\n")


def _meta_json(m: table.TableMeta) -> dict:
    return {
        "name": m.name,
        "version": m.version,
        "layout": m.layout.value,
        "partition_by": list(m.partition_by),
        "partition_count": m.partition_count,
        "columns": [c.to_dict() for c in m.columns],
        "row_count": m.row_count,
        "file_count": m.file_count,
        "bytes": m.bytes,
        "as_index": m.as_index,
        "has_data": m.has_data,
        "display_name": m.display_name,
        "description": m.description,
        "tags": list(m.tags),
        "created_at": m.created_at,
        "updated_at": m.updated_at,
    }


@table_app.command()
def sniff(
    name: str = typer.Argument(None),
    resync: bool = typer.Option(False, help="忽略快检强制全量"),
    all_: bool = typer.Option(False, "--all", help="扫描根目录发现并同步所有表"),
):
    """用户更新数据后同步元数据+统计"""
    reports = table.sniff_all() if all_ else [table.sniff(name)]
    for r in reports:
        print(f"[{r.name}] v{r.version_before} -> v{r.version_after}"
              f" changed={r.changed} layout={r.layout.value} partitions={r.partition_count}"
              + (" (implicit-registered)" if r.implicit_registered else ""))
        for d in r.diffs:
            print(f"  {d.kind}: {d.rel_path}")


@table_app.command("list")
def list_cmd(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册表"""
    metas = table.list()
    if json:
        _print_json([_meta_json(m) for m in metas])
    else:
        for m in metas:
            print(f"{m.name:<24} v{m.version} {m.layout.value:<7}"
                  f" files={m.file_count:<5} rows={m.row_count or 0:<8} partitions={m.partition_count}")


@table_app.command()
def describe(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """表元数据"""
    m = table.describe(name)
    if json:
        _print_json(_meta_json(m))
    else:
        print(f"name:       {m.name}")
        print(f"version:    {m.version}")
        print(f"layout:     {m.layout.value}")
        print(f"partition:  {', '.join(m.partition_by) or '-'}  count={m.partition_count}")
        print(f"size:       files={m.file_count} rows={m.row_count} bytes={m.bytes}")
        print(f"columns:    {', '.join(c.name + (':' + (c.data_type or '?') if c.data_type else '') for c in m.columns)}")


@table_app.command()
def status(name: str):
    """只读对账：catalog 与磁盘差异"""
    s = table.status(name)
    print(f"name:       {s.name}")
    print(f"registered: {s.registered}")
    print(f"consistent: {s.consistent}")
    for d in s.diffs:
        print(f"  {d.kind}: {d.rel_path}")
    if s.consistent and not s.diffs:
        print("  ok (catalog matches disk)")
    elif not s.diffs:
        print("  signature mismatch (content changed in place)")


@table_app.command()
def schema(name: str):
    """仅 schema（不读数据）"""
    for col, dtype in table.schema(name).items():
        print(f"{col}: {dtype}")


@table_app.command()
def select(
    name: str,
    columns: str = typer.Option(None, help="逗号分隔列"),
    where: str = typer.Option(None, help="谓词，如 date>=2020-01-01"),
    partition: str = typer.Option(None, help="分区路径，如 year=2020"),
    limit: int = typer.Option(None, help="行数限制"),
    out: str = typer.Option(None, help="输出 parquet 路径"),
    exclude_tool: bool = typer.Option(False, "--exclude-tool", "--exclude_tool", help="剔除工具字段（ignore_cols）"),
):
    """读取表（lazy，可裁剪/输出）"""
    lf = table.select(
        name,
        columns=columns.split(",") if columns else None,
        where=where,
        partition=partition,
        exclude_tool=exclude_tool,
    )
    if limit is not None:
        lf = lf.limit(limit)
    if out:
        lf.sink_parquet(out)
        print(f"written: {out} rows={lf.collect().height}")
    else:
        print(lf.collect())


@table_app.command()
def create(
    name: str = typer.Argument(None, help="表名；配合 --all 可省略"),
    all: bool = typer.Option(False, "--all", help="注册 tables/ 下所有未注册且有数据的表"),
):
    """注册表（仅 catalog；--all 批量发现注册）"""
    if all:
        if name is not None:
            raise typer.BadParameter("--all 与 name 互斥")
        reports = table.create_all()
        if not reports:
            print("no unregistered tables found")
        for r in reports:
            print(f"registered {r.name} v{r.version_after} layout={r.layout.value}")
        return
    if name is None:
        raise typer.BadParameter("需要提供 name 或使用 --all")
    handle = table.create(name)
    print(f"task={handle.task_id} status={handle.status}")


@table_app.command()
def drop(name: str, with_data: bool = typer.Option(False, "--with-data", "--with_data", help="框架永不删数据文件，仅删登记")):
    """删除注册与元数据"""
    handle = table.drop(name, with_data=with_data)
    print(f"task={handle.task_id} status={handle.status}")


@table_app.command()
def rename(old: str, new: str):
    """改名（目录 tables/old → tables/new，并同步 catalog）"""
    handle = table.rename(old, new)
    print(f"task={handle.task_id} status={handle.status}")


@table_app.command()
def update(
    name: str,
    display_name: str = typer.Option(None, "--display-name"),
    desc: str = typer.Option(None, "--desc"),
    bump: bool = typer.Option(False, help="强制 bump version"),
):
    """更新描述性元数据"""
    m = table.update(name, display_name=display_name, description=desc, bump=bump)
    print(f"updated: {m.name} v{m.version}")


@config_app.command()
def show(json: bool = typer.Option(False, help="JSON 输出")):
    """查看当前配置（配置文件路径 + 生效值）"""
    c = load_config()
    p = config_path()
    out = {
        "config_file": str(p),
        "data_path": c.data_path,
        "ignore_cols": list(c.ignore_cols),
        "resolved_data_path": str(resolve_data_path()),
    }
    if json:
        _print_json(out)
    else:
        for k, v in out.items():
            print(f"{k:<20} {v if not isinstance(v, list) else ','.join(v)}")


@config_app.command()
def set(
    data_path: str = typer.Option(None, "--data-path", help="默认数据根目录"),
    ignore_cols: str = typer.Option(None, "--ignore-cols", help="忽略的工具字段，逗号分隔（可多个）"),
):
    """修改配置并写入 stkoe.json"""
    c = load_config()
    new = StkoeConfig(
        data_path=data_path or c.data_path,
        ignore_cols=tuple(ignore_cols.split(",")) if ignore_cols else c.ignore_cols,
    )
    p = save_config(new)
    print(f"written: {p}")


@mock_app.command()
def gen(
    name: str,
    kind: str = typer.Option("klday", help="klday|tdcal|feature|common|index"),
    n_syms: int = typer.Option(100, "--syms", help="股票数量"),
    start: str = typer.Option("2020-01-01", help="开始日期"),
    end: str = typer.Option("2023-12-31", help="结束日期"),
    seed: int = typer.Option(12345, help="随机种子"),
    partition_by: str = typer.Option(None, "--partition-by", "--partition_by", help="hive 分区列（如 year）"),
):
    """生成 mock 演示表并 sniff 注册"""
    gens = {
        "tdcal": mock_mod.tdcal,
        "common": lambda: mock_mod.common(n_syms, start, end, seed),
        "klday": lambda: mock_mod.klday(n_syms, start, end, seed),
        "feature": lambda: mock_mod.feature(name, n_syms, start, end, seed),
        "index": lambda: mock_mod.index(n_syms, start, end),
    }
    if kind not in gens:
        raise typer.BadParameter(f"unknown kind: {kind} (use {'|'.join(gens)})")
    df = gens[kind]()
    report = mock_mod.write(name, df, partition_by=partition_by)
    print(f"[{report.name}] v{report.version_before} -> v{report.version_after}"
          f" layout={report.layout.value} rows={len(df)}")


@task_app.command("list")
def task_list(
    status: str = typer.Option(None, help="按状态过滤：submitted|running|paused|succeeded|failed|cancelled"),
    type: str = typer.Option(None, help="按类型过滤，如 dataset_materialize"),
    limit: int = typer.Option(100, help="条数上限"),
):
    """任务列表"""
    handles = task_mod.task_list(status=status, type=type, limit=limit)
    if not handles:
        print("no tasks")
    for h in handles:
        prog = f"{h.progress * 100:.0f}%" if h.status in ("running", "paused") else ""
        print(f"{h.task_id[:8]:<10} {h.status:<10} {h.type:<24} {h.object_ref:<20} {prog} {h.stage}")


@task_app.command()
def stop(
    task_id: str = typer.Argument(None, help="任务 id（--all 时省略）"),
    all_tasks: bool = typer.Option(False, "--all", help="停止所有运行/暂停任务并清理完成态任务"),
):
    """停止任务（协作式）"""
    if all_tasks:
        stopped = task_mod.task_stop_all()
        cleaned = task_mod.task_clean()
        print(f"stop requested: {stopped} running, cleaned {cleaned} finished task(s)")
        return
    if not task_id:
        raise typer.BadParameter("需要提供 task_id 或使用 --all")
    try:
        h = task_mod.task_stop(task_id)
        print(f"stop requested: {task_id} status={h.status}")
    except KeyError as e:
        raise typer.BadParameter(str(e))


@task_app.command()
def clean():
    """删除全部完成态任务（succeeded/failed/cancelled，日志级联删除）"""
    n = task_mod.task_clean()
    print(f"cleaned {n} finished task(s)")


@task_app.command()
def pause(task_id: str):
    """暂停任务（协作式，下一个分区边界生效）"""
    try:
        h = task_mod.task_pause(task_id)
        print(f"paused: {task_id} progress={h.progress:.0%}")
    except KeyError as e:
        raise typer.BadParameter(str(e))


@task_app.command()
def resume(task_id: str):
    """恢复已暂停任务"""
    try:
        h = task_mod.task_resume(task_id)
        print(f"resumed: {task_id} status={h.status}")
    except KeyError as e:
        raise typer.BadParameter(str(e))


@task_app.command()
def log(
    task_id: str,
    tail: int = typer.Option(None, help="只显示最后 N 条"),
    after_seq: int = typer.Option(0, "--after-seq", "--after_seq", help="增量拉取：只取 seq 大于该值的日志"),
):
    """查看任务日志"""
    entries = task_mod.task_log(task_id, after_seq=after_seq)
    if tail is not None:
        entries = entries[-tail:]
    if not entries:
        print("(no log entries)")
    for e in entries:
        print(f"[{e.seq}] {e.ts} {e.level:<7} {e.message}")


@dataset_app.command()
def create(
    name: str,
    index_table: str = typer.Argument(..., help="索引表（提供 join 键）"),
    tables: list[str] = typer.Argument(None, help="参与 join 的表"),
    keys: str = typer.Option(None, help="join 键，逗号分隔（缺省自动取公共列）"),
    no_materialize: bool = typer.Option(False, "--no-materialize", help="只注册不物化"),
    sync: bool = typer.Option(False, help="同步物化（默认后台）"),
    force: bool = typer.Option(False, "--force", help="已存在时覆盖重建（清空旧物化产物）"),
):
    """注册 dataset（默认后台自动物化）；已存在时不加 --force 报错"""
    try:
        h = dataset_mod.create(
            name, index_table, *tables,
            keys=keys.split(",") if keys else None,
            materialize=not no_materialize,
            background=not sync,
            force=force,
        )
    except dataset_mod.DatasetExistsError as e:
        raise typer.BadParameter(str(e))
    print(f"task={h.task_id} status={h.status}")


@dataset_app.command("list")
def dataset_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册 dataset"""
    metas = dataset_mod.list()
    if json:
        _print_json([dm.to_dict() for dm in metas])
        return
    for dm in metas:
        print(f"{dm.name:<24} v{dm.version} keys={','.join(dm.keys) or '-'} "
              f"tables={len(dm.tables) + 1} mat={dm.materialized} parts={dm.partition_by or '-'}")


@dataset_app.command()
def describe(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """dataset 元数据"""
    dm = dataset_mod.describe(name)
    if json:
        _print_json(dm.to_dict())
        return
    print(f"name:        {dm.name}")
    print(f"version:     {dm.version}")
    print(f"index:       {dm.index_table}")
    print(f"tables:      {', '.join(dm.tables) or '-'}")
    print(f"keys:        {', '.join(dm.keys) or '-'}")
    print(f"partition:   {', '.join(dm.partition_by) or '-'}  materialized={dm.materialized}")
    print(f"columns:     {', '.join(f'{c.name}:{c.source_table}.{c.source_field}' for c in dm.columns)}")


@dataset_app.command()
def status(name: str):
    """只读对账：依赖是否过期 / 物化状态"""
    s = dataset_mod.status(name)
    print(f"name:          {s.name}")
    print(f"registered:    {s.registered}")
    print(f"materialized:  {s.materialized}")
    print(f"materializing: {s.materializing}")
    print(f"consistent:    {s.consistent}")
    if s.pending_partitions:
        print(f"pending:       {', '.join(s.pending_partitions)}")


@dataset_app.command()
def sniff(name: str, resync: bool = typer.Option(False, help="强制全量重物化")):
    """检查依赖并增量重物化"""
    r = dataset_mod.sniff(name, resync=resync)
    print(f"[{r.name}] v{r.version_before} -> v{r.version_after} changed={r.changed} "
          f"incremental={r.incremental} parts={r.partition_by or '-'}")
    for p in r.rebuilt_partitions:
        print(f"  rebuilt: {p}")


@dataset_app.command()
def select(
    name: str,
    columns: str = typer.Option(None, help="逗号分隔列"),
    where: str = typer.Option(None, help="谓词，如 date>=2020-01-01"),
    partition: str = typer.Option(None, help="分区，如 2020"),
    limit: int = typer.Option(None, help="行数限制"),
):
    """读取 dataset（lazy；物化完成走物化，否则实时 join）"""
    lf = dataset_mod.select(name, columns=columns.split(",") if columns else None,
                            where=where, partition=partition)
    if limit is not None:
        lf = lf.limit(limit)
    print(lf.collect())


@dataset_app.command()
def schema(name: str):
    """dataset 视图 schema"""
    for col, dtype in dataset_mod.schema(name).items():
        print(f"{col}: {dtype}")


@dataset_app.command()
def drop(name: str, with_data: bool = typer.Option(False, "--with-data", "--with_data", help="同时删除物化产物")):
    """删除注册（with_data 删物化产物）"""
    h = dataset_mod.drop(name, with_data=with_data)
    print(f"task={h.task_id} status={h.status}")


@dataset_app.command()
def rename(old: str, new: str):
    """改名（目录 + catalog）"""
    h = dataset_mod.rename(old, new)
    print(f"task={h.task_id} status={h.status}")


def _print_stat(df, label: str | None = None):
    if label:
        print(f"--- {label} ---")
    print(df)


@stat_app.command()
def create(
    name: str,
    group_col: list[str] = typer.Option(None, "--group-col", "--group_col", help="按列分组统计（可多次）"),
    all_: bool = typer.Option(False, "--all", help="统计 'all' + 逐索引列全部分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（忽略缓存有效性）"),
    sync: bool = typer.Option(False, help="同步执行（默认后台）"),
):
    """预计算 dataset 统计（缺省仅 'all'；产物写 stats/<name>/group=*/stats.parquet）"""
    h = stat_mod.create(name, group_col=group_col, all_=all_, force=refresh, background=not sync)
    print(f"task={h.task_id} status={h.status}")


@stat_app.command("list")
def stat_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册 stat"""
    metas = stat_mod.list()
    if json:
        _print_json([sm.to_dict() for sm in metas])
        return
    for sm in metas:
        print(f"{sm.name:<24} v{sm.version} dataset={sm.dataset:<24} groups={','.join(sm.groups) or '-'}")


@stat_app.command()
def describe(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """stat 元数据"""
    sm = stat_mod.describe(name)
    if json:
        _print_json(sm.to_dict())
        return
    print(f"name:        {sm.name}")
    print(f"version:     {sm.version}")
    print(f"dataset:     {sm.dataset}")
    print(f"groups:      {', '.join(sm.groups) or '-'}")


@stat_app.command()
def status(name: str):
    """stat 一致性：分组缓存是否与当前数据标识（data_key）一致"""
    s = stat_mod.status(name)
    print(f"name:        {s.name}")
    print(f"registered:  {s.registered}")
    if s.registered:
        print(f"dataset:     {s.dataset}")
        print(f"groups:      {', '.join(s.groups) or '-'}")
        print(f"consistent:  {s.consistent}")
        if s.stale_groups:
            print(f"stale:       {', '.join(s.stale_groups)}（sniff 重算）")


@stat_app.command()
def sniff(name: str):
    """重算 data_key 失配的分组（幂等）"""
    r = stat_mod.sniff(name)
    print(f"[{r['name']}] dataset={r['dataset']} recomputed={','.join(r['recomputed']) or '-'} "
          f"fresh={','.join(r['fresh']) or '-'}")


@stat_app.command()
def select(
    name: str,
    group_col: str = typer.Option(None, "--group-col", "--group_col", help="按列分组统计"),
    all_: bool = typer.Option(False, "--all", help="返回 'all' + 逐索引列全部分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（默认读缓存，缺失/过期自动重算）"),
):
    """读 dataset 统计（默认读缓存；--all 返回全部分组）"""
    res = stat_mod.select(name, group_col=group_col, all_=all_, refresh=refresh)
    if all_:
        for group, df in res.items():
            _print_stat(df, group)
    else:
        print(res)


@stat_app.command()
def drop(name: str):
    """删除统计注册与产物（stats/<name>/ + stkoe_depends 边）"""
    h = stat_mod.drop(name)
    print(f"task={h.task_id} status={h.status}")


@stat_app.command()
def rename(old: str, new: str):
    """改名（stats/ 目录 + catalog，关联 dataset 改名时级联触发）"""
    h = stat_mod.rename(old, new)
    print(f"task={h.task_id} status={h.status}")


if __name__ == "__main__":
    app()
