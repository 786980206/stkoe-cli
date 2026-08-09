"""CLI：stkoe 命令入口（typer）

统一动词：add / get / del / set / meta / list / scan（table · dataset · stat 对齐），
外加 table col（字段元数据）、dataset describe（meta 别名）。
CLI 默认同步执行（直接返回结果）；REPL 默认后台（返回 TaskHandle，见 __main__）。
"""
import sys

import orjson
import typer

from . import table
from . import mock as mock_mod
from . import task as task_mod
from . import dataset as dataset_mod
from . import stat as stat_mod
from . import field as field_mod
from .settings import StkoeConfig, config_path, load_config, resolve_data_path, save_config
from .task import TaskHandle

app = typer.Typer(name="stkoe", help="DataCenter 数据管理框架", no_args_is_help=True)
table_app = typer.Typer(help="table 子命令：原始表资产（add/get/del/set/meta/list/scan/col）", no_args_is_help=True)
config_app = typer.Typer(help="配置子命令：查看/修改 stkoe.json", no_args_is_help=True)
mock_app = typer.Typer(help="mock 子命令：生成演示数据", no_args_is_help=True)
task_app = typer.Typer(help="任务子命令：进度/日志/暂停/取消", no_args_is_help=True)
dataset_app = typer.Typer(help="dataset 子命令：索引表+多表 join 逻辑数据集", no_args_is_help=True)
stat_app = typer.Typer(help="stat 子命令：table/dataset 统计资产（stats/ 目录，经 stkoe_depends 关联）", no_args_is_help=True)
field_app = typer.Typer(help="field 子命令：dataset 派生指标（catalog 注册，formula 存根）", no_args_is_help=True)
app.add_typer(table_app, name="table")
app.add_typer(config_app, name="config")
app.add_typer(mock_app, name="mock")
app.add_typer(task_app, name="task")
app.add_typer(dataset_app, name="dataset")
app.add_typer(stat_app, name="stat")
app.add_typer(field_app, name="field")
server_app = typer.Typer(help="gRPC 子命令：启动数据服务（默认端口 9569，`config set --grpc-port` 可改）", no_args_is_help=True)
app.add_typer(server_app, name="server")


@server_app.command()
def run(port: int = typer.Option(None, help="端口（缺省取配置 grpc_port）")):
    """前台运行 gRPC 服务（阻塞；REPL 已同步后台启动）"""
    from ..grpc.server import serve as grpc_serve
    srv = grpc_serve(port)
    print(f"stkoe gRPC listening on {srv.host}:{srv.port}")
    srv.wait()


def _print_json(obj):
    sys.stdout.buffer.write(orjson.dumps(obj) + b"\n")


def _finish(res, *, action: str = "", name: str = ""):
    """同步结果直接打印；后台 TaskHandle 打印任务登记"""
    if isinstance(res, TaskHandle):
        print(f"task={res.task_id} status={res.status} action={action or name}")
        return
    return res


def _table_json(m: table.TableMeta) -> dict:
    return {
        "name": m.name,
        "version": m.version,
        "layout": m.layout.value,
        "partition_by": list(m.partition_by),
        "partition_count": m.partition_count,
        "columns": [c.to_dict() for c in m.columns],
        "consistent": m.consistent,
        "display_name": m.display_name,
        "description": m.description,
        "tags": list(m.tags),
        "created_at": m.created_at,
        "updated_at": m.updated_at,
    }


# ---------- table ----------

@table_app.command()
def add(
    name: str = typer.Argument(None, help="表名；配合 --all 可省略"),
    all: bool = typer.Option(False, "--all", help="发现并注册 tables/ 下所有未注册且有数据的表"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局：CLI 同步 / REPL 后台）"),
):
    """注册表（发现资产语义：目录不存在报错；已注册报错用 scan 刷新）"""
    r = _finish(table.add(name, all=all, background=background))
    if r is None:
        return
    if all:
        if not r:
            print("no unregistered tables found")
        for x in r:
            print(f"[{x.name}] v{x.version_before} -> v{x.version_after}"
                  f" layout={x.layout.value} partitions={x.partition_count}"
                  + (" (implicit)" if x.implicit_registered else ""))
    else:
        print(f"[{r.name}] v{r.version_before} -> v{r.version_after}"
              f" layout={r.layout.value} partitions={r.partition_count}")


@table_app.command("list")
def table_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册表"""
    metas = table.list()
    if json:
        _print_json([_table_json(m) for m in metas])
    else:
        for m in metas:
            print(f"{m.name:<24} v{m.version} {m.layout.value:<7} files={len(m.files):<5} "
                  f"partitions={m.partition_count} consistent={m.consistent}")


@table_app.command()
def meta(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """表元数据：版本/布局/分区/文件/列"""
    m = table.meta(name)
    if json:
        _print_json(_table_json(m))
    else:
        print(f"name:       {m.name}")
        print(f"version:    {m.version}")
        print(f"layout:     {m.layout.value}")
        print(f"partition:  {', '.join(m.partition_by) or '-'}  count={m.partition_count}")
        print(f"files:      {len(m.files)}  consistent={m.consistent}")
        print(f"columns:    {', '.join(c.name + (':' + c.data_type if c.data_type else '') for c in m.columns)}")


@table_app.command("get")
def get_cmd(
    name: str,
    columns: str = typer.Option(None, help="逗号分隔列"),
    where: str = typer.Option(None, help="谓词，如 date>=2020-01-01"),
    partition: str = typer.Option(None, help="分区路径，如 year=2020"),
    limit: int = typer.Option(None, help="行数限制"),
    out: str = typer.Option(None, help="输出 parquet 路径"),
    exclude_tool: bool = typer.Option(False, "--exclude-tool", "--exclude_tool", help="剔除工具字段（ignore_cols）"),
):
    """读表数据（读前自动保鲜；可裁剪/输出）"""
    lf = table.get_lazy(name, columns=columns.split(",") if columns else None,
                        where=where, partition=partition, exclude_tool=exclude_tool)
    if limit is not None:
        lf = lf.limit(limit)
    if out:
        df = lf.collect()
        df.write_parquet(out)
        print(f"written: {out} rows={df.height}")
    else:
        print(lf.collect())


@table_app.command("del")
def del_cmd(name: str, force: bool = typer.Option(False, "--force", help="级联删除依赖方（dataset/stat 一并清理）")):
    """删除表注册（绝不删用户数据文件）"""
    _finish(table.del_(name, force=force), name=name)


@table_app.command()
def rename(old: str, new: str):
    """改名（目录 tables/old → tables/new，并同步 catalog/下游引用）"""
    m = table.rename(old, new)
    print(f"renamed: {m.name} v{m.version}")


@table_app.command()
def set(
    name: str,
    display_name: str = typer.Option(None, "--display-name"),
    desc: str = typer.Option(None, "--desc"),
    tags: str = typer.Option(None, help="标签，逗号分隔"),
    new_name: str = typer.Option(None, "--new-name", help="改名（等价 table rename）"),
):
    """修改表级元数据（display_name/description/tags）"""
    m = table.set(name, display_name=display_name, description=desc,
                  tags=tags.split(",") if tags else None, new_name=new_name)
    print(f"updated: {m.name} v{m.version} display_name={m.display_name}")


@table_app.command()
def col(
    name: str,
    column: str = typer.Argument(..., help="字段名"),
    display_name: str = typer.Option(None, "--display-name"),
    desc: str = typer.Option(None, "--desc"),
    unit: str = typer.Option(None, "--unit"),
):
    """更新字段（列）元数据"""
    m = table.col(name, column, display_name=display_name, description=desc, unit=unit)
    c = next(c for c in m.columns if c.name == column)
    print(f"column {m.name}.{column}: display_name={c.display_name} description={c.description}")


@table_app.command()
def scan(
    name: str = typer.Argument(None, help="表名；缺省配合 --all 扫描全部"),
    all: bool = typer.Option(False, "--all", help="扫描 tables/ 下全部目录（含未注册）"),
    resync: bool = typer.Option(False, "--resync", help="忽略快检强制全量读 footer"),
    cascade: bool = typer.Option(True, "--cascade/--no-cascade", help="变更后触发下游（默认开启）"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """扫描同步元数据（幂等：无差异不 bump 版本）；变更后自动触发下游"""
    reports = table.scan(name, all=all, resync=resync, cascade=cascade, background=background)
    if isinstance(reports, TaskHandle):
        print(f"task={reports.task_id} status={reports.status}")
        return
    rst = reports if all else [reports]
    for r in rst:
        print(f"[{r.name}] v{r.version_before} -> v{r.version_after}"
              f" changed={r.changed} layout={r.layout.value} partitions={r.partition_count}"
              + (" (implicit-registered)" if r.implicit_registered else ""))
        if r.diffs:
            for d in r.diffs:
                print(f"  {d.kind}: {d.rel_path}")
        if r.triggered:
            print(f"  triggered: {', '.join(r.triggered)}")


# ---------- config ----------

@config_app.command()
def show(json: bool = typer.Option(False, help="JSON 输出")):
    """查看当前配置（配置文件路径 + 生效值）"""
    c = load_config()
    p = config_path()
    out = {
        "config_file": str(p),
        "data_path": c.data_path,
        "ignore_cols": list(c.ignore_cols),
        "grpc_port": c.grpc_port,
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
    grpc_port: int = typer.Option(None, "--grpc-port", help="gRPC 服务端口（缺省 9569）"),
):
    """修改配置并写入 stkoe.json"""
    c = load_config()
    new = StkoeConfig(
        data_path=data_path or c.data_path,
        ignore_cols=tuple(ignore_cols.split(",")) if ignore_cols else c.ignore_cols,
        grpc_port=grpc_port if grpc_port is not None else c.grpc_port,
    )
    p = save_config(new)
    print(f"written: {p}")


# ---------- mock ----------

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
    """生成 mock 演示表并注册"""
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
    res = mock_mod.write(name, df, partition_by=partition_by)
    if isinstance(res, TaskHandle):
        print(f"task={res.task_id} status={res.status} action=mock_write（后台生成中）")
        return
    report = res
    print(f"[{report.name}] v{report.version_before} -> v{report.version_after}"
          f" layout={report.layout.value} rows={len(df)}")


# ---------- task ----------

@task_app.command("list")
def task_list(
    status: str = typer.Option(None, help="按状态过滤：submitted|running|paused|succeeded|failed|cancelled"),
    type: str = typer.Option(None, help="按类型过滤，如 dataset_add"),
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


# ---------- dataset ----------

@dataset_app.command()
def add(
    name: str,
    index_table: str = typer.Argument(..., help="索引表（提供 join 键）"),
    tables: list[str] = typer.Argument(None, help="参与 join 的表"),
    keys: str = typer.Option(None, help="join 键，逗号分隔（缺省=index 全部列）"),
    no_materialize: bool = typer.Option(False, "--no-materialize", help="只注册不物化"),
    force: bool = typer.Option(False, "--force", help="已存在时覆盖重建"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """注册 dataset（join 规格校验 → 注册 → 自动物化；get 前未物化也会自动）"""
    try:
        r = dataset_mod.add(name, index_table, *tables,
                           keys=keys.split(",") if keys else None,
                           materialize=not no_materialize,
                           force=force, background=background)
    except dataset_mod.DatasetExistsError as e:
        raise typer.BadParameter(str(e))
    if isinstance(r, TaskHandle):
        print(f"task={r.task_id} status={r.status}（物化将后台完成）")
    else:
        print(f"registered: {r.name} v{r.version} keys={','.join(r.keys)} "
              f"materialized={r.materialized} partition={r.partition_gran or '-'}")


@dataset_app.command("list")
def dataset_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册 dataset"""
    metas = dataset_mod.list()
    if json:
        _print_json([dm.to_dict() for dm in metas])
        return
    for dm in metas:
        print(f"{dm.name:<24} v{dm.version} keys={','.join(dm.keys) or '-'} "
              f"tables={len(dm.tables) + 1} mat={dm.materialized} "
              f"gran={dm.partition_gran or '-'} curated={dm.curated}")


@dataset_app.command()
def meta(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """dataset 元数据"""
    dm = dataset_mod.meta(name)
    if json:
        _print_json(dm.to_dict())
        return
    print(f"name:        {dm.name}")
    print(f"version:     {dm.version}")
    print(f"index:       {dm.index_table}")
    print(f"tables:      {', '.join(dm.tables) or '-'}")
    print(f"keys:        {', '.join(dm.keys) or '-'}")
    print(f"partition:   {', '.join(dm.partition_by) or '-'} gran={dm.partition_gran or '-'} "
          f"curated={dm.curated}")
    print(f"columns:     {', '.join(f'{c.name}:{c.source_table}.{c.source_field}' for c in dm.columns)}")


# describe 是 dataset 的兼容别名（老 REPL 习惯）
describe = meta


@dataset_app.command("get")
def dataset_get(
    name: str,
    columns: str = typer.Option(None, help="逗号分隔列"),
    where: str = typer.Option(None, help="谓词，如 date>=2020-01-01"),
    partition: str = typer.Option(None, help="分区，如 2020"),
    limit: int = typer.Option(None, help="行数限制"),
):
    """读取 dataset（物化缺失/过期时先自动增量物化再读）"""
    lf = dataset_mod.get_lazy(name, columns=columns.split(",") if columns else None,
                              where=where, partition=partition)
    if limit is not None:
        lf = lf.limit(limit)
    print(lf.collect())


@dataset_app.command("scan")
def dataset_scan(
    name: str = typer.Argument(None, help="dataset 名；配合 --all 扫描全部"),
    all: bool = typer.Option(False, "--all", help="增量重物化全部"),
    resync: bool = typer.Option(False, "--resync", help="强制全量重物化"),
    cascade: bool = typer.Option(True, "--cascade/--no-cascade", help="变更后级联下游 stat"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """检查依赖并增量重物化（幂等）；变更后级联通知下游 stat"""
    if all:
        if name:
            raise typer.BadParameter("--all 与 name 互斥")
        res = dataset_mod.scan(None, all=True, resync=resync, cascade=cascade, background=background)
        if isinstance(res, TaskHandle):
            print(f"task={res.task_id} status={res.status}")
            return
        for r in res:
            print(f"[{r.name}] v{r.version_before} -> v{r.version_after}"
                  f" changed={r.changed} incremental={r.incremental} rebuilt={len(r.rebuilt_partitions)}")
        return
    r = dataset_mod.scan(name, resync=resync, cascade=cascade, background=background)
    if isinstance(r, TaskHandle):
        print(f"task={r.task_id} status={r.status}（物化中）")
        return
    print(f"[{r.name}] v{r.version_before} -> v{r.version_after} changed={r.changed} "
          f"incremental={r.incremental} partition={','.join(r.partition_by) or '-'}")
    for p in r.rebuilt_partitions:
        print(f"  rebuilt: {p}")
    if r.triggered:
        print(f"  triggered: {', '.join(r.triggered)}")


@dataset_app.command("del")
def dataset_del(name: str, force: bool = typer.Option(False, "--force", help="级联删除下游 stat"),
                with_data: bool = typer.Option(True, "--with-data/--no-with-data",
                                               help="同时删除物化产物（默认删除）")):
    """删除 dataset 注册与物化产物"""
    _finish(dataset_mod.del_(name, force=force, with_data=with_data), name=name)


@dataset_app.command()
def rename(old: str, new: str):
    """改名（目录 + catalog，关联 stat 级联改名）"""
    m = dataset_mod.rename(old, new)
    print(f"renamed: {m.name} v{m.version}")


# ---------- stat ----------

@stat_app.command()
def add(
    name: str = typer.Argument(..., help="目标 table/dataset 名"),
    group_col: list[str] = typer.Option(None, "--group-col", "--group_col", help="按列分组统计（可多次）"),
    all: bool = typer.Option(False, "--all", help="统计 'all' + 逐索引/业务列分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（忽略缓存有效性）"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """创建统计资产（缺省仅 'all'；产物 stats/<name>/group=*/stats.parquet）"""
    _finish(stat_mod.add(name, group_col=group_col, all_=all, refresh=refresh, background=background),
            name=name)


@stat_app.command("list")
def stat_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册 stat"""
    metas = stat_mod.list()
    if json:
        _print_json([sm.to_dict() for sm in metas])
        return
    for sm in metas:
        print(f"{sm.name:<24} v{sm.version} target={sm.target_type}:{sm.target_name:<20} "
              f"groups={','.join(sm.groups) or '-'} stale={len(sm.stale_groups)}")


@stat_app.command()
def meta(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """stat 元数据（分组/是否 stale）"""
    sm = stat_mod.meta(name)
    if json:
        _print_json(sm.to_dict())
        return
    print(f"name:        {sm.name}")
    print(f"version:     {sm.version}")
    print(f"target:      {sm.target_type}:{sm.target_name}")
    print(f"groups:      {', '.join(sm.groups) or '-'}")
    if sm.stale_groups:
        print(f"stale:       {', '.join(sm.stale_groups)}（scan 重算）")


@stat_app.command("get")
def stat_get(
    name: str,
    group_col: str = typer.Option(None, "--group-col", "--group_col", help="按列分组统计"),
    all: bool = typer.Option(False, "--all", help="返回 'all' + 逐列分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（默认读缓存，缺失/过期自动重算）"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """读统计（默认读缓存；缺失/过期自动重算；--all 返回全部分组）"""
    res = stat_mod.get(name, group_col=group_col, all_=all, refresh=refresh, background=background)
    if isinstance(res, TaskHandle):
        print(f"task={res.task_id} status={res.status} action=stat_get（计算中）")
        return
    if all:
        for group, df in res.items():
            print(f"--- {group} ---")
            print(df)
    else:
        print(res)


@stat_app.command()
def scan(
    name: str = typer.Argument(None, help="stat 名（缺省配合 --all）"),
    all: bool = typer.Option(False, "--all", help="扫描全部已注册 stat"),
    refresh: bool = typer.Option(False, "--refresh", help="强制全量重算"),
    background: bool | None = typer.Option(None, "--background", help="后台执行（缺省跟随全局）"),
):
    """重算 data_key 失配的分组（幂等）"""
    if all and name:
        raise typer.BadParameter("--all 与 name 互斥")
    res = stat_mod.scan(name, all=all, refresh=refresh, background=background)
    if isinstance(res, TaskHandle):
        print(f"task={res.task_id} status={res.status}")
        return
    rs = res if all else [res]
    for r in rs:
        print(f"[{r['name']}] target={r['target']} "
              f"recomputed={','.join(r['recomputed']) or '-'} fresh={','.join(r['fresh']) or '-'}")


@stat_app.command("del")
def stat_del(name: str):
    """删除统计注册与产物（stats/<name>/ + stkoe_depends 边）"""
    _finish(stat_mod.del_(name), name=name)


@stat_app.command()
def rename(old: str, new: str):
    """改名（stats/ 目录 + catalog + 依赖边）"""
    m = stat_mod.rename(old, new)
    if m is None:
        raise typer.BadParameter(f"stat not registered: {old}")
    print(f"renamed: {m.name} v{m.version}")


if __name__ == "__main__":
    app()

# ---------- field ----------

@field_app.command()
def add(name: str, dataset: str,
        formula: str = typer.Option(None, help="指标公式（存根，不物化计算）"),
        display_name: str = typer.Option(None, help="显示名称")):
    """注册指标：绑定 dataset + 公式存根（catalog 登记）"""
    m = field_mod.create(name, dataset, formula=formula,
                         **({"display_name": display_name} if display_name else {}))
    print(f"registered: {m.name} dataset={m.dataset} v{m.version}")


@field_app.command("list")
def field_list(json: bool = typer.Option(False, help="JSON 输出")):
    """列出已注册指标"""
    metas = field_mod.list()
    if json:
        _print_json([m.to_dict() for m in metas])
        return
    for m in metas:
        print(f"{m.name:<24} v{m.version} dataset={m.dataset:<20} formula={m.formula or '-'}")


@field_app.command()
def meta(name: str, json: bool = typer.Option(False, help="JSON 输出")):
    """指标元数据"""
    m = field_mod.meta(name)
    if json:
        _print_json(m.to_dict())
        return
    print(f"name:        {m.name}")
    print(f"version:     {m.version}")
    print(f"dataset:     {m.dataset}")
    print(f"formula:     {m.formula or '-'}")
    print(f"display:     {m.display_name}")
    print(f"description: {m.description}")
    print(f"tags:        {', '.join(m.tags) or '-'}")


@field_app.command()
def rename(old: str, new: str):
    """改名（catalog + 依赖边）"""
    m = field_mod.rename(old, new)
    print(f"renamed: {m.name} v{m.version}")


@field_app.command("del")
def field_del(name: str):
    """删除指标注册"""
    field_mod.del_(name)
    print(f"deleted: {name}")

