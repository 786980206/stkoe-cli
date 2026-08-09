"""CLI：stkoe 命令入口（typer）

统一动词：add / get / del / set / meta / list / scan（table · dataset · stat 对齐）。

输出约定（v0.5.0）：结构化结果（报告/元数据/任务）一律 JSON 一行（orjson），
表格查询（get）原样打印 polars DataFrame；不提供 --json 开关（默认即 JSON）。
所有命令默认同步执行，``--async`` 显式转后台（返回 task_id，``task get`` 查询）。
"""
import sys

import orjson
import polars as pl
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
def run(host: str | None = typer.Option(None, help="绑定地址（缺省取配置 grpc_host）"),
     port: int | None = typer.Option(None, help="端口（缺省取配置 grpc_port）"),
     reload: bool = typer.Option(False, "--reload", help="监听 stkoe 源码变更自动重启（开发用）")):
    """前台运行 gRPC 服务（阻塞；REPL 已同步后台启动）"""
    if reload:
        from ..grpc.server import serve_reload
        serve_reload(port, host)
        return
    from ..grpc.server import serve as grpc_serve
    srv = grpc_serve(port, host)
    print(f"stkoe gRPC listening on {srv.host}:{srv.port}")
    srv.wait()


def _print_json(obj):
    sys.stdout.buffer.write(orjson.dumps(obj) + b"\n")


def _jsonable(obj):
    """dataclass/嵌套容器 → JSON 可序列化（dataclass 经 to_dict）"""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(x) for x in obj]
    return obj


def _emit(res):
    """统一结果输出：TaskHandle/结构化 → JSON 一行；DataFrame → 原样打印"""
    if isinstance(res, TaskHandle):
        _print_json(res.to_dict())
        return
    if isinstance(res, pl.DataFrame):
        print(res)
        return
    _print_json(_jsonable(res))


# ---------- table ----------

@table_app.command()
def add(
    name: str = typer.Argument(None, help="表名；配合 --all 可省略"),
    all: bool = typer.Option(False, "--all", help="发现并注册 tables/ 下所有未注册且有数据的表"),
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
    dbt_manifest: str | None = typer.Option(None, "--dbt-manifest", help="DBT manifest 路径（文件/项目目录/自动发现），合并同名模型元数据"),
):
    """注册表（发现资产语义：目录不存在报错；已注册报错用 scan 刷新）"""
    _emit(table.add(name, all=all, background=async_, dbt_manifest=dbt_manifest))


@table_app.command("list")
def table_list():
    """列出已注册表"""
    _emit([m for m in table.list()])


@table_app.command()
def meta(name: str):
    """表元数据：版本/布局/分区/文件/列"""
    _emit(table.meta(name))


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
        _print_json({"written": out, "rows": df.height})
    else:
        print(lf.collect())


@table_app.command("del")
def del_cmd(name: str, force: bool = typer.Option(False, "--force", help="级联删除依赖方（dataset/stat 一并清理）")):
    """删除表注册（绝不删用户数据文件）"""
    r = table.del_(name, force=force)
    _emit(r if r is not None else {"deleted": name})


@table_app.command()
def rename(old: str, new: str):
    """改名（目录 tables/old → tables/new，并同步 catalog/下游引用）"""
    _emit(table.rename(old, new))


@table_app.command()
def set(
    name: str,
    display_name: str = typer.Option(None, "--display-name"),
    desc: str = typer.Option(None, "--desc"),
    tags: str = typer.Option(None, help="标签，逗号分隔"),
    new_name: str = typer.Option(None, "--new-name", help="改名（等价 table rename）"),
    dbt_manifest: str | None = typer.Option(None, "--dbt-manifest",
                                            help="DBT manifest 路径，合并同名模型元数据（表/列描述）"),
):
    """修改表级元数据（display_name/description/tags，可配合 --dbt-manifest）"""
    _emit(table.set(name, display_name=display_name, description=desc,
                    tags=tags.split(",") if tags else None, new_name=new_name,
                    dbt_manifest=dbt_manifest))


@table_app.command()
def col(
    name: str,
    column: str = typer.Argument(..., help="字段名"),
    display_name: str = typer.Option(None, "--display-name"),
    desc: str = typer.Option(None, "--desc"),
    unit: str = typer.Option(None, "--unit"),
):
    """更新字段（列）元数据"""
    _emit(table.col(name, column, display_name=display_name, description=desc, unit=unit))


@table_app.command()
def scan(
    name: str = typer.Argument(None, help="表名；缺省配合 --all 扫描全部"),
    all: bool = typer.Option(False, "--all", help="扫描 tables/ 下全部目录（含未注册）"),
    resync: bool = typer.Option(False, "--resync", help="忽略快检强制全量读 footer"),
    cascade: bool = typer.Option(True, "--cascade/--no-cascade", help="变更后触发下游（默认开启）"),
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """扫描同步元数据（幂等：无差异不 bump 版本）；变更后自动触发下游"""
    _emit(table.scan(name, all=all, resync=resync, cascade=cascade, background=async_))


# ---------- config ----------

@config_app.command()
def show():
    """查看当前配置（配置文件路径 + 生效值）"""
    c = load_config()
    p = config_path()
    _print_json({
        "config_file": str(p),
        "data_path": c.data_path,
        "ignore_cols": list(c.ignore_cols),
        "grpc_host": c.grpc_host,
        "grpc_port": c.grpc_port,
        "resolved_data_path": str(resolve_data_path()),
    })


@config_app.command()
def set(
    data_path: str = typer.Option(None, "--data-path", help="默认数据根目录"),
    ignore_cols: str = typer.Option(None, "--ignore-cols", help="忽略的工具字段，逗号分隔（可多个）"),
    grpc_host: str = typer.Option(None, "--grpc-host", help="gRPC 服务绑定地址（缺省 127.0.0.1）"),
    grpc_port: int = typer.Option(None, "--grpc-port", help="gRPC 服务端口（缺省 9569）"),
):
    """修改配置并写入 stkoe.json"""
    c = load_config()
    new = StkoeConfig(
        data_path=data_path or c.data_path,
        ignore_cols=tuple(ignore_cols.split(",")) if ignore_cols else c.ignore_cols,
        grpc_host=grpc_host or c.grpc_host,
        grpc_port=grpc_port if grpc_port is not None else c.grpc_port,
    )
    p = save_config(new)
    _print_json({"written": str(p)})


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
    _emit(mock_mod.write(name, df, partition_by=partition_by))


# ---------- task ----------

@task_app.command("list")
def task_list(
    status: str = typer.Option(None, help="按状态过滤：submitted|running|paused|succeeded|failed|cancelled"),
    type: str = typer.Option(None, help="按类型过滤，如 dataset_add"),
    limit: int = typer.Option(100, help="条数上限"),
):
    """任务列表"""
    _emit(task_mod.task_list(status=status, type=type, limit=limit))


@task_app.command()
def stop(
    task_id: str = typer.Argument(None, help="任务 id（--all 时省略）"),
    all_tasks: bool = typer.Option(False, "--all", help="停止所有运行/暂停任务并清理完成态任务"),
):
    """停止任务（协作式）"""
    if all_tasks:
        stopped = task_mod.task_stop_all()
        cleaned = task_mod.task_clean()
        _print_json({"stopped": stopped, "cleaned": cleaned})
        return
    if not task_id:
        raise typer.BadParameter("需要提供 task_id 或使用 --all")
    try:
        _emit(task_mod.task_stop(task_id))
    except KeyError as e:
        raise typer.BadParameter(str(e))


@task_app.command()
def clean():
    """删除全部完成态任务（succeeded/failed/cancelled，日志级联删除）"""
    _print_json({"cleaned": task_mod.task_clean()})


@task_app.command()
def pause(task_id: str):
    """暂停任务（协作式，下一个分区边界生效）"""
    try:
        _emit(task_mod.task_pause(task_id))
    except KeyError as e:
        raise typer.BadParameter(str(e))


@task_app.command()
def resume(task_id: str):
    """恢复已暂停任务"""
    try:
        _emit(task_mod.task_resume(task_id))
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
    _emit(entries)


@task_app.command()
def get(task_id: str):
    """任务详情：状态/进度/阶段/错误 + 结果（result，异步任务完成可拉取）"""
    try:
        _emit(task_mod.task_get(task_id))
    except KeyError as e:
        raise typer.BadParameter(str(e))


# ---------- dataset ----------

@dataset_app.command()
def add(
    name: str,
    index_table: str = typer.Argument(..., help="索引表（提供 join 键）"),
    tables: list[str] = typer.Argument(None, help="参与 join 的表"),
    keys: str = typer.Option(None, help="join 键，逗号分隔（缺省=index 全部列）"),
    no_materialize: bool = typer.Option(False, "--no-materialize", help="只注册不物化"),
    force: bool = typer.Option(False, "--force", help="已存在时覆盖重建"),
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """注册 dataset（join 规格校验 → 注册 → 自动物化；get 前未物化也会自动）"""
    try:
        _emit(dataset_mod.add(name, index_table, *tables,
                              keys=keys.split(",") if keys else None,
                              materialize=not no_materialize,
                              force=force, background=async_))
    except dataset_mod.DatasetExistsError as e:
        raise typer.BadParameter(str(e))


@dataset_app.command("list")
def dataset_list():
    """列出已注册 dataset"""
    _emit([dm for dm in dataset_mod.list()])


@dataset_app.command()
def meta(name: str):
    """dataset 元数据"""
    _emit(dataset_mod.meta(name))


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
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """检查依赖并增量重物化（幂等）；变更后级联通知下游 stat"""
    if all and name:
        raise typer.BadParameter("--all 与 name 互斥")
    _emit(dataset_mod.scan(name, all=all, resync=resync, cascade=cascade, background=async_))


@dataset_app.command("del")
def dataset_del(name: str, force: bool = typer.Option(False, "--force", help="级联删除下游 stat"),
                with_data: bool = typer.Option(True, "--with-data/--no-with-data",
                                               help="同时删除物化产物（默认删除）")):
    """删除 dataset 注册与物化产物"""
    r = dataset_mod.del_(name, force=force, with_data=with_data)
    _emit(r if r is not None else {"deleted": name})


@dataset_app.command()
def rename(old: str, new: str):
    """改名（目录 + catalog，关联 stat 级联改名）"""
    _emit(dataset_mod.rename(old, new))


# ---------- stat ----------

@stat_app.command()
def add(
    name: str = typer.Argument(..., help="目标 table/dataset 名"),
    group_col: list[str] = typer.Option(None, "--group-col", "--group_col", help="按列分组统计（可多次）"),
    all: bool = typer.Option(False, "--all", help="统计 'all' + 逐索引/业务列分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（忽略缓存有效性）"),
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """创建统计资产（缺省仅 'all'；产物 stats/<name>/group=*/stats.parquet）"""
    _emit(stat_mod.add(name, group_col=group_col, all_=all, refresh=refresh, background=async_))


@stat_app.command("list")
def stat_list():
    """列出已注册 stat"""
    _emit([sm for sm in stat_mod.list()])


@stat_app.command()
def meta(name: str):
    """stat 元数据（分组/是否 stale）"""
    _emit(stat_mod.meta(name))


@stat_app.command("get")
def stat_get(
    name: str,
    group_col: str = typer.Option(None, "--group-col", "--group_col", help="按列分组统计"),
    all: bool = typer.Option(False, "--all", help="返回 'all' + 逐列分组"),
    refresh: bool = typer.Option(False, "--refresh", help="强制重算（默认读缓存，缺失/过期自动重算）"),
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """读统计（默认读缓存；缺失/过期自动重算；--all 返回全部分组）"""
    res = stat_mod.get(name, group_col=group_col, all_=all, refresh=refresh, background=async_)
    if isinstance(res, TaskHandle):
        _print_json(res.to_dict())
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
    async_: bool = typer.Option(False, "--async", help="后台执行（提交线程池，返回 task_id）"),
):
    """重算 data_key 失配的分组（幂等）"""
    if all and name:
        raise typer.BadParameter("--all 与 name 互斥")
    _emit(stat_mod.scan(name, all=all, refresh=refresh, background=async_))


@stat_app.command("del")
def stat_del(name: str):
    """删除统计注册与产物（stats/<name>/ + stkoe_depends 边）"""
    r = stat_mod.del_(name)
    _emit(r if r is not None else {"deleted": name})


@stat_app.command()
def rename(old: str, new: str):
    """改名（stats/ 目录 + catalog + 依赖边）"""
    m = stat_mod.rename(old, new)
    if m is None:
        raise typer.BadParameter(f"stat not registered: {old}")
    _emit(m)


# ---------- field ----------

@field_app.command()
def add(name: str, dataset: str,
        formula: str = typer.Option(None, help="指标公式（存根，不物化计算）"),
        display_name: str = typer.Option(None, help="显示名称")):
    """注册指标：绑定 dataset + 公式存根（catalog 登记）"""
    _emit(field_mod.create(name, dataset, formula=formula,
                           **({"display_name": display_name} if display_name else {})))


@field_app.command("list")
def field_list():
    """列出已注册指标"""
    _emit([m for m in field_mod.list()])


@field_app.command()
def meta(name: str):
    """指标元数据"""
    _emit(field_mod.meta(name))


@field_app.command()
def rename(old: str, new: str):
    """改名（catalog + 依赖边）"""
    _emit(field_mod.rename(old, new))


@field_app.command("del")
def field_del(name: str):
    """删除指标注册"""
    field_mod.del_(name)
    _print_json({"deleted": name})


if __name__ == "__main__":
    app()
