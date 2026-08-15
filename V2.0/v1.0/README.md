# stkoe

量化数据管理 / 因子研究框架（自 DataCenter 仓库拆分的独立项目）。

专注后端任务：数据资产登记、逻辑数据集、指标管理、统计物化、因子研究与
gRPC 数据服务。不包含任何前端（Portal/Web UI 已于 v0.4.8 移除）。

## 功能总览

| 模块 | 说明 |
|---|---|
| `data/` | 数据管理框架：只读表资产（sniff 自动注册）、逻辑数据集（join/增量/自动分区）、指标（field）、统计物化（stat）、任务管理（后台/暂停/取消） |
| `data/plugins/` | 数据插件（`wsdata` 实时接入、`mock` 演示数据） |
| `factor/` | 因子研究框架：core 构建器、zoo 因子库、operators、testers（IC/分组收益/稳定性等测试与绘图） |
| `barra/` | Barra 风格因子 |
| `grpc/` | gRPC 数据服务（Execute JSON / Select Arrow IPC / RunTask 流式 / Health） |

## 快速开始

### 环境要求

- Python >= 3.13
- 包管理用 [uv](https://docs.astral.sh/uv/)

### 安装

```bash
uv sync                 # 安装依赖
```

### 运行

```bash
uv run python -m stkoe                    # 交互式 REPL（Tab 补全，自动起 gRPC 服务）
uv run python -m stkoe table list         # 列出已注册表
uv run python -m stkoe server run         # 前台 gRPC 服务（默认端口 9569）
```

### 测试

```bash
uv run pytest -q                          # 全量测试
```

## 数据模型

- **数据根目录**优先级：`STKOE_LOCAL_DATA` 环境变量 > 配置 `data_path` > `~/.stkoe`
- **配置查找**：`STKOE_CONFIG` 环境变量 > `./stkoe.json` > `~/.stkoe.json`
- **表数据只读**：用户用外部工具更新数据文件，框架只登记元数据，绝不改写用户数据
- **目录布局**：

```
<data_root>/
├── catalog.db            # SQLite 元数据（stkoe_objects/data_files/file_stats/depends/tasks）
├── tables/<name>/        # 用户表数据（只读观察，sniff 自动注册）
├── datasets/<name>/      # dataset 物化产物（part=<v>/data.parquet 或 data.parquet）
├── stats/<name>/         # stat 统计物化（group=<all|col>/stats.parquet）
└── fields/<name>/        # field 物化存档
```

- **catalog 对象身份**为 `(type, name)` 复合唯一：table / dataset / field / stat
- **依赖图** `stkoe_depends` 登记资源依赖（dataset→table、stat→dataset、field→dataset），
  用于 rename/drop 级联保护

## gRPC 服务

服务定义见 `src/stkoe/grpc/stkoe.proto`，仅绑定 127.0.0.1（本地服务）。

| RPC | 用途 | 数据形态 |
|---|---|---|
| `Execute` | 元数据/列表/状态等小结果 | JSON 字符串 |
| `Select` | 表格查询（table/dataset/stat/field） | Arrow IPC 帧 + schema JSON，支持分页/过滤/排序 |
| `RunTask` | 长任务（物化/公式/统计） | 服务端流式事件：log / progress / result / done / error |
| `Health` | 存活探活 + 版本 | 状态/版本 |

示例（Python + grpcio）：

```python
import grpc
import polars as pl
from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc

ch = grpc.insecure_channel("127.0.0.1:9569")
stub = stkoe_pb2_grpc.StkoeServiceStub(ch)

# Execute：元数据 JSON
resp = stub.Execute(stkoe_pb2.ExecuteRequest(cmd="table", args=["list"]))
items = json.loads(resp.json_out)

# Select：Arrow IPC 直接读
resp = stub.Select(stkoe_pb2.SelectRequest(name="t1", where="close >= 10"))
df = pl.read_ipc(resp.ipc)

# RunTask：流式事件
for ev in stub.RunTask(stkoe_pb2.TaskRequest(
        cmd="dataset", args=["materialize", "ds"], task_id="t1")):
    print(ev.type, ev.message)
```

## 设计要点

- **sniff 幂等**：无差异重复 sniff 不 bump version；发现未注册目录自动注册（version=1）
- **读路径不碰数据**：select 走 catalog 文件级裁剪（谓词→列统计），不读 footer、不 collect
- **读前快检**：`_ensure_fresh` 比对文件签名，不一致自动 sniff
- **布局自动识别**：SINGLE / FLAT / HIVE，不提供手动指定分区键
- **dataset**：join 键由 index 表定义（缺省=全部列，各成员表必须存在）；增量按
  `partition_deps` 源文件签名重算失配分区；自动分区镜像 index HIVE 键或按行数/时间跨度
- **stat**：与 dataset 解耦的独立统计物化视图，缓存有效性键 `data_key`，`--refresh` 强制重算
- **任务**：submitted → running ⇄ paused → succeeded/failed/cancelled，日志增量拉取，
  pause/stop 在分区边界协作式生效

## 目录结构

```
stkoe/
├── pyproject.toml
├── src/stkoe/
│   ├── __main__.py        # CLI 入口 + REPL（prompt_toolkit，Tab 补全）
│   ├── data/              # 数据管理框架
│   │   ├── table.py       # table 业务（sniff/add/meta/del/rename…）
│   │   ├── dataset.py     # 逻辑数据集（scan/create/materialize/增量/自动分区）
│   │   ├── stat.py        # 统计物化（create/select/sniff/…，与 dataset 解耦）
│   │   ├── field.py       # 指标管理（catalog 注册 + formula 物化）
│   │   ├── task.py        # 任务管理（同步/后台 + 日志/进度/暂停/取消）
│   │   ├── query.py       # 谓词解析 + 文件级裁剪
│   │   ├── util.py        # 通用能力（FileInfo/layout/signature/diff）
│   │   ├── settings.py    # 配置（data_path/ignore_cols/grpc_port）
│   │   ├── dbt.py         # DBT manifest 元数据导入
│   │   ├── mock.py        # 参数化演示数据
│   │   ├── cli.py         # typer 子命令
│   │   ├── catalog/       # SQLite schema + 行访问（db/spec/access/json）
│   │   └── plugins/       # wsdata / mock 数据插件
│   ├── factor/            # 因子框架（core/zoo/operators/testers）
│   ├── barra/             # Barra 风格因子
│   └── grpc/              # gRPC 服务（stkoe.proto + server.py）
└── tests/                 # pytest（conftest 提供 make_df/write_single/write_hive）
```

## 演进记录

完整演进记录见 [AGENTS.md](AGENTS.md)「演进记录」章节。
