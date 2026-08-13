# stkoe

stkoe 数据服务（重构版）。当前阶段：gRPC 服务端 + 表/数据集/统计三类数据资产。

## 环境要求

- Python >= 3.13
- 包管理用 [uv](https://docs.astral.sh/uv/)

## 安装

```bash
uv sync                 # 安装依赖
uv run stkoe serve      # 前台运行 gRPC 服务（默认 127.0.0.1:9569）
```

## 配置（stkoe.json）

- 查找优先级：`STKOE_CONFIG` 环境变量 > `./stkoe.json`（若存在）> `~/.stkoe/stkoe.json`
- 写入位置：`STKOE_CONFIG`（若设置）> `./stkoe.json`
- 键名保持输入形态（含连字符），支持任意字段

```bash
uv run stkoe config show                          # 查看生效配置
uv run stkoe config set --grpc-host 0.0.0.0       # 写入 {"grpc-host": "0.0.0.0"}
uv run stkoe config set --grpc-port 9000
```

`stkoe serve` 缺省 host/port 取 `grpc-host` / `grpc-port`；`--host` / `--port` 显式覆盖。
配置同样可通过 gRPC `Execute(source="config", action="show"|"set", args=["--key", "value"])` 读写。

## CLI 命令

```bash
uv run stkoe serve                                # 前台运行 gRPC 服务
uv run stkoe config show | set                    # 配置读写
uv run stkoe table <action> <args...>             # table 命令（走 Execute 同步分发）
uv run stkoe dataset <action> <args...>           # dataset 命令（走 Execute 同步分发）
uv run stkoe stat <action> <args...>              # stat 命令（走 Execute 同步分发）
```

`table` / `dataset` / `stat` 子命令与 gRPC `Execute` 行为完全一致（同一分发实现）：

```bash
uv run stkoe table list --candidate               # 未登记但含 parquet 的表目录（「新建本地表」候选）
uv run stkoe table list                           # 已注册表
uv run stkoe table meta demo                      # 表元数据
uv run stkoe table add demo                       # 注册表
uv run stkoe table set demo --display_name 演示表  # 更新表元数据
uv run stkoe table col demo sym --display_name 代码 --unit 元   # 更新列元数据
uv run stkoe table get demo                       # 读表（返回 IPC 元信息）
uv run stkoe table delete demo                    # 删除表注册（数据文件保留）

uv run stkoe dataset add ds1 index m1 --keys sym,date    # 注册数据集（不物化）
uv run stkoe dataset scan ds1                     # 物化数据集（新增/覆盖表也行）
uv run stkoe dataset get ds1                      # 读数据集（curated 读 parquet，否则实时 join）
uv run stkoe dataset meta ds1                     # 数据集元数据
uv run stkoe dataset set ds1 --display_name 演示数据集
uv run stkoe dataset delete ds1                   # 删除数据集注册

uv run stkoe stat scan dataset ds1                # 扫描覆盖率统计（stats/dataset/ds1/coverage/）
uv run stkoe stat scan table demo --kind coverage
uv run stkoe stat get dataset ds1 --partition_by all   # 读指定分区
uv run stkoe stat get dataset ds1                 # 读全部分区
uv run stkoe stat meta dataset ds1                # 统计元数据（已扫描的分区列表）
uv run stkoe stat delete dataset ds1              # 删除统计产物（数据文件保留）
```

## gRPC 协议

协议定义见 `src/stkoe/grpc/stkoe.proto`：

| RPC | 用途 | 数据形态 |
|---|---|---|
| `Execute` | 命令执行 | 服务端流式：首条 `DataHeader`，成功后跟随 `JsonData` / `ArrowTable` |
| `SubmitTask` | 后台任务 | 提交返回 `task_id` |
| `SubscribeTask` | 任务订阅 | 服务端流式 `TaskEvent`（seq/progress/message/data/state） |
| `Health` | 存活探活 + 版本 | 状态/版本 |

请求统一为 `<source> <action> <args...>` 位置参数形态（`args` 等价于
`stkoe <source> <action> <args...>`）。

已注册的 `(source, action)`：

| source | action | 说明 |
|---|---|---|
| `version` | `""` / `get` | 服务版本 |
| `config` | `show` / `""` | 生效配置 |
| `config` | `set` | 写配置（`--key value`） |
| `table` | `add` / `get` / `delete`(del) / `list` / `meta` / `set` / `col` | 表资产全套动词 |
| `dataset` | `add` / `get` / `meta` / `list` / `set` / `scan` / `delete`(del) | 逻辑数据集（add 只注册、scan 才物化） |
| `stat` | `scan` / `get` / `meta` / `list` / `delete`(del) | 数据统计（coverage 覆盖率） |

### Execute 流式约定

- 首条消息恒为 `DataHeader`：`code=0` 成功 / 非 0 业务错误（`message` 带原因）
- 成功后可跟随 0..N 条数据消息：`JsonData`（小结果 JSON）或 `ArrowTable`（表格 Arrow IPC）
- 每条数据消息带 `name` 用于区分

### 客户端示例（Python + grpcio）

```python
import grpc
from stkoe.grpc import stkoe_pb2, stkoe_pb2_grpc

ch = grpc.insecure_channel("127.0.0.1:9569")
stub = stkoe_pb2_grpc.StkoeServiceStub(ch)

# Health
h = stub.Health(stkoe_pb2.HealthRequest())
print(h.status, h.version)

# Execute：流式，首条 DataHeader
for r in stub.Execute(stkoe_pb2.ExecuteRequest(source="version", action="")):
    if r.WhichOneof("type") == "header":
        print("code:", r.header.code, r.header.message)
    elif r.WhichOneof("type") == "json":
        print(r.json.name, r.json.data)

# SubmitTask + SubscribeTask
resp = stub.SubmitTask(stkoe_pb2.SubmitTaskRequest(source="version", action="get"))
for r in stub.SubscribeTask(stkoe_pb2.SubscribeTaskRequest(task_id=resp.task_id, replay=True)):
    if r.WhichOneof("type") == "event":
        print(r.event.state, r.event.progress, r.event.message)
```

## 测试

```bash
uv run pytest -q
```

## 目录结构

```
src/stkoe/
├── __main__.py        # python -m stkoe
├── cli.py             # stkoe serve / config 命令入口
├── args.py            # 共享 flag 解析
├── jsonutil.py        # 统一 orjson 序列化
├── settings.py        # stkoe.json 配置（StkoeConfig dataclass）
├── grpc/
│   ├── stkoe.proto    # 协议定义
│   ├── stkoe_pb2.py / stkoe_pb2_grpc.py   # protoc 生成
│   ├── dispatch.py    # (source, action, args) Execute 命令分发
│   └── server.py      # StkoeService 实现 + StkoeServer
├── table/             # 表数据资产（TableController）
│   ├── spec.py        # TableLayout/ColumnMeta/TableMeta/TableScanReport 等 dataclass
│   ├── util.py        # parquet 指纹/布局识别/footer/差异对比
│   ├── catalog.py     # SQLite catalog（stkoe_objects/stkoe_data_files/stkoe_file_stats）
│   ├── query.py       # 谓词解析 + 文件级裁剪（prune_files）
│   ├── controller.py  # TableController：async add/get/delete/list/meta/set/col
│   └── handlers.py    # 任务框架接入（source="table"）
├── dataset/           # 逻辑数据集（DatasetController）
│   ├── spec.py        # DatasetMeta/DatasetScanReport dataclass
│   ├── controller.py  # async add/get/meta/list/set/scan/delete（add 只注册，物化走 scan）
│   └── handlers.py    # 任务框架接入（source="dataset"）
├── stat/              # 数据统计资产（StatController）
│   ├── spec.py        # StatFile/StatMeta/StatScanReport dataclass
│   ├── calc.py        # calc_stats：按 dtype 分桶算覆盖率统计（ALL_COLS 输出）
│   ├── controller.py  # async scan/get/meta/list/delete（cov 写入 stats/ 目录，不进 catalog）
│   └── handlers.py    # 任务框架接入（source="stat"）
├── mock/              # 演示数据生成（stkoe mock demo/gen，替代 scripts/gen_example_data.py）
│   ├── gen.py         # 生成器（tdcal/common/index/feature/klday/m1 + demo）+ write（只写盘不注册）
│   └── handlers.py    # 任务框架接入（source="mock"）
└── task/              # 任务框架
    ├── model.py       # Task / TaskEvent / TaskResult / TaskContext
    ├── registry.py    # TaskHandler + TaskRegistry
    ├── store.py       # SQLite：TaskStore(task) / EventStore(task_event)
    ├── scheduler.py   # asyncio 调度器（独立事件循环线程）
    ├── logs.py        # LogStore → tasks/<id>/task.log
    ├── results.py     # ResultStore → tasks/<id>/<name> 大结果
    ├── handlers.py    # 内置 Handler（version/config/mock）
    └── manager.py     # TaskManager 编排核心
```

> v1.0/ 为旧版参考实现（v0.5.1），本仓库正在按新需求重新构造。
