# stkoe

stkoe 数据服务（重构版）。当前阶段：gRPC 服务器骨架，实现 `stkoe.proto` 协议。

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
│   ├── controller.py  # TableController：async add/get/delete/list/meta
│   └── handlers.py    # 任务框架接入（source="table"）
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
