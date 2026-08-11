# AGENTS.md - stkoe（重构版）开发指南

本文件为在仓库根目录工作的 AI 代理提供项目信息、代码风格、架构约定与近期变更记录。

> `v1.0/` 是旧版参考实现（v0.5.1），本仓库当前代码在 `src/stkoe/` 按新协议重新构造，
> **不要修改 `v1.0/` 下的代码**（其自带 `v1.0/AGENTS.md` 仅供参考）。

## 项目概述

stkoe 数据服务（gRPC）。当前为重构版骨架：实现 `stkoe.proto` 协议的服务端
（`stkoe serve`）+ 单文件测试客户端（`gclient.py` REPL）。

- Python >= 3.13；包管理只用 [uv](https://docs.astral.sh/uv/)（不用 pip）
- 依赖：grpcio / orjson / polars / pyarrow；dev 组：grpcio-tools / pytest

## 常用命令

```bash
uv sync                              # 安装依赖
uv run stkoe serve                   # 前台运行 gRPC 服务（默认 127.0.0.1:9569）
uv run pytest -q                     # 标准测试方式
```

本机（Windows 无 3.13）可用预建 .venv 跑测试：

```bash
.venv/Scripts/python.exe -m pytest tests -q
```

仓库无 lint / typecheck 配置；提交前以全量 pytest 通过为准。

## 目录结构

```
src/stkoe/
├── cli.py             # stkoe serve / config 命令入口
├── args.py            # --key value / --key=value / --flag 解析（parse_flags）
├── jsonutil.py        # 统一 orjson 序列化（dumps_str/loads）
├── logutil.py         # 统一 logger（LOG）+ setup_logging()（默认 INFO，STKOE_LOG_LEVEL 可覆盖）
├── settings.py        # stkoe.json 配置（StkoeConfig / load_config / save_config）
├── grpc/
│   ├── stkoe.proto + stkoe_pb2*.py     # 协议 + protoc 生成
│   ├── dispatch.py    # Execute 同步命令分发（@handler 注册；version/config/table）
│   └── server.py      # StkoeService 实现 + StkoeServer + 请求 INFO 日志
├── table/             # 表数据资产（TableController，async 接口）
│   ├── spec.py        # TableLayout/ColumnMeta/TableMeta/TableScanReport dataclass
│   ├── util.py        # parquet 指纹/布局识别/footer/差异/signature
│   ├── catalog.py     # SQLite catalog（stkoe_objects/stkoe_data_files/stkoe_file_stats）
│   ├── query.py       # 谓词解析 + 文件级裁剪（prune_files）
│   ├── controller.py  # async add/get/delete/list/meta/set（阻塞 IO 走 asyncio.to_thread）
│   └── handlers.py    # 任务版 Handler（source="table"，注册进 TaskRegistry）
└── task/              # 任务框架
    ├── model.py       # Task / TaskEvent / TaskResult / TaskContext
    ├── manager.py     # TaskManager 编排核心（cancel/subscribe 语义见下）
    ├── registry.py    # TaskHandler + TaskRegistry
    ├── store.py       # SQLite：TaskStore(task) / EventStore(task_event)
    ├── scheduler.py   # asyncio 调度器（独立事件循环线程）
    ├── logs.py / results.py / handlers.py
```

`gclient.py`（仓库根）：gRPC 测试 REPL，输入 `e:<source> <action> <args...>` /
`s:...` / `t:<task_id>` / `c:<task_id> action` / `h`。

## 代码与提交约定

- 文档字符串用中文；类型注解用 `X | None` 语法；不可变结构优先 `@dataclass(frozen=True)`
- 导入顺序：标准库 → 第三方 → 项目内部；私有方法 `_prefix`
- 提交信息风格：`feat(xxx): 中文描述` / `fix(xxx): ...`（如 `feat(tools): 单文件 gRPC 测试客户端`）
- 日志用标准库 `logging`（`from ..logutil import LOG`），**不是 loguru**（旧版 v1.0 才是）

## 架构要点（改代码前必读）

### 双命令路径：Execute 与 SubmitTask 各自注册

同一业务命令有两条路径，**新增动词必须两处都注册并保持行为对齐**：

| 路径 | 分发 | 处理器签名 | 注册位置 |
|---|---|---|---|
| `e:<source> <action>`（Execute，同步流式返回） | `grpc/dispatch.py` | `fn(args, data_dir=None) -> list[Result]`，`@handler(source, action)` | dispatch.py 内 |
| `s:<source> <action>`（SubmitTask，后台任务） | `TaskManager.registry` | `async run(ctx) -> TaskResult`，`TaskHandler` 子类 | 模块 `register(registry)` |

- `dispatch` 的 table 处理器直接调 `TableController`（内部 `asyncio.run` 收敛，gRPC 线程无事件循环）
- Execute 的 `e:table get` 返回 `[JsonData(行数/列数), ArrowTable(IPC)]` 两条消息
- `Result.kind`: json → `JsonData`，table → `ArrowTable`；`Result.json/Result.table` 工厂方法

### data_dir 透传

`StkoeServer.data_dir` → `_StkoeServicer` → `_execute_stream` → `dispatch(..., data_dir=...)`，
保证 Execute 与 SubmitTask 用同一数据目录。处理器不用 `load_config()` 默认路径绕过它。

### 日志约定

- `serve()` 入口调 `setup_logging()`；默认 INFO；`STKOE_LOG_LEVEL` 环境变量可覆盖
- gRPC 层每个 RPC 打两条 INFO：「接收请求 <RPC>: ...」与「完成 <RPC>: ... 耗时=…ms」，
  含 peer；SubmitTask 失败打 WARNING；服务启停打 INFO

### TaskManager 不变式

1. **取消是协作式**：`cancel()` 对 pending 任务直接终态；对 running 只置 `_cancelled` 标记，
   Handler 在检查点 `ctx.is_cancelled()` 抛 `TaskCancelled` 自行退出——**不要**在
   `_finalize` 里提前清标记（曾导致取消后任务继续跑完的 zombie bug，见变更记录）
2. **Subscribe EOF 顺序**：先消费事件队列再判终态（队列里可能有刚落库未读的终态事件），
   终态事件先落库/入队、再摘除 `_live`；EOF 分支要补读兜底
3. `subscribe` 首条恒为 DataHeader（0 成功 / 非 0 业务错误）；终态后 EOF

### TableController 不变式

1. **绝不写/删用户 parquet**：`delete` 只删 catalog 登记，数据目录保留（可重新 add）
2. `add` 是"发现资产"语义：目录不存在报错、已注册报 `TableExistsError`（更新用 scan）
3. `set` 只更新元数据：`display_name/description/source/tags`（tags 逗号分隔），
   任意其他键进 `extra`，版本递增；未注册报 `TableNotFoundError`，不做隐式注册
4. 读前快检 `_ensure_fresh`：stat 签名一致则继续，不一致自动 scan；未注册目录隐式注册
5. `signature()` = sha256(排序后的 `rel_path|size|mtime_ns`)，相对表根

### 测试

- `tests/test_grpc.py`：`srv`/`client` fixture（StkoeServer 起真实 gRPC，port=0 自动分配）
- `tests/test_table.py`：controller 直测 + 任务版链路（`_await`/`_mgr_result` 助手）
- 流式断言用 `_collect`（先 DataHeader，再数据消息）
- 全量 56 用例，多连跑需稳定（曾修过时序竞态，新增用例注意时序敏感）

## 近期变更记录

### 2026-08 table col + list --candidate + CLI table 子命令 + dataset 规划

- **`table col` 列元数据更新**（参照 v1.0 table.py）：`table col <name> <column> --display_name/
  --description/--unit/--formula/--tags <v>`，只改 catalog 列说明不改数据文件，版本递增；
  表未注册/列不存在报错；`ColumnMeta.from_dict` 规范化 tags 为 tuple。三条路径同时注册：
  Execute（`e:table col ...`）、任务版（`s:table col ...`）、CLI
- **`table list --candidate`**：返回未登记但含 parquet 的表目录（「新建本地表」候选），
  对照 v1.0 `candidates()`；Execute/任务版/CLI 三处对齐，返回 JSON `["cand", ...]`
- **CLI 通用分发**：`stkoe table <action> <args...>` 走 Execute 同步分发（`cli._cmd_dispatch`），
  与 gRPC Execute 行为完全一致；`--help` 补充说明
- 测试：新增 col/list --candidate 的 controller / 任务版 / gRPC 全链路用例，全量 59 用例绿

### 2026-08 gRPC 日志 + table set + 任务竞态修复

- **gRPC 请求日志**：新增 `logutil.py`（`LOG` + `setup_logging`）；`server.py` 每个 RPC
  INFO 记录「接收请求/完成」+ 耗时 + peer；启停有日志（`STKOE_LOG_LEVEL` 可覆盖）
- **Execute 支持 table 全套动词**：`e:table add/get/delete/del/list/meta/set`（与任务版对齐），
  `dispatch` 处理器签名统一为 `fn(args, data_dir=None)`，data_dir 全链路透传
- **`table set` 元数据更新**：`TableController.set()`（display_name/description/source/tags +
  任意键进 extra，版本递增），任务版（`table/handlers.py`）与 Execute 版（`dispatch.py`）同时注册
- **fix: 取消标记被提前清除**：`cancel()` 原实现置标记后 `_finalize` 立即 `discard`，
  运行中 Handler 永远看不到取消，任务显示 cancelled 后还在跑（zombie）。
  改为 running 任务只置标记、由 Handler 检查点 `TaskCancelled` 协作式退出
- **fix: Subscribe EOF 丢终态事件**：EOF 判定与终态事件入队存在竞态，改为先消费事件队列再判终态
- 测试：新增 Execute table 全链路 / set / 缺参 / 请求日志断言等用例，全量 56 用例绿