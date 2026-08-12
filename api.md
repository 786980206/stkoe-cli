# stkoe API 文档

> stkoe 数据服务（gRPC）对外接口全量说明：gRPC 协议、Execute 命令、后台任务、CLI、测试客户端与配置。
> **约定：任何 API 变更（新增/修改动词、参数、返回字段）时，必须同步更新本文件。**

## 1. 总览

所有业务命令统一为 `<source> <action> <args...>` 位置参数形态，等价于 `stkoe <source> <action> <args...>`。

- **source**：`version` / `config` / `table` / `dataset` / `stat` / `task` / `mock`
- **action**：`add` / `get` / `list` / `meta` / `set` / `col` / `scan` / `delete`（`del` 别名）/ `show`
- **args**：action 之后的位置参数 + `--key value` flag

同一业务命令有**双路径**，行为对齐：

| 路径 | 入口 | 返回 | 适用 |
|---|---|---|---|
| Execute（同步流式） | `e:<source> <action> <args...>` | DataHeader + JsonData / ArrowTable | 元数据/列表/表格读取等小任务 |
| SubmitTask（后台任务） | `s:<source> <action> <args...>` | 立即返回 `task_id`，事件流见 §4 | 物化/统计等长任务 |

### 1.1 参数解析约定（parse_flags）

- `--key value`、`--key=value`、`--flag`（无值 → 布尔 True）
- 键名保持用户输入形态（含连字符，如 `--exclude-tool`、`--partition_by` 与 `--partition-by` 均可识别）
- 位置参数：`add/get/list/...` 需要的位置参数（如表名）写在 flags 之前

### 1.2 通用返回约定（Execute）

- 第一条消息恒为 `DataHeader`：`code=0` 成功 / `code!=0` 业务错误（`message` 为原因）
- 成功后按需跟随：`JsonData`（小结果 JSON）或 `ArrowTable`（表格数据，Arrow **IPC Stream** 格式，字节流可直接 `pa.ipc.open_stream` 读取；`meta` 为元信息 JSON）
- `Result.kind`：`json` → `JsonData`，`table` → `ArrowTable`

### 1.3 数据目录（data_dir）透传

`StkoeServer.data_dir` → `_StkoeServicer` → `_execute_stream` → `dispatch(...)`，保证 Execute 与 SubmitTask 用同一数据目录。命令行直接调用（无 data_dir）时回退 `load_config().data_dir`。

---

## 2. gRPC 协议

proto：`src/stkoe/grpc/stkoe.proto`（package `stkoe`），编译产物 `stkoe_pb2*.py`。

### 2.1 RPC 一览

| RPC | 请求 | 响应 | 说明 |
|---|---|---|---|
| `Execute` | `ExecuteRequest` | `stream ExecuteResponse` | 同步命令执行，首条恒为 DataHeader |
| `SubmitTask` | `SubmitTaskRequest` | `SubmitTaskResponse` | 提交后台任务，立即返回 `task_id` |
| `SubscribeTask` | `SubscribeTaskRequest` | `stream SubscribeTaskResponse` | 订阅任务事件流，终态后 EOF |
| `TaskControl` | `TaskControlRequest` | `TaskControlResponse` | `cancel` / `pause` / `resume` |
| `Health` | `HealthRequest` | `HealthResponse` | 存活探活 + 版本 |

### 2.2 消息结构

```
ExecuteRequest { source, action, args[] }        SubmitTaskRequest 同 ExecuteRequest
ExecuteResponse = oneof { header, json, table }   DataHeader{ code, message }
JsonData { name, data }                            ArrowTable { name, data, meta }
SubmitTaskResponse { header, task_id }             SubscribeTaskRequest { task_id, replay }
SubscribeTaskResponse = oneof { header, event }    TaskEvent { seq, time, progress, message, data, state }
TaskControlRequest { task_id, action }             TaskControlResponse { header, task_id }
HealthRequest {}                                   HealthResponse { status, version }
```

- `ArrowTable.data`：Arrow **IPC Stream** 格式（`df.write_ipc_stream` / `pa.ipc.open_stream` 配对）
- `ArrowTable.meta`：JSON 字符串，形如 `{"name","rows","total","columns":[...]}`，见 §3.2
- `TaskEvent.state`：`pending` / `running` / `paused` / `succeeded` / `failed` / `cancelled`
- `SubscribeTask.replay=true` 先回放历史事件，否则只推订阅后事件；首条恒为 `DataHeader(code=0)`，任务终态后 EOF

---

## 3. Execute 命令（`e:...`）

### 3.1 全命令表

| source | action | 位置参数 | flags | 返回 |
|---|---|---|---|---|
| version | （空）/ `get` | — | — | JsonData `{"version"}` |
| config | （空）/ `show` | — | — | JsonData `{"config_file", "grpc-host", "grpc-port", "data-dir", ...extra}` |
| config | `set` | — | `--<key> <value> ...`（任意键） | JsonData `{"written", "set"}` |
| task | （空）/ `list` | — | `--state <state>` | JsonData `{"tasks": [...]}`（按创建时间倒序） |
| table | `add` | `<name>` | `--all` | JsonData（TableScanReport） |
| table | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| table | `scan` | `<name>` | `--all` `--resync` | JsonData（TableScanReport 或 []） |
| table | `list` | — | `--candidate` | JsonData（TableMeta[] 或 候选名[]） |
| table | `meta` | `<name>` | — | JsonData（TableMeta） |
| table | `set` | `<name>` | `--display_name/--description/--source/--tags <v>` + 任意键 | JsonData（TableMeta） |
| table | `col` | `<name> <column>` | `--display_name/--description/--unit/--formula/--tags <v>` | JsonData（TableMeta） |
| table | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| dataset | `add` | `<name> <index> [member...]` | `--keys k1,k2` `--materialize` | JsonData（DatasetMeta） |
| dataset | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| dataset | `list` | — | — | JsonData（DatasetMeta[]） |
| dataset | `meta` | `<name>` | — | JsonData（DatasetMeta） |
| dataset | `set` | `<name>` | `--display_name/--description/--tags <v>` + 任意键 | JsonData（DatasetMeta） |
| dataset | `scan` | `<name>` | `--all` `--resync` | JsonData（DatasetScanReport 或 []） |
| dataset | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| stat | `scan` | `<table\|dataset> <name>` | `--kind <kind>`（默认 coverage） | JsonData（StatScanReport） |
| stat | `get` | `<table\|dataset> <name>` | `--partition_by <p>` `--kind <kind>` | JsonData + ArrowTable（§3.6） |
| stat | `meta` | `<table\|dataset> <name>` | `--kind <kind>` | JsonData（StatMeta） |
| stat | `list` | — | — | JsonData（StatMeta[]） |
| stat | `delete`/`del` | `<table\|dataset> <name>` | `--kind <kind>` | JsonData `{"deleted"}` |

> `table scan` 为显式重扫对账（幂等）：无文件差异不 bump 版本；`--all` 批量重扫全部已注册表。
> 内容刷新也可由 `add` 与读取前快检（`_ensure_fresh`）隐式完成。

### 3.2 `table get` / `dataset get` 的 ArrowTable.meta

```json
{
  "name": "demo",
  "rows": 3,            // 本次返回行数
  "total": 3,           // 过滤后（未加 limit）总行数
  "columns": [          // 返回列的完整列元数据（来自 catalog）
    { "name": "sym", "display_name": "证券代码", "description": "...",
      "data_type": "String", "unit": null, "formula": null,
      "tags": ["key","code"], "as_index": true, "is_tool": false,
      "source_table": "index", "source_field": "sym" }
  ]
}
```

- `rows < total` 当且仅当传了 `--limit`（或 `--offset`）
- 非 catalog 列（如 hive 分区键）回退为 `{"name", "data_type"}`；dataset 列额外带 `source_table`/`source_field` 血缘

`--offset N` 跳过起始 N 行（与 `--limit` 组合实现分页）；`meta.total` 恒为过滤后（未加 limit/offset）的总行数。

### 3.3 `where` 谓词语法（`--where`）

单列范围谓词（文件级裁剪 + 过滤）：

```
sym == "a"                    等值
price >= 1.0 && price <= 3.0  等价写法：1.0 <= price <= 3.0
date >= 2024-01-01            开区间（> / >=）
```

支持类型：整数、浮点、ISO 日期 `YYYY-MM-DD`、字符串字面量。其余写法报错。

### 3.4 `--partition` 语义

- **table**：匹配 `partition_path`（hive 目录 `key=value`，精确或 `key=value...` 前缀）
- **dataset**：过滤物化分区列 `part`（前缀匹配；`identity` 镜像 index 的 hive 值，`year/month/date` 为分区值）；未分区时指定报错

### 3.5 dataset 分区策略

- 镜像 index 的 HIVE 时间分区键 → `identity`
- 否则 index 行数 ≥ 100 万且存在时间键 → `year` / `month` / `date`（按目标分区 50 万行选粒度）
- 否则 → `flat`（单文件 `data.parquet`）

### 3.6 `stat get` 返回

- **不指定 `--partition_by`**：每个分区一对消息 —— `JsonData{name="stat/<p>", data={"partition","rows","columns"}}` + `ArrowTable`
- **指定 `--partition_by <p>`**：一对 —— `JsonData{name=<target>, data={"partition","rows","columns"}}` + `ArrowTable`
- 分区名：`all`（全量）+ 每个索引列（dataset 取 keys；table 取非工具列）

### 3.7 返回数据模型字段

- **TableScanReport**：`name, version_before, version_after, layout(single/flat/hive), partition_by, partition_count, diffs[{rel_path,kind(added/removed/changed),...}], changed, implicit_registered`
- **TableMeta**：`name, version, layout, display_name, description, tags, source, extra, partition_by, partition_count, files[{rel_path,partition,size,mtime_ns}], columns[ColumnMeta], consistent, created_at, updated_at`
- **DatasetMeta**：`name, version, index_table, tables, keys, columns[ColumnMeta], partition_by, partition_gran(''/year/month/date/identity), materialized, materialized_at, curated, pending_partitions, validation, extra, display_name, description, tags, created_at, updated_at`
- **DatasetScanReport**：`name, version_before, version_after, materialized, changed, incremental, partition_by, rebuilt_partitions, triggered`
- **StatMeta / StatScanReport**：`target_type, target_name, kind, partitions[], files[{partition, rel_path, rows, size}], created_at, updated_at`
- **ColumnMeta**：`name, display_name, description, data_type, unit, formula, tags[], as_index, is_tool, source_table, source_field`

---

## 4. 后台任务（`s:...`）

### 4.1 提交

`SubmitTask(source, action, args)` 立即返回 `header + task_id`（`code=0` 成功）。任务在独立事件循环线程执行。

支持的 `source/action` 与 Execute 命令表（§3.1）对齐（version/config/mock/table/dataset/stat 全部动作），结果放在**终态事件的 `data`**（JSON 字符串）。

### 4.2 事件流（SubscribeTask）

```
header(code=0) → TaskEvent×N → EOF
```

每个 `TaskEvent`：`seq`（单调递增）、`time`、`progress`（0~1）、`message`、`data`（终态事件携带结果 JSON）、`state`。

生命周期事件序列（以 `s:dataset scan ds1` 为例）：

| state | message | 说明 |
|---|---|---|
| `pending` | 任务已创建: dataset scan | submit 时 |
| `running` | 任务开始: dataset scan | 开始执行 |
| `running` | ds1: part=2024-01-01（1/2） | 逐分区物化进度（progress=0.5） |
| `running` | ds1: part=2024-01-02（2/2） | progress=1.0 |
| `succeeded` | 任务完成 | progress=1.0，`data`=结果 JSON |

失败：`failed` 事件 `message` 为错误原因；取消：`cancelled`。

### 4.3 状态机

```
pending → running → succeeded
                  ↘ failed
                  ↘ cancelled
        running ⇄ paused（暂停中）
```

### 4.4 TaskControl（`c:<task_id> <action>`）

| action | 语义 |
|---|---|
| `cancel` | **协作式**：pending 直接终态；running 置取消标记，Handler 在检查点（`ctx.is_cancelled()`）抛 `TaskCancelled` 自行退出 |
| `pause` | 置暂停标记 + 状态 `paused`；Handler 在检查点 `wait_if_paused()` 挂起 |
| `resume` | 清暂停标记 + 状态回 `running` |

`c` 返回 `header(code=0/2) + task_id`。

### 4.5 任务元操作

- `e:task list`：按创建时间倒序，`--state` 过滤。任务项：`task_id, source, action, args, state, progress, created_at, started_at, finished_at, error, result_ref`
- **大结果落盘**：`table/dataset/stat get` 用 `ctx.put_result` 写 `tasks/<task_id>/<name>`（Arrow IPC / parquet），任务项只存 `result_ref`；`s:table get` 的 `data` 含 `{"name","rows","total","columns","result_ref"}`
- `stop`（服务停止）：先在跑任务统一收尾为 `cancelled`，DB 不遗留 orphan

### 4.6 `mock` 示例任务

`s:mock`：分 5 步推进进度（progress 0.2~1.0）+ 写日志 + 落盘结果 `{"steps":5}`；支持取消与暂停。可作为协议联调样例。

---

## 5. CLI（`stkoe`）

| 命令 | 说明 |
|---|---|
| `stkoe serve [--host H] [--port P]` | 前台运行 gRPC 服务；缺省取 stkoe.json（默认 `127.0.0.1:9569`） |
| `stkoe config show` | 查看生效配置（含 config_file） |
| `stkoe config set --<key> <value> ...` | 设置任意配置项（写入 stkoe.json） |
| `stkoe table <action> <args...>` | table 命令（走 Execute 同步分发，行为与 `e:table ...` 一致） |
| `stkoe dataset <action> <args...>` | dataset 命令 |
| `stkoe stat <action> <args...>` | stat 命令 |
| `stkoe task list [--state <state>]` | 任务列表 |

CLI 的 `table/dataset/stat` 表格结果以 ` <table <name>: N 字节 IPC> ` 形式占位打印。

---

## 6. 测试客户端（`gclient.py`）

```bash
python gclient.py [host:port]   # 缺省从配置读 grpc-host/grpc-port
```

| 输入 | 说明 |
|---|---|
| `h` | Health 探活 |
| `e:<source> <action> [args...]` | Execute（JSON/表格打印；表格附带 `meta:`） |
| `s:<source> <action> [args...]` | SubmitTask（自动订阅到终态） |
| `t:<task_id>` | SubscribeTask（replay） |
| `c:<task_id> cancel\|pause\|resume` | TaskControl |
| `q` / `exit` | 退出 |

示例：

```
e:table list
e:table get demo --where "price >= 1.0" --limit 10
s:dataset scan ds1
s:stat scan dataset ds1
t:<task_id>
```

---

## 7. 配置（stkoe.json）

- **查找优先级**：`STKOE_CONFIG` 环境变量 > `./stkoe.json` > `~/.stkoe/stkoe.json`
- **写入位置**：`STKOE_CONFIG` > `./stkoe.json`
- **已知键**：

| 键 | 默认 | 说明 |
|---|---|---|
| `grpc-host` | `127.0.0.1` | gRPC 监听地址 |
| `grpc-port` | `9569` | gRPC 监听端口 |
| `data-dir` | `~/.stkoe` | 数据目录（表/数据集/统计/catalog/任务库） |

- 任意自定义键保留在 `extra`（`config show` 透出，`config set` 原样写入）

日志：`STKOE_LOG_LEVEL` 环境变量可覆盖默认 INFO 级别。

---

## 8. 数据存储布局

```
<data-dir>/
├── stkoe.json                 # 配置（可放 cwd）
├── catalog.db                 # 资产 catalog（table/dataset 登记、文件清单、列统计、依赖）
├── tasks.db                   # 任务库（TaskStore / EventStore）
├── tasks/<task_id>/           # 任务日志 task.log + 结果文件（ResultStore）
├── tables/<name>/             # 用户 parquet（只读，绝不写/删）
├── datasets/<name>/           # dataset 物化产物（data.parquet 或 part=<v>/data.parquet）
└── stats/<type>/<name>/<kind>/<partition>.parquet   # 统计产物（不进 catalog）
```

- **catalog.db vs tasks.db 分离**：catalog 管资产，tasks.db 管任务与事件
- **表删除只删 catalog 登记，绝不删用户 parquet**（可重新 `add` 发现）；dataset 删除含物化产物
- **stat 资产不进 catalog**：文件夹存在即已扫描，`meta`/`list` 读目录

### 8.1 覆盖率统计输出列（ALL_COLS）

`group | field | data_type | count | null_count | nunique | min | q25 | q50 | q75 | max | mean | min_date | max_date`

---

## 9. 典型工作流

```bash
# 建表（发现资产）
stkoe table add index
# 建逻辑数据集（join index+m1 on keys），显式物化
stkoe dataset add ds1 index m1 --keys sym,date
stkoe dataset scan ds1
# 统计覆盖率（all + 每个索引列一个分组文件）
stkoe stat scan dataset ds1
# gRPC 读取
gclient> e:dataset get ds1 --where "date >= 2024-01-01" --limit 100
gclient> e:stat get dataset ds1 --partition_by all
# 后台物化 + 订阅进度
gclient> s:dataset scan ds1
gclient> t:<task_id>
```
