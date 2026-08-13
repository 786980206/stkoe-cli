# stkoe API 文档

> stkoe 数据服务（gRPC）对外接口全量说明：gRPC 协议、Execute 命令、后台任务、CLI、测试客户端与配置。
> **约定：任何 API 变更（新增/修改动词、参数、返回字段）时，必须同步更新本文件。**

## 1. 总览

所有业务命令统一为 `<source> <action> <args...>` 位置参数形态，等价于 `stkoe <source> <action> <args...>`。

- **source**：`version` / `config` / `table` / `dataset` / `fieldset` / `sample` / `feature` / `factor` / `test` / `stat` / `task` / `mock`
- **action**：`add` / `get` / `list` / `meta` / `set` / `col` / `scan` / `check` / `test` / `delete`（`del` 别名）/ `show`
- **单侧动词例外**：`mock`（空 action）仅 SubmitTask 可用（示例任务，见 §4.6）；`mock demo`/`mock gen` 双路径可用（见 §3.1/§4.1）；`task` 仅 Execute 可用（任务元操作，见 §4.5）
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
| mock | `demo` | — | `--n-syms N`（默认 300） `--n-days N`（默认 500，交易日数，从 2024-01-01 起） | JsonData（写入清单：`[{name, path, rows, columns}]`，写 `tables/index` + `tables/m1`，不注册） |
| mock | `gen` | `<name>` | `--kind <kind>`（默认 index；`tdcal/common/index/feature/klday/m1`） `--n-syms N` `--n-days N` `--start S` `--end E` `--seed N` `--col C` | JsonData（单表写入清单） |
| table | `add` | `<name>` | `--all`；单表可带 `--display_name/--description/--source/--tags <v>` + 任意键 | JsonData（TableScanReport） |
| table | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| table | `scan` | `<name>` | `--all` | JsonData（TableScanReport 或 []） |
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
| stat | `scan` | `<table\|dataset\|test> <name>` | `--kind <kind>`（`coverage` 默认 / `storage` / 测试器：`bucket_returns` `factor_returns` `bucket_turnover` `autocorrelation` `ic`）；`<name>` 单位置 + `--kind <测试器>` 简写 → test 目标 | JsonData（StatScanReport） |
| stat | `get` | `<table\|dataset\|test> <name>` | `--partition_by <p>` `--kind <kind>`；单位置 `<name>` 简写 → test 目标 | JsonData + ArrowTable（§3.6） |
| stat | `meta` | `<table\|dataset\|test> <name>` | `--kind <kind>`；单位置 `<name>` 简写 → test 目标 | JsonData（StatMeta） |
| stat | `list` | — | — | JsonData（StatMeta[]） |
| stat | `delete`/`del` | `<table\|dataset\|test> <name>` | `--kind <kind>`；单位置 `<name>` 简写 → test 目标 | JsonData `{"deleted"}` |
| fieldset | `add` | `<name>` | `--dataset <d>`（必选） `--engine <e>`（默认 polars） `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（FieldsetMeta） |
| fieldset | `add` | `<name> <field>` | `--formula <表达式>`（必选） `--display_name/--description/--unit/--tags <v>` | JsonData（FieldsetMeta，指标 validated=False） |
| fieldset | `set` | `<name>` | `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（FieldsetMeta） |
| fieldset | `set` | `<name> <field>` | `--formula/--display_name/--description/--unit/--tags <v>` | JsonData（FieldsetMeta；改公式 → validated 复位 False） |
| fieldset | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| fieldset | `meta` | `<name>` | — | JsonData（FieldsetMeta） |
| fieldset | `meta` | `<name> <field>` | — | JsonData（FieldMeta） |
| fieldset | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| fieldset | `delete`/`del` | `<name> <field>` | — | JsonData（FieldsetMeta） |
| fieldset | `list` | — | — | JsonData（FieldsetMeta[]） |
| fieldset | `scan` | `<name>` | `--all` `--resync` | JsonData（FieldsetScanReport 或 []） |
| fieldset | `check` | `<name> <field>` | `--all` | JsonData（FieldsetCheckResult[]） |
| fieldset | `test` | `<name>` | `--formula <表达式>`（必选） | JsonData `{"ok",...}` + ArrowTable（成功时） |
| sample | `add` | `<name>` | `--dataset <d>`（必选） `--engine <e>`（默认 polars） `--formula <表达式>`（可为空） `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（SampleMeta） |
| sample | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| sample | `meta` | `<name>` | — | JsonData（SampleMeta） |
| sample | `list` | — | — | JsonData（SampleMeta[]） |
| sample | `set` | `<name>` | `--engine <e>` `--formula <表达式>` `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（SampleMeta） |
| sample | `check` | `<name>` | — | JsonData（SampleCheckResult） |
| sample | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| feature | `add` | `<name>` | `--engine <e>`（默认 polars） `--formula <表达式>`（必填） `--display_name/--description/--unit/--tags/--source <v>` + 任意键 | JsonData（FeatureMeta） |
| feature | `set` | `<name>` | `--engine/--formula/--display_name/--description/--unit/--tags/--source <v>` + 任意键 | JsonData（FeatureMeta） |
| feature | `meta` | `<name>` | — | JsonData（FeatureMeta） |
| feature | `list` | — | — | JsonData（FeatureMeta[]） |
| feature | `delete`/`del` | `<name>` | `--force`（下游 factor 依赖存在时） | JsonData `{"deleted"}` |
| feature | `test` | `<name>` | `--sample <s>`（必选，样本池名） | JsonData（FeatureTestResult）+ ArrowTable（有结果时） |
| factor | `add` | `<name>` | `--feature <f>`（必选，已注册因子公式） `--sample <s>`（必选，已注册样本池） `--engine <e>`（默认 polars） `--pipeline <算子链>`（默认 `nothing()`，`\|` 分隔） `--factor_col <列名>`（默认 = feature 名） + 元数据键 | JsonData（FactorMeta） |
| factor | `get` | `<name>` | `--where <谓词>` `--partition <p>` `--limit N` `--offset N` | **ArrowTable**（§3.2 约定；列 = 样本索引 + 1 因子列） |
| factor | `set` | `<name>` | `--feature/--sample/--engine/--pipeline/--factor_col + 元数据键`（改定义 → 物化失效） | JsonData（FactorMeta） |
| factor | `meta` | `<name>` | — | JsonData（FactorMeta） |
| factor | `list` | — | — | JsonData（FactorMeta[]） |
| factor | `check` | `<name>` | — | JsonData（FactorCheckResult） |
| factor | `scan` | `<name>` | `--all` `--resync` | JsonData（FactorScanReport 或 []） |
| factor | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| test | `add` | `<name>` | `--factor <f>`（必选，已注册因子） `--returns <col>`（默认 `r`） `--groupby <col>`（默认 `ic`） `--marketcap <col>`（默认 `fv`） `--factor_col <col>`（默认 = factor 的 factor_col） `--by_group` `--quantiles N`（默认 5） `--periods p1,p2,..`（默认 `1,5,10`） `--date_range start,end`（默认 `2023-01-01,2026-01-01`） `--rolling_window N`（默认 252） + 元数据键 | JsonData（FactorTestMeta）；sample 缺 date/sym/returns/groupby/marketcap 列 → 报错 |
| test | `get` | `<name>` | `--where <谓词>` `--limit N` `--offset N` | **ArrowTable**（测试数据集：date/sym/sample/returns/group/marketcap/factor/d{no}/factor_quantile） |
| test | `set` | `<name>` | `--returns/--groupby/--marketcap/--factor_col/--by_group/--quantiles/--periods/--date_range/--rolling_window + 元数据键`；`--spec <p1,p2,..>`（简写，等价于 `--periods`）；改配置 → 物化失效 | JsonData（FactorTestMeta） |
| test | `meta` | `<name>` | — | JsonData（FactorTestMeta） |
| test | `list` | — | — | JsonData（FactorTestMeta[]） |
| test | `check` | `<name>` | — | JsonData（FactorTestCheckResult） |
| test | `scan` | `<name>` | `--all` `--resync` | JsonData（FactorTestScanReport 或 []） |
| test | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |

> `table scan` 为显式重扫对账（幂等）：无文件差异不 bump 版本；`--all` 批量重扫全部已注册表。
> 内容刷新也可由 `add` 与读取前快检（`_ensure_fresh`）隐式完成。
> `table add` 单表可携带初始元数据（键语义与 `table set` 一致，仅首次注册生效；`--all` 时不适用）。

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
- **`--kind storage`（存续统计）**：`stat scan table <name>` 只对表磁盘 parquet 做 stat 聚合，
  输出列 `partition_by | partition_value | storage_size | file_no`；`all` 分区为
  `__all__/__all__` 全表总量，其余分区（如 `year`）文件按表 hive 分区键逐值一行

### 3.7 返回数据模型字段

- **TableScanReport**：`name, version_before, version_after, layout(single/flat/hive), partition_by, partition_count, diffs[{rel_path,kind(added/removed/changed),...}], changed, implicit_registered`
- **TableMeta**：`name, version, layout, display_name, description, tags, source, extra, partition_by, partition_count, files[{rel_path,partition,size,mtime_ns}], columns[ColumnMeta], consistent, created_at, updated_at`
- **DatasetMeta**：`name, version, index_table, tables, keys, columns[ColumnMeta], partition_by, partition_gran(''/year/month/date/identity), materialized, materialized_at, curated, pending_partitions, validation, extra, display_name, description, tags, created_at, updated_at`
- **DatasetScanReport**：`name, version_before, version_after, materialized, changed, incremental, partition_by, rebuilt_partitions, triggered`
- **StatMeta**：`target_type, target_name, kind, partitions[], files[{partition, rel_path, rows, size}], created_at, updated_at`
- **StatScanReport**：`target_type, target_name, kind, partitions[], files[{partition, rel_path, rows, size}]`
- **ColumnMeta**：`name, display_name, description, data_type, unit, formula, tags[], as_index, is_tool, source_table, source_field`
- **FieldsetMeta**：`name, version, dataset, engine, keys[], fields[FieldMeta], partition_by, partition_gran, materialized, materialized_at, curated, columns[ColumnMeta]（源 dataset 列）, extra, display_name, description, tags[], source, created_at, updated_at`
- **FieldMeta**：`name, formula, display_name, description, unit, tags[], validated（是否已 check）`
- **FieldsetScanReport**：`name, version_before, version_after, materialized, changed, fields_count, partition_by, rebuilt_partitions[]`
- **FieldsetCheckResult**：`fieldset, field, ok, message`
- **SampleMeta**：`name, version, dataset, engine, formula, keys[]（源 dataset 主键）, columns[ColumnMeta]（dataset_with_fieldset 列 = 源列 + fieldset 衍生指标列）, display_name, description, tags[], source, extra, created_at, updated_at`
- **SampleCheckResult**：`sample, ok, rows, columns[], message`
- **FeatureMeta**：`name, version, engine, formula, display_name, description, unit, tags[], source, extra, created_at, updated_at`
- **FeatureTestResult**：`feature, sample, ok, valid, rows, columns[], message`
- **FactorMeta**：`name, version, feature, sample, pipeline, engine, factor_col, keys[]（样本索引）, partition_by, partition_gran(''/year/month/date/identity，镜像源 dataset）, materialized, materialized_at, curated, columns[ColumnMeta]（源 sample 视图列）, field（Factor 因子列 FieldMeta）, extra, display_name, description, tags[], source, created_at, updated_at`
- **FieldMeta（factor）**：`name, formula（源 feature 公式）, display_name, description, unit, tags[]`
- **FactorScanReport**：`name, version_before, version_after, materialized, changed, partition_by, rebuilt_partitions[]`
- **FactorCheckResult**：`factor, ok, rows, columns[], message`（`ok` 条件：计算成功、含全部索引列、因子列恰好 1 列、行数 > 0）
- **FactorTesterSpec**：`by_group, quantiles, periods[], date_range[]（start,end）, rolling_window`
- **FactorTestMeta**：`name, version, factor, sample, returns, groupby, marketcap, factor_col, spec[FactorTesterSpec], keys[]（date/sym）, materialized, materialized_at, curated, columns[ColumnMeta]（sample 视图列 + 测试必需列）, extra, display_name, description, tags[], source, created_at, updated_at`
- **FactorTestScanReport**：`name, version_before, version_after, materialized, changed, rows, quantiles, periods[]`
- **FactorTestCheckResult**：`test, ok, rows, columns[], message`（`ok` 条件：构造成功、含全部必需列、行数 > 0）

### 3.8 fieldset 衍生指标集（公式引擎）

- **指标集** 基于一个已注册 **dataset** 创建（`--dataset`），keys 继承源 dataset 主键；
  指标（field）用公式表达式在源数据集列上逐行计算
- **公式语言**：运行在列作用域里的 polars 表达式（如 `x*2`、`pl.col("x")*2`、`date.dt.year()`），
  用当前引擎 eval；引擎插件注册制（`register_engine`），当前仅 `polars`
- **校验**：`check` 基于源 dataset 视图求值，**结果行数 == 源行数** 才算通过 →
  指标 `validated=True`；公式编译/执行失败或行数不一致 → 校验失败（保持未校验）
- **物化**：`scan` 只落盘 `keys + 已校验指标`（跳过未校验指标），幂等（依赖签名不变则跳过）；
  物化目录 `fieldsets/<name>/`，布局镜像源 dataset（源已分区则按同分区键/粒度 `part=<v>/`，否则单文件）
- **读取**：物化完成且与源+公式一致（`curated`）读物化 parquet；否则实时基于源 dataset
  视图计算，不隐式物化（显式 `scan` 触发）
- **生命周期**：指标 add/set 后 `validated=False`；`set --formula` 会复位校验位；
  `fieldset test --formula` 即时求值返回成功/失败 + 结果数据

### 3.9 sample 样本池（基于 dataset 的过滤产物，无物化）

- 样本池 = 作用在 `dataset_with_fieldset` 上施加过滤 `--formula` 之后的**动态产物**，
  **没有物化概念**：不落盘、不 scan，`get`/`check` 每次读取时实时构造
- **`dataset_with_fieldset` 构造**（get / check 共用）：
  1. 读源 dataset 视图（物化且一致读 parquet，否则实时 join 视图）
  2. 查找 catalog 中 `dataset == 源 dataset` 的全部 fieldset，取其**已校验**指标
     （`validated=True` 且带 formula）在源视图上逐行计算，并按源 keys `left join` 出衍生列
- **过滤公式**：列作用域 polars 布尔表达式（如
  `(date>='2026-01-01')&(sym.is_in(['000001.SZ','000002.SZ']))`），经
  `sample/engine.py` 引擎插件 eval 后 `filter`；引擎当前仅 `polars`（`--engine`）
- **formula 为空** → 直接返回整个 `dataset_with_fieldset`
- **`sample check`**：过滤后结果集**包含全部索引列（源 keys）且行数 > 0** 才算有效；
  公式编译/执行失败 → 不有效（message 含原因）
- **依赖**：sample → 源 dataset（删除源 dataset 需 `--force`）；`set` 可改
  formula/engine 及元数据（版本递增），读取无需重新校验

### 3.10 feature 因子定义库（纯定义，无物化）

- **因子（feature）** = 一条命名公式（如 `ma5`、`rsi`），登记于 catalog（type='feature'），
  **没有物化概念**、不依赖具体表/dataset：`add` 只记录 `engine + formula + 元数据`
- **公式语言**：与 fieldset/样本过滤一致，用 `feature/engine.py` 引擎插件（当前仅 `polars`）
  在样本视图列作用域里 eval，逐行计算
- **`feature test <name> --sample <s>`**：在指定样本池的 `dataset_with_fieldset` 视图上
  即时求值 —— 公式执行成功且结果行数 == 样本行数 → `valid=True` 并返回结果
  ArrowTable（单列 `field`）；聚合公式或执行失败 → `valid=False` / `ok=False`
- **`add` 必须提供 `--formula <表达式>`**（空 formula 会被拒绝，见 §3.1）；`feature test` 在
  样本视图上即时求值
- **依赖**：feature 是**纯定义、不依赖任何资产**，删除源 dataset/sample 不影响 feature
- 导入顺序与取值规则与 §3.8 一致：源列名可直接当表达式用（`x*2`）

### 3.11 factor 最终因子（feature 公式 + sample 视图 + pipeline 算子链 + 物化）

- **因子（factor）** = 在 **sample**（样本池的 `dataset_with_fieldset` + filter 动态视图）上
  经 **feature**（命名公式）逐行算出因子列，再经 **pipeline**（算子链）变换后的**最终产物**；
  输出结构恒为「样本索引列 + 一列因子列」（列名 = `--factor_col`，默认取 feature 名）
- **pipeline 算子链**：`|` 分隔的算子调用（如 `nothing()|standardlize()`），每段为 `name()`；
  算子注册制（`register_operator`，当前仅 `nothing()`，原样返回），后续算子按注册即可扩展
- **物化**：`factor scan` 落盘到 `factors/<name>/`，**布局镜像源 sample 的 dataset**（源已分区则
  按同分区键/粒度 `part=<v>/data.parquet`，否则单文件 `data.parquet`）；**幂等**——依赖签名
  （sample 的 dataset data_key + feature formula + pipeline hash）不变则跳过；`--resync` 强制重建
- **读取**：物化完成且与源+feature+pipeline 一致（`curated`）读物化 parquet；否则实时基于
  sample 视图计算，不隐式物化（显式 `scan` 触发）
- **校验**：`factor check` 实时计算——成功、含全部索引列、因子列恰好 1 列、行数 > 0 才算
  `ok=True`；聚合公式（行数 != 样本行数）→ 校验失败
- **依赖**：factor → feature、factor → sample（删除上游需 `--force`）；`set` 改定义键
  （feature/sample/pipeline/factor_col）后物化失效（`materialized=False`、`curated=False`），
  读取自动回退实时计算
- **公式引擎**：与 §3.8/§3.10 一致（列作用域 polars eval，当前仅 `polars`）

### 3.12 factor_test 因子测试数据集（`test` 源 + `stat scan ... --kind <测试器>`）

- **测试数据集（test）** = 在 **factor 关联的 sample** 视图上，结合测试必需列
  （`date/sym` + returns/groupby/marketcap 列）生成的一份因子测试面板；注册于 catalog
  （type='factor_test'）。`test add` 时若 sample 视图缺少这些列 → **报错拒绝创建**
- **Schema**：`date / sym / sample(1观测/0非观测/-1因子空剔除) / returns / group /
  marketcap / factor / d{no}（sym 内前向累计收益）/ factor_quantile（截面分位，by_group
  时组内）`
- **测试列命名**：`--returns/--groupby/--marketcap`（默认 `r/ic/fv`）指定 sample 视图中的
  收益/分组/市值列名；因子列名取 factor 的 `factor_col`
- **物化**：`test scan` 落盘 `factor_tests/<name>/data.parquet`（flat 单文件）；**幂等**——
  依赖签名（factor 依赖 hash + spec + 测试列名）不变则跳过；`--resync` 强制重建
- **读取**：物化且 curated 读 parquet，否则实时构造（不隐式物化）；`set` 改配置
  （returns/groupby/marketcap/spec 键）后物化失效自动回退实时
- **校验**：`test check` 实时构造——成功、含全部必需列、行数 > 0 才算 `ok=True`
- **依赖**：test → factor（删除 factor 需 `--force`）
- **测试器（stat 集成）**：`stat scan test <name> --kind <kind>` 或
  `stat scan <name> --kind <kind>`（单位置参数简写）运行测试器并把各命名产物写入
  `stats/test/<name>/<kind>/<output>.parquet`；`stat get` 用 `--partition_by <output>` 读单产物。
  单位置简写在 Execute 与 SubmitTask 两条路径均可用（`s:stat scan <name> --kind <kind>`）
  - `coverage` → `cvg_date`（`date/SF2S/F2T/S2T/X2S` 覆盖率）
  - `ic` → `ic_d{no}`（`IC(d{no})/RankIC(d{no})/GIC(d{no})/RankGIC(d{no})`，按 `date`）
  - `autocorrelation` → `ac_d{no}`（`AC(d{no})/RankAC(d{no})`，按 `date`）
  - `bucket_returns` → `rtn_date`（`date + E(d{no})/SE(d{no})`）/ `exr_date`（`EXR(d{no})`，
    按 `date` 均值中心化）/ `gbr_date`（`GBR(d{no})`，按 `date+group` 组内中心化）
  - `bucket_turnover` → `tr_d{no}`（`TR(d{no})` 分位换手率，按 `date`）
  - `factor_returns` → `fr_d{no}`（`fw_ls/fw_raw/fw_ind/fw_ind_raw/eq_raw/eq_ind/ls/top_raw/
    bottom_raw/ls_ind/hold/mkt` + `*_cum` 累计序列，按 `date`）

---

## 4. 后台任务（`s:...`）

### 4.1 提交

`SubmitTask(source, action, args)` 立即返回 `header + task_id`（`code=0` 成功）。任务在独立事件循环线程执行。

支持的 `source/action` 与 Execute 命令表（§3.1）对齐（version/config/table/dataset/fieldset/sample/feature/factor/test/stat 全部动作；`mock demo`/`mock gen` 与 Execute 对齐、`mock`（空 action）仅任务版、`task` 仅 Execute，见 §1），结果放在**终态事件的 `data`**（JSON 字符串）。

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
- **大结果落盘**：`table/dataset/fieldset/sample/stat/factor/test get` 用 `ctx.put_result` 写 `tasks/<task_id>/<name>`（Arrow IPC / parquet），任务项只存 `result_ref`；`s:... get` 的 `data` 含 `{"name","rows","total","columns","result_ref"}`
- `stop`（服务停止）：先在跑任务统一收尾为 `cancelled`，DB 不遗留 orphan

### 4.6 `mock` 示例任务与造数

- `s:mock`（空 action）：分 5 步推进进度（progress 0.2~1.0）+ 写日志 + 落盘结果 `{"steps":5}`；支持取消与暂停。可作为协议联调样例。
- `s:mock demo` / `s:mock gen <name> --kind <kind>`：任务版 mock 造数，把 parquet 写到 `tables/`（与 Execute 行为一致，见 §3.1），不注册 catalog。

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
| `stkoe fieldset <action> <args...>` | fieldset 命令（add/get/meta/list/set/scan/delete/check/test） |
| `stkoe sample <action> <args...>` | sample 命令（add/get/meta/list/set/check/delete；无物化） |
| `stkoe feature <action> <args...>` | feature 命令（add/set/meta/list/delete/test；纯定义，无物化） |
| `stkoe factor <action> <args...>` | factor 命令（add/get/meta/list/set/check/scan/delete；可物化） |
| `stkoe test <action> <args...>` | test 命令（add/get/meta/list/set/check/scan/delete；因子测试数据集） |
| `stkoe mock demo` | 生成演示源表 index + m1（默认 300 只 × 500 日，写 `tables/`，需 `table add` 注册） |
| `stkoe mock gen <name> --kind <kind>` | 参数化生成单张表（tdcal/common/index/feature/klday/m1） |
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
├── fieldsets/<name>/          # fieldset 物化产物（keys + 已校验指标；布局镜像源 dataset）
├── factors/<name>/            # factor 物化产物（样本索引 + 1 因子列；布局镜像源 dataset）
├── factor_tests/<name>/       # 因子测试数据集物化产物（data.parquet，flat 单文件）
└── stats/<type>/<name>/<kind>/<partition>.parquet   # 统计产物（不进 catalog）
```

- **catalog.db vs tasks.db 分离**：catalog 管资产，tasks.db 管任务与事件
- **表删除只删 catalog 登记，绝不删用户 parquet**（可重新 `add` 发现）；dataset 删除含物化产物
- **stat 资产不进 catalog**：文件夹存在即已扫描，`meta`/`list` 读目录
- **sample 无物化产物**：只登记于 catalog（type='sample'），读取动态构造 dataset_with_fieldset
- **feature 纯定义**：只登记于 catalog（type='feature'），无任何磁盘产物
- **factor 物化产物**：`factors/<name>/`（仅索引列 + 因子列），布局镜像源 dataset；删除 factor 时一并清理
- **factor_test 物化产物**：`factor_tests/<name>/data.parquet`（测试数据集面板，flat 单文件）；
  测试器产物 `stats/test/<name>/<kind>/<output>.parquet`（stat 命名输出）；删除 test 时一并清理

### 8.1 覆盖率统计输出列（ALL_COLS）

`group | field | data_type | count | null_count | nunique | min | q25 | q50 | q75 | max | mean | min_date | max_date`

### 8.2 存续统计输出列（STORAGE_COLS，`--kind storage`）

`partition_by | partition_value | storage_size | file_no`

---

## 9. 典型工作流

```bash
# mock 造数（生成演示 parquet 到 tables/，替代 scripts/gen_example_data.py）
stkoe mock demo
# 建表（发现资产）
stkoe table add index
# 建逻辑数据集（join index+m1 on keys），显式物化
stkoe dataset add ds1 index m1 --keys sym,date
stkoe dataset scan ds1
# 统计覆盖率（all + 每个索引列一个分组文件）
stkoe stat scan dataset ds1
# 衍生指标集（基于 ds 计算新字段，check 通过后物化）
stkoe fieldset add fs1 --dataset ds1
stkoe fieldset add fs1 ma5 --formula "price.rolling_mean(5)"
stkoe fieldset check fs1 ma5
stkoe fieldset scan fs1
# 样本池（基于 dataset_with_fieldset 过滤，无物化）
stkoe sample add sp1 --dataset ds1 --formula "(date>='2026-01-01')&(price>0)"
stkoe sample check sp1
# 因子定义库（命名公式，test 在样本池视图上求值）
stkoe feature add ma5 --formula "price.rolling_mean(5)" --unit "元"
stkoe feature test ma5 --sample sp1
# 最终因子（在样本池视图上算因子列 + pipeline 变换，可物化）
stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
stkoe factor check fac1
stkoe factor scan fac1
# 因子测试数据集（要求 sample 含 date/sym/returns/groupby/marketcap 列）+ 测试器
stkoe test add t1 --factor fac1 --returns r --groupby ic --marketcap fv
stkoe test check t1
stkoe test scan t1
stkoe stat scan t1 --kind ic
gclient> e:stat get t1 --kind ic --partition_by ic_d1
# gRPC 读取
gclient> e:dataset get ds1 --where "date >= 2024-01-01" --limit 100
gclient> e:fieldset get fs1 --columns k,ma5 --limit 100
gclient> e:sample get sp1 --limit 100
gclient> e:feature test ma5 --sample sp1
gclient> e:factor get fac1 --limit 100
gclient> e:test get t1 --limit 100
gclient> e:stat get dataset ds1 --partition_by all
# 后台物化 + 订阅进度
gclient> s:fieldset scan fs1
gclient> t:<task_id>
```
