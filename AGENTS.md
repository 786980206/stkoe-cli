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
│   ├── controller.py  # async add/get/delete/list/meta/set/col/scan/data_key（阻塞 IO 走 asyncio.to_thread）
│   └── handlers.py    # 任务版 Handler（source="table"，注册进 TaskRegistry）
├── dataset/           # 逻辑数据集（DatasetController，async 接口）
│   ├── spec.py        # DatasetMeta/DatasetScanReport dataclass
│   ├── controller.py  # async add/get/meta/list/set/scan/delete（add 只注册，物化走 scan）
│   └── handlers.py    # 任务版 Handler（source="dataset"，注册进 TaskRegistry）
├── fieldset/          # 衍生指标集（FieldsetController，async 接口）
│   ├── spec.py        # FieldMeta/FieldsetMeta/FieldsetScanReport/FieldsetCheckResult dataclass
│   ├── engine.py      # 公式引擎插件（CalcEngine + register/get；仅 polars）
│   ├── controller.py  # async add/get/meta/list/set/scan/delete/check/test + 指标级操作
│   └── handlers.py    # 任务版 Handler（source="fieldset"，注册进 TaskRegistry）
├── sample/            # 样本池（SampleController，async 接口；无物化）
│   ├── spec.py        # SampleMeta/SampleCheckResult dataclass
│   ├── engine.py      # 过滤引擎插件（SampleEngine + register/get；仅 polars）
│   ├── controller.py  # async add/get/meta/list/set/check/delete（读时动态构造 dataset_with_fieldset）
│   └── handlers.py    # 任务版 Handler（source="sample"，注册进 TaskRegistry）
├── feature/           # 因子定义库（FeatureController，async 接口；纯定义，无物化）
│   ├── spec.py        # FeatureMeta/FeatureTestResult dataclass
│   ├── engine.py      # 公式引擎插件（复用 CalcEngine 注册表；仅 polars）
│   ├── controller.py  # async add/set/meta/list/test/delete（test 在 sample 视图上求值）
│   └── handlers.py    # 任务版 Handler（source="feature"，注册进 TaskRegistry）
├── factor/            # 最终因子（FactorController，async 接口）
│   ├── spec.py        # FactorMeta/FactorScanReport/FactorCheckResult/FieldMeta dataclass
│   ├── engine.py      # 算子注册表（FactorOperator/NothingOperator）+ pipeline 解析 + 公式引擎
│   ├── controller.py  # async add/get/meta/list/set/check/scan/delete（sample 视图算因子列→算子链→物化）
│   └── handlers.py    # 任务版 Handler（source="factor"，注册进 TaskRegistry）
├── factor_test/       # 因子测试数据集（FactorTestController，async 接口）
│   ├── spec.py        # FactorTesterSpec/FactorTestMeta/FactorTestScanReport/FactorTestCheckResult
│   ├── tester.py      # 测试数据集准备 + 六类测试器（bucket_returns/factor_returns/
│   │                  #   bucket_turnover/autocorrelation/ic/coverage，纯 polars）
│   ├── controller.py  # async add/get/meta/list/set/check/scan/delete + tester 产物写入
│   └── handlers.py    # 任务版 Handler（source="test"，注册进 TaskRegistry）
├── stat/              # 数据统计资产（StatController，async 接口）
│   ├── spec.py        # StatFile/StatMeta/StatScanReport dataclass
│   ├── calc.py        # calc_stats：按 dtype 分桶算覆盖率统计（ALL_COLS 输出）
│   ├── controller.py  # async scan/get/meta/list/delete（cov 写入 stats/ 目录，不进 catalog）
│   └── handlers.py    # 任务版 Handler（source="stat"，注册进 TaskRegistry）
├── mock/              # 演示数据生成（stkoe mock demo/gen，替代 scripts/gen_example_data.py）
│   ├── gen.py         # 生成器（tdcal/common/index/feature/klday/m1 + demo）+ write（只写盘不注册）
│   └── handlers.py    # 任务版 Handler（source="mock"，注册进 TaskRegistry）
├── graph/             # V3.0 资产图（graphqlite 嵌入式图数据库 + GraphService，见 graph-design.md）
│   ├── model.py       # DataChangeEvent / ColumnMeta / FieldMeta / AssetMeta / DependencyEdge
│   ├── store.py       # GraphStore：节点/边 CRUD + 血缘遍历（BFS，带环保护）+ 物理指纹普通表
│   ├── events.py      # 事件合并（symbol/datetime 并集、field 交集）与积累（水位线）
│   ├── controller.py  # GraphController：资产 CRUD + 依赖约束 + notify_change/resolve(_all)
│   ├── service.py     # GraphService：table/index/panel/fieldset/sample/feature/factor/test
│   │                  #   统一服务（登记/依赖/版本走 graph；实时视图 + 物化落盘）
│   ├── handlers.py    # 各资产 Handler（v3.0-def.py 形态：table/index/panel/fieldset/…/graph）
│   └── errors.py      # AssetNotFound/Exists、DependencyError、CycleError 等
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
- Execute 的 `e:table get` 返回单条 `ArrowTable(IPC)`，元数据（rows/total/columns 列说明）并入 `ArrowTable.meta`
- `Result.kind`: json → `JsonData`，table → `ArrowTable`；`Result.json/Result.table` 工厂方法

### API 文档同步

仓库根 `api.md` 是对外 API 全量文档（gRPC 协议 / Execute / SubmitTask / CLI / 配置 / 存储布局）。
**新增/修改任何对外接口（动词、参数、返回字段、配置键）时，必须同步更新 `api.md`。**

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
- 全量 220 用例，多连跑需稳定（曾修过时序竞态，新增用例注意时序敏感）

## 当前状态与下一步

**当前**：V3.0 图重构完成——table/index/panel（原 dataset）/fieldset/sample/feature/factor/test
全部基于 graph 实现（`graph/service.py` 的 GraphService），Execute 与 SubmitTask 三路径统一；
旧 catalog.db 废弃（登记/依赖/版本进 graph 节点/边，物理指纹表迁入 catalog.db 普通表；
tasks.db 独立保留）；`graph lineage/nodes/stats` 已接入 gRPC Execute 通道，
portal 前端"血缘关系"抽屉/完整页已联调（见 README.md / graph-design.md §6-7）。

**下一步**（详见 README「路线图」）：
1. panel 物化（scan 落盘）、index 唯一性校验等物理细节
2. 任务版 table/dataset handler 残余清理（V2.0 controller 死代码评估）
3. 列级血缘（列节点图）、version_list 裁剪、图算法（PageRank 等）

## 近期变更记录

### 2026-08 panel add 移除 keys 参数：keys 由 index 推断（symbol_col + datetime_col）

- `panel add` / `dataset add` 不再接受 `--keys`：panel 的索引键 = index 节点的
  `symbol_col + datetime_col`（去空去重，兜底 sym/date），由 `service.panel_add` 推断后
  写入节点（PanelHandler 保留 keys 属性存储）；旧 `--keys` 参数被忽略
- dispatch/任务版去掉 keys 解析；测试表统一为 sym/date 结构（k 单键表改为
  symbol+datetime 双键），断言同步；全量 286 用例绿

### 2026-08 V3 语义修正：scan → update（上游传导就绪检查 + 物化/标记有效）

- **scan 语义模糊 → 改称 update**（V3 handler 定义的 `update`/`materialize` 形态）：
  服务层各资产新增 `xxx_update`；`xxx_scan` 保留为旧名别名（同实现）
- **update 的传导就绪检查**：`GraphController.assert_ready` —— BFS 递归检查该节点**全部
  上游链**（不只直接依赖）必须 valid；任一上游未就绪 → `DependencyError`（指出先 update 谁），
  只有上游完全就绪才执行更新/物化——为后续 graph 任务 pipeline（统一构建依赖任务列表）打基础
- **资产物化语义**：源头（table/index）天然 valid，update=重扫对账；物化资产
  （fieldset/factor/test）update=校验+落盘并置 valid；无物化资产（panel/sample/feature）
  update=传导检查就绪后置 valid；上游变化 → 全链置脏（valid=False）→ 只能经 update 依次
  恢复有效
- **修 review §1 数据过期 bug**：factor/test 物化幂等仅当节点 valid 时生效；上游置脏
  （valid=False）后 update 强制重建（hash 依赖版本不变，靠 valid 标志驱动重建）
- 测试：test_graph_service.py 补传导拦截用例；各模块 `_gsetup` 建链后依次 update 就绪；
  api.md/example.md 命令主推 update（scan 标注旧名别名）；全量 286 用例绿

### 2026-08 V3.0 全面切 graph：table/index/panel/fieldset/sample/feature/factor/test 三路径统一走 GraphService + catalog.db 废弃

- **GraphService 新增 factor/test 方法**：factor 实时计算（sample 视图求 feature 公式 →
  拼索引+因子列 → pipeline 算子链）与物化（`factors/<name>/data.parquet`，flat 单文件，
  幂等签名 = 上游 feature/sample 的 graph 版本 + engine/pipeline/factor_col hash）；
  test 数据构造（sample 视图 + 测试必需列 → `prepare_factor_data`）与物化
  （`factor_tests/<name>/data.parquet`）；`test_data()` 供 stat 测试器复用
- **dispatch factor/test 处理器切 GraphService**（Execute）；stat 的 test 目标改走 graph
  （`StatController._scan_test_sync` 用 GraphService.test_data + factor_test/tester.py，
  删除对 V2.0 FactorTestController 的依赖）
- **任务版 handler 全面切 GraphService**：fieldset/sample/feature/factor/factor_test 的
  TaskHandler 改为同步调用 + `asyncio.to_thread`（GraphStore 连接
  `check_same_thread=False` 支持任务线程顺序使用）
- **fieldset check 写回 validated**：通过后写回节点 `validated=True`
  （视图/物化只取已校验字段，对齐 V2.0 语义）
- **sample 依赖 fieldset**：血缘链 table/index → panel → fieldset → sample → factor；
  `sample add --fieldset`；sample_check keys 经 fieldset → panel 解析
- **CLI 补 `index`/`panel` 子命令**（dataset 为旧别名转发）
- 测试：test_grpc.py 的 sample/fieldset/feature/factor/test Execute 用例改 graph 造数
  （新增 `_seed_panel_chain`/`_seed_factor_chain`），test_fieldset/sample/feature/factor/
  factor_test 任务版用例改 graph 造数（`_gsetup`），test_graph_service.py 补 factor/test
  用例，全量 298 用例绿

### 2026-08 graph 血缘经 gRPC Execute 通道输出（portal 血缘模块后端）

- **`graph` source 注册进 Execute 分发**（dispatch.py，仅 Execute，JSON 返回）：
  - `e:graph lineage [--node <type:name>] [--depth N]` → Cytoscape elements payload
    （缺 node 全图；库不存在返回空图）
  - `e:graph nodes [--type <t>]` → 节点摘要列表（中心节点选择器用）
  - `e:graph stats` → 节点/边统计
  - 数据源 `<data-dir>/catalog.db`（graphqlite），按 data_dir 透传打开
- **抽 `graph/export.py` 纯函数**：`build_payload` / `node_summaries`（供 dispatch、
  tools/graph-viewer/export.py 复用；tools 版已改为薄 CLI 包装，去掉重复实现）
- **portal 集成**：前端右上角"血缘关系"按钮 → 右侧抽屉（Cytoscape.js 渲染）；
  Tauri 端 Rust `fetch_graph_lineage/nodes/stats` 命令经现有 gRPC Execute 拉取
  JSON（不新起 HTTP 服务，走 stkoe-cli 既有通道）
- **文档**：api.md §1/§3.1/§3.13 补 graph 命令与 payload 结构
- 测试：test_graph.py 增 dispatch 直测 + gRPC Execute 端到端 8 例，全量 271 用例绿

### 2026-08 tools/graph-viewer：Cytoscape.js 血缘可视化工具

- **新增 `tools/graph-viewer/`**（详见其 README.md）：
  - `export.py`：把 graphqlite 图数据库导出为 Cytoscape elements JSON ——
    `python tools/graph-viewer/export.py <catalog.db> [--node <type:name>] [--depth N]
    [--output] [--pretty] [--no-meta]`；全图或中心节点上下游子图均可
  - `index.html`：Cytoscape.js 交互式探索页（前端库本地化于 vendor/，离线可用）——
    类型着色（源头 table/index 为八角形）、图例可隐藏类型、单击看详情（meta/版本/
    有效态/事件数）、选中节点高亮**上游（琥珀）/下游（青）**、聚焦子图、搜索定位、
    dagre/cose/breadthfirst/concentric/grid 多布局；数据加载支持 URL `?data=`、
    拖拽 JSON、粘贴 JSON（file:// 直接打开会提示用静态服务）
  - `vendor/`：cytoscape 3.34.1 / dagre 0.8.5 / cytoscape-dagre 4.0.0（npmmirror 下载，
    MIT，见 VENDOR.md）
- **graph 初始版本补为时间戳**：`GraphController.add` 的初始 version 原为写死 1，
  改为 `new_version()`（与 _bump 一致）
- **数据约定**：节点 `id` = `<type>:<name>`；边方向 = 依赖方向，`role` 表示角色，
  `join` 仅 table → panel 边带
- 验证：export.py 全图/子图/缺库路径 + JSON 结构校验（无悬空边、join 位置正确）+
  node 无头渲染冒烟（8 节点/7 边，上游下游查询正确）；全量 263 用例绿

### 2026-08 graph 设计修正（评审反馈）：时间戳版本 + 血缘方向

- **版本号改为高精度时间戳**：`graph/version.py` 新增 `new_version()` ——
  取 `time.time_ns()` 纳秒时间戳（int，有业务含义、可直接看出变更时间），
  同一纳秒/时钟回拨时以上次版本 +1 兜底保证严格单调；`_bump`/`AssetMeta`
  相应改为时间戳版本（不再 int 递增）
- **血缘方向修正**：
  - **join 只出现在 table → panel 边上**：panel 以 index 为索引 join 成员表，
    `role=member` 边带 `detail.join`（left_join/asof_join），`role=index` 边不带 join
  - **sample 基于 fieldset 衍生**：血缘链改为
    `table/index → panel → fieldset → sample → factor`（原实现 sample 直接依赖
    panel）；`SampleHandler.add` 参数改为 `fieldset`、DEPENDS 边 role=fieldset、
    `DEFINITION_KEYS["sample"]` 同步改为 `{"fieldset","engine","formula"}`
- **文档同步**：graph-design.md §1.1/§1.2/§2.1/§3.1/§4.1 修正（版本、血缘子图、
  join 位置、sample 依赖）；AGENTS.md 目录结构 version.py
- **测试**：test_graph.py 版本断言全部改为时间戳单调比较、fixture 链
  `sp1` 依赖 `fs1`、删除顺序/传播链断言按新链调整 + 链式积累用例加强，
  全量 263 用例绿

### 2026-08 V3.0 图数据库血缘重构（graphqlite 落地：图 CRUD + 事件响应全流程）

- **V2.0 全量备份**：现有代码/测试/文档（src、tests、scripts、api.md、example.md、
  README、gclient.py、pyproject 等）拷贝至仓库根 `V2.0/`，作为重构基线
- **graphqlite 选型落地**：嵌入式图数据库（SQLite 扩展，Cypher 查询 + 图算法），
  PyPI `graphqlite>=0.6.0` 已入 pyproject 依赖；Windows + CPython 3.13 实测可用。
  关键实测结论：① 变长路径遍历 `-[:DEPENDS*1..N]->` 可用（血缘上下游）；② Cypher
  内不支持事务语句，但用原生 SQL `BEGIN/COMMIT/ROLLBACK` 包裹多语句 cypher 写入
  可整体回滚（控制器 `txn()` 依赖此保证「建节点+建边」原子性）；③ graphqlite
  `connection.cypher` 参数 JSON 用 ensure_ascii=True，**非 ASCII 参数会被损坏**
  （`改名` → `u6539u540d`），`GraphStore._cypher` 自实现 ensure_ascii=False 规避
- **新增 `graph` 模块**（详见仓库根 `graph-design.md`）：
  - `model.py`：DataChangeEvent / ColumnMeta / FieldMeta / AssetMeta / DependencyEdge，
    节点 label = 资产类型（table/index/panel/fieldset/sample/feature/factor/tester/
    model/stat），id = `"<type>:<name>"`；`version` + `version_list`（version →
    DataChangeEvent）记录版本事件日志
  - `store.py`：GraphStore（节点/边 CRUD、`deps_of`/`dependents` 出/入边、BFS 血缘
    遍历带环保护、txn 事务、`_cypher` 中文安全）
  - `events.py`：事件合并（symbol/datetime scope 并集、field scope 交集；None=全集）
    与积累（`required_version` 水位线之后的事件）
  - `controller.py`：GraphController 资产 CRUD + 依赖约束（**无下游才可删除**，
    force 绕过）+ `notify_change`（铸版本 + BFS 下游置脏）+ `resolve`/`resolve_all`
    （拓扑重算：积累事件 → storage 钩子 → 版本递增 + 出边水位对齐）；成环抛
    `CycleError`；物理数据存储暂未接入（`NullStorage` no-op 钩子，后续替换）
  - `handlers.py`：v3.0-def.py 形态的 10 类资产 Handler + GraphHandler（list/get/
    upstream/downstream/stale/scan）
- **pyproject**：加 `graphqlite>=0.6.0` 依赖 + `[tool.pytest.ini_options]`
  `testpaths=["tests"]`（防止收集 V2.0/tests）+ `norecursedirs` + `-p no:cacheprovider`
  （本机 .pytest_cache ACL 异常，绕开缓存写入）
- **测试**：新增 `tests/test_graph.py` 40 例（事件合并/存储层/CRUD/依赖约束/血缘
  传播/拓扑重算/handler 全链路/持久化），全量 263 用例绿
- 注：uv.lock 未含 graphqlite（沙箱内 uv 不可用），本机 `.venv` 已手动装入 wheel；
  真机 `uv sync` 会补锁

### 2026-08 table `--type` 表类型 + dataset index_table 约束

- **`table add/set --type <v>` 表类型**：新增标准元数据字段 `type`（默认空字符串），
  可设为 `index` 或任意自定义值；仅作分类标识，除 dataset 约束外不影响其他流程
- **dataset index_table 约束**：`dataset add` 的 index 表必须为 `--type index` 的 table
  （`scan_spec` 校验，非 index 类型报 `must be type 'index'`）；成员表无类型要求；
  Execute / 任务版 / CLI 三处经 `_add_sync` 自动对齐
- 旧库兼容：`_to_meta` 对未设 type 的表回退空字符串；`_sync_source_meta` 对旧 dataset
  （index 非 index 类型）仅跳过列同步，不影响物化
- 文档同步：api.md §3.1（table add/set 补 `--type`、dataset add 补约束）、§3.7 TableMeta
  补 `type`；example.md §1 `table add index --type index`；测试补 table type / dataset
  约束 / Execute 全链路 3 例，全量 222 用例绿

### 2026-08 版本 0.6.0（tag v0.6.0）：mock 接口 + 测试提速 + 依赖整理

- **mock 接口 / demo 默认 300×500 / SQLite 提速**：见下两条记录
- **numpy 升为直接依赖**：polars 已不再依赖 numpy，`mock/gen.py` 的 `import numpy`
  在全新 `uv sync` 环境会缺失，故在 pyproject.toml 声明 `numpy>=2.5.2`
- **镜像源进项目**：pyproject.toml 加 `[[tool.uv.index]]`（tsinghua 默认源），
  不再依赖 `~/.config/uv/uv.toml`，uv.lock 统一镜像 URL
- **`.gitignore` 补 `example-data/` + `stkoe.example.json`**（example.md 演练产物）

### 2026-08 测试提速：SQLite catalog 减少 fsync（全量 220 用例 222s → 105s）

慢文件系统（如本 Linux 环境）上，SQLite 每次 `commit`/`close` 的 fsync 高达 ~40-100ms，
而 Windows NVMe 仅 ~1ms —— 这是全量测试比 Windows 慢数倍的主因（user 时间仅 ~7s，
几乎全是 I/O 等待）。三处修复均在 `table/catalog.py` + `task/store.py`：

- **`synchronous=NORMAL`**（WAL 模式下）取代默认 FULL：commit 不再逐条 fsync，
  由 checkpoint 批量落盘；WAL 语义下该设置安全（进程崩溃最多丢 checkpoint 前已提交
  的少量数据，库不损坏）
- **DDL 每库只跑一次**：`new_conn()` 原本每次都 `executescript(_SCHEMA)`（8 条建表/建索引），
  改为 `Catalog._schema_done` 按库路径缓存，后续连接只 connect + pragma
- **anchor 常开连接**：close() 成为该库最后一个连接时会触发 WAL checkpoint（~100ms/次），
  在 Catalog 实例上保持一条不读写的 anchor 连接，令短连接 close 不再触发 checkpoint

- 测试：全量 220 用例绿，`test_dataset.py` 23.5s→11s，整包 222s→105s

### 2026-08 stat coverage 内存优化（calc_stats 流式化）

- **问题**：`stat scan dataset coverage` 对宽表内存爆炸（unpivot 原始数据 = 行数×列数，
  10M×24 列峰值 ~5GB，卡爆整机）
- **`calc.py` 重写 calc_stats**：改为**按 dtype 类别聚合再 unpivot**——数值/字符串/时间
  三类各做一次流式 `group_by`（每列每指标一个聚合列，输出窄表行数=组数），再对窄表
  按指标逐列 unpivot 并以 `(g, count, field)` join 拼成 ALL_COLS 长表；全程不对原始
  数据 unpivot。数值列聚合前先 cast 到 unpivot 超类型（`base.select(cols).unpivot()
  .collect_schema()` 取 dtype，仅解析 schema 不读数据），与旧实现的 min/max 字符串
  形态、mean 精度逐值一致（parity 脚本全等）
- **`controller.py`**：`_scan_sync` 直接对 calc_stats 返回的 LazyFrame
  `sink_parquet`（流式落盘，不再 collect + write_parquet），行数改为写后
  `_parquet_rows` 回读；storage 分支同改为 `lazy().sink_parquet`
- **效果**：5M 行 × 28 列全量 + 2 个索引分组 16s 完成、峰值 +371MB；行为/输出
  与旧版完全一致（test_stat.py 13 例 + 全量 220 例绿）
- 注意：`sink_parquet` 对低基数 join 键（如仅 300 个取值）会估算巨量输出并挂死，
  属测试数据病态场景；真实 dataset 视图 keys（date+sym）高基数无碍

### 2026-08 mock 改造为 stkoe mock 接口（替代 scripts/gen_example_data.py）

- **新增 `mock` 模块**：把 `scripts/gen_example_data.py` 的造数能力内建为 `stkoe mock`
  接口，参考 v1.0 `data/mock.py` 生成器设计（tdcal/common/index/feature/klday/m1 + write）
- **`stkoe mock demo`**：生成 example.md 演示源表 index + m1（**默认 300 只 × 500 个交易日
  = 15 万行**，`--n-syms/--n-days` 可调）到 `<data_dir>/tables/`；**只写盘不注册**，仍由
  `table add` 发现登记（保持「发现资产」语义）
- **`stkoe mock gen <name> --kind <kind>`**：参数化生成单张表（kind：
  tdcal/common/index/feature/klday/m1，`--n-syms/--start/--end/--seed/--col` 可选）
- **日期列用字符串形态**（如 `"2024-01-01"`）：sample/feature 公式过滤形如
  `(date >= '2024-01-02')`，与 tests/test_sample.py 约定一致
- **多路径注册**：Execute（dispatch.py `@handler("mock", demo/gen)`）/ SubmitTask
  （mock/handlers.py）/ CLI（cli.py `_cmd_mock`）三处对齐；`s:mock`（空 action）仍是
  task/handlers.py 的 MockProgressHandler 示例任务（框架联调用）
- **删除 `scripts/gen_example_data.py`**；example.md §0.2 改用 `stkoe mock demo`、
  §1 `table get --where` 日期加引号、§10 补任务版 `s:mock demo`/`gen`；api.md
  §1/§3.1/§4.1/§4.6/§5/§9 同步
- 测试：新增 `tests/test_mock.py`（demo/gen 全链路 + 任务版 + Execute）+ `--n-syms/--n-days`
  生效用例补进 test_grpc.py，全量 220 用例绿（`test_task_list_ordered_and_filtered`
  等时序敏感用例偶发 flaky，与 mock 改动无关）

### 2026-08 整体审视修复 + 版本 0.5.0（tag v0.5.0）

- **stat 任务版单位置 test 简写对齐**：`stat/handlers.py` 的 `_target` 支持单位置参数
  → test 目标（scan 需 `--kind <测试器>`，get/meta/delete 无条件），与 Execute 对齐
- **`test set --spec` 任务版修复**：`factor_test/handlers.py` 把 `--spec <p1,p2,..>` 逗号串
  转 `{"periods": [...]}` 再透传（此前任务版崩溃）
- **`factor set --engine` 改为定义键**：`factor/controller.py` `_set_sync` 把 `engine` 纳入
  定义键（校验 `get_engine` + 物化失效），与 api.md 文档一致
- **文档同步**：api.md §1 source/action 列表补 `test`/`check`/`test` + 单侧动词例外
  （mock 仅任务版、task 仅 Execute）；§3.1 table scan 移除无效 `--resync`、feature add
  formula 必填、feature delete 下游仅 factor、test add 补 `--factor_col`、test set 补
  `--spec`、stat 行补单位置简写；§3.7 补 StatScanReport 与 factor_test 四个数据模型；
  §3.10 formula 必填；§3.12 补测试器输出列与任务版简写说明；§4.1 对齐声明修正；§8 补
  `factor_tests/` 存储目录；AGENTS.md feature 模块 formula 必填、factor set 定义键补 engine；
  dispatch.py/proto 注释 source 列表更新
- 测试：新增 factor set engine 定义键、任务版 test set --spec、任务版 stat 单位置简写
  3 用例，全量 198 用例绿

### 2026-08 factor_test 因子测试数据集模块（test add/scan + stat 测试器集成）

- **新增 `factor_test` 模块**：因子测试数据集（test）= 在 **factor 关联的 sample** 视图上，
  结合测试必需列（`date/sym/returns/groupby/marketcap`）生成的面板；注册于 catalog
  （type='factor_test'）。**`test add` 要求 sample 视图含这些列，缺失报错拒绝创建**
- **测试数据集 Schema**：`date/sym/sample/returns/group/marketcap/factor/d{no}/
  factor_quantile`（`d{no}`=sym 内前向累计收益，`factor_quantile`=date(+group) 截面分位）
- **测试列命名**：`--returns/--groupby/--marketcap`（默认 `r/ic/fv`）；因子列取 factor 的
  `factor_col`；`JobSpec` 类 `FactorTesterSpec`（by_group/quantiles/periods/date_range/
  rolling_window）存 meta，`set` 可改（物化失效）
- **物化** `test scan`：落盘 `factor_tests/<name>/data.parquet`（flat 单文件）；**幂等**——
  依赖签名（factor 依赖 hash + spec + 测试列名）不变则跳过；`--resync` 强制重建
- **读取**：物化且 curated 读 parquet，否则实时构造，不隐式物化；`test check` 校验构造
  成功 + 含必需列 + 行数 > 0
- **依赖登记**：test → factor（stkoe_depends），删除 factor 需 `--force`
- **测试器（stat 集成）**：`stat scan test <name> --kind <kind>`（也支持单位置参数简写
  `stat scan <name> --kind <kind>`），六类测试器（bucket_returns/factor_returns/
  bucket_turnover/autocorrelation/ic/coverage）产物写 `stats/test/<name>/<kind>/<output>.parquet`；
  `stat get/meta/delete` 对 test 目标复用 stat 文件逻辑（单位置参数 → test）
- **多路径注册**：Execute（dispatch.py）/ SubmitTask（handlers.py）/ CLI（cli.py）三处对齐；
  `api.md` §3.1/§3.12/§4.1/§4.5/§5/§8/§9 同步
- 测试：`tests/test_factor_test.py` 全链路（CRUD/必需列拒绝/实时构造/scan 幂等与 resync/
  curated 失效/依赖阻断/stat testers/任务版 32 例）+ `tests/test_grpc.py` Execute 链路，
  全量 195 用例绿

### 2026-08 factor 最终因子模块（feature 公式 + sample 视图 + pipeline 算子链 + 物化）

- **新增 `factor` 模块**：最终因子（factor）= 在 **sample** 视图上经 **feature** 公式逐行
  算出因子列，再经 **pipeline** 算子链变换的产物；输出结构恒为「样本索引列 + 一列因子列」
  （列名默认取 feature 名，`--factor_col` 可改）；注册于 catalog（type='factor'）
- **算子注册表**（engine.py）：`FactorOperator` 接口 + `register_operator`/`get_operator`，
  当前仅 `nothing()`（恒等）；pipeline 语法 `|` 分隔的 `name()`（如
  `nothing()|standardlize()`），`parse_pipeline` 逐段解析，后续算子注册即可扩展
- **物化** `factor scan`：落盘 `factors/<name>/`，**布局镜像源 dataset**（源已分区则按同
  分区键/粒度 `part=<v>/data.parquet`，否则单文件）；**幂等**——依赖签名（sample 的 dataset
  data_key + feature formula + pipeline）不变则跳过；`--resync` 强制重建
- **读取**：物化且 curated 读物化 parquet（含 hive 分区列 `part`），否则实时基于 sample
  视图计算，**不隐式物化**；`factor check` 校验计算成功 + 含全部索引列 + 因子列恰好 1 列 + 行数 > 0
- **依赖登记**：factor → feature、factor → sample（stkoe_depends），删除上游需 `--force`；
  `set` 改定义键（feature/sample/engine/pipeline/factor_col）后物化失效，读取自动回退实时
- **feature 删除增加依赖阻断**：FeatureController.delete 检查 `dependents`（factor 依赖存在时
  需 `--force`，参照 sample/dataset），Execute/任务版/CLI 三处对齐
- **多路径注册**：Execute（dispatch.py）/ SubmitTask（handlers.py）/ CLI（cli.py）三处对齐；
  `api.md` §3.1/§3.7/§3.11/§4.1/§4.5/§5/§8/§9 同步
- 测试：`tests/test_factor.py` 全链路（CRUD/实时计算/样本过滤/pipeline/check/scan 幂等与
  resync/curated 失效/依赖阻断/任务版 24 例）+ `tests/test_grpc.py` Execute 链路，全量 162 用例绿

### 2026-08 feature 因子定义库模块（命名公式，纯定义无物化）

- **新增 `feature` 模块**：因子（feature）= 一条**命名公式**，注册于 catalog
  （type='feature'）；**纯定义、无物化**、不依赖具体表/dataset/sample；
  `add` 必须提供 `engine + formula`（formula 为空直接报错），再叠加元数据
- **公式引擎插件制**（engine.py）：`FeatureEngine` 接口 + `register_engine`/`get_engine`
  注册表，当前仅 `polars`（列作用域 eval，与 fieldset/样本过滤一致）
- **`feature test <name> --sample <s>`**：在指定样本池的 `dataset_with_fieldset` 视图上
  即时求值 —— 公式执行成功且结果行数 == 样本行数 → `valid=True` 并返回结果
  ArrowTable（单列 `field`）；聚合公式 → `valid=False`（message 说明需逐行）；
  公式报错 → `ok=False` 无结果；样本未注册 → 报错
- **`set` 只改定义与元数据**（engine/formula/display_name/description/unit/tags/source
  + 任意键进 extra，版本递增）；未注册报 `FeatureNotFoundError`
- **读时构造 `dataset_with_fieldset`**：复用 sample 的实时视图构造（well-behaved）
- **多路径注册**：Execute（dispatch.py）/ SubmitTask（handlers.py）/ CLI（cli.py）
  三处对齐；`api.md` §3.1/§3.7/§3.10/§4.1/§5/§8/§9 同步
- 测试：`tests/test_feature.py` 全链路（CRUD/元信息/test 求值·过滤·校验/聚合无效/
  执行失败/任务版）+ `tests/test_grpc.py` Execute 链路，全量 137 用例绿

### 2026-08 sample 样本池模块（基于 dataset_with_fieldset 的过滤产物，无物化）

- **新增 `sample` 模块**：基于已注册 **dataset** 创建样本池，注册于 catalog
  （type='sample'）；样本池**没有物化概念**，`get`/`check` 每次读取时实时构造
- **`dataset_with_fieldset` 构造**（get/check 共用）：先读源 dataset 视图
  （物化且 curated 读 parquet，否则实时 join 视图），再查 catalog 中
  `dataset == 源 dataset` 的全部 fieldset，取其**已校验**指标在源视图逐行计算并按
  keys `left join` 出衍生列
- **过滤引擎插件制**（engine.py）：`SampleEngine` 接口 + `register_engine`/`get_engine`
  注册表，当前仅 `polars`（列作用域布尔表达式 eval 后 `filter`）；formula 为空 →
  返回整个 `dataset_with_fieldset`
- **`sample check`**：过滤后结果集**含全部源索引列且行数 > 0** 才算有效；
  公式执行失败 → 不有效（message 含原因）
- **依赖登记**：sample → dataset（stkoe_depends），删除源 dataset 需 `--force`；
  `set` 可改 formula/engine 及元数据（版本递增）
- **多路径注册**：Execute（dispatch.py）/ SubmitTask（handlers.py）/ CLI（cli.py）
  三处对齐；`api.md` §3.1/§3.9/§3.7/§5/§8 同步
- 测试：`tests/test_sample.py` 全链路（CRUD/无公式全量/过滤/fieldset 衍生列/check
  有效性/依赖阻断/任务版），全量 122 用例绿

### 2026-08 fieldset 衍生指标集模块（公式引擎 + 指标生命周期 + 物化）

- **新增 `fieldset` 模块**：基于已注册 **dataset** 创建衍生指标集，注册于 catalog
  （type='fieldset'）；**指标（field）** 用 polars 表达式公式在源 dataset 列上逐行
  计算，add/set 后 `validated=False`，`check`（结果行数==源行数）通过才参与物化
- **公式引擎插件制**（engine.py）：`CalcEngine` 接口 + `register_engine`/`get_engine` 注册表，
  当前仅 `polars`（列作用域 eval）；`fieldset test --formula` 即时求值
- **物化** `fieldset scan`：落盘 `fieldsets/<name>/`，keys + 已校验指标，布局镜像源
  dataset 分区；幂等（依赖签名不变跳过）；读取 curated 读物化 parquet，否则实时计算
- **依赖登记**：fieldset → dataset（stkoe_depends），删除源 dataset 需 `--force`
- **多路径注册**：Execute（dispatch.py）/ SubmitTask（handlers.py）/ CLI（cli.py）三处对齐；
  `api.md` §3.1/§3.8/§3.7/§8 同步
- 测试：`tests/test_fieldset.py` 全链路（CRUD/check/scan 幂等/依赖阻断/任务版），
  全量 106 用例绿（gRPC 用例需 unset 系统 proxy 环境变量）

### 2026-08 stat `--kind storage` 存续统计（表文件存储占用/文件数）

- **`stat scan table <name> --kind storage`**：只对表磁盘 parquet 做 stat 聚合（不读数据页），
  输出列 `partition_by | partition_value | storage_size | file_no`；`all` 分区为
  `__all__/__all__` 全表总量，其余分区文件按表 hive 分区键逐值一行；`get` 可 `--partition_by`
  读取单分区。`calc_storage`（calc.py）+ `_scan_storage_sync`（controller.py）实现，
  调度/任务版/CLI 三处经 `kind` 透传自动对齐；`api.md` §3.6/§8.2 同步
- 测试：`tests/test_stat.py` 新增 hive/flat/全部区 storage 用例，全量 92 用例绿

### 2026-08 stat 数据统计资产（coverage 覆盖率）+ CLI stat 子命令

- **新增 `stat` 模块**：`stkoe stat scan <target_type> <name> [--kind coverage]` 扫描
  `table` 或 `dataset` 目标，写 `stats/<target_type>/<name>/<kind>/` 目录下 parquet 文件：
  分区 = `["all", *索引列]`（dataset 取 keys，table 取非 tool 列），每分区一个文件；
  `stat get <...> [--partition_by <p>]` 读取全部或指定分区
- **覆盖率统计**（calc.py 按 dtype 分桶）：`group | field | data_type | count | null_count |
  nunique | min | q25 | q50 | q75 | max | mean | min_date | max_date`，分桶后 unpivot 重排回
  源列序；`ALL_COLS` 常量输出
- **stat 资产不进 catalog**：扫描结果纯文件系统产物（文件夹存在即已扫描），
  `meta`/`list` 读目录；目录缺失抛 `StatNotFoundError`
- 文件排序 `_ordered`：`all` 恒首位，其余按名字母序（scan/get/meta 三处一致）
- 三条路径同时注册（Execute `e:stat ...` / 任务版 `s:stat ...` / CLI `stkoe stat ...`）；
  任务版 get 每分区 `put_result`（IPC）；测试 `tests/test_stat.py` 全链路，全量 81 用例绿

### 2026-08 dataset 物化解耦：add 只注册、scan 才物化 + 列元数据继承

- **`dataset add` 不再自动物化**：默认只注册（`materialize=False`），物化统一走显式
  `dataset scan`；`--materialize` 可显式要求在 add 时物化（Execute/任务版/CLI 三处对齐）
- **`dataset get` 不隐式物化**：物化完成且与源一致（curated）读 parquet，否则返回
  实时 join 视图（`_view_lf`），数据一致性靠显式 scan 保证
- **dataset 列元数据继承源表**：`scan_spec` 构造 dataset 列时继承源列的
  display_name/description/unit/formula/tags（`_inherit_col_meta`）；源表 `table col`
  改动经 `dataset scan` 的 `_sync_source_meta` 自动覆盖 dataset 列说明
  （`_cols_equal` 签名含全部列元数据）；dataset 列不支持直接修改（无 `dataset col`，
  `dataset set` 只改 dataset 级 display_name/description/source/tags）
- 测试：新增 dataset 物化解耦 / 列元数据继承 / scan 覆盖 / set 不动列 等用例，
  全量 72 用例绿

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