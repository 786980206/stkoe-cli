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
├── dbt.py             # dbt manifest.json 元数据桥接（table/index add 应用模型/列说明）
├── grpc/
│   ├── stkoe.proto + stkoe_pb2*.py     # 协议 + protoc 生成
│   ├── dispatch.py    # Execute 同步命令分发（@handler 注册；version/config/table）
│   └── server.py      # StkoeService 实现 + StkoeServer + 请求 INFO 日志
├── table/             # 表数据资产（登记/版本/元数据走 graph，见 graph/service.py）
│   ├── errors.py      # 共享错误与常量（DependencyError/TableNotFoundError/Exists/DEFAULT_IGNORE_COLS）
│   ├── spec.py        # TableLayout/ColumnMeta/FileDiff dataclass
│   ├── util.py        # parquet 指纹/布局识别/footer/差异/signature
│   ├── query.py       # 谓词解析 + 文件级裁剪（prune_files）
│   └── handlers.py    # 任务版 Handler（source="table"，注册进 TaskRegistry）
├── index/             # 索引资产（symbol/datetime 列，独立物理目录 index/，走 GraphService）
│   └── handlers.py    # 任务版 Handler（source="index"，注册进 TaskRegistry）
├── panel/             # 逻辑数据集（index 表 + 成员表 join，走 GraphService）
│   └── handlers.py    # 任务版 Handler（source="panel"，注册进 TaskRegistry）
├── fieldset/          # 衍生指标集（走 GraphService）
│   ├── spec.py        # FieldMeta dataclass
│   ├── engine.py      # 公式引擎插件（CalcEngine + register/get；仅 polars）
│   └── handlers.py    # 任务版 Handler（source="fieldset"，注册进 TaskRegistry）
├── sample/            # 样本池（fieldset 视图 ∩ 指定 index 键集合，无物化）
│   └── handlers.py    # 任务版 Handler（source="sample"，注册进 TaskRegistry）
├── feature/           # 因子定义库（走 GraphService；纯定义，无物化）
│   ├── engine.py      # 公式引擎插件（复用 CalcEngine 注册表；仅 polars）
│   └── handlers.py    # 任务版 Handler（source="feature"，注册进 TaskRegistry）
├── factor/            # 最终因子（走 GraphService）
│   ├── engine.py      # 算子注册表（FactorOperator/NothingOperator）+ pipeline 解析 + 公式引擎
│   └── handlers.py    # 任务版 Handler（source="factor"，注册进 TaskRegistry）
├── factor_test/       # 因子测试数据集（走 GraphService）
│   ├── spec.py        # FactorTesterSpec dataclass
│   ├── tester.py      # 测试数据集准备 + 六类测试器（bucket_returns/factor_returns/
│   │                  #   bucket_turnover/autocorrelation/ic/coverage，纯 polars）
│   └── handlers.py    # 任务版 Handler（source="test"，注册进 TaskRegistry）
├── stat/              # 数据统计资产（StatController，async 接口）
│   ├── spec.py        # StatFile/StatMeta/StatScanReport dataclass
│   ├── calc.py        # calc_stats：按 dtype 分桶算覆盖率统计（ALL_COLS 输出）
│   ├── controller.py  # async scan/get/meta/list/delete（cov 写入 stat/ 目录，不进 catalog）
│   └── handlers.py    # 任务版 Handler（source="stat"，注册进 TaskRegistry）
├── mock/              # 演示数据生成（stkoe mock demo/gen，替代 scripts/gen_example_data.py）
│   ├── gen.py         # 生成器（tdcal/common/index/feature/klday/m1 + demo）+ write（只写盘不注册）
│   └── handlers.py    # 任务版 Handler（source="mock"，注册进 TaskRegistry）
├── graph/             # V3.0 资产图（graphqlite 嵌入式图数据库 + GraphService，见 README.md §2）
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

### GraphService 设计约束（V3.0 全局）

1. **update 语义**（源头 update=重扫对账；scan 旧名已清理）：上游传导就绪检查
   （`assert_ready` BFS 全链 valid）；源头不齐失败
2. **下游物化按 index 的 `materialize_partition` 时间桶分区**：panel/fieldset/factor/test
   统一继承其 index 的物化粒度（yearly/monthly/daily，默认 yearly）分桶落盘
   （`part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/data.parquet`，文件内保留 part 列；
   与 index 物理是否分区无关）；对外读取剔除 part 列（`_scan_materialized`）；
   增量删桶时保留桶内区间外旧行合并写回（桶粒度粗于增量区间，见 `_rewrite_buckets`）
3. **沿链增量物化**（不找最上游）：`_upstream_scope` 用 `graph._accumulated`（按出边
   required_version 水位取直接依赖未消费事件）得 datetime 区间；有区间且已有物化 → 分区
   删受影响分区重算 / flat 删区间+合并写回；`--resync`/首次/无区间 → 全量
4. **get 三态**（`_require_materialized`）：已物化（curated）读物化；本应物化但未物化
   （panel/fieldset/factor/test）→ 报错提示先 `<type> update <name>`；sample/feature 恒实时
5. **依赖查询用 Cypher**（变长/批量），不用 Python 循环：`store._walk` 逐层批量
   `MATCH (a)-[r:DEPENDS]->(n) WHERE a.id IN $ids`
6. **单一实现**：业务只在 GraphService 一份；CLI/Execute/task handlers 只是薄参数解析
7. **resolve 收口**：update 成功后走 `graph.resolve` → 铸版本 + 合并事件入 version_list +
   出边 required_version 对齐 + valid/materialized；`set(self_invalidate=...)` 定义键变更
   置脏自身（fieldset check 写回 validated 用 `self_invalidate=False` 例外）
8. 目录单数：`table/ index/ panel/ fieldset/ factor/ factor_test/ stat/ task/`

实测结论（graphqlite / 版本 / 幂等 / polars）：
- graphqlite 变长路径 `-[:DEPENDS*1..N]->` 可用但 `length(p)` 对多跳恒返回 1（不可靠）；
  批量 `MATCH ... WHERE a.id IN $ids` 逐层拿下一层+边属性（可靠，用于 `_walk`）
- Python 3.13 `isolation_level=''`（legacy）：`GraphStore.execute` txn() 外 DML 必须立即
  commit（否则 close 回滚，指纹残留 bug 的根因）
- 幂等 materialized 读位置：`resolve` 把 materialized 放节点顶层而非 extra → 幂等判断用
  `node.get("materialized") or extra.get("materialized")`
- polars：`is_between` 需 `pl.lit()` 包裹；多文件 dtype 不一致用 `vertical_relaxed`
- asof join：String 日期先 cast Date 再 `join_asof`，结果 cast 回 String（触发 UserWarning 无碍）

### 双命令路径：Execute 与 SubmitTask 各自注册

同一业务命令有两条路径，**新增动词必须两处都注册并保持行为对齐**：

| 路径 | 分发 | 处理器签名 | 注册位置 |
|---|---|---|---|
| `e:<source> <action>`（Execute，同步流式返回） | `grpc/dispatch.py` | `fn(args, data_dir=None) -> list[Result]`，`@handler(source, action)` | dispatch.py 内 |
| `s:<source> <action>`（SubmitTask，后台任务） | `TaskManager.registry` | `async run(ctx) -> TaskResult`，`TaskHandler` 子类 | 模块 `register(registry)` |

- Execute 的 `e:table get` 返回单条 `ArrowTable(IPC)`，元数据（rows/total/columns 列说明）并入 `ArrowTable.meta`
- `Result.kind`: json → `JsonData`，table → `ArrowTable`；`Result.json/Result.table` 工厂方法

### API 文档同步

仓库根 `README.md` 是唯一入口文档（数据资产与图设计 §2 / 对外 API 全量说明 §5-§13 /
配置 / 存储布局 / 设计对照 §14；原 api.md、graph-design.md、graph-v3-gap.md 已并入）。
**新增/修改任何对外接口（动词、参数、返回字段、配置键）时，必须同步更新 `README.md`
的 API 文档节（§5-§13）。**

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

### TableController 不变式（已随 V2.0 死代码删除）

原 V2.0 `TableController`（SQLite catalog 登记层）已于 2026-08 冗余清理中删除，
业务统一走 GraphService。仍有效的物理约定由 `graph/service.py` 承担：

1. **绝不写/删用户 parquet**：`table_delete` 只删 graph 节点/指纹登记，数据目录保留（可重新 add）
2. `table_add` 是"发现资产"语义：目录不存在报错、已注册报 `TableExistsError`（更新用 update）
3. 读前快检签名：stat 签名一致则继续，不一致自动重扫对账；未注册目录隐式注册
4. `signature()` = sha256(排序后的 `rel_path|size|mtime_ns`)，相对表根

### 测试

- `tests/test_grpc.py`：`srv`/`client` fixture（StkoeServer 起真实 gRPC，port=0 自动分配）
- `tests/test_*.py`：各资产**任务版链路**测试（graph 语义，`_await`/`_mgr_result` 助手轮询
  终态事件落库再取 JSON）；源头造数统一走各文件的 `_gsetup`（GraphService 建链 + 依次
  update 就绪）与 `_write_idx`（index/ 目录）
- 流式断言用 `_collect`（先 DataHeader，再数据消息）
- **V2.0 死代码 controller 已删除**（2026-08 冗余清理，业务只剩 GraphService 一份；历史
  代码可从 git 历史 + `V2.0/` 备份区恢复），随迁的 113 例死代码直测一并移除。
  `V2.0/tests/` 现存文件（test_grpc/stat/task_manager/config/mock 等）是历史基线快照，
  与当前代码不兼容、默认不运行（pyproject `testpaths=["tests"]` + `norecursedirs` 排除 V2.0）
- 全量 194 用例约 44s（V3 graph/gRPC/任务链路为主），多连跑需稳定；**改动后优先只跑相关
  文件**：`.venv/Scripts/python.exe -m pytest tests/test_graph.py tests/test_grpc.py -q`

#### 测试提速与排查经验（V2→V3 迁移中总结）

1. **全量 pytest 是最大耗时项**：V3 前全量 ~60s，其中约一半（113 例）是 V2.0 死代码
   controller 的回归测试——测的是已废弃实现，对当前 graph 代码无价值。已移出默认全量，
   全量降到 ~40s。**小改动不要跑全量**，只跑相关测试文件即可。
2. **批量文本替换易误伤**：一次替换多处（如 `_root(` → `_asset_root(asset_type,...)`）会命中
   无 `asset_type` 变量的方法（NameError），或漏掉带路径前缀的写法（`os.path.join(base,
   "tables")`）。对策：替换前先 grep 列出全部匹配点逐条确认；替换后立即跑相关测试，
   报错先怀疑替换误伤。
3. **edit 频繁遇「file changed」**：文件被其他工具/命令改动后 edit 需先 read；批量改多个
   文件时按顺序 read→edit，避免来回重读。
4. **任务版用例偶发 flaky**：终态事件落库与轮询存在竞态，`_mgr_result`/`_result` 助手会
   轮询等终态事件落库再取 JSON；新增任务版断言务必走 helper，不要立刻读 `mgr.get()`。
5. **全量测试命令**：`.venv/Scripts/python.exe -m pytest tests -q`（沙箱内 uv 不可用，本机用
   预建 .venv）；Windows NVMe 上 SQLite fsync 快，Linux 慢文件系统注意 I/O 等待（曾因此
   全量 222s → 105s，见变更记录「SQLite catalog 减少 fsync」）。

## 当前状态与下一步

**当前**：V3.0 图重构完成——table/index/panel/fieldset/sample/feature/factor/test
全部基于 graph 实现（`graph/service.py` 的 GraphService），Execute 与 SubmitTask 三路径统一；
旧 catalog.db 废弃（登记/依赖/版本进 graph 节点/边，物理指纹表迁入 catalog.db 普通表；
tasks.db 独立保留）；`graph lineage/nodes/stats` 已接入 gRPC Execute 通道，
portal 前端"血缘关系"抽屉/完整页已联调（见 README.md §2/§6.13）。

**下一步**（详见 README「路线图」）：
1. 列级血缘（列节点图）、图算法（PageRank 等）
2. 持续优化循环：结构清晰 / 容错 / 数据处理性能（每项提交文档 + Git）

## 近期变更记录

### 2026-08 docs: 文档整合——api.md / graph-design.md / graph-v3-gap.md 并入 README.md

- **README.md 成为唯一入口文档**：整合原 4 份文档并去重——图设计（节点/边/版本/事件/
  存储层，原 graph-design.md）、对外 API 全量说明（命令表/数据模型/资产语义/任务版/
  CLI/gclient/配置/存储布局/典型工作流/portal 指南/增量语义，原 api.md）、设计对照与
  实现出入（E1-E7/G1-G12/评审遗留，原 graph-v3-gap.md）；原 api.md / graph-design.md /
  graph-v3-gap.md 已删除（git 历史可恢复）
- **交叉引用收口**：AGENTS.md「API 文档同步」节改指 README.md §5-§13（原 api.md）；
  目录结构注释/当前状态改指 README.md §2；`graph/__init__.py` docstring 改指 README.md §2
- 保留：`example.md`（全流程演练）、`AGENTS.md`（开发指南）、`v3.0-def.py`（设计对照基准）

### 2026-08 refactor(cli): config show 改名 config get

- `stkoe config get` / `e:config get`（空 action 仍等价）取代 `show`（不留别名）；
  CLI/Execute 两路径同步，返回结构不变（`{"config_file", ...}`）
- 测试：test_config.py / test_grpc.py 用例名与 action 更新；全量 211 用例绿
- 文档：api.md §1/§3.1/§5/§7/gclient 速查、README、example.md、AGENTS.md

### 2026-08 feat(cli): stkoe serve 支持 --config 指定配置文件

- `stkoe serve [--host H] [--port P] [--config <路径>]`：`--config` 显式指定配置文件
  （启动前设置 STKOE_CONFIG 环境变量，等价于 `STKOE_CONFIG=... stkoe serve`；
  生效配置查找优先级不变：`--config`/环境变量 > `./stkoe.json` > `~/.stkoe/stkoe.json`）
- 测试：test_config.py +1 例（_apply_config_flag 设置/不设置环境变量）；全量 211 用例绿
- 文档：api.md CLI 表 + §7 查找优先级、AGENTS.md

### 2026-08 feat(dbt): 配置 dbt-manifest-file——table/index add 自动应用 dbt 模型元数据

- **配置键**：`stkoe config set --dbt-manifest-file <路径>`（StkoeConfig 新增已知键
  `dbt_manifest_file`，config get 透出；路径 expanduser，相对路径按 cwd 解析）
- **新模块 `src/stkoe/dbt.py`**：解析 dbt 编译产物 `target/manifest.json`，按
  `name`（回退 `alias`）匹配 nodes/sources 的 model/source 节点；资产级提取
  `description` + `meta.display_name/source/tags`，列级提取 `description` +
  `meta.display_name/unit/tags`（tags 归一化为 list）
- **应用语义**：`table_add`/`index_add`（含 `--all` 批量）先应用 manifest 元数据，
  再应用 add 参数——**参数显式指定 > manifest > 默认**；`_scan_disk` 新增
  `col_meta` 参数按列名合并列说明（仅 add 传入；update 不重应用，已有列说明保留）
- **错误语义**：配置了但文件缺失/解析失败 → add 抛 ValueError（配置错误显式暴露）；
  manifest 无匹配节点 → 静默（表不在 dbt 项目属正常）
- 测试：新增 tests/test_dbt.py 9 例（解析单元 / config 读写 / table·index add 应用 /
  参数覆盖 / --all 批量 / 未配置无影响 / 文件缺失报错）；全量 210 用例绿
- 文档：api.md §7 配置表 + §3.1 add 下注、AGENTS.md 目录结构

### 2026-08 refactor: 清理过时概念——dataset 旧别名 + scan 旧名别名彻底移除

- **dataset 兼容层删除**：`src/stkoe/dataset/`（__init__ + handlers 别名注册层）整体删除；
  dispatch 的 `@handler("dataset", ...)` 双注册（7 个动词）、`task/manager.py` 的
  dataset 注册、CLI `stkoe dataset` 子命令全部移除——命令层只留 `panel`
- **fieldset 参数/存储改名**：`fieldset add --dataset <panel>` → `--panel <panel>`；
  fieldset 节点属性与定义键 `dataset` → `panel`（`DEFINITION_KEYS["fieldset"] =
  {panel, fields}`，边 role=dataset → panel），FieldsetMeta 输出键同步
- **stat 目标收敛**：stat 的 `dataset` 目标类型移除（`target_type == "panel"` 只留 panel；
  api.md stat 目标 `<table|panel|test>`）；错误消息示例 dataset → panel
- **scan 旧名别名删除**：service 层 `table_scan/index_scan/fieldset_scan/factor_scan/
  test_scan` 5 个别名方法、dispatch 5 个 `@handler(..., "scan")` 双注册、任务版 5 个
  `XScanHandler`（改 XUpdateHandler）全部删除——资产只保留 `update` 动词；
  **stat scan 保留**（stat 自身动词，非别名）；`_scan_disk/_scan_materialized` 等
  内部方法与 CalcEngine.scan 保留
- 测试：test_dataset.py 删除（dataset 用例移除、panel 用例迁入新建 test_panel.py）；
  scan→update 调用全面替换（test_grpc/table/fieldset/factor/factor_test/graph_service）；
  全量 201 用例绿
- 文档：api.md（source/action 列表、§3.1 各资产 update 行、stat 目标、FieldsetMeta、
  §9/CLI/gclient 速查）、example.md、README（模块表/命令/目录结构/用例数）、
  graph-design.md（节点表/路线图勾选）、AGENTS.md（目录结构/设计约束 #1/当前状态）

### 2026-08 feat(sample): 样本池改为 fieldset 视图 ∩ 指定 index 键集合（去除公式过滤）

- **参数格式**：`sample add <name> <fieldset> <index>`（位置参数；原
  `--fieldset/--formula/--engine` 移除，`sample set` 改 `--index` 为定义键）
- **筛选逻辑**：样本池 = fieldset 视图（panel 全列 + 已校验指标）按筛选 index 的
  (symbol, datetime) 键集合 semi join——只保留键存在于该 index 数据中的行；index 键列名
  与视图 keys 不同名时按位置映射（symbol → keys[0]、datetime → keys[-1]）
- **血缘/定义键**：sample 新增 DEPENDS 边 → index（role=index，筛选参照）；
  `DEFINITION_KEYS["sample"] = {fieldset, index}`；`sample set --index` 改筛选参照置脏
- **清理**：`sample/engine.py`（SampleEngine 公式引擎插件）随公式过滤废弃删除、
  `get_sample_engine` 死函数移除；sample_meta 输出 index 字段（去掉 engine/formula）
- **事件语义**：index 成为 sample 直接上游——index 变化事件对 sample 立即可见
  （accumulated/version_list 裁剪按两上游边水位）
- 测试：test_sample.py 重写（idx2 键过滤 + set --index）+ test_grpc Execute 三参数
  （新增 idx2 造数）+ 各 _gsetup 补 index 参数 + test_graph 边数 6→7、事件流断言适配；
  全量 202 用例绿
- 文档：api.md §3.1/§3.9/§3.11/§8/§9/gclient 速查、example.md §5 + 血缘链、
  graph-design.md §2.1（sample → index 边）、AGENTS.md 目录结构

### 2026-08 docs: example.md 更新为 v0.7.1 全量模拟案例 + 文档残留清理

- **example.md 重写**：全流程演练对齐 v0.7.1 语义——物化按 index.materialize_partition
  时间桶分区（`part=<YYYY>[/<MM>[/<DD>]]` 布局 + 对外剔除 part 列 + `--partition` 按桶读取）、
  update 主推 / get 三态、增量更新演示（追加 2026 数据 → 沿链 update → 新桶 part=2026 出现）、
  stat 测试器、任务版 + graph 血缘可视化；修正 demo 写盘路径（`index/index`）、
  原案例会失败的 m2 成员表（补 `mock gen m2` 步骤）、
  补 `sample update`/`feature update` 传导就绪步骤（factor update 有上游未就绪拦截，端到端验证暴露）
- **graph-v3-gap.md 残留清理**：E6 定义行（README 路线图 version_list 裁剪已 ✅）、
  G4 行（"物化全量重算" → 已收敛 E3 增量消费）
- **api.md 残留清理**：mock demo 写 `index/index` + `table/m1`（§3.1/§4.1）、
  panel update 描述补物化（§3.1）、panel 分区继承下注（§3.1）、§9 典型工作流重写为
  当前语义（修 `table add index` 重复 / `price`、`k` 列不存在的错误示例）
- 验证：example.md 全链路命令在临时目录端到端跑通（mock→panel→fieldset→sample→
  feature→factor→test→stat→增量→graph，跨年双桶 + 增量新桶断言）

### 2026-08 版本 0.7.1（tag v0.7.1）：下游物化按 index.materialize_partition 时间桶分区

- **pyproject 版本 0.7.0 → 0.7.1**：发布下游继承物化粒度（panel/fieldset/factor/test
  按 index.materialize_partition 时间桶分区落盘）行为变更，详见下方 feat 记录

### 2026-08 feat: 物化按 index.materialize_partition 时间桶分区（下游继承物化粒度）

- **语义修正**：panel/fieldset/factor/test 物化不再"镜像 index 物理分区键"（物理 flat 则
  单文件），统一继承 index 的 `materialize_partition`（yearly/monthly/daily，默认 yearly）
  按**时间桶**落盘：`part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/data.parquet`
  （`_partition_plan` 替代 `_index_partition_keys`；`_write_partitioned` 按 gran 从
  时间键提取桶值生成 part 列）
- **对外隐藏桶列**：新增 `_scan_materialized`（hive 分区还原 + `exclude("part")`），
  panel/fieldset/factor/test 的 get/视图读取统一走它——返回列集合与实时视图一致，
  下游链（fieldset/sample/stat）不感知 part；内部增量删桶用原生
  `read_parquet(hive_partitioning=True)` 保留 part
- **修粗桶×细区间增量丢数据**：时间桶粒度（年）粗于增量区间（天）——删桶会把桶内
  未变化行一起删掉、且新增日期可能与旧数据同桶（affected 空 → 覆盖丢数据）；
  新增 `_rewrite_buckets`：受影响桶 = 旧数据命中区间行的桶 ∪ 增量数据所在桶，
  删桶后保留桶内 `~dt_expr` 旧行与增量合并写回（4 个 update 增量分区分支统一接入）
- 测试：+2 例（monthly/daily 粒度继承 + 跨年增量开新桶旧桶不动）+ 适配 5 处物化布局断言
  （`data.parquet` → `part=2024`）；全量 202 用例绿
- 文档：api.md §3.4/§3.5/§3.7/§3.10/§3.12/§8/§11（物化产物布局与分区策略）、
  AGENTS.md 设计约束 #2

### 2026-08 评审遗留 4 项全部解决（§8/§9/§10/§13）

- **§8 错误体系统一**：`service` 的 `_require_node`/`_scan_sync`/`table_add`/`index_add`/
  `fieldset_check` 不再抛 `TableNotFoundError`（'panel not registered' 报 Table 错语义错位），
  全改 `AssetNotFoundError`；stat 的 test 目标未注册 catch 同步；顺带清理死导入 DependencyError
- **§9 三路径补齐**：新增 `index/handlers.py` 与 `panel/handlers.py` 任务版（s:index/s:panel，
  get 走 put_result IPC），`dataset/handlers.py` 改为别名注册层（实现只留一份）；
  CLI 新增 `stkoe graph lineage/nodes/stats` 子命令（复用 Execute 分发）；修
  `_graph_store` 缺省 data_dir 回退 load_config（CLI 此前恒空图）
- **§10 返回字段完整化**：`_sample_view_cols` 升级为完整列元数据（panel 列继承 ColumnMeta
  全键 + fieldset 字段继承 FieldMeta，未知列回退 name+data_type）；`sample_meta` 从
  AssetMeta 扁平形态改为 V2.0 形态 dict（keys/columns/valid，与 factor/test 对齐）；
  `_meta_dict` 补 index 专属键（symbol_col/datetime_col/materialize_partition）
- **§13 图读取收口**：新增 `GraphService.open_graph_store` 类方法（缺省 data_dir +
  expanduser + catalog.db/graph.db 命名回退 + 不存在返回 None），`dispatch._graph_store`
  改薄转发——连接管理/命名回退只此一处
- 测试：+4 用例（§8 错误类型 / s:index / s:panel 任务版 / CLI graph），全量 200 用例绿
- 文档：api.md（graph 单侧例外/CLI 表格/§3.13 标题 + 任务版说明）、graph-v3-gap.md 评审遗留全 ✅

### 2026-08 版本 0.7.0（tag v0.7.0）：V3.0 全量落地收尾——文档清理 + 会话存档归档

- **删除 `Todo.md` 会话断点存档**：任务已全部完成（冗余清理/E5/优化循环），独有信息
  （GraphService 设计约束 8 条 + 关键技术实测结论）已并入本文件「架构要点」新节
  「GraphService 设计约束」；命令速查/已知 flaky 本文件已有
- **删除 `graph-migration-review.md` 迁移评审**：13 项问题中已解决 11 项（§1/§3/§4/§5/§6/
  §7/§11/§12 及部分 §2/§9），未解决项（§8 错误体系统一、§9 index/panel 任务版 + CLI graph、
  §13 图读取重复、§10 返回字段形态）已并入 graph-v3-gap.md「评审遗留」清单跟踪
- **README 命令示例修正**：`dataset add --keys` → `panel add`（keys 由 index 推断）；
  `stkoe graph lineage/nodes/stats` 当时 CLI 无 graph 子命令（用 gclient 的 `e:graph ...`），
  已由后续 §9 补上 CLI graph 子命令（见下方变更记录）
- **pyproject 版本 0.6.0 → 0.7.0**（V3.0 graph 重构 + 冗余清理 + P0/P1/P2/E5 + 优化全部落地）
- 测试：全量 196 用例绿

### 2026-08 优化：`stkoe serve` Ctrl+C 优雅退出

- `_cmd_serve` 原 `srv.wait()` 裸奔——Ctrl+C 直接抛 KeyboardInterrupt，`srv.stop()`
  从不调用（TaskManager 不清理、WAL 不落 checkpoint）；改为 try/except/finally
  收口：收到中断打 INFO 后统一 `srv.stop()`（幂等：停 gRPC + TaskManager）
- 测试：test_config/test_grpc 相关 42 例绿

### 2026-08 优化：GraphStore WAL/busy_timeout + dispatch 线程本地 GraphService 缓存

- **GraphStore 补 SQLite pragma**：`journal_mode=WAL` + `synchronous=NORMAL`（对齐
  task/store.py 与旧 catalog 的 fsync 提速结论）+ 显式 `busy_timeout=10000`——
  多连接并发（多任务并行 / Execute 与任务同库）时等锁而非立刻报 locked
- **修 Execute 连接泄漏**：`dispatch._graph_service` 原每次调用新建 GraphService 且
  从不 close（每个 Execute 泄漏一个 SQLite 连接，WAL 下持锁影响 checkpoint）；
  改为**线程本地缓存**（key = data_dir 真实路径）：worker 线程内顺序复用同一服务，
  连接数有界（线程数 × 目录数），跨线程各自独立连接（文件锁 + busy_timeout 兜底）
- 测试：graph/grpc/config 相关 116 例绿

### 2026-08 fix: resolve 自身变更事件记录语义（E5）+ upsert/delete 各记一条

- **E5-1 upsert/delete 同存只记一条**：`resolve` 原实现
  `_bump(props, accumulated["upsert"] or accumulated["delete"])` 短路丢 delete；
  改为 `_record_events` 把两类积累事件**各记一条版本事件**（对齐源头 notify_change
  的"有增删记两个版本事件"约定，不丢动作与范围语义）；`_bump` 支持链式铸版本
  （version_list 基底显式传入，一次 resolve 多事件连续 bump，幂等性不变）
- **E5-2 上游 field_scope 原样照抄**：`resolve` 新增 `own_event` 参数——service 层
  传入自身变更事件时，记录主体的 `field_scope` 用自身的（`fieldset_update` 传
  `DataChangeEvent(field_scope=[已校验字段名])`，记录"我重算产出"而非上游列名）；
  symbol/datetime 未指定时继承积累事件并集（None=全集）——保证下游
  `_upstream_scope` 沿链取 datetime 范围不丢
- **修链上范围丢失 bug**：own_event 范围继承初版用 `_union(None, x)` 会误吞为全集
  （fieldset 记录的事件丢 datetime → factor/test 增量回退全量，2 例回归暴露）；
  改为"own 未指定才继承积累范围"，85 例 graph 测试全绿
- 测试：`test_resolve_records_both_actions`（两类各记一条 + 动作/范围断言）、
  `test_resolve_own_event_field_scope`（own field + 范围继承）
- 文档：graph-v3-gap.md E5 标 ✅

### 2026-08 冗余清理：删除 V2.0 死代码 controller（业务只剩 GraphService 一份）

- **删除 6 个死 controller**（约 2500 行）：`src/stkoe/{dataset,fieldset,sample,feature,
  factor,factor_test}/controller.py` —— 仅 V2.0/tests 死代码直测引用，当前业务全部走
  GraphService；`table/catalog.py`（旧 SQLite catalog，仅死代码引用）一并删除
- **`table/controller.py` 拆分**：TableController（V2.0 登记层）废弃删除；仍被活代码引用的
  `DEFAULT_IGNORE_COLS / DependencyError / TableNotFoundError / TableExistsError` 迁至
  新文件 `table/errors.py`（graph/service.py、stat/controller.py 引用同步更新）
- **spec 瘦身**：table/spec.py 删 FileMeta/TableMeta/TableScanReport（留 TableLayout/
  ColumnMeta/FileDiff）；fieldset/spec.py 删 FieldsetMeta/ScanReport/CheckResult（留
  FieldMeta，engine 类型提示用）；factor_test/spec.py 删 FactorTestMeta/ScanReport/
  CheckResult（留 FactorTesterSpec）；dataset/sample/feature/factor 的 spec.py 整体删除
- **`__init__.py` 清导出**：dataset/fieldset/sample/feature/factor 改为纯 docstring
  （dataset 注明旧别名转发 panel）；factor_test 仅保留 FactorTesterSpec；table 改从
  errors 导出
- **V2.0/tests 死代码直测移除**：7 个随迁文件（test_table/dataset/fieldset/sample/feature/
  factor/factor_test.py，113 例）随死代码一并删除（可自 git 恢复）；V2.0/tests 余下
  文件为历史基线快照，保持原样
- **graph/handlers.py 评估结论：保留**——service.py 实际调用（PanelHandler.add/
  FieldsetHandler.add/SampleHandler.add/FeatureHandler.add/FactorHandler.add/
  TesterHandler.add），是 v3.0-def.py 形态的图账本层，非冗余
- 测试：全量 194 用例绿；`tests/test_stat.py` 移除残留 tctl fixture（TableController 导入）
- 文档：AGENTS.md 目录结构/测试节/状态同步

### 2026-08 沿链增量物化 + get 三态收口（P2 落地）

- **fix: test 增量物化漏写盘**：`_test_scan_one` flat 增量分支计算了合并 `df` 但未写回
  （`_write_partitioned` 只在全量 else 分支调用），导致返回 rows=3 而磁盘仍为旧 2 行；
  补 `df.write_parquet(out_path)`（对照 `_factor_scan_one` 同分支已写盘）
- **fix: factor/test 幂等判断 materialized 读错位置**：幂等条件用 `extra.get("materialized")`
  恒 False——`resolve` 把 materialized 放在节点顶层而非 extra → 二次 update 永不幂等
  （`changed` 恒 True）；改为 `node.get("materialized") or extra.get("materialized")`
- **测试适配 get 三态**：任务版/Execute/stat 路径在读取物化资产前先 update/scan
  （test_dataset 的 dataset get、test_factor 的 factor get、test_grpc 的 fieldset/factor/test
  get、test_factor_test 的 stat 测试器）——未物化先读已按设计报错
- 测试：全量 194 用例绿

### 2026-08 中间节点铸版本 + panel/fieldset 物化（P1 落地）

- **panel/sample/feature update 统一走 `graph.resolve` 收口**：`resolve` 新增
  `mark_materialized`（无物化资产不置 materialized）与 `extra`（物化哈希/水位并入 extra，
  不额外 bump）参数；有积累事件 → 铸版本 + 合并事件入 version_list + 出边 required_version
  对齐，无事件不空 bump（幂等）——E4 事件水位链断档修复
- **panel 物化**：`panel_update` 把 join 视图落盘 `panel/<name>/data.parquet`
  （`_panel_hash` 依赖上游版本签名 + consumed 水位）；`panel_get` 物化且 curated 读物化、
  否则实时 join；上游变化 → curated 失效回退实时；`panel_delete` 清理物化目录
- **fieldset 衍生字段物化**：`fieldset_update` 把 keys + 已校验字段落盘
  `fieldset/<name>/data.parquet`（`_fieldset_hash` = panel 版本 + 字段公式 + engine）；
  `_fieldset_view_lf` 物化且 curated 读物化字段（fields_only 直接返回 / 全视图 join panel）
- **fix: task max_seq 防御**：连接跨线程共享（check_same_thread=False）下并发 fetchone 可能
  返回 None → `row[0]` 崩溃（subscribe replay 偶发 flaky 的 root cause）；改为
  None/空值回退 0（最多全量 replay，不崩不丢）
- 测试：panel 物化/curated 失效/版本推进、fieldset 字段物化/curated、sample 链上铸版本
  （version_list 记录）5 例；全量 190 用例绿
- 文档：graph-v3-gap.md E3/E4 标 ✅、结论 P1 落地、P2 剩余（panel/fieldset 增量、symbol_scope
  提取、version_list 裁剪等）

### 2026-08 V3 Event 增量闭环 P0：范围化事件 + factor/test 增量物化

- **P0-1 物理变化 → 范围化事件**（`service._change_events`）：
  - added/changed 文件 → `action="upsert"`；removed 文件 → `action="delete"`（一次 scan 有增删
    时记两个版本事件）；`datetime_scope` 统一为 **[min, max] 区间**（hive 分区键=
    datetime_col 用分区值，其余读变化文件 footer 的 datetime 列 min/max，不读数据页）；
  - 兜底：范围取不到也发全集事件（保证下游置脏不丢）
- **P0-2 factor/test update 增量物化**：
  - `_upstream_sources`（BFS 收集全部 table/index 源头）+ `_upstream_scope`（源头 version_list
    中 `version > consumed` 事件的 datetime 区间，`extra.consumed_versions` 记各源头水位）；
  - 已有物化且区间明确 → 读旧物化删范围 + 仅重算范围内行（`_factor_compute`/`_test_build`
    加 `dt_range`，`is_between(pl.lit(lo), pl.lit(hi))`，String/ISO 字典序可比）→
    `vertical_relaxed` 合并写回；`--resync`/首次/无范围 → 全量兜底
  - 中间节点（panel/sample/feature）不记事件，故从源头直接收集（绕过 E4 断档）
- 测试：P0-1 事件范围/delete/分区提取 2 例 + P0-2 factor/test 增量（compute 带 dt_range 断言）
  + resync 全量回退，全量 185 用例绿
- 文档：graph-v3-gap.md E1/E2/E3 标 ✅ 已修、结论更新为 P0 落地

### 2026-08 panel add 成员表 join 方式可配置（默认 asof，可选 left）

- **参数格式**：`panel add <name> <index> [member[:join]...]`——每个 member 可带
  `:asof` / `:left`（归一化为 `asof_join`/`left_join`，未知值报错），**缺省 asof join**
  （原默认 left join）
- **实现**：`PanelHandler.add` 解析 dict / (name, join) 元组 / "name:join" 字符串并
  `_norm_join` 归一化；`GraphService.panel_add` 透传 tables（不再强制 left_join）；
  `_panel_lazy` 按每个成员的 join 类型执行——left 走精确等值 join，asof 走
  `join_asof`（等值键 keys[:-1] 作 by + 时间键 keys[-1] 作 on，backward 就近匹配；
  String 日期先 cast Date 再 asof，结果列 cast 回 String 保持下游公式的字符串比较语义）
- Execute / 任务版（dataset handler 转发 panel）与 CLI 均支持；边 `role=member` 的
  `detail.join` 记录实际 join 类型
- 测试：test_graph_service.py 增 `test_panel_add_join_types`（三种形态归一化 + 边断言）、
  `test_panel_add_unknown_join_error`、`test_panel_get_asof_join_backward`（01/03 无精确行
  → backward 取 01/02 值）；`test_panel_add_edges` 默认断言改 asof_join；全量 180 用例绿
- 文档：api.md §3.1 panel add 行 + 表下注、example.md §2、graph-design.md §2.1 示例边
- 注：asof join 对 by 分组的排序校验会触发 polars UserWarning（无碍，已先 sort）

### 2026-08 fix: graph lineage/nodes/stats 空图 —— _graph_store 未 expanduser

- **现象**：配置默认 `data_dir="~/.stkoe"`（未展开字符串）时，`table list` 正常（GraphService
  内部 `Path(data_dir).expanduser()`），但 `graph lineage/nodes/stats` 恒空——`dispatch.
  _graph_store` 用 `os.path.join(data_dir, name)` 拼路径，字面 `"~/.stkoe\catalog.db"`
  不存在 → 返回 None → 空图。形成"有节点但血缘空"的假象（portal 经 gRPC 命中）
- **修复**：`_graph_store` 打开库前 `data_dir = os.path.expanduser(data_dir)`（与 GraphService
  行为对齐）
- **测试**：test_graph.py `test_lineage_with_tilde_data_dir`（monkeypatch expanduser 映射
  "~" → 测试库，验证 lineage/nodes/stats 都能查到）；全量 177 用例绿
- **注意**：运行中的服务进程加载的是启动时代码，修复后需重启 `stkoe serve` 才生效

### 2026-08 index add --all 批量发现 + 修 delete 指纹残留（legacy 事务模式）

- **`index add --all`**：批量发现 `index/` 下未登记且含 parquet 的目录（同 `table add --all`），
  返回 `indexes` 数组；批量时 `--symbol-col/--datetime-col/--materialize-partition` 对全部
  新发现统一生效。Execute（dispatch `_index_add`）与 CLI（`stkoe index add --all`）同步支持；
  测试：test_graph_service.py `test_index_add_all` + test_grpc.py `test_execute_index_add_all`
- **fix: delete 资产后指纹残留**：Python 3.13 默认 `sqlite3.isolation_level=''`（legacy 模式），
  `GraphStore.execute` 在 txn() 外的 DML（`fingerprint_clear` 的 DELETE）隐式开启事务但不
  提交，连接 close() 时回滚 → `table_delete`/`index_delete` 清指纹从未真正持久化（同连接内
  SELECT 可见未提交状态，测试未暴露；新进程可见残留）。修复：`GraphStore.execute` 对
  txn() 外的写语句（INSERT/UPDATE/DELETE/REPLACE）立即 commit；txn() 内的仍由 txn() 统一
  提交。回归测试 `test_delete_clears_fingerprint_persistently`（跨连接验证）
- 文档：api.md §3.1 index add 行补 `--all` + 表下注说明；全量 176 用例绿

### 2026-08 V2.0 死代码测试移出默认全量（tests → V2.0/tests）+ 测试经验沉淀

- **拆分 7 个混合测试文件**：tests/test_{table,dataset,fieldset,sample,feature,factor,
  factor_test}.py 各自保留 graph 语义的任务版链路用例（`test_task_framework_*`；
  factor_test 另保留 stat 测试器集成用例），V2.0 死代码 controller 直测（共 113 例）移入
  `V2.0/tests/` 同名文件。默认 pytest 不收集：`testpaths=["tests"]` + norecursedirs 排除 V2.0
- 移入文件头部注明归档性质与单独运行命令；V2.0/tests 其余文件是 f290378 历史基线快照
  （与当前代码不兼容），未被改动；原始基线测试可从 git f290378 恢复
- 效果：全量 286 例 → 默认全量 173 例（约 60s → 约 40s）；用例总数不变（173+113=286），
  移入的死代码测试仍可单独运行全绿
- AGENTS.md「测试」节补「测试提速与排查经验」：全量耗时主因 / 批量替换误伤 /
  edit 需先 read / 任务版轮询 helper / 只跑相关测试

### 2026-08 index 资产独立物理目录 index/（不再共用 table/）

- `GraphService` 新增 `indexs_root`（`<data_dir>/index/`）：`index add/update/get/meta`
  扫描/读取 `index/<name>/`，与 table 的 `table/<name>/` 分离（`_asset_root` 按类型选目录）；
  `index list --candidate` 扫 `index/` 下的未登记目录
- `mock demo`/`mock gen --kind index` 写 `index/index`（`mock.write` 加 `subdir` 参数）；
  测试：index 表写入统一走 `_write_idx`（index/），table 保持 `table/`；stat 的 table 目标
  用例改用成员表 m1（index 不再是 table）；全量 286 用例绿
- 文档：api.md §8 存储布局补 `index/` 目录、example.md index 命令改 `index` 前缀

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
  拼索引+因子列 → pipeline 算子链）与物化（`factor/<name>/data.parquet`，flat 单文件，
  幂等签名 = 上游 feature/sample 的 graph 版本 + engine/pipeline/factor_col hash）；
  test 数据构造（sample 视图 + 测试必需列 → `prepare_factor_data`）与物化
  （`factor_test/<name>/data.parquet`）；`test_data()` 供 stat 测试器复用
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
  = 15 万行**，`--n-syms/--n-days` 可调）到 `<data_dir>/table/`；**只写盘不注册**，仍由
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
  `factor_test/` 存储目录；AGENTS.md feature 模块 formula 必填、factor set 定义键补 engine；
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
- **物化** `test scan`：落盘 `factor_test/<name>/data.parquet`（flat 单文件）；**幂等**——
  依赖签名（factor 依赖 hash + spec + 测试列名）不变则跳过；`--resync` 强制重建
- **读取**：物化且 curated 读 parquet，否则实时构造，不隐式物化；`test check` 校验构造
  成功 + 含必需列 + 行数 > 0
- **依赖登记**：test → factor（stkoe_depends），删除 factor 需 `--force`
- **测试器（stat 集成）**：`stat scan test <name> --kind <kind>`（也支持单位置参数简写
  `stat scan <name> --kind <kind>`），六类测试器（bucket_returns/factor_returns/
  bucket_turnover/autocorrelation/ic/coverage）产物写 `stat/test/<name>/<kind>/<output>.parquet`；
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
- **物化** `factor scan`：落盘 `factor/<name>/`，**布局镜像源 dataset**（源已分区则按同
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
  `table` 或 `dataset` 目标，写 `stat/<target_type>/<name>/<kind>/` 目录下 parquet 文件：
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