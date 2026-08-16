# stkoe

stkoe 数据服务（gRPC）：管理**表 / 索引 / 面板 / 衍生指标集 / 样本池 / 因子 / 因子测试集 / 统计**等数据资产。

- **图资产模型**：`src/stkoe/graph/` 图模块（graphqlite 嵌入式图库，节点/边/版本/事件响应）
  + `GraphService` 统一服务入口（table/index/panel/fieldset/sample/feature/factor/tester
  三路径统一，资产登记/依赖/版本/血缘全部走图）——**业务实现分资产模块**
  （`table/ops.py` / `panel/ops.py` / …，见 §15 目录结构），GraphService 保留图交互
  与共享基础设施，公共 API 薄委托到各模块。
- **本文档是唯一入口文档**：数据资产与图设计、对外 API 全量说明、配置、存储布局、
  测试与路线图都在这里。

## 1. 数据资产（做了什么）

### 1.1 资产一览

| 模块 | 能力 |
|---|---|
| `table` | 注册/读取/删除本地 parquet 表、列元数据、`list --candidate`（登记/版本/依赖走 graph） |
| `index` | 独立索引资产主体（symbol/datetime 列 + `materialize_partition`），table 恒 type="table" |
| `panel` | 逻辑数据集（index 表 + 成员表 join，keys 由 index 推断），update 物化（时间桶分区） |
| `fieldset` | 衍生指标集（公式引擎 polars 插件制），check 校验写回 validated |
| `sample` | 样本池（fieldset 视图 ∩ 指定 index 键集合，无物化，实时构造） |
| `feature` | 因子定义库（命名公式，纯定义），`feature test` 在样本视图即时求值 |
| `factor` | 最终因子（feature 公式 + sample 视图 + pipeline 算子链），物化、幂等 |
| `tester` | 因子测试数据集（factor_tester）+ 六类测试器（stat 集成） |
| `stat` | 覆盖率 / 存续统计（storage），输出 parquet 产物，scan 后在 graph 登记 Stat 节点 |
| `mock` | 演示数据生成（`stkoe mock demo`/`gen`） |
| `task` | 后台任务框架（SubmitTask/SubscribeTask/TaskControl，协作式取消） |

血缘链：`table/index → panel → fieldset → sample（+ 筛选 index）→ factor → tester`（另有 feature → factor）。

### 1.2 三路径统一（Execute / SubmitTask / CLI）

同一业务命令有三条路径，行为对齐、只留一份实现（GraphService）：

| 路径 | 入口 | 返回 | 适用 |
|---|---|---|---|
| Execute（同步流式） | `e:<source> <action> <args...>` | DataHeader + JsonData / ArrowTable | 元数据/列表/表格读取等小任务 |
| SubmitTask（后台任务） | `s:<source> <action> <args...>` | 立即返回 `task_id`，事件流见 §7 | 物化/统计等长任务 |
| CLI（同步分发） | `stkoe <source> <action> <args...>` | 与 Execute 一致（JSON/占位打印） | 脚本/命令行 |

- `Result.kind`：`json` → `JsonData`，`table` → `ArrowTable`
- **单侧动词例外**：`mock`（空 action）仅 SubmitTask（示例任务，见 §7.5）；`task` 仅 Execute；
  `graph` 仅 Execute + CLI（无任务版，见 §6.13）

## 2. 图设计（资产血缘关系图）

> 用**嵌入式图数据库 graphqlite**（[colliery-io/graphqlite](https://github.com/colliery-io/graphqlite)，
> SQLite 扩展，Cypher 查询 + 图算法）记录资产之间的**血缘关系**。实现代码在 `src/stkoe/graph/`。

### 2.1 设计目标

资产血缘记录在图中：**节点** = 资产（label = 资产类型，`id = "<type>:<name>"`），
**DEPENDS 边** = 依赖关系（依赖方 → 被依赖方，带 `required_version` + detail）。由此
支持：全链路血缘查询（某个表影响了哪些下游？某个因子依赖哪些上游？）、版本级更新
事件积累与沿链增量传播（边 `required_version` 即消费水位，见 §2.4）。

### 2.2 节点模型

通用属性（所有资产）：`name / display_name / description / tags / source / version（高精度
时间戳，纳秒，单调递增）/ version_list（version → 事件）/ materialized / valid /
create_time / update_time / extra`。

类型专属属性：

| Label | 节点类 | 专属属性 |
|---|---|---|
| `Table` | TableNode | `columns`（ColumnMeta[]） |
| `Index` | IndexNode | `columns`、`symbol_col`、`datetime_col`、`materialize_partition` |
| `Panel` | PanelNode | `index`（Index 节点 id）、`tables`（{name: join 类型}）、`keys` |
| `Fieldset` | FieldsetNode | `panel`（Panel 节点 id）、`fields`（{field: FieldMeta}） |
| `Sample` | SampleNode | `fieldset`（Fieldset 节点 id）、`index`（筛选参照节点 id） |
| `Feature` | FeatureNode | `engine`、`formula`、`unit` |
| `Factor` | FactorNode | `feature`（节点 id）、`sample`（节点 id）、`engine`、`pipeline`、`factor_col` |
| `Tester` | TesterNode | `factor`（节点 id）、`returns/groupby/marketcap`、`spec`（quantiles/periods/…） |
| `Model` | ModelNode | （预留） |
| `Stat` | StatNode | `target_type`、`target_name`、`kind`、`partitions[]`、`files[{partition, rel_path, rows, size}]`（scan 后登记，见 §10） |
| `Column` | 列节点（列级血缘） | `name`（列名）、`asset`（所属资产节点 id）、`asset_type`、`data_type/unit/formula/display_name/description/tags/as_index/source_table/source_field`；另有 `(column)-[:BELONGS_TO]->(资产)` 边标注所属（见 §2.3） |

ColumnMeta / FieldMeta：`name, display_name, description, data_type, unit, formula, tags,
as_index, is_tool, source_table, source_field`。**列级血缘**：DEPENDS 边 `detail.columns`
的字段映射（`{派生列: 源列 | [源列...]}`）物化为**独立列节点图**——每列一个 `Column`
节点（id = `column:<资产 id>.<列名>`），派生列与源列之间建 `(column) -[:DERIVES]-> (column)`
边（方向与 DEPENDS 一致：派生列 → 源列）；源头列（table/index）随登记/重扫对账，
字段/公式变更（fieldset `add_field/set_field`）自动重派发，`fieldset update` 时全量
对账重派发（历史/旧库字段缺边自动补齐，见 §2.3）。

### 2.3 边模型（DEPENDS + DERIVES + BELONGS_TO）

- **DEPENDS 方向**：`(依赖方) -[:DEPENDS]-> (被依赖方)` —— 出边 = 我依赖谁（上游），入边 = 谁依赖我（下游）
- **DEPENDS 边属性**：`required_version`（依赖方已消费的被依赖方版本，物化时对齐）、
  `detail`（`{"role": "index"/"member"/"panel"/"fieldset"/"feature"/"sample"/"factor",
  "join": ..., "columns": {派生列: 源列 | [源列...]}}`；`join` 仅 table → panel 边带；
  `columns` 为**字段映射**，由 controller 物化为列节点图，见 §2.2）、`create_time`
- **DERIVES 方向**：`(column:派生列) -[:DERIVES]-> (column:源列)`（与 DEPENDS 同向：
  派生列 → 其来源列）；列节点 label = `Column`，id = `column:<资产 id>.<列名>`
  （如 `column:fieldset:fs1.ma5`）；资产删除时其列节点级联删除（DETACH 连带 DERIVES 边）
- **BELONGS_TO 方向**：`(column) -[:BELONGS_TO]-> (所属资产)` —— **列属于哪个资产**；
  每列恰好一条（建列/对账/跨依赖引用统一经 `store.upsert_column`，属性 `asset` 与边
  同点写入零漂移）。它把**列级血缘（DERIVES）与资产级血缘（DEPENDS）接成一张图**：
  从列可经 BELONGS_TO 走进资产层，从资产可经 `columns_of` 走进列层——任意两层可
  连续遍历（如「因子列 → 所属因子 → DEPENDS 链 → 源头表 → 其列」一条路径走通）；
  `graph analyze` 的 **`consistency`** 用它对两层血缘做交叉校验：跨资产 DERIVES 边
  的所属资产之间必须存在 DEPENDS 路径，不一致即报告（抓列级与资产级漂移）
- **列级血缘映射来源**（**按信息量收敛粒度**：透传索引键不建字段映射，只保留
  "衍生字段 → 数据字段"的有意义关系）：
  - panel 列 → index/成员表列（同名透传；与 index 同名的成员列不重复映射）
  - fieldset keys → panel keys；**字段列 → 公式引用的 panel 列**（标识符 ∩ panel
    视图列；`fieldset update` 时全量对账重派发——历史/旧库字段缺边自动补齐）
  - sample 视图列 → fieldset 列（透传）；sample keys → 筛选 index 的 symbol/datetime 列
  - factor：**只留因子列**——`factor_col` DERIVES → feature 公式引用的 sample 视图列
    （一条或多条边）；keys（sym/date 索引透传）不建字段级映射
  - tester：**不做列级血缘**（无列节点）——测试面板的派生字段（keys/returns/
    group/marketcap/d{no}/factor_quantile）对资产血缘无信息量，资产级
    `DEPENDS → factor` 已表达"因子数据来源"；字段 schema 在 tester meta 展示

典型血缘子图：

```
Index:index ──DEPENDS(role=index)─────────────────▶ Panel:ds1
Table:m1  ───DEPENDS(role=member, join=asof_join)─▶ Panel:ds1
Panel:ds1 ───DEPENDS(role=panel)──────────────────▶ Fieldset:fs1
Fieldset:fs1 ─DEPENDS(role=fieldset)──────────────▶ Sample:sp1
Index:idx2 ──DEPENDS(role=index)──────────────────▶ Sample:sp1   ← 样本筛选参照
Sample:sp1 ──DEPENDS(role=sample)─────────────────▶ Factor:fac1
Feature:ma5f ─DEPENDS(role=feature)───────────────▶ Factor:fac1
```

- **join 只出现在 table → panel 边上**：`role=member` 边带 `detail.join`
  （`asof_join` 缺省 / `left_join`）；`role=index` 边不带 join。成员表 join 方式在
  `panel add` 时按 `member:asof`/`member:left` 指定（缺省 asof，asof 按 datetime 键
  就近匹配、left 为精确等值 join）。
- **sample 基于 fieldset 衍生 + 按筛选 index 键集合裁剪**（`sample add <name> <fieldset>
  <index>`：只保留 (symbol, datetime) 键存在于该 index 数据中的行，不再支持公式过滤）。
- Panel 建节点时同时建边（`Panel → Index` role=index、`Panel → 每张成员表` role=member）；
  边的 `required_version` 初始 = 被依赖方当前版本。
- 删除约束：**节点存在入边（下游依赖）时禁止删除**（除非 `--force`，force 时先删下游边/节点）；
  删除节点时一并删除其出边。

### 2.4 版本与事件（DataChangeEvent）

- **版本号 = 变更时刻的高精度时间戳**（`time.time_ns()` 纳秒，int，可直接看出变更时间）；
  同一纳秒或时钟回拨时以上次版本 +1 兜底，保证严格单调。`version_list` 记录
  `{version: event}`，即「这个版本发生了什么数据变化」。
- **变更即版本**：任何改变节点定义/数据的操作（table 数据变化、set 定义键、物化重算）
  都会铸新版本号并把对应 DataChangeEvent 追加进 `version_list`。

```
DataChangeEvent {
  "field_scope":    list[str] | None,   # 影响的字段范围；None = 所有字段
  "symbol_scope":   list[str] | None,   # 影响的标的范围；None = 所有标的
  "datetime_scope": list[Any] | None,   # 影响的时间范围（如 [start, end]）；None = 所有时间
  "action":         "upsert" | "delete"
}
```

**事件合并（accumulate）**：积累事件 = 上游 `version_list` 中 `version > 边.required_version`
的所有事件合并——按 action 分 upsert/delete 两类，`symbol_scope`/`datetime_scope` 取**并集**
（None=全集吞并一切）、`field_scope` 取**交集**；输出恒为 `{"upsert": ..., "delete": ...}`。

### 2.5 增删改查 + 事件响应流程

| 操作 | 流程 | 约束 |
|---|---|---|
| `add` | 铸版本（时间戳）→ 建节点（label=类型）→ 返回节点 | 同 `type:name` 已存在 → `AssetExistsError` |
| `add + 建边`（Panel/Fieldset/Sample/Factor/Tester） | 先建节点，再逐条建 DEPENDS 边（required_version = 被依赖方当前版本），**同一事务** | 被依赖方不存在 → `AssetNotFoundError` |
| `get` / `meta` | 读节点（+ 可选上下游血缘）/ 全量属性（含 version_list） | 不存在 → `AssetNotFoundError` |
| `set` | patch 属性；若改到**定义键**（formula/pipeline/成员表…）→ 铸新版本 + 记事件 + **下游失效** | 不存在 → 报错；不隐式注册 |
| `col`（Table/Index/Fieldset 字段） | 改 columns/fields 内字段元数据 → 版本递增 | 字段不存在 → 报错 |
| `delete` | **无入边**（无下游）才可删：删出边 + 删节点；`force` 时先递归删下游 | 有下游且非 force → `DependencyError` |
| `list` | 按 label 列出节点摘要 | — |

事件响应（血缘传播）：

```
上游数据变化
   │  notify_change(node, event)
   ▼
① 铸新版本 + 事件追加进 version_list
② 传播：BFS 沿入边（下游方向）标记所有传递下游 valid=False, materialized=False（stale）
   ▼
③ 下游更新（显式 update / resolve / resolve_all 拓扑重算）
   对每个出边（依赖）：取 (边.required_version, 依赖方.version] 的事件 → 合并
    → 调 storage 物化钩子 → 铸本节点新版本 + 合并事件写入 version_list
    → 出边 required_version 对齐 = 依赖方当前版本 → valid=True
```

要点：**边 = 消费水位**（水位线以下事件在下次 update 合并取出，支持多次变更一次重算）；
**传播只置脏、不算账**（`notify_change` 只 BFS 置脏，「算出积累事件」发生在 update/resolve 时）；
**拓扑顺序**（`resolve_all` 先依赖后依赖方；成环报错，血缘图应为 DAG）。

### 2.6 存储层（graphqlite 落地）

- graphqlite = SQLite 扩展（C），PyPI `graphqlite>=0.6.0`，无 Python 依赖，
  Windows + CPython 3.13 实测可加载；入口 `graphqlite.connect(db_path)` → `conn.cypher(...)`。
- **实测能力**：变长路径遍历 `-[:DEPENDS*1..5]->` ✅；`WHERE/SET/DELETE/MERGE/collect/ORDER BY`
  ✅；`$param` 参数、嵌套 dict/list 属性 ✅；**事务**（Cypher 内不支持 BEGIN，用原生 SQL
  BEGIN/COMMIT/ROLLBACK 包裹多语句写入可整体回滚，控制器 `txn()` 保证「建节点+建边」原子性）✅；
  文件型持久化 ✅。
- **存储布局**：`<data-dir>/catalog.db`（单文件 SQLite + graphqlite 扩展，节点/边由扩展内部表
  承载，应用层只经 Cypher 访问）；`GraphStore` 封装 txn / 节点 ops / 边 ops / 上下游遍历。
- **与既有代码关系**：`graph/` 全面接管资产登记（GraphService）；Execute/SubmitTask/CLI
  三路径行为对齐；物理数据读取/物化由 GraphService 直接做（parquet + 指纹），
  stat 数据源亦走 graph。

## 3. 路线图

剩余规划：
1. **ModelNode 资产**：ASSET_TYPES 含 `model`（预留节点），暂无 add/update 实现
2. **测试**：图模块更多边界用例 + gRPC 全链路回归（持续）

## 4. 环境要求与安装

- Python >= 3.13；包管理用 [uv](https://docs.astral.sh/uv/)
- 依赖：graphqlite / grpcio / orjson / polars / pyarrow（dev：grpcio-tools / pytest / numpy）

```bash
uv sync                 # 安装依赖
uv run stkoe serve      # 前台运行 gRPC 服务（默认 127.0.0.1:9569）
uv run pytest -q        # 全量测试
```

## 5. 配置（stkoe.json）

- **查找优先级**：`STKOE_CONFIG` 环境变量（或 `stkoe serve --config <路径>`）> `./stkoe.json` > `~/.stkoe/stkoe.json`
- **写入位置**：与读取同一文件（生效配置所在文件，即 `config get` 的 `config_file`）
- **已知键**：

| 键 | 默认 | 说明 |
|---|---|---|
| `grpc-host` | `127.0.0.1` | gRPC 监听地址 |
| `grpc-port` | `9569` | gRPC 监听端口 |
| `data-dir` | `~/.stkoe` | 数据目录（表/索引/物化产物/统计/任务库，含 catalog.db 图库） |
| `dbt-manifest-file` | `""` | dbt 编译产物 `target/manifest.json` 路径（expanduser，相对路径按当前工作目录解析）。配置后 **table/index add 时先应用 manifest 元数据**（按 name/alias 匹配 model/source 节点：资产级 `description` + `meta.display_name/source/tags`，列级 `description` + `meta.display_name/unit/tags`），**add 参数显式指定的值覆盖 manifest**；文件缺失/解析失败 → add 报错；无匹配节点 → 静默 |

- 任意自定义键保留在 `extra`（`config get` 透出，`config set` 原样写入）
- 日志：`STKOE_LOG_LEVEL` 环境变量可覆盖默认 INFO 级别

```bash
uv run stkoe config get | set --<key> <value> ...
uv run stkoe config set --dbt-manifest-file ./dbt-project/target/manifest.json
```

## 6. Execute 命令（`e:...`）

所有业务命令统一为 `<source> <action> <args...>` 位置参数形态，等价于 `stkoe <source> <action> <args...>`。

- **source**：`version` / `config` / `table` / `index` / `panel` / `fieldset` / `sample` / `feature` / `factor` / `tester` / `stat` / `task` / `mock` / `graph`
- **action**：`add` / `get` / `list` / `meta` / `set` / `col` / `update` / `check` / `test` / `delete`（`del` 别名）
- **参数解析**：`--key value`、`--key=value`、`--flag`（无值 → True）；键名保持用户输入形态
  （含连字符）；位置参数写在 flags 之前
- **返回约定**：首条恒为 `DataHeader`（`code=0` 成功 / 非 0 业务错误）；成功后按需跟随
  `JsonData`（小结果 JSON）或 `ArrowTable`（Arrow **IPC Stream**，`meta` 为元信息 JSON）

### 6.1 全命令表

| source | action | 位置参数 | flags | 返回 |
|---|---|---|---|---|
| version | （空）/ `get` | — | — | JsonData `{"version"}` |
| config | （空）/ `get` | — | — | JsonData `{"config_file", "grpc-host", "grpc-port", "data-dir", "dbt-manifest-file", ...extra}` |
| config | `set` | — | `--<key> <value> ...`（任意键） | JsonData `{"written", "set"}` |
| task | （空）/ `list` | — | `--state <state>` | JsonData `{"tasks": [...]}`（按创建时间倒序） |
| mock | `demo` | — | `--n-syms N`（默认 300） `--n-days N`（默认 500，交易日数，从 2024-01-01 起） | JsonData（写入清单：`[{name, path, rows, columns}]`，写 `index/index`（index 资产目录）+ `table/m1`，不注册） |
| mock | `gen` | `<name>` | `--kind <kind>`（默认 index；`tdcal/common/index/feature/klday/m1`） `--n-syms N` `--n-days N` `--start S` `--end E` `--seed N` `--col C` | JsonData（单表写入清单） |
| table | `add` | `<name>` | `--all`；单表可带 `--display_name/--description/--source/--tags <v>` + 任意键（类型由 label 承载，table 恒 "table"；`--type` 等分类键进 extra） | JsonData（TableScanReport） |
| table | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| table | `update` | `<name>` | `--all` | JsonData（TableScanReport 或 []；显式重扫对账，幂等） |
| table | `list` | — | `--candidate` | JsonData（TableMeta[] 或 候选名[]） |
| table | `meta` | `<name>` | — | JsonData（TableMeta） |
| table | `set` | `<name>` | `--display_name/--description/--source/--tags <v>` + 任意键（`--type` 进 extra） | JsonData（TableMeta） |
| table | `col` | `<name> <column>` | `--display_name/--description/--unit/--formula/--tags <v>` | JsonData（TableMeta） |
| table | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| index | `add` | `<name>` | `--all`（批量发现 `index/` 下未登记且含 parquet 的目录）；单表可带 `--symbol-col <col>`（默认 `sym`） `--datetime-col <col>`（默认 `date`） `--materialize-partition <v>`（默认 `yearly`）+ 元数据键 | JsonData（TableScanReport，type="index"；`--all` 返回数组） |
| index | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| index | `meta` | `<name>` | — | JsonData（IndexMeta） |
| index | `list` | — | `--candidate`（返回未登记 index 但含 parquet 的表目录候选） | JsonData（IndexMeta[] 或 候选名[]） |
| index | `set` | `<name>` | `--display_name/--description/--source/--tags <v>` + 任意键 | JsonData（IndexMeta） |
| index | `col` | `<name> <column>` | `--display_name/--description/--unit/--formula/--tags <v>` | JsonData（IndexMeta） |
| index | `update` | `<name>` | `--all` | JsonData（TableScanReport 或 []；显式重扫对账，幂等） |
| index | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| panel | `add` | `<name> <index> [member[:join]...]` | + 元数据键（index 为已注册 index 资产；member 为已注册 table，可带 `:asof`/`:left` 指定 join 方式，**缺省 asof**；**keys 由 index 推断** = symbol_col + datetime_col，不再接受 `--keys`） | JsonData（PanelMeta） |
| panel | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--limit N` `--offset N` | **ArrowTable**（无 JsonData；实时 join 视图） |
| panel | `meta` | `<name>` | — | JsonData（PanelMeta） |
| panel | `list` | — | — | JsonData（PanelMeta[]） |
| panel | `set` | `<name>` | `--display_name/--description/--tags <v>` + 任意键 | JsonData（PanelMeta） |
| panel | `update` | `<name>` | — | JsonData（PanelMeta；传导检查上游 index/成员表就绪后**物化 join 视图**——按 index 的 `materialize_partition` 时间桶分区落盘 `panel/<name>/part=<v>/`，见 §6.5；增量按积累区间只重算受影响时间桶） |
| panel | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| stat | `scan` | `<table\|panel\|tester> <name>` | `--kind <kind>`（`coverage` 默认 / `storage` / 测试器：`bucket_returns` `factor_returns` `bucket_turnover` `autocorrelation` `ic`）；`--partition <p>[,<p>...]`（coverage 按需只算指定分区，见 §6.6）；`<name>` 单位置 + `--kind <测试器>` 简写 → tester 目标 | JsonData（StatScanReport） |
| stat | `get` | `<table\|panel\|tester> <name>` | `--partition_by <p>` `--kind <kind>`；单位置 `<name>` 简写 → tester 目标 | JsonData + ArrowTable（§6.6） |
| stat | `meta` | `<table\|panel\|tester> <name>` | `--kind <kind>`；单位置 `<name>` 简写 → tester 目标 | JsonData（StatMeta） |
| stat | `list` | — | — | JsonData（StatMeta[]） |
| stat | `delete`/`del` | `<table\|panel\|tester> <name>` | `--kind <kind>`；单位置 `<name>` 简写 → tester 目标 | JsonData `{"deleted"}` |
| fieldset | `add` | `<name>` | `--panel <panel 名>`（必选，已注册 panel） `--engine <e>`（默认 polars） `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（FieldsetMeta） |
| fieldset | `add` | `<name> <field>` | `--formula <表达式>`（必选） `--window_size <N>`（滚动窗口回看宽度，默认 0；用于事件范围展开） `--display_name/--description/--unit/--tags <v>` | JsonData（FieldsetMeta，指标 validated=False） |
| fieldset | `set` | `<name>` | `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（FieldsetMeta） |
| fieldset | `set` | `<name> <field>` | `--formula/--window_size/--display_name/--description/--unit/--tags <v>` | JsonData（FieldsetMeta；改公式 → validated 复位 False） |
| fieldset | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--fields-only` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| fieldset | `meta` | `<name>` | — | JsonData（FieldsetMeta） |
| fieldset | `meta` | `<name> <field>` | — | JsonData（FieldMeta） |
| fieldset | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| fieldset | `delete`/`del` | `<name> <field>` | — | JsonData（FieldsetMeta） |
| fieldset | `list` | — | — | JsonData（FieldsetMeta[]） |
| fieldset | `update` | `<name>` | `--all` `--resync` | JsonData（FieldsetScanReport 或 []；显式物化，幂等；传导检查上游 panel 就绪） |
| fieldset | `check` | `<name> <field>` | `--all` | JsonData（FieldsetCheckResult[]） |
| fieldset | `test` | `<name>` | `--formula <表达式>`（必选） | JsonData `{"ok",...}` + ArrowTable（成功时） |
| sample | `add` | `<name> <fieldset> <index>` | `--display_name/--description/--tags/--source <v>` + 任意键（fieldset 为已注册 fieldset；index 为**样本筛选参照**——样本池 = fieldset 视图 ∩ 该 index 的 (symbol, datetime) 键集合，不再支持公式过滤） | JsonData（SampleMeta） |
| sample | `get` | `<name>` | `--columns a,b` `--where <谓词>` `--partition <p>` `--exclude-tool` `--limit N` `--offset N` | **ArrowTable**（无 JsonData） |
| sample | `meta` | `<name>` | — | JsonData（SampleMeta） |
| sample | `list` | — | — | JsonData（SampleMeta[]） |
| sample | `set` | `<name>` | `--index <index 名>`（改筛选参照 → 定义键变更置脏） `--display_name/--description/--tags/--source <v>` + 任意键 | JsonData（SampleMeta） |
| sample | `update` | `<name>` | — | JsonData（SampleMeta；传导检查上游 fieldset 链 + 筛选 index 就绪后标记有效，无物化） |
| sample | `check` | `<name>` | — | JsonData（SampleCheckResult） |
| sample | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| feature | `add` | `<name>` | `--engine <e>`（默认 polars） `--formula <表达式>`（必填） `--window_size <N>`（滚动窗口回看宽度，默认 0） `--display_name/--description/--unit/--tags/--source <v>` + 任意键 | JsonData（FeatureMeta） |
| feature | `set` | `<name>` | `--engine/--formula/--window_size/--display_name/--description/--unit/--tags/--source <v>` + 任意键 | JsonData（FeatureMeta；window_size 为定义键，变更置脏下游） |
| feature | `meta` | `<name>` | — | JsonData（FeatureMeta） |
| feature | `list` | — | — | JsonData（FeatureMeta[]） |
| feature | `delete`/`del` | `<name>` | `--force`（下游 factor 依赖存在时） | JsonData `{"deleted"}` |
| feature | `update` | `<name>` | — | JsonData（FeatureMeta；纯定义资产，标记有效） |
| feature | `test` | `<name>` | `--sample <s>`（必选，样本池名） | JsonData（FeatureTestResult）+ ArrowTable（有结果时） |
| factor | `add` | `<name>` | `--feature <f>`（必选，已注册因子公式） `--sample <s>`（必选，已注册样本池） `--engine <e>`（默认 polars） `--pipeline <算子链>`（默认 `nothing()`，`\|` 分隔） `--factor_col <列名>`（默认 = feature 名） + 元数据键 | JsonData（FactorMeta） |
| factor | `get` | `<name>` | `--where <谓词>` `--partition <p>` `--limit N` `--offset N` | **ArrowTable**（§6.2 约定；列 = 样本索引 + 1 因子列） |
| factor | `set` | `<name>` | `--feature/--sample/--engine/--pipeline/--factor_col + 元数据键`（改定义 → 物化失效） | JsonData（FactorMeta） |
| factor | `meta` | `<name>` | — | JsonData（FactorMeta） |
| factor | `list` | — | — | JsonData（FactorMeta[]） |
| factor | `check` | `<name>` | — | JsonData（FactorCheckResult） |
| factor | `update` | `<name>` | `--all` `--resync` | JsonData（FactorScanReport 或 []；显式物化，幂等；传导检查上游 sample/feature 全链就绪，未就绪拒绝更新） |
| factor | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| tester | `add` | `<name>` | `--factor <f>`（必选，已注册因子） `--returns <col>`（默认 `r`） `--groupby <col>`（默认 `ic`） `--marketcap <col>`（默认 `fv`） `--factor_col <col>`（默认 = factor 的 factor_col） `--by_group` `--quantiles N`（默认 5） `--periods p1,p2,..`（默认 `1,5,10`） `--date_range start,end`（默认 `2023-01-01,2026-01-01`） `--rolling_window N`（默认 252） + 元数据键 | JsonData（FactorTesterMeta）；sample 缺 date/sym/returns/groupby/marketcap 列 → 报错 |
| tester | `get` | `<name>` | `--where <谓词>` `--limit N` `--offset N` | **ArrowTable**（测试数据集：date/sym/sample/returns/group/marketcap/factor/d{no}/factor_quantile） |
| tester | `set` | `<name>` | `--returns/--groupby/--marketcap/--factor_col/--by_group/--quantiles/--periods/--date_range/--rolling_window + 元数据键`；`--spec <p1,p2,..>`（简写，等价于 `--periods`）；改配置 → 物化失效 | JsonData（FactorTesterMeta） |
| tester | `meta` | `<name>` | — | JsonData（FactorTesterMeta） |
| tester | `list` | — | — | JsonData（FactorTesterMeta[]） |
| tester | `check` | `<name>` | — | JsonData（FactorTesterCheckResult） |
| tester | `update` | `<name>` | `--all` `--resync` | JsonData（FactorTesterScanReport 或 []；显式物化，幂等；传导检查上游 factor 全链就绪） |
| tester | `delete`/`del` | `<name>` | `--force` | JsonData `{"deleted"}` |
| graph | `lineage` | — | `--node <type:name>` `--columns` `--column <type:name.col>` `--depth N` | JsonData（Cytoscape elements payload，见 §6.13；缺 `--node` 为全图） |
| graph | `nodes` | — | `--type <t>` | JsonData（节点摘要列表：id/type/name/display_name/version/valid/materialized） |
| graph | `columns` | — | `--node <type:name>` | JsonData（列节点清单，见 §6.13） |
| graph | `update` | — | `--node <type:name>` `--all` | JsonData（沿链级联更新报告，见 §6.13） |
| graph | `analyze` | — | `--node <type:name>[,<type:name>...]` | JsonData（page_rank/degree/components，见 §6.13） |
| graph | `impact` | — | `--node <type:name>` `--column <type:name.col>` `--depth N` | JsonData（下游影响，见 §6.13） |
| graph | `stats` | — | — | JsonData `{"node_count","edge_count","column_count","derives_count","belongs_count"}` |

> `table update` 为显式重扫对账（幂等）：无文件差异不 bump 版本；`--all` 批量重扫全部已注册表。
> 内容刷新也可由 `add` 与读取前快检（`_ensure_fresh`）隐式完成。
> `table add` 单表可携带初始元数据（键语义与 `table set` 一致，仅首次注册生效；`--all` 时不适用）。
> 配置了 `dbt-manifest-file`（§5）时 add 先应用 manifest 元数据，参数显式指定覆盖。
> `index add` 同语义：`--all` 批量发现 `index/` 下未登记且含 parquet 的目录（已登记/空目录跳过），
> 返回 `indexes` 数组；批量时 `--symbol-col/--datetime-col` 等参数对全部新发现统一生效。
> 类型由节点 label 承载：table 恒 "table"，index 是独立资产（`index add`）；
> `--type` 参数仅作分类标识进 extra（如 `--type=index` 不再约束 panel 注册）。

### 6.2 `table get` / `index get` / `panel get` 的 ArrowTable.meta

```json
{
  "name": "demo",
  "rows": 3,            // 本次返回行数
  "total": 3,           // 过滤后（未加 limit）总行数
  "columns": [          // 返回列的完整列元数据（来自 graph）
    { "name": "sym", "display_name": "证券代码", "description": "...",
      "data_type": "String", "unit": null, "formula": null,
      "tags": ["key","code"], "as_index": true, "is_tool": false,
      "source_table": "index", "source_field": "sym" }
  ]
}
```

- `rows < total` 当且仅当传了 `--limit`（或 `--offset`）；`--offset N` 跳过起始 N 行
- 非登记列（如 hive 分区键）回退为 `{"name", "data_type"}`；panel 列额外带 `source_table`/`source_field` 血缘

### 6.3 `where` 谓词语法（`--where`）

单列范围谓词（文件级裁剪 + 过滤）：`sym == "a"`（等值）、`price >= 1.0 && price <= 3.0`
（等价 `1.0 <= price <= 3.0`）、`date >= 2024-01-01`（开区间）。支持类型：整数、浮点、
ISO 日期 `YYYY-MM-DD`、字符串字面量。其余写法报错。

### 6.4 `--partition` 语义

- **table / index**：匹配 `partition_path`（hive 目录 `key=value`，精确或 `key=value...` 前缀）
- **panel / factor 等物化资产**：按物化时间桶 `part` 前缀匹配（如 `--partition 2024` 取 2024 年桶）

### 6.5 物化分区策略（继承 index.materialize_partition）

panel/fieldset/factor/tester 物化统一**继承其 index 的 `materialize_partition`**（`yearly` 默认 /
`monthly` / `daily`）按**时间桶**分桶落盘：`part=<YYYY>`（yearly）/ `part=<YYYY-MM>`（monthly）/
`part=<YYYY-MM-DD>`（daily），桶目录 `part=<v>/`（polars `PartitionBy` 原生分区写出，
文件名为序号 `<n>.parquet`；文件内保留 `part` 列，读取 hive 分区还原后**对外剔除**——
get 返回列集合与实时视图一致）。与 index 物理是否分区无关；
`materialize_partition` 未知 / 无时间键 → 单文件兜底。增量物化按 datetime 区间删受影响桶
并保留桶内区间外旧行合并写回（桶粒度粗于区间，见 §11）。**全量物化写前清空物化目录**
——`PartitionBy` 只写数据里存在的桶、不删除新数据中已消失的旧桶目录（整年数据被删后
全量重写会残留陈旧桶的 phantom 行），故全量重写一律先清空再落盘；数据为空时落一个
保留 schema 的空 `data.parquet`（读取路径不因"无 parquet 文件"报错）。

- **粒度引导**：`index add` 登记扫描时若发现 index 数据**跨多年**且粒度仍为默认
  `yearly`，报告附带 `partition_hint` 提示——yearly 桶下增量重写按**整个年份桶**
  替换，跨年大表 + 频繁增量会反复重写整桶，建议 `index set --materialize-partition
  monthly/daily`（下游物化继承，见 §6.5 上）；数据单年或已显式细化粒度则不提示

### 6.6 `stat get` 返回

- **不指定 `--partition_by`**：每个分区一对消息 —— `JsonData{name="stat/<p>", data={"partition","rows","columns"}}` + `ArrowTable`
- **指定 `--partition_by <p>`**：一对 —— `JsonData{name=<target>, data={"partition","rows","columns"}}` + `ArrowTable`
- 分区名：`all`（全量）+ 每个索引列（panel 取 keys；table 取非工具列）
- **`stat scan --partition <p>[,<p>...]`（按需扫描）**：只计算指定分区（未知名报错）——
  coverage 全量 = 每索引列一个分组文件，粗桶大表（千万行 × 数十列）的分区内存/耗时
  随分区数线性放大（每分区一次全表聚合，组数高的列哈希内存可达数 GB），
  日常只需 `all` + 常用索引列（如 `--partition all,date,sym`）可秒级完成；
  局部扫描只覆盖指定分区文件，其余保留（`stat meta` 可见存量）
- **`--kind storage`（存续统计）**：`stat scan table <name>` 只对表磁盘 parquet 做 stat 聚合，
  输出列 `partition_by | partition_value | storage_size | file_no`；`all` 分区为
  `__all__/__all__` 全表总量，其余分区（如 `year`）文件按表 hive 分区键逐值一行

### 6.7 返回数据模型字段

- **TableScanReport**：`name, version_before, version_after, layout(single/flat/hive), partition_by, partition_count, diffs[{rel_path,kind(added/removed/changed),...}], changed, implicit_registered`
- **TableMeta**：`name, version, layout, type(恒 "table"), display_name, description, tags, source, extra, partition_by, partition_count, files[{rel_path,partition,size,mtime_ns}], columns[ColumnMeta], consistent, created_at, updated_at`
- **IndexMeta**：`name, version, layout, symbol_col, datetime_col, materialize_partition, display_name, description, tags, source, extra, partition_by, partition_count, files[], columns[ColumnMeta], consistent, created_at, updated_at`
- **PanelMeta**：`name, version, index(Index 节点 id，如 "index:idx"), tables{成员: join 方式}, keys, columns[ColumnMeta]（index 列在前，成员去重；带 source_table/source_field）, display_name, description, tags, source, extra, created_at, updated_at`
- **StatMeta**：`target_type, target_name, kind, partitions[], files[{partition, rel_path, rows, size}], created_at, updated_at`
- **StatScanReport**：`target_type, target_name, kind, partitions[], files[{partition, rel_path, rows, size}]`
- **ColumnMeta**：`name, display_name, description, data_type, unit, formula, tags[], as_index, is_tool, source_table, source_field`
- **FieldsetMeta**：`name, version, panel(基于的 panel 名), engine, keys[], fields[FieldMeta], materialized, materialized_at, curated, columns[ColumnMeta]（源 panel 列）, extra, display_name, description, tags[], source, created_at, updated_at`
- **FieldMeta**：`name, formula, display_name, description, unit, tags[], validated（是否已 check）, window_size（滚动窗口回看宽度，0=逐行；用于 data change event 范围展开）, required_fields（公式引用的上游列——panel 视图列 ∪ 同集字段，add_field/set_field 时自动计算登记）`
- **FieldsetScanReport**：`name, version, materialized, rows, fields_count`
- **FieldsetCheckResult**：`fieldset, field, ok, message`
- **SampleMeta**：`name, version, fieldset(依赖的 fieldset 名), index(筛选参照 index 名), keys[]（fieldset 底层 panel 主键）, columns[ColumnMeta]（panel 视图列 + fieldset 衍生指标列）, display_name, description, tags[], source, extra, created_at, updated_at`
- **SampleCheckResult**：`sample, ok, rows, columns[], message`
- **FeatureMeta**：`name, version, engine, formula, window_size, display_name, description, unit, tags[], source, extra, created_at, updated_at`
- **FeatureTestResult**：`feature, sample, ok, valid, rows, columns[], message`
- **FactorMeta**：`name, version, feature, sample, pipeline, engine, factor_col, keys[]（样本索引）, partition_by, partition_gran, materialized, materialized_at, curated, columns[ColumnMeta]（源 sample 视图列）, field（Factor 因子列 FieldMeta）, extra, display_name, description, tags[], source, created_at, updated_at`（graph 版按 index.materialize_partition 时间桶物化：partition_by=`["part"]`、partition_gran=`yearly/monthly/daily`）
- **FieldMeta（factor）**：`name, formula（源 feature 公式）, display_name, description, unit, tags[]`
- **FactorScanReport**：`name, version_before, version_after, materialized, changed, partition_by, rebuilt_partitions[]`
- **FactorCheckResult**：`factor, ok, rows, columns[], message`（`ok` 条件：计算成功、含全部索引列、因子列恰好 1 列、行数 > 0）
- **FactorTesterSpec**：`by_group, quantiles, periods[], date_range[]（start,end）, rolling_window`
- **FactorTesterMeta**：`name, version, factor, sample, returns, groupby, marketcap, factor_col, spec[FactorTesterSpec], keys[]（样本索引键）, materialized, materialized_at, curated, columns[ColumnMeta]（sample 视图列 + 测试必需列）, extra, display_name, description, tags[], source, created_at, updated_at`
- **FactorTesterScanReport**：`name, version_before, version_after, materialized, changed, rows, quantiles, periods[]`
- **FactorTesterCheckResult**：`tester, ok, rows, columns[], message`（`ok` 条件：构造成功、含全部必需列、行数 > 0）

### 6.8 fieldset 衍生指标集（公式引擎）

- **指标集** 基于一个已注册 **panel** 创建（`--panel <panel 名>`），keys 继承 panel 主键；
  指标（field）用公式表达式在 panel 视图列上逐行计算
- **公式语言**：运行在列作用域里的 polars 表达式（如 `x*2`、`pl.col("x")*2`、`date.dt.year()`），
  用当前引擎 eval；引擎插件注册制（`register_engine`），当前仅 `polars`
- **校验**：`check` 基于 panel 实时 join 视图求值，**结果行数 == 源行数** 才算通过 →
  指标 `validated=True`（graph 版 check 通过后写回节点，视图/物化只取已校验字段）；
  公式编译/执行失败或行数不一致 → 校验失败（保持未校验）
- **读取**：`get` **默认返回 panel 视图 + fieldset 已校验指标 join 拼接后的完整视图**
  （left join on keys，panel 为左表）；`--fields-only` 只返回衍生数据（keys + 已校验指标）
- **血缘**：table/index → panel → fieldset → sample → factor（sample 另依赖筛选 index）；删除上游需 `--force`
- **生命周期**：指标 add/set 后 `validated=False`；`set --formula` 会复位校验位；
  `fieldset test --formula` 即时求值返回成功/失败 + 结果数据

### 6.9 sample 样本池（fieldset 视图 ∩ 指定 index 键集合，无物化）

- 样本池 = 在 **fieldset 视图**（panel 全列 + 已校验衍生指标）上按**指定 index 的
  (symbol, datetime) 键集合**做 semi join 的**动态产物**：只保留键存在于该 index 数据中
  的行；**没有物化概念**，不落盘，`get`/`check` 每次读取时实时构造
- **`sample add <name> <fieldset> <index>`**：fieldset 为已注册 fieldset（样本内容），
  index 为已注册 index 资产（样本筛选参照——通常是目标研究区间/标的范围的索引表）；
  index 键列名与视图 keys 不同名时按位置映射（symbol → keys[0]，datetime → keys[-1]）
- **构造**（get / check 共用）：读 fieldset 视图 → semi join 该 index 的键列（去重）→
  只保留命中行；样本筛选仅支持按 index 键集合（无公式过滤）
- **`sample check`**：过滤后结果集**包含全部索引列（fieldset 底层 panel keys）且行数 > 0**
  才算有效（如筛选 index 与 fieldset 无交集 → 行数 0 → 不有效）
- **依赖**：sample → fieldset、sample → index（删除上游需 `--force`）；
  `set --index` 改筛选参照（定义键变更置脏，版本递增），读取无需重新校验

### 6.10 feature 因子定义库（纯定义，无物化）

- **因子（feature）** = 一条命名公式（如 `ma5`、`rsi`），登记于 graph（feature 节点），
  **没有物化概念**、不依赖具体表/panel：`add` 只记录 `engine + formula + 元数据`
- **公式语言**：与 fieldset 一致，用 `feature/engine.py` 引擎插件（当前仅 `polars`）
  在样本视图列作用域里 eval，逐行计算
- **`feature test <name> --sample <s>`**：在指定样本池视图（panel 全列 + 已校验指标 +
  筛选 index 键集合）上即时求值 —— 公式执行成功且结果行数 == 样本行数 → `valid=True`
  并返回结果 ArrowTable（单列 `field`）；聚合公式或执行失败 → `valid=False` / `ok=False`
- **`add` 必须提供 `--formula <表达式>`**（空 formula 会被拒绝）；`feature test` 在样本视图上即时求值
- **依赖**：feature 是**纯定义、不依赖任何资产**，删除上游 panel/fieldset/sample 不影响 feature

### 6.11 factor 最终因子（feature 公式 + sample 视图 + pipeline 算子链 + 物化）

- **因子（factor）** = 在 **sample**（fieldset 视图 ∩ index 键集合的动态视图）上
  经 **feature**（命名公式）逐行算出因子列，再经 **pipeline**（算子链）变换后的**最终产物**；
  输出结构恒为「样本索引列 + 一列因子列」（列名 = `--factor_col`，默认取 feature 名）
- **pipeline 算子链**：`|` 分隔的算子调用（如 `nothing()|standardlize()`），每段为 `name()`；
  算子注册制（`register_operator`，当前仅 `nothing()`，原样返回），后续算子按注册即可扩展
- **物化**：`factor update` 落盘 `factor/<name>/part=<v>/`（时间桶，见 §6.5）；
  **幂等**——依赖签名（上游 feature/sample 的 graph 版本 + engine/pipeline/factor_col hash）
  不变则跳过；`--resync` 强制重建
- **批量（`--all`）**：`factor update --all` 同 sample 多因子**共享视图计算、分别
  物化**——按 sample 分组，每组只构建一次 sample 视图（join 链 + 一次 collect，
  列投影 = keys + 组内全部公式引用列并集），组内按引擎分组一次
  `FactorEngine.fields` 算齐全部因子列（polars 单 select，同公式去重共享一列），
  各因子按自己的增量范围过滤后**各自写盘**（增量/幂等语义与单因子一致；
  `FactorEngine.fields` 为批量引擎插件接口，见 `factor/engine.py`）
- **读取**：物化完成且与源+feature+pipeline 一致（`curated`）读物化 parquet；否则实时基于
  sample 视图计算，不隐式物化（显式 `update` 触发）
- **校验**：`factor check` 实时计算——成功、含全部索引列、因子列恰好 1 列、行数 > 0 才算
  `ok=True`；聚合公式（行数 != 样本行数）→ 校验失败
- **依赖**：factor → feature、factor → sample（删除上游需 `--force`）；`set` 改定义键
  （feature/sample/pipeline/factor_col）后物化失效（`materialized=False`、`curated=False`），
  读取自动回退实时计算

### 6.12 factor_tester 因子测试数据集（`tester` 源 + `stat scan ... --kind <测试器>`）

- **测试数据集（tester）** = 在 **factor 关联的 sample** 视图上，结合测试必需列
  （`date/sym` + returns/groupby/marketcap 列）生成的一份因子测试面板；注册于 graph
  （tester 资产）。`tester add` 时若 sample 视图缺少这些列 → **报错拒绝创建**
- **Schema**：`date / sym / sample(1观测/0非观测/-1因子空剔除) / returns / group /
  marketcap / factor / d{no}（sym 内前向累计收益）/ factor_quantile（截面分位，by_group
  时组内）`
- **测试列命名**：`--returns/--groupby/--marketcap`（默认 `r/ic/fv`）指定 sample 视图中的
  收益/分组/市值列名；因子列名取 factor 的 `factor_col`
- **物化**：`tester update` 落盘 `factor_tester/<name>/part=<v>/`（时间桶，见 §6.5）；**幂等**——
  依赖签名（factor 依赖 hash + spec + 测试列名）不变则跳过；`--resync` 强制重建
- **读取**：物化且 curated 读 parquet，否则实时构造（不隐式物化）；`set` 改配置
  （returns/groupby/marketcap/spec 键）后物化失效自动回退实时
- **校验**：`tester check` 实时构造——成功、含全部必需列、行数 > 0 才算 `ok=True`
- **依赖**：tester → factor（删除 factor 需 `--force`）
- **测试器（stat 集成）**：`stat scan tester <name> --kind <kind>` 或
  `stat scan <name> --kind <kind>`（单位置参数简写）运行测试器并把各命名产物写入
  `stat/tester/<name>/<kind>/<output>.parquet`；`stat get` 用 `--partition_by <output>` 读单产物。
  单位置简写在 Execute 与 SubmitTask 两条路径均可用（`s:stat scan <name> --kind <kind>`）
  - `coverage` → `cvg_date`（`date/SF2S/F2T/S2T/X2S` 覆盖率）
  - `ic` → `ic_d{no}`（`IC(d{no})/RankIC(d{no})/GIC(d{no})/RankGIC(d{no})`，按 `date`）
  - `autocorrelation` → `ac_d{no}`（`AC(d{no})/RankAC(d{no})`，按 `date`）
  - `bucket_returns` → `rtn_date`（`date + E(d{no})/SE(d{no})`）/ `exr_date`（`EXR(d{no})`，
    按 `date` 均值中心化）/ `gbr_date`（`GBR(d{no})`，按 `date+group` 组内中心化）
  - `bucket_turnover` → `tr_d{no}`（`TR(d{no})` 分位换手率，按 `date`）
  - `factor_returns` → `fr_d{no}`（`fw_ls/fw_raw/fw_ind/fw_ind_raw/eq_raw/eq_ind/ls/top_raw/
    bottom_raw/ls_ind/hold/mkt` + `*_cum` 累计序列，按 `date`）

### 6.13 `graph` 血缘图（graphqlite 图数据，Execute + CLI；无任务版）

- **数据来源**：`<data-dir>/catalog.db`（graphqlite 嵌入式图库，资产血缘 DEPENDS 边 +
  列级血缘 Column 节点/DERIVES 边 + BELONGS_TO 所属边，见 §2 图设计）；
  库不存在时返回空图（`node_count=0`）
- **`graph lineage`** 返回 Cytoscape.js elements payload（前端可直接渲染）；
  **`--columns`** 叠加列级血缘（涉及资产的 Column 节点 + DERIVES 边 +
  `(column)-[:BELONGS_TO]->(资产)` 所属边，边 data 带 `type` 标注
  `DEPENDS/DERIVES/BELONGS_TO`）；
  **`--column <type:name.col>`**（如 `fieldset:fs1.ma5`）以某列为中心返回列级血缘子图
  （上游来源列 + 下游派生列 + 所属资产上下文）：

```json
{
  "graph": { "exported_at": "…", "center": "panel:ds1" | null,
             "node_count": 7, "edge_count": 6, "types": ["factor", "..."] },
  "elements": {
    "nodes": [{ "data": { "id": "table:index", "type": "table", "name": "index",
                          "label": "index", "version": 1755…, "valid": true,
                          "materialized": false, "meta": { "…": "…" } } }],
    "edges": [{ "data": { "id": "panel:ds1->table:m1", "source": "panel:ds1",
                          "target": "table:m1", "role": "member", "join": "left_join",
                          "required_version": 1755… } }]
  }
}
```

- 节点 `id` = `"<type>:<name>"`（列节点 `"column:<资产 id>.<列名>"`），`type` 决定前端着色；
  边方向 = 依赖方向（依赖方 → 被依赖方 / 派生列 → 源列），`join` 仅 table → panel 边带
- `--node <type:name>` 只导出该节点上下游子图（`--depth N` 限制深度，须为正整数）；
  `graph nodes --type <t>` 供前端中心节点选择器使用（`--type column` 列列节点）
- **`graph columns [--node <type:name>]`**：列节点清单（全部，或指定资产的列，
  含 data_type/formula/as_index 等属性）
- **`graph stats`**：`node_count/edge_count`（资产/DEPENDS 口径）+ `column_count/derives_count`
  （列级血缘口径）+ `belongs_count`（BELONGS_TO 列所属边口径）
- **`graph update [--node <type:name>] [--all]`**：沿链级联 update——`--node` 更新该资产 +
  其**全部下游链**（BFS 闭包含自身），`--all` 按拓扑序更新图中全部资产节点；每个节点都走
  各自 `*_update`（内含上游传导就绪检查），**拓扑序保证任一节点更新时上游已就绪**；闭包外
  上游未就绪 → 报错中止（未更新的节点保持原状）。返回
  `{"node", "scope": "downstream"|"all", "updated": [{"node", "version_before",
  "version_after", "result"}...]}`（`result` 为各 `*_update` 返回值，`version_*` 为统一
  可比口径：未推进 = 该节点本次无真实变更）
- **`graph analyze [--node <type:name>[,<type:name>...]]`**：图算法（**纯 Python 实现**，不依赖
  graphqlite 内置算法）——`page_rank`（有向，DEPENDS 方向：rank 沿边流向被依赖方，
  被更多下游依赖的资产得分更高）/ `degree`（`in_degree`=被依赖的下游数、
  `out_degree`=上游依赖数、`degree` 合计，降序）/ `components`（弱连通分量，
  按 size 降序）；默认全图资产节点（列节点不参与），`--node` 限定该资产
  上下游子图（含自身），**逗号分隔多个 `--node` 取各自闭包的并集批量分析**
  （不存在的节点跳过不报错）；另含 **`consistency`**——列级血缘（DERIVES）↔ 资产级
  血缘（DEPENDS）跨层一致性校验清单：跨资产 DERIVES 边的所属资产之间必须存在
  DEPENDS 路径，不一致即报告（空 = 两层血缘完全吻合，见 §2.3 BELONGS_TO）
- **`graph impact --node <type:name> | --column <type:name.col> [--depth N]`**：
  下游影响分析——`--node` 返回该资产 DEPENDS 下游闭包（`assets`，带 depth）+
  其全部列的 DERIVES 下游列闭包（`columns`）；`--column` 返回以该列为中心的
  DERIVES 下游列闭包（`columns`）+ 受影响列所属资产（`assets`，去重按最小 depth）
- **可视化**：**portal 前端右上角"血缘关系"抽屉**（Tauri 经 gRPC 拉取渲染）

## 7. 后台任务（`s:...`）

### 7.1 提交与事件流

`SubmitTask(source, action, args)` 立即返回 `header + task_id`（`code=0` 成功）。任务在独立
事件循环线程执行。支持的 `source/action` 与 Execute 命令表（§6.1）对齐（table/index/panel/
fieldset/sample/feature/factor/tester/stat/mock 全部动作；`task` 仅 Execute、`graph` 仅
Execute/CLI，见 §1.2），结果放在**终态事件的 `data`**（JSON 字符串）。

```
header(code=0) → TaskEvent×N → EOF
```

每个 `TaskEvent`：`seq`（单调递增）、`time`、`progress`（0~1）、`message`、`data`（终态事件携带结果 JSON）、`state`。

生命周期事件序列（以 `s:panel update ds1` 为例）：

| state | message | 说明 |
|---|---|---|
| `pending` | 任务已创建: panel update | submit 时 |
| `running` | 任务开始: panel update | 开始执行 |
| `running` | ds1: part=2024-01-01（1/2） | 逐分区物化进度（progress=0.5） |
| `running` | ds1: part=2024-01-02（2/2） | progress=1.0 |
| `succeeded` | 任务完成 | progress=1.0，`data`=结果 JSON |

失败：`failed` 事件 `message` 为错误原因；取消：`cancelled`。

### 7.2 状态机

```
pending → running → succeeded
                  ↘ failed
                  ↘ cancelled
        running ⇄ paused（暂停中）
```

### 7.3 TaskControl（`c:<task_id> <action>`）

| action | 语义 |
|---|---|
| `cancel` | **协作式**：pending 直接终态；running 置取消标记，Handler 在检查点（`ctx.is_cancelled()`）抛 `TaskCancelled` 自行退出 |
| `pause` | 置暂停标记 + 状态 `paused`；Handler 在检查点 `wait_if_paused()` 挂起 |
| `resume` | 清暂停标记 + 状态回 `running` |

### 7.4 任务元操作

- `e:task list`：按创建时间倒序，`--state` 过滤。任务项：`task_id, source, action, args, state, progress, created_at, started_at, finished_at, error, result_ref`
- **大结果落盘**：`table/index/panel/fieldset/sample/stat/factor/tester get` 用 `ctx.put_result` 写 `task/<task_id>/<name>`（Arrow IPC / parquet），任务项只存 `result_ref`；`s:... get` 的 `data` 含 `{"name","rows","total","columns","result_ref"}`
- `stop`（服务停止）：先在跑任务统一收尾为 `cancelled`，DB 不遗留 orphan

### 7.5 `mock` 示例任务与造数

- `s:mock`（空 action）：分 5 步推进进度（progress 0.2~1.0）+ 写日志 + 落盘结果 `{"steps":5}`；支持取消与暂停。可作为协议联调样例。
- `s:mock demo` / `s:mock gen <name> --kind <kind>`：任务版 mock 造数，把 parquet 写到 `index/` + `table/`（与 Execute 行为一致，见 §6.1），只写盘不注册。

## 8. gRPC 协议

proto：`src/stkoe/grpc/stkoe.proto`（package `stkoe`），编译产物 `stkoe_pb2*.py`。

### 8.1 RPC 一览

| RPC | 请求 | 响应 | 说明 |
|---|---|---|---|
| `Execute` | `ExecuteRequest` | `stream ExecuteResponse` | 同步命令执行，首条恒为 DataHeader |
| `SubmitTask` | `SubmitTaskRequest` | `SubmitTaskResponse` | 提交后台任务，立即返回 `task_id` |
| `SubscribeTask` | `SubscribeTaskRequest` | `stream SubscribeTaskResponse` | 订阅任务事件流，终态后 EOF |
| `TaskControl` | `TaskControlRequest` | `TaskControlResponse` | `cancel` / `pause` / `resume` |
| `Health` | `HealthRequest` | `HealthResponse` | 存活探活 + 版本 |

### 8.2 消息结构

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
- `ArrowTable.meta`：JSON 字符串，形如 `{"name","rows","total","columns":[...]}`，见 §6.2
- `TaskEvent.state`：`pending` / `running` / `paused` / `succeeded` / `failed` / `cancelled`
- `SubscribeTask.replay=true` 先回放历史事件，否则只推订阅后事件；首条恒为 `DataHeader(code=0)`，任务终态后 EOF

### 8.3 data_dir 透传

`StkoeServer.data_dir` → `_StkoeServicer` → `_execute_stream` → `dispatch(...)`，保证 Execute
与 SubmitTask 用同一数据目录。命令行直接调用（无 data_dir）时回退 `load_config().data_dir`。

### 8.4 serve 运行期缓存与一致性

- **GraphService 连接缓存**：Execute 处理器在**线程本地**缓存 GraphService（key =
  data_dir 真实路径）——同线程内顺序复用同一 SQLite 连接（连接数有界：线程数 ×
  目录数），跨线程各自独立连接（SQLite 文件锁 + `busy_timeout` 兜底）。因此
  **进程内新代码不生效**：修改代码后必须重启 `stkoe serve`
- **读前快检**：table/index `get`/lazy 读取前比对磁盘签名
  （sha256(rel_path|size|mtime_ns)）与登记指纹——一致直接读；不一致**自动重扫对账**
  （铸版本 + 下游置脏）；未登记目录**隐式注册**
- **update 幂等**：`table update`/`index update`/下游物化 update 依赖签名不变 →
  `changed=False` 跳过重建（物化资产另要求 curated 哈希一致，见 §6.11/§6.12）
- **物化读取**：panel/fieldset/factor/tester `get` 在物化且 curated 时读 `part=<v>/`
  物化 parquet；上游变化使 curated 失效 → 自动回退实时视图（数据一致性靠显式
  update 恢复，见 §11）
- **stat 产物独立**：统计文件只落盘 `stat/` 目录，读取按目录直读（无内存缓存）

## 9. 测试客户端（`gclient.py`）

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

示例：`e:table list` / `e:table get demo --where "price >= 1.0" --limit 10` /
`s:panel update ds1` / `s:stat scan panel ds1` / `t:<task_id>`

## 10. 数据存储布局

```
<data-dir>/
├── stkoe.json                 # 配置（可放 cwd）
├── catalog.db                   # 资产库：图节点/边（登记/依赖/版本/血缘）+ 物理指纹普通表
│                              #   （stkoe_data_files / stkoe_file_stats）
├── tasks.db                   # 任务库（TaskStore / EventStore），独立保留
├── task/<task_id>/           # 任务日志 task.log + 结果文件（ResultStore）
├── table/<name>/             # 表（table 资产）parquet（只读，绝不写/删）
├── index/<name>/             # 索引（index 资产）parquet，独立目录（只读，绝不写/删）
├── factor/<name>/            # factor 物化产物（样本索引 + 1 因子列，时间桶 part=<v>/，见 §6.5）
├── factor_tester/<name>/       # 因子测试数据集物化产物（时间桶 part=<v>/，见 §6.5）
├── panel/<name>/             # panel 物化产物（join 视图，时间桶 part=<v>/，见 §6.5）
├── fieldset/<name>/          # fieldset 物化产物（keys + 已校验字段，时间桶 part=<v>/，见 §6.5）
└── stat/<type>/<name>/<kind>/<partition>.parquet   # 统计产物（scan 后进 graph 登记）
```

- **catalog.db vs tasks.db 分离**：catalog.db 管资产（图节点/边 + 物理指纹表，单文件同事务），
  tasks.db 管任务与事件流（高频写与资产低频强一致分开，避免写锁竞争与 WAL checkpoint 干扰）
- **表删除只删登记（graph 节点/指纹），绝不删用户 parquet**（可重新 `add` 发现）；index 资产物理目录为 `index/`（与 table 的 `table/` 分离）
- **stat 进图登记**：`stat scan` 成功后登记图内 `Stat` 节点
  （`stat:<target_type>/<target_name>/<kind>`，含目标引用/分区/文件清单/时间）+
  `(Stat)-[:DEPENDS]->目标` 边（role=target）——graph nodes/lineage/stats 可见，
  目标下游闭包含 stat；**物理文件仍是唯一数据源**，节点是登记镜像（重复 scan
  幂等更新；`stat delete` 同步删节点；目标资产删除时级联清理，删除目标需
  `--force`——有统计引用视为下游）
- **sample 无物化产物**：只登记于 graph（依赖 fieldset + 筛选 index），读取动态构造
  fieldset 视图 ∩ index 键集合
- **feature 纯定义**：只登记于 graph，无任何磁盘产物
- **factor 物化产物**：`factor/<name>/part=<v>/`（仅索引列 + 因子列，时间桶见 §6.5）；
  幂等——上游 feature/sample 版本 + pipeline/factor_col 签名不变则跳过；删除 factor 时一并清理
- **factor_tester 物化产物**：`factor_tester/<name>/part=<v>/`（测试数据集面板，时间桶见 §6.5）；
  测试器产物 `stat/tester/<name>/<kind>/<output>.parquet`（stat 命名输出）；删除 tester 时一并清理

### 10.1 统计输出列

- **覆盖率（ALL_COLS）**：`group | field | data_type | count | null_count | nunique | min | q25 | q50 | q75 | max | mean | min_date | max_date`
- **存续（STORAGE_COLS，`--kind storage`）**：`partition_by | partition_value | storage_size | file_no`

## 11. 增量更新与物化语义（graph 事件驱动）

### 11.1 资产分类：哪些资产会触发物化

| 资产 | update 行为 | 物化产物 |
|---|---|---|
| **table / index**（源头） | 重扫对账（`update`）：物理变化 → 铸版本 + 事件入 version_list + **全链下游置脏**；天然 valid，无物化 | 无（物理 parquet 即数据） |
| **panel** | join 视图**物化落盘**（增量：按积累区间重算受影响时间桶）+ 铸版本 + 水位对齐 | `panel/<name>/part=<v>/`（时间桶，见 §6.5） |
| **fieldset** | 衍生字段（keys + 已校验字段）**物化落盘**（增量同上）+ 铸版本 | `fieldset/<name>/part=<v>/` |
| **factor** | **增量物化**：按源头积累事件区间只重算该区间（受影响桶）并合并写回；`--resync` 全量 | `factor/<name>/part=<v>/` |
| **tester** | **增量物化**（同 factor） | `factor_tester/<name>/part=<v>/` |
| **sample / feature** | 无物化，update 只**铸版本**（消费事件入 version_list）+ 出边水位对齐 | 无（实时构造） |
| **stat** | scan 后登记 `Stat` 节点 + `(Stat)-[:DEPENDS]->目标` 边（无物化/版本语义，手动 `stat scan` 触发） | `stat/<target>/<name>/<kind>/*.parquet` |

读取端（panel/fieldset/factor/tester `get`）：**物化且 curated 读物化 parquet，否则实时计算**
（curated = 已物化且依赖签名 == 当前签名；上游版本变化 → curated 失效自动回退实时）。

### 11.2 遇到新数据变更事件时，分别如何处理

1. **源头物理变化**（`table/` 或 `index/` 目录新增/修改/删除 parquet 文件）
   → `table update <name>`（或读取前的快检自动重扫）对账，`diff_files` 得到文件级 diff：
   - added / changed 文件 → **upsert 事件**；removed 文件 → **delete 事件**
     （一次变更同时有增删时，两类事件各记一个版本）；
   - 事件带 **datetime 区间 `[min, max]`**：hive 分区键 = datetime_col 时用分区值，
     否则读变化文件 footer 的 datetime 列 min/max（只读元数据、不读数据页）；
     取不到范围 → 全集（None）；
   - 事件带 **symbol_scope（变化标的集合）**：登记了 `symbol_col` 的资产（index）——
     hive 分区键 = symbol_col 时用分区值；小文件（≤50 万行，真实日更增量文件的量级）
     读该列 distinct；大文件全量重写/removed 文件取不到时回退 None（全集，增量退化为
     纯时间裁剪——大文件读全列 distinct 的代价远大于按标的裁剪的收益）；
     未登记 symbol_col 的资产（table）→ None（全集）；
   - `notify_change`：源头**铸版本 + 事件入 version_list + BFS 全链下游置脏**
     （valid=False，materialized=False）。
2. **逐级 update 恢复**（必须按依赖顺序，先上游后下游；`assert_ready` 检查全链就绪）：
   - `panel update`：join 视图重新物化落盘（**增量**——按积累事件「datetime 区间 ×
     symbol 集合」删受影响时间桶/行并保留未变化标的行合并写回，见 §6.5）；
     有积累事件 → 铸版本（合并事件入 version_list）；出边 `required_version` 对齐
     被依赖方当前版本；
   - `fieldset update`：衍生字段重新物化（增量同上）+ 铸版本；
     **窗口展开**：字段带 `window_size`（滚动回看窗口 w）时，输入在 `[lo, hi]` 变化
     会让输出 `[lo, hi+w-1]` 都受影响——增量重算区间与自身事件 `datetime_scope`
     都按最大窗口向前展开（fieldset 字段 / feature 公式的 `window_size` 同理：
     factor 增量重算与自身事件按 feature 窗口展开；tester 的 `d{no}` 是前向收益
     窗口，按 `max(periods)-1` 向后展开 lo）；窗口只影响时间维度，symbol 集合
     沿链原样透传（自身事件带 symbol_scope）；
   - `sample update` / `feature update`：只铸版本（无物化）；
   - `factor update`：**增量**——从全部源头（table/index）收集
     `version > consumed` 的积累事件，得 datetime 区间 + symbol 集合；已有物化且
     区间明确 → 读旧物化删「区间 × 标的」命中行（受影响桶）+ **仅重算命中行**
     合并写回；无区间 / 首次 / `--resync` → 全量重算；成功后记录各源头水位
     （`extra.consumed_versions`）；
   - `tester update`：同 factor（在 sample 视图上按区间 × 标的构造）。
3. **幂等**：update 时节点 valid 且依赖签名一致 → 直接返回 `changed=False` 跳过重建；
   上游变化已把 valid 置 False，因此再次 update 必然重建（保证不数据过期）。
4. **读取**：`get` 时物化且 curated 读物化；curated 失效（签名变化）自动回退实时，
   数据一致性靠显式 update 恢复。

```text
数据流（一次变更）：
  table/index 文件变化
     │  table update（重扫对账）
     ▼
  源头节点：版本 +1，version_list 记录 upsert/delete 事件（带 datetime 区间）
     │  notify_change：BFS 全链置脏（valid=False）
     ▼
  panel（物化）→ fieldset（物化字段）→ sample/feature（铸版本）→ factor（增量）→ tester（增量）
     │  每级 update：assert_ready 检查上游就绪 → 按自身语义物化/铸版本 → 水位对齐
     ▼
  get 读物化（curated）或实时；graph lineage 显示新版本
```

## 12. 典型工作流

全流程可复制演练见 **example.md**（mock 造数 → 物化 → 测试器 → 清理）。此处为精简主干：

```bash
# mock 造数（写 index/index + table/m1，不注册）
stkoe mock demo
# 发现源头资产（index 独立资产；materialize_partition 默认 yearly，见 §6.5）
stkoe index add index --symbol-col sym --datetime-col date
stkoe table add m1
# 逻辑数据集（panel：index + 成员表 join，keys 由 index 推断）
stkoe panel add ds1 index m1
stkoe panel update ds1                      # 物化 panel/ds1/part=<YYYY>[/<MM>[/<DD>]]/
# 衍生指标集（check 通过 → validated；update 物化 keys + 已校验指标）
stkoe fieldset add fs1 --panel ds1
stkoe fieldset add fs1 x2 --formula "x * 2.0"
stkoe fieldset check fs1 x2
stkoe fieldset update fs1
# 样本池（fieldset 视图 ∩ 筛选 index 键集合，无物化）+ 因子定义库（命名公式，无物化）
stkoe sample add sp1 fs1 index
stkoe sample check sp1
stkoe sample update sp1
stkoe feature add ma5 --formula "x * 2.0"
stkoe feature test ma5 --sample sp1
stkoe feature update ma5
# 最终因子（物化 factor/fac1/part=<v>/）+ 测试数据集（物化 factor_tester/t1/part=<v>/）
stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
stkoe factor check fac1
stkoe factor update fac1
stkoe tester add t1 --factor fac1 --returns r --groupby ic --marketcap fv
stkoe tester check t1
stkoe tester update t1
# 因子测试器（stat 集成；单位置简写 → tester 目标）
stkoe stat scan t1 --kind ic
# gRPC 读取（物化且 curated 读物化 parquet，对外列不含 part）
gclient> e:panel get ds1 --where "date >= 2024-01-01" --limit 100
gclient> e:fieldset get fs1 --limit 100
gclient> e:sample get sp1 --limit 100
gclient> e:factor get fac1 --limit 100
gclient> e:tester get t1 --limit 100
gclient> e:stat get t1 --kind ic --partition_by ic_d1
# 后台物化 + 订阅进度（任务版）
gclient> s:fieldset update fs1
gclient> t:<task_id>
```

## 13. portal 前端调用流程指南

> 本节面向 **portal（Tauri 前端）项目的 Agent**：portal **不直接管理数据目录**，
> 所有资产/血缘/数据操作都经 gRPC Execute 通道调用运行中的 stkoe 服务
> （默认 `127.0.0.1:9569`，用户自启 `stkoe serve`）完成。下方按功能场景给出
> 推荐调用顺序与关键参数，前端按此实现即可。

### 13.1 通用调用方式

- 单个命令 = 一次 `ExecuteRequest{source, action, args}`（args 为扁平字符串数组，
  形如 `["<name>", "--key", "value"]`；参数解析见 §6）；
- 响应为流式：首条恒为 `DataHeader`（`code=0` 成功；非 0 业务错误，`message` 含原因），
  随后 0..N 条 `JsonData`（`{name, data}`，data 为 JSON 字符串）或单条 `ArrowTable`
  （IPC bytes，仅 `* get` 类命令返回；meta 见 §6.2）；
- **data_dir 一致性（关键）**：CLI（`stkoe <cmd>`）与服务的 data_dir 都来自同一份
  `stkoe.json` 配置（默认 `~/.stkoe`）。若用 CLI 添加了资产、再用 portal 查血缘，
  必须确保两者指向同一 data_dir，且服务用**最新代码**重启过。

### 13.2 推荐调用流程（按页面/功能组织）

#### A. 启动与健康检查
1. `e:version`（空 action）→ 版本号；失败说明服务未启动或端口不对；
2. `e:config get` → 生效配置（含 `data-dir`），用于与 CLI 目录核对。

#### B. 资产浏览（列表页 / 详情）
1. 源头：`e:table list` / `e:index list`（JSON 数组）；
2. 派生：`e:panel list` / `e:fieldset list` / `e:sample list` / `e:feature list` /
   `e:factor list` / `e:tester list`；
3. 单个详情：`e:<source> meta <name>`（完整元数据：列、版本、valid、
   materialized/curated 等）。

#### C. 血缘图（右上角抽屉 / 完整页）
1. `e:graph nodes` → 全部节点摘要（id/type/name/version/valid/materialized），
   供"中心节点"选择器；
2. `e:graph lineage`（缺 `--node` 全图）或 `e:graph lineage --node <type:name>
   --depth N`（子图）→ Cytoscape elements payload（§6.13），前端用 Cytoscape.js 渲染；
3. `e:graph stats` → 节点/边计数（空图提示用）。

#### D. 数据读取（详情表格）
- `e:table get` / `e:index get` / `e:panel get` / `e:fieldset get` / `e:sample get` /
  `e:factor get` / `e:tester get` → **ArrowTable（IPC）**，meta 含 rows/total/columns
  列说明；`--where` / `--limit` / `--offset` / `--columns` 分页过滤。

#### E. 资产创建（向导/表单）
- 源头：`e:table add <name>`（发现 `table/<name>/` 下的 parquet）；
  `e:index add <name> --symbol-col sym --datetime-col date`；
- 面板：`e:panel add <name> <index> [member[:join]...]`——member 可带
  `:asof`/`:left` 指定 join 方式（缺省 asof），keys 由 index 推断、不传 `--keys`；
- 衍生：`e:fieldset add <name> --panel <panel>` →
  `e:fieldset add <name> <field> --formula <expr>` → `e:fieldset check <name> <field>`
  （check 通过才参与物化）；
- 样本：`e:sample add <name> <fieldset> <index>`（样本池 = fieldset 视图 ∩ index 键集合）；
- 因子：`e:feature add <name> --formula <expr>`；
  `e:factor add <name> --feature <f> --sample <s> [--pipeline <链>]`；
- 测试集：`e:tester add <name> --factor <fac> [--returns/--groupby/--marketcap]`。

#### F. 更新 / 就绪（数据变化后刷新）
1. **源头变化**：`e:table update <name>`（或 `e:index update`）
   → 重扫对账；内部自动把变化写入版本事件并**置脏整条下游链**；
2. **链路就绪**：按依赖顺序依次
   `e:panel update <name>` → `e:fieldset update <name>` → `e:sample update` /
   `e:feature update` → `e:factor update` → `e:tester update`；
   **顺序不可乱**：上游未就绪时 `update` 报 `DependencyError`（message 会指出先 update 谁）；
3. 之后重拉 `e:graph lineage` / `e:<source> meta` 即可看到新版本与新数据。

### 13.3 注意事项

- `* get` 返回 ArrowTable，其余返回 JsonData；先判 `DataHeader.code` 再取数据；
- 后台耗时操作（物化）可 `s:<source> <action>` 提交任务，用 `t:<task_id>` 轮询或
  SubscribeTask 订阅进度（§7）；
- 哪些资产 update 会物化、如何按事件区间增量，见 §11；
- portal 不应自行读写 `data_dir`/`catalog.db`——一律经服务 Execute 通道。

## 14. 测试

```bash
uv run pytest -q        # 默认全量 278 用例（graph 模块 + gRPC/资产任务版链路）
```

- 改动后优先只跑相关文件：`.venv/Scripts/python.exe -m pytest tests/test_graph.py tests/test_grpc.py -q`

## 15. 目录结构

```
src/stkoe/
├── cli.py / args.py / jsonutil.py / logutil.py / settings.py / dbt.py
├── grpc/               # stkoe.proto + 编译产物 + dispatch.py（Execute 分发）+ server.py
├── storage/            # 数据存储访问层：polars parquet 读写与数据计算的标准接口
│   │                   #   scan（读）/ write_all·write_incremental·write_incremental_flat
│   │                   #   （全量/增量物化）/ layout·meta（布局/指纹）/ query（裁剪）/
│   │                   #   calc_stats·calc_storage（统计）——替换 DuckDB 等只改本层
│   ├── read.py         # scan：目录（hive）/单文件/文件列表 → LazyFrame（谓词/列裁剪/剔除内部列）
│   ├── write.py        # write_all（全量：单文件/时间桶 PartitionBy/clean）
│   │                   # write_incremental（分区桶增量）/ write_incremental_flat（flat 增量）
│   ├── layout.py       # disk_files / detect_layout / partition_of / hive_value
│   ├── meta.py         # footer / signature / diff_files / columns_union
│   ├── query.py        # parse_pred / to_expr / prune_files（SQL 文件级裁剪）
│   ├── calc.py         # calc_stats（ALL_COLS）/ calc_storage（STORAGE_COLS）
│   └── spec.py         # FileInfo / FileDiff / ColumnMeta / TableLayout
├── table/ …/ factor_tester/    # 资产模块：ops.py = 资产业务实现（GraphService 薄委托入口），
│                               #   handlers.py = 任务版 Handler，engine/spec = 领域组件
├── graph/              # 资产血缘图（graphqlite）+ GraphService
│   ├── model.py        # DataChangeEvent / AssetMeta / DependencyEdge / 列元数据
│   ├── store.py        # GraphStore：节点/边 CRUD + BFS 血缘遍历 + txn 事务
│   ├── export.py       # build_payload / column_payload / node_summaries（→ Cytoscape elements JSON）
│   ├── events.py       # 事件合并（并集/交集）与积累（required_version 水位线）
│   ├── controller.py   # GraphController：CRUD + 依赖约束 + notify_change/resolve(_all)
│   ├── service.py      # GraphService：图交互 + 共享基础设施（登记/事件/列解析/级联
│   │                  #   update）；各资产公共 API 仅薄委托到对应模块 ops.py
│   ├── materialize.py  # 物化计划（时间桶分区方案/沿链 index 定位；读写落盘走 storage）
│   ├── handlers.py     # 资产 Handler（图账本层）
│   ├── analyze.py      # 图算法 + 影响分析（page_rank / degree / components / impact）
│   ├── version.py      # 高精度时间戳版本号 + now_iso
│   └── errors.py
└── task/               # 后台任务框架
```

## 16. 文档索引

- **本文件（README.md）**：唯一入口——数据资产与图设计（§2）、对外 API 全量说明
  （§5-§13）、配置、存储布局、测试、路线图
- [`example.md`](example.md)：全流程演练（mock 造数 → 因子测试）
- `AGENTS.md`：开发指南（目录结构/架构要点/变更记录）
