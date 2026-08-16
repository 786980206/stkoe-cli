# stkoe V3.0 图设计（血缘关系图）

> 本文档是 V3.0 重构的图设计蓝图：用**嵌入式图数据库 graphqlite**
> （[colliery-io/graphqlite](https://github.com/colliery-io/graphqlite)，SQLite 扩展，
> Cypher 查询 + 图算法）记录资产之间的**血缘关系（lineage）**。
> 实现代码在 `src/stkoe/graph/`；本阶段**不接真实物理数据存储**（parquet 读取/物化），
> 先把「图的增删改查 + 版本 + 事件响应」全流程跑通。

## 0. 背景与目标

V2.0（已备份至 `V2.0/`）用 SQLite catalog 的 `stkoe_depends` 表记录**一跳**依赖
（`obj_type/obj_name → dep_type/dep_name/detail`），无法回答：
- 全链路血缘（某个表影响了哪些下游？某个因子依赖哪些上游？）
- 版本级更新事件积累与增量传播（上游数据变了，下游该重算哪些范围？）

V3.0 将资产与依赖整体建模为**图**：

| V2.0 概念 | V3.0 图概念 |
|---|---|
| `stkoe_objects`（type+name 登记） | **节点**（label = 资产类型，`id = "<type>:<name>"`） |
| `stkoe_depends` 出边（obj→dep） | **DEPENDS 边**（依赖方 → 被依赖方，带 `required_version` + detail） |
| `deps_of(obj)` 出边查询 | 上游遍历：`MATCH (n)-[:DEPENDS*1..N]->(m)` |
| `dependents(obj)` 入边查询 | 下游遍历：`MATCH (n)<-[:DEPENDS*1..N]-(m)` |
| 版本（int，元数据变更递增） | 节点 `version` + `version_list`（version → DataChangeEvent） |
| 物化失效（materialized=False） | `valid/materialized` 标记 + 边 `required_version` 漂移检测 |

## 1. 节点模型（AssetNode 族）

### 1.1 通用节点属性（所有资产）

graphqlite 节点：**label = 资产类型**；**`id` 属性 = `"<type>:<name>"`**（类型+名字唯一）；
其余属性：

| 属性 | 类型 | 说明 |
|---|---|---|
| `name` | str | 资产名（用户可见，类型内唯一） |
| `display_name` | str | 展示名 |
| `description` | str | 描述 |
| `tags` | list[str] | 标签 |
| `source` | str | 来源 |
| `version` | int | **当前版本**（高精度时间戳，纳秒；单调递增，见 §3.1） |
| `version_list` | dict[int, event] | **版本事件日志**：version → DataChangeEvent（见 §3） |
| `materialized` | bool | 是否已物化（有物理产物） |
| `valid` | bool | 是否有效（上游变化后未重算 → False） |
| `create_time` / `update_time` | str | ISO 时间 |
| `extra` | dict | 任意扩展键 |

### 1.2 类型专属属性（对应 v3.0-def.py）

| Label | 节点类 | 专属属性 |
|---|---|---|
| `Table` | TableNode | `columns`（ColumnMeta[]） |
| `Index` | IndexNode | `columns`、`symbol_col`、`datetime_col`、`materialize_partition` |
| `Panel` | PanelNode（≈ V2.0 dataset） | `index`（Index 节点 id）、`tables`（{name: join 类型}）、`keys` |
| `Fieldset` | FieldsetNode | `dataset`（Panel 节点 id）、`fields`（{field: FieldMeta}） |
| `Sample` | SampleNode | `fieldset`（Fieldset 节点 id）、`engine`、`formula` |
| `Feature` | FeatureNode | `engine`、`formula`、`unit` |
| `Factor` | FactorNode | `feature`（节点 id）、`sample`（节点 id）、`engine`、`pipeline`、`factor_col` |
| `Tester` | TesterNode | `factor`（节点 id）、`returns/groupby/marketcap`、`spec`（quantiles/periods/…） |
| `Model` | ModelNode | （预留） |
| `Stat` | StatNode | （预留） |

ColumnMeta / FieldMeta 结构沿用 V2.0：
`name, display_name, description, data_type, unit, formula, tags, as_index, is_tool, source_table, source_field`。
**列级血缘**：本阶段在 DEPENDS 边的 `detail` 中记录（如 Panel→Table 边 detail 记录该表贡献了哪些列），
不建独立列节点；后续可升级为 `(column) -[:DERIVES]-> (column)` 列节点图。

## 2. 边模型（DependencyEdge / DEPENDS）

- **关系类型**：`DEPENDS`
- **方向**：`(依赖方) -[:DEPENDS]-> (被依赖方)` —— 与 V2.0 `stkoe_depends` 语义一致：
  出边 = 我依赖谁（上游），入边 = 谁依赖我（下游）。
- **边属性**：

| 属性 | 类型 | 说明 |
|---|---|---|
| `required_version` | int | 依赖方已消费的被依赖方版本（物化时对齐，见 §4） |
| `detail` | dict | 角色与映射：`{"role": "index"/"member"/"panel"/"fieldset"/"feature"/"sample"/"factor", "join": ..., "fields": [...]}`；`join` 仅 table → panel 边带 |
| `create_time` | str | 建边时间 |

### 2.1 典型血缘子图

```
Index:index ──DEPENDS(role=index)─────────────────▶ Panel:ds1
Table:m1  ───DEPENDS(role=member, join=asof_join)─▶ Panel:ds1
Panel:ds1 ───DEPENDS(role=panel)──────────────────▶ Fieldset:fs1
Fieldset:fs1 ─DEPENDS(role=fieldset)──────────────▶ Sample:sp1
Index:idx2 ──DEPENDS(role=index)──────────────────▶ Sample:sp1   ← 样本筛选参照
Sample:sp1 ──DEPENDS(role=sample)─────────────────▶ Factor:fac1
Feature:ma5f ─DEPENDS(role=feature)───────────────▶ Factor:fac1
```

- **血缘链**：`table / index → panel → fieldset → sample → factor`（另有 feature → factor）。
- **join 只出现在 table → panel 边上**：panel 以 index 为索引去 join 其他成员表，
  仅 ``role=member`` 边带 ``detail.join``（``asof_join`` 缺省 / ``left_join``）；
  ``role=index`` 边**不带 join**。成员表 join 方式在 ``panel add`` 时按
  ``member:asof``/``member:left`` 指定（缺省 asof）。
- **sample 基于 fieldset 衍生 + 按筛选 index 键集合裁剪**（`sample add <name> <fieldset>
  <index>`：只保留 (symbol, datetime) 键存在于该 index 数据中的行，不再支持公式过滤），
  不直接依赖 panel；sample → index 边 role=index（**筛选参照**，无 join）。
- Panel 建节点时**同时建边**：`Panel → Index`（role=index）、`Panel → 每张成员表`（role=member）；
  边的 `required_version` 初始 = 被依赖方当前版本。
- 删除约束：**节点存在入边（下游依赖）时禁止删除**（除非 `--force`，force 时先删下游边/节点）。
  删除节点时一并删除其出边（依赖关系随之消失）。

## 3. 版本与事件（DataChangeEvent）

### 3.1 版本

- **版本号 = 变更时刻的高精度时间戳**：`time.time_ns()` 纳秒（int），可直接看出变更时间、
  带业务含义；同一纳秒或时钟回拨时以上次版本 +1 兜底，保证严格单调。
  `version_list` 记录 `{version: event}`，即「这个版本发生了什么数据变化」。
- **变更即版本**：任何改变节点**定义/数据**的操作（table 数据变化、set 定义键、物化重算）
  都会铸新版本号并把对应 DataChangeEvent 追加进 `version_list`。

### 3.2 DataChangeEvent

```
{
  "field_scope":    list[str] | None,   # 影响的字段范围；None = 所有字段
  "symbol_scope":   list[str] | None,   # 影响的标的范围；None = 所有标的
  "datetime_scope": list[Any] | None,   # 影响的时间范围（如 [start, end]）；None = 所有时间
  "action":         "upsert" | "delete"
}
```

物理表每次数据变化 = 对「时间范围 × 标的 × 字段」的一批 upsert / delete。
Table/Index 通过 `notify_change(event)` 登记（本阶段不接真实数据源，由调用方显式触发）。

### 3.3 事件合并（accumulate）

「积累的更新事件」= 上游 `version_list` 中 `version > 边.required_version` 的所有事件**合并**：

- 按 `action` 分两类（upsert / delete），同类合并：
  - `symbol_scope` / `datetime_scope` 取**并集**（None = 全集，吞并一切）
  - `field_scope` 取**交集**（None = 全集）—— 只有共同受影响的字段才需要一起重算
- 输出恒为 `{"upsert": merged_event, "delete": merged_event}`（可为空）。

## 4. 增删改查 + 事件响应流程

### 4.1 节点 CRUD（GraphController / handlers）

| 操作 | 流程 | 约束 |
|---|---|---|
| `add` | 铸版本（时间戳）→ 建节点（label=类型）→ 返回节点 | 同 `type:name` 已存在 → `AssetExistsError` |
| `add + 建边`（Panel/Fieldset/Sample/Factor/Tester） | 先建节点，再逐条建 DEPENDS 边（required_version = 被依赖方当前版本），**同一事务** | 被依赖方不存在 → `AssetNotFoundError` |
| `get` | 读节点（+ 可选上下游血缘） | 不存在 → `AssetNotFoundError` |
| `meta` | 读节点全量属性（含 version_list） | 同上 |
| `set` | patch 属性；若改到**定义键**（formula/pipeline/成员表…）→ 铸新版本 + 记事件 + **下游失效** | 不存在 → 报错；不隐式注册 |
| `col`（Table/Index/Fieldset 字段） | 改 columns/fields 内字段元数据 → 版本递增 | 字段不存在 → 报错 |
| `delete` | **无入边**（无下游）才可删：删出边 + 删节点；`force` 时先递归删下游 | 有下游且非 force → `DependencyError` |
| `list` | 按 label 列出节点摘要 | — |

### 4.2 事件响应（血缘传播）

```
上游数据变化
   │  TableHandler.notify_change(table, event)
   ▼
① 铸新版本 + 事件追加进 table.version_list
② 传播：BFS 沿入边（下游方向）标记所有传递下游
     valid=False, materialized=False, stale（记录触发源）
   ▼
③ 下游更新（显式 update / resolve / resolve_all 拓扑重算）
    对每个出边（依赖）：取 (边.required_version, 依赖方.version] 的事件 → 合并
     → 调 storage 物化钩子（本阶段：记录性 no-op）
     → 铸本节点新版本 + 合并事件写入 version_list
     → 出边 required_version 对齐 = 依赖方当前版本
     → valid=True（物化成功则 materialized=True）
```

要点：

- **边 = 消费水位**：`required_version` 是「已消费到哪个版本」的水位线；
  水位线以下的事件在下一次 `update` 时被合并取出，天然支持**多次变更一次重算**。
- **传播只置脏、不算账**：`notify_change` 只沿下游 BFS 置 `valid=False`；
  「算出积累事件」发生在 `resolve/update` 时（v3.0-def.py `PanelHandler.update` 语义）。
- **拓扑顺序**：`resolve_all` 按「先依赖后依赖方」拓扑序处理，避免下游读到半新状态；
  存在环时中止并报错（血缘图应为 DAG）。

### 4.3 GraphHandler（图级查询）

| 操作 | 查询 | 说明 |
|---|---|---|
| `graph list <type>` | `MATCH (n:<Type>) RETURN n` | 节点列表 |
| `graph get <type> <name>` | 入边/出边 + 变长遍历 | 完整上下游血缘：`{node, upstream[], downstream[]}` |
| `graph upstream <type> <name> [--depth N]` | `MATCH (n)-[:DEPENDS*1..N]->(m)` | 上游（依赖链） |
| `graph downstream <type> <name> [--depth N]` | `MATCH (n)<-[:DEPENDS*1..N]-(m)` | 下游（影响链） |
| `graph stale` | 查询 `valid=false` 节点 | 待重算清单 |

## 5. 存储层（graphqlite）落地

### 5.1 选型结论（已实测验证）

- graphqlite = SQLite 扩展（C），PyPI `graphqlite>=0.6.0`，`py3-none` 平台 wheel
  （win_amd64 / manylinux / macosx），内置 `graphqlite.dll`，**无 Python 依赖**，
  Requires-Python >= 3.8；Windows + CPython 3.13（SQLite 3.47.1）实测可加载。
- 入口：`graphqlite.connect(db_path)` → `conn.cypher(query, params)`；
  高阶 `graphqlite.graph(db_path)` 提供 `upsert_node/get_node/delete_node/upsert_edge/
  get_edges_from/get_edges_to/stats` 及图算法（pagerank/community/paths/…）。
- **实测能力**（本仓库 `graph/` 实现依赖）：
  - 变长路径遍历 `-[:DEPENDS*1..5]->`（上下游血缘）✅
  - `WHERE / SET / DELETE / DETACH DELETE / MERGE / collect / ORDER BY / LIMIT` ✅
  - `$param` 参数、嵌套 dict/list 属性、多 label ✅
  - **事务**：Cypher 内不支持 BEGIN/ROLLBACK，但用原生 SQL `BEGIN`/`COMMIT`/`ROLLBACK`
    包裹多语句 cypher 写入可整体回滚（已实测）→ 控制器用 `txn()` 保证「建节点+建边」原子性
  - 文件型持久化（连接重开数据仍在）✅
- 官方文档：[Getting Started (Python)](https://colliery-io.github.io/graphqlite/latest/tutorials/getting-started.html)、
  [Python API](https://colliery-io.github.io/graphqlite/latest/reference/python-api.html)、
  [Knowledge Graph](https://colliery-io.github.io/graphqlite/latest/tutorials/knowledge-graph.html)

### 5.2 存储布局

`<data-dir>/catalog.db`（单文件 SQLite + graphqlite 扩展）。节点/边数据由扩展内部表承载，
应用层只通过 Cypher 访问。`GraphStore` 封装：

```
GraphStore(db_path)
├── txn()                       # BEGIN/COMMIT/ROLLBACK 上下文
├── node ops: create_node / get_node / patch_node / delete_node / list_nodes(label)
├── edge ops: create_edge / get_edge / patch_edge / delete_edge
│             edges_from(id) / edges_to(id) / edges_between(src, tgt)
└── traversal: upstream(id, depth) / downstream(id, depth) / stale_nodes()
```

### 5.3 与既有代码的关系

- `graph/` 已**全面接管资产登记**：table/index/panel/fieldset/sample/feature/factor/test 的
  登记/依赖/版本全部走图节点/边（`GraphService`，见 `graph/service.py`）；V2.0 的
  table/dataset/… controller 保留在 `src/` 与 `V2.0/` 作为参考实现，新代码不再走 catalog.db。
- Execute（dispatch）与 SubmitTask（任务版 handler）三路径已对齐到 GraphService；
  物理数据读取/物化由 GraphService 直接做（parquet + 指纹），stat 数据源亦走 graph。

## 6. 本阶段交付

- [x] V2.0 全量备份（代码/测试/文档）
- [x] graphqlite 调研 + 落地到 `.venv`（wheel 0.6.0）+ pyproject 依赖声明
- [x] 图设计文档（本文档）
- [x] `src/stkoe/graph/`：model / errors / store / version / events / controller / handlers
- [x] 测试 `tests/test_graph.py`：节点 CRUD、依赖约束、版本/事件积累、血缘传播、
      handler 全流程
- [x] **gRPC Execute 通道输出**：`graph lineage/nodes/stats` 命令（api.md §3.13），
      portal 前端"血缘关系"模块（右上角抽屉 + 展开完整页面，Cytoscape.js 渲染）
- [x] **GraphService 全面接管**：table/index/panel/fieldset/sample/feature/factor/test
      三路径（Execute + SubmitTask + CLI）统一走 graph；catalog.db 废弃（物理指纹表
      迁入 catalog.db 普通表）；factor/test 物化落盘 factor/、factor_test/

## 7. 下一步（路线图）

- [ ] **panel 物化**：panel scan 落盘 + index 唯一性校验等物理细节
- [ ] **V2.0 清理**：任务版 table/dataset handler 切 graph、V2.0 controller 死代码评估
- [ ] **列级血缘**：DEPENDS 边 detail 的字段映射升级为独立列节点图
      （`(column) -[:DERIVES]-> (column)`）
- [ ] **版本/事件沉淀**：version_list 过期裁剪、跨依赖事件精确并集
- [ ] **图算法**：graphqlite 内置算法（PageRank/中心性/连通分量）做资产重要性分析
- [ ] **图模块边界测试 + gRPC 全链路回归**
