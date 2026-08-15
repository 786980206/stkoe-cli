# V3 定义 vs 当前实现 出入清单（对照 v3.0-def.py）

> 对照基准：仓库根 `v3.0-def.py`（V3.0 初始设计定义）+ `graph-design.md`。
> 结论：**Event 增量更新语义（版本水位 + 范围化事件 + 增量物化）在设计上已实现大半，
> 但"物理变化 → 范围化事件"的入口与"事件 → 增量物化"的出口两条链路未接通**，
> 中间件（accumulate/merge/水位对齐）是通的但被"全量重算"绕过。

---

## 一、Event 增量更新逻辑出入（重点）

### E1【核心】物理变化 → 事件丢失 symbol/datetime 范围

- **定义**（v3.0-def.py 顶部注释）："每一次原始物理表的数据变化，本质上就是对**某一个时间范围内的一批标的**的指定字段数据的 upsert 或 delete"——事件必须带 `symbol_scope` / `datetime_scope` / `field_scope` / `action`。
- **当前**：`service._scan_disk` 的 `notify_change` 只构造
  `DataChangeEvent(action="upsert", field_scope=[新列名])`——**只有字段名，没有 symbol/datetime 范围**；文件级 diff（`T.diff_files`）已能区分文件增删，但没有把"新增/变化文件里覆盖的标的与日期范围"提取进事件。
- **后果**：`version_list` 里的事件信息量不足以支撑"从上游重算哪个范围"的增量物化；下游即使 accumulate 也只知道"哪些字段变了"，不知道"哪些标的时间段要重算"。

### E2【核心】delete 事件完全缺失

- **定义**：`action: Literal["upsert", "delete"]`，物理删除 → delete 事件 → 下游按范围删对应数据。
- **当前**：物理文件被删（diff 含 removed）时，事件仍是 `action="upsert"`（field_scope 为当前新列，甚至为空列表）——**下游永远收不到 delete 事件**，被删的标的/时间在派生资产里无法清除（全量重算时才能抹掉）。

### E3【核心】增量物化未落地（update = 全量重算 / 标记有效）

- **定义**（PanelHandler.update → materialize）：积累事件合并出 `{upsert, delete}` → **按范围从上游提取增量数据 upsert 进现有物化存储、按范围 delete 现有数据** → 边水位对齐 + 节点有效。
- **当前**：
  - panel / sample / feature：无物化，update 只 `assert_ready` + 实时构造 + `patch valid=True`（panel 的"物化"语义已被"实时 join 视图"替代——README 路线图把 **panel 物化（scan 落盘）** 列为待办，属已知未完成项）；
  - fieldset / factor / test：update = **全量重算**落盘（幂等仅当节点 valid 且依赖 hash 不变），没有按 Event 范围做增量。
- **性质**：事件链的中间件（accumulate / merge / 水位对齐）都实现了，但**消费端（物化）没有增量路径**——这是"事件驱动增量更新"未闭环的核心。

### E4 边水位线（required_version）维护不完整

- **定义**：materialize 成功后必须"把边的 required_version 跟 source 节点 version 对齐 + 节点状态改有效"。
- **当前**：
  - `graph.resolve` 做了出边水位对齐——**只有 fieldset_update 走 resolve**；
  - `panel_update` / `sample_update` / `feature_update` 直接 `patch valid`，**不调 resolve、不铸版本、不记录合并事件**；
  - 后果：事件水位链在**中间节点断档**——源头 table/index 有事件，末端 fieldset/factor/test 全量重算（掩盖问题），但中间 panel/sample/feature 的 `version_list` 恒空、出边 `required_version` 永不推进，依赖它们的下游若走 accumulate 只能拿到源头事件、拿不到"中间层已消费/重算"的事件。
- **性质**：水位线语义在部分节点上是断的；`accumulate(version_list, required_version)` 的"水位之后"过滤在这些链路上不可信。

### E5 resolve 的"自身变更事件"记录语义混淆

- **定义**：update 后节点版本应体现"我消费了哪些上游事件"（下游据此感知）。
- **当前**：`resolve` 里
  `self._bump(props, accumulated["upsert"] or accumulated["delete"])`——
  ① upsert/delete 同时存在时**只记一个**；② 把**上游的 field_scope** 原样当作自己的变更事件写入 version_list（对 fieldset 而言，记录的应是它重算产生的字段，而非上游字段名）。
- **性质**：小出入，语义不严谨，不影响全量重算路径。

### E6 version_list 无限增长、无裁剪

- **定义**：未定义裁剪策略；README 路线图 **"version_list 裁剪"** 已列为待办。
- **当前**：`_bump` 每版本追加一条事件，永不清除（每次 notify/update 都增长）。
- **性质**：已知未完成项，属性能问题（节点属性体积随版本线性增长）。

### E7 notify_change 的传播与置脏

- **定义**：未细化传播深度；"下游失效需 update 恢复"。
- **当前**：`notify_change` 铸版本 + 事件入日志 + **BFS 全深度下游置脏**（`valid=False, materialized=False`，不 bump 下游版本）；已修过"置脏不 bump 版本导致物化幂等误跳"的 bug（factor/test 幂等仅当 valid）。
- **性质**：✓ 与定义一致且是合理增强（`assert_ready` 传导检查亦为新增增强）。

---

## 二、其他出入（非 Event）

| # | 定义（v3.0-def） | 当前实现 | 性质 |
|---|---|---|---|
| G1 | Handler 的 `get`"物化且有效时才返回物理数据" | handler `get` 返回节点元数据（示例占位）；物理数据读取走 service 的 `xxx_get`（实时视图 / 物化 parquet） | 形态分工差异，行为正确 |
| G2 | `IndexHandler.add` 校验 symbol/datetime 列**唯一性** | `index_add` 无唯一性校验 | 已知未完成（README 路线图） |
| G3 | `PanelHandler.get` 物化+有效才返回 | panel 恒实时 join（无物化） | 设计演变（简化）；panel 物化列为待办 |
| G4 | `FieldsetHandler.on_change` 输出 {upsert, delete} 供消费 | `ctrl.accumulated` 存在，但 service 层无消费方（物化全量重算） | 接口空转，属 E3 的一部分 |
| G5 | `GraphHandler.scan()` 空定义 | graph 命令为 lineage/nodes/stats | 语义不同，无实质出入 |
| G6 | `AssetNode.version: str` | `int`（time_ns 时间戳，可直接排序比较） | 类型改进 ✓ |
| G7 | 节点列存 `columns: dict[str, ColumnsMeta]` | 平铺 `columns` 列表 + `_norm_cols` 规范化 | 形态差异，行为等价 |
| G8 | storage 钩子负责物化 | controller 层 `NullStorage` no-op 占位，service 层直接落盘（fieldset/factor/test 真实写 `<data_dir>/<type>/`） | 两层并存，物理层在 service 侧 |
| G9 | `StatNode` 是图资产 | stat **不进 catalog**（纯文件系统产物，api.md §3.6 明确） | **设计出入**：stat 在图外，血缘图看不到 stat 节点 |
| G10 | `ModelNode` 资产 | 无 model 实现（ASSET_TYPES 含 "model" 但无 add/update） | 未实现（后续规划） |
| G11 | 无 `assert_ready` | update 前强制"全部上游链 valid"（上游不齐则失败） | 新增增强（对应"上游不齐 update 失败"需求）✓ |
| G12 | 依赖方"积累事件"驱动 | 上游变化 → 置脏（valid=False）驱动重建；版本水位为辅 | 简化后的形态，配合幂等修复 |

---

## 三、结论与建议优先级

1. **P0（事件闭环）**：物理层 diff（`table/util.py diff_files` 文件级已有）→ 行级范围提取（新增文件的 symbol/datetime min-max，或至少文件级分区范围）→ `notify_change` 事件带上 `symbol_scope`/`datetime_scope`，文件删除 → `action="delete"`。
2. **P0（增量物化）**：给 fieldset/factor/test（及未来的 panel 物化）的 update 增加"按积累事件范围重算增量"路径；全量重算保留为 `--resync` 兜底。
3. **P1（水位一致性）**：panel/sample/feature update 走统一收口（如 `resolve` 或等价逻辑），确保出边 `required_version` 对齐；否则 accumulate 的"水位之后"过滤不可信。
4. **P1（resolve 事件语义）**：记录"本次重算产生的字段范围"而非直接透传上游事件；upsert/delete 分别记录（合并成两个版本条目或一个复合事件）。
5. **P2**：stat 是否纳入图资产（或保留图外，文档明示）；index 唯一性校验；version_list 裁剪（按边水位清理已消费事件）。
