# V3 定义 vs 当前实现 出入清单（对照 v3.0-def.py）

> 对照基准：仓库根 `v3.0-def.py`（V3.0 初始设计定义）+ `graph-design.md`。
> 状态：**P0 已落地**（范围化事件 + factor/test 增量物化）；中间件（accumulate/merge/水位）
> 与消费端（增量物化）已接通，剩余 P1/P2 见文末。

---

## 一、Event 增量更新逻辑出入（重点）

### E1【已修 ✅】物理变化 → 事件丢失 symbol/datetime 范围

- **定义**（v3.0-def.py 顶部注释）："每一次原始物理表的数据变化，本质上就是对**某一个时间范围内的一批标的**的指定字段数据的 upsert 或 delete"——事件必须带 `symbol_scope` / `datetime_scope` / `field_scope` / `action`。
- **当前（已修）**：`service._change_events` 从文件 diff 提取范围——hive 分区路径含 `<datetime_col>=<v>` 时用分区值，其余读变化文件 footer 的 datetime 列 min/max（只读元数据）；`datetime_scope` 统一为 **[min, max] 区间**（字符串/ISO 字典序可比）；added/changed → upsert，removed → delete。
- **剩余**：`symbol_scope` 仍为 None（时间范围内的全部标的）；精确行级/标的级范围需读数据页，未做（P2 可选）。

### E2【已修 ✅】delete 事件完全缺失

- **定义**：`action: Literal["upsert", "delete"]`，物理删除 → delete 事件 → 下游按范围删对应数据。
- **当前（已修）**：removed 文件 → `action="delete"` 事件（范围来自 catalog 指纹 `partition_path` 的分区值；flat 无分区则 None 全集）；一次 scan 同时有增删时**记两个版本事件**（upsert + delete 各一次 bump）。

### E3【已修 ✅】增量物化未落地（update = 全量重算 / 标记有效）

- **定义**（PanelHandler.update → materialize）：积累事件合并出 `{upsert, delete}` → **按范围从上游提取增量数据 upsert 进现有物化存储、按范围 delete 现有数据** → 边水位对齐 + 节点有效。
- **当前（已修）**：
  - **factor / test update 增量物化**：`_upstream_scope` 从全部源头（table/index）收集 `version > consumed` 的积累事件 → datetime 区间；已有物化且区间明确时，读旧物化 + 范围外保留 + 仅重算范围内行（`_factor_compute`/`_test_build` 带 `dt_range`）合并写回；`extra.consumed_versions` 记录各源头水位（供下次判定）；`--resync` 或首次/无范围 → 全量重算兜底；
  - **panel update 物化**：join 视图落盘 `panel/<name>/data.parquet`，`panel_get` 物化且 curated 读物化、否则实时 join；`_panel_hash` 依赖上游版本 → 上游变化 curated 失效回退实时；
  - **fieldset update 物化衍生字段**：keys + 已校验字段落盘 `fieldset/<name>/data.parquet`，`_fieldset_view_lf` 物化且 curated 读物化字段（fields_only 直接返回 / 全视图与 panel join）；
  - sample / feature 仍无物化（实时构造，update 只铸版本）。
- **剩余**：panel/fieldset 物化暂无增量（全量重算 + curated 失效兜底）；`symbol_scope` 未参与过滤。

### E4【已修 ✅】边水位线（required_version）维护不完整

- **定义**：materialize 成功后必须"把边的 required_version 跟 source 节点 version 对齐 + 节点状态改有效"。
- **当前（已修）**：`graph.resolve` 支持 `mark_materialized`/`extra` 参数；**panel/sample/feature
  update 统一走 resolve**——有积累事件则铸版本并记入 version_list、出边 required_version
  对齐被依赖方当前版本、无事件不空 bump（幂等）。事件水位链在中间节点不再断档
  （`_upstream_scope` 仍从源头收集，但中间节点自身的事件日志已开始积累）。
- **性质**：`accumulate(version_list, required_version)` 的"水位之后"过滤现在全链可信。

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

1. **P0 ✅ 已落地**：物理 diff → 范围化事件（`_change_events`，datetime 区间 + delete 事件）；
   factor/test update 增量物化（`_upstream_scope` 源头水位 + `dt_range` 区间重算 + 合并写回 + `--resync` 兜底）。
2. **P1 ✅ 已落地**：panel/sample/feature update 统一走 `resolve`（铸版本 + 事件入 version_list +
   出边水位对齐）；**panel update 物化**（join 视图落盘 + get 读物化 + curated 失效回退）；
   **fieldset update 物化衍生字段**（keys + 已校验字段落盘 + 视图拼接读物化）。
3. **P2 剩余**：panel/fieldset 物化的增量路径（当前全量重算 + curated 失效兜底，可复用 factor
   的 `dt_range` 机制）；symbol_scope 提取（读数据页）；fieldset `fields` 变更的 curated 联动；
   stat 是否纳入图资产；index 唯一性校验；version_list 裁剪（按 consumed 水位清理已消费事件）。
