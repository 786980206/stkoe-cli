# V3 定义 vs 当前实现 出入清单（对照 v3.0-def.py）

> 对照基准：仓库根 `v3.0-def.py`（V3.0 初始设计定义）+ `graph-design.md`。
> 状态：**P0/P1/P2 已落地**（范围化事件 + factor/test 增量物化 + 沿链增量物化 +
> get 三态 + version_list 裁剪 + index 唯一性校验）；E5 事件记录语义已修；
> 剩余可选项见文末。

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
  - **沿链增量物化（全部物化资产）**：`_upstream_scope` 沿链收集**直接依赖**未消费事件
    （`graph._accumulated` 按出边 `required_version` 水位）→ datetime 区间；已有物化且区间
    明确时——分区场景只删受影响分区文件重算写回，flat 场景删区间+合并写回
    （`_factor_compute`/`_test_build`/`_panel_lazy`/fieldset 计算均带 `dt_range`）；
    `--resync`/首次/无区间 → 全量重算兜底；
  - **panel/fieldset/factor/test 物化**：`panel/<name>/`、`fieldset/<name>/`、
    `factor/<name>/`、`factor_test/<name>/`，分区布局**镜像 index**（`_index_partition_keys`
    沿链找 index 取其分区键，分区写保留分区列 + hive 目录 `key=value/data.parquet`）；
  - **get 三态**（`_require_materialized`）：已物化（curated）→ 读物化；未物化且资产
    本应物化（panel/fieldset/factor/test）→ 报错提示先 `<type> update <name>`；
    sample/feature 恒实时；
  - sample / feature 仍无物化（实时构造，update 只铸版本）。
- **剩余**：`symbol_scope` 未参与过滤（P2 可选）。

### E4【已修 ✅】边水位线（required_version）维护不完整

- **定义**：materialize 成功后必须"把边的 required_version 跟 source 节点 version 对齐 + 节点状态改有效"。
- **当前（已修）**：`graph.resolve` 支持 `mark_materialized`/`extra` 参数；**panel/sample/feature
  update 统一走 resolve**——有积累事件则铸版本并记入 version_list、出边 required_version
  对齐被依赖方当前版本、无事件不空 bump（幂等）。事件水位链在中间节点不再断档
  （`_upstream_scope` 仍从源头收集，但中间节点自身的事件日志已开始积累）。
- **性质**：`accumulate(version_list, required_version)` 的"水位之后"过滤现在全链可信。

### E5【已修 ✅】resolve 的"自身变更事件"记录语义混淆

- **定义**：update 后节点版本应体现"我消费了哪些上游事件"（下游据此感知）。
- **当前（已修）**：
  - upsert/delete 同时积累时**各记一条版本事件**（对齐源头 ``notify_change`` 的
    "有增删记两个版本事件"约定，不丢动作与范围语义）；
  - ``resolve(..., own_event=...)`` 支持 service 层传入自身变更事件：``field_scope``
    用自身的（fieldset 记录重算出的字段名，而非上游列名）；symbol/datetime 范围
    未指定时继承积累事件并集（None=全集）——下游 ``_upstream_scope`` 沿链取
    datetime 范围不丢；
  - ``_bump`` 支持链式铸版本（version_list 基底显式传入），一次 resolve 多事件
    连续 bump。
- 测试：``test_resolve_records_both_actions``（两类各记一条 + 动作/范围断言）、
  ``test_resolve_own_event_field_scope``（own field_scope + 范围继承）。

### E6【已修 ✅】version_list 无限增长、无裁剪

- **定义**：未定义裁剪策略；README 路线图 **"version_list 裁剪"** 已列为待办。
- **当前（已修）**：`_bump` 铸版本后顺带 `_prune_version_list`——按所有下游边
  `required_version` 的 min 裁剪：`version <= min_rv` 的事件已被全部下游消费，可安全删除；
  `> min_rv` 保留（`accumulate` 仍可取到未消费事件）。测试
  `test_version_list_pruned_by_consumed` 验证。

### E7 notify_change 的传播与置脏

- **定义**：未细化传播深度；"下游失效需 update 恢复"。
- **当前**：`notify_change` 铸版本 + 事件入日志 + **BFS 全深度下游置脏**（`valid=False, materialized=False`，不 bump 下游版本）；已修过"置脏不 bump 版本导致物化幂等误跳"的 bug（factor/test 幂等仅当 valid）。
- **性质**：✓ 与定义一致且是合理增强（`assert_ready` 传导检查亦为新增增强）。

---

## 二、其他出入（非 Event）

| # | 定义（v3.0-def） | 当前实现 | 性质 |
|---|---|---|---|
| G1 | Handler 的 `get`"物化且有效时才返回物理数据" | handler `get` 返回节点元数据（示例占位）；物理数据读取走 service 的 `xxx_get`（实时视图 / 物化 parquet） | 形态分工差异，行为正确 |
| G2 | `IndexHandler.add` 校验 symbol/datetime 列**唯一性** | ✅ 已修：`_check_index_unique`（symbol+datetime 组合唯一校验） | 已修（P2-2） |
| G3 | `PanelHandler.get` 物化+有效才返回 | ✅ panel 物化 + get 三态（物化且 curated 读物化，否则报错提示 update） | 已修（P2） |
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
