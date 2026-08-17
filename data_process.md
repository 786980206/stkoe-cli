# stkoe 数据处理文档

本文档说明 stkoe 当前对**各类资产的增量处理方法与全量物化方法**的实现细节，
与代码一一对应（`src/stkoe/{storage,graph,panel,fieldset,sample,factor,factor_tester,stat}/`）。
对外命令与数据模型见 README §5-§13；本文只讲数据怎么算、怎么落盘、怎么做增量。

## 0. 总览

### 0.1 资产链与物化归属

```
table ─┐                          ┌→ factor → tester
index ─┴→ panel → fieldset → sample ─┘        （tester 为资产链末端）
                                    （无物化）  （无物化）
```

| 资产 | 是否有物化 | 物化内容 | 触发方式 |
|---|---|---|---|
| table / index | 否（源头表） | 无；只有图内指纹/版本登记 | update = 重扫对账 |
| panel | 是 | index + 成员表 join 视图（全列） | `panel update` |
| fieldset | 是 | keys + 已校验字段列（公式结果） | `fieldset update` |
| sample | 否（实时视图） | 无；fieldset 视图 ∩ index 键集合 | `sample update` = 确认就绪 + 铸版本 |
| factor | 是 | keys + 单一因子列（公式 + pipeline 算子链） | `factor update [--all]` |
| tester | 是 | 测试数据集（keys/sample/returns/group/marketcap/factor/d{no}/factor_quantile） | `tester update` |
| stat | 独立统计资产 | coverage / storage / tester kind 产物 | `stat scan`（与增量无关） |

### 0.2 两种物化布局

下游 4 个可物化资产（panel/fieldset/factor/tester）统一继承其 index 的
`materialize_partition` 时间桶粒度（yearly/monthly/daily，默认 yearly）：

- **分区布局**（时间桶）：`<type>/<name>/part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/<n>.parquet`，
  文件内保留 `part` 列（String）；
- **flat 布局**（单文件）：`<type>/<name>/data.parquet`。

判定：`graph/materialize.py::partition_plan` —— 沿血缘链找到依赖的 index，
`gran` 为 yearly/monthly/daily 且有时间键（keys 末列）→ 分区；gran 未知 /
无 index / 无时间键 → flat。

### 0.3 增量 vs 全量的判定（4 个物化资产统一）

```
增量 = 上游积累事件有明确 datetime 区间（_upstream_scope 非 None）
     AND 已有物化（分区目录存在 / flat 文件存在）
     AND 非 --resync
其余（首次 / 无区间 / 无旧物化 / --resync / 幂等不通过）→ 全量
```

- 幂等跳过（不算增量也不算全量）：节点 `valid` 且 `dependency_hash == 当前 hash`
  且已物化 → 直接返回 `changed: False`，版本不推进。
- 全量路径与增量路径完成后都走 `graph.resolve` 收口：铸版本 + 合并消费事件入
  `version_list` + 出边 required_version 对齐 + `valid/materialized` 置位。

## 1. 存储层读写接口（storage/）

所有物化读写的**唯一入口**，替换底层引擎（polars → DuckDB 等）只改本层：

- `storage.scan(root, partition=None, columns=None, where=None, exclude=("part",))`
  —— 目录（hive 分区还原）/单文件/文件列表 → `LazyFrame`。默认剔除内部 `part`
  列（对外列集合与实时视图一致）；**增量合并旧桶读取必须传 `exclude=()` 保留
  part 列**（`write_incremental` 按桶过滤/重写要用）。
- `storage.write_all(df|lf, out_dir, partition_keys, gran, dt_col, clean=False)`
  —— **全量落盘**：无分区键 → `data.parquet`；有分区键 → 原生 Hive 分区写出
  （`pl.PartitionBy` 一次流式求值，`include_key=True` 文件内保留 String 的 part 列）；
  `clean=True` 写前 `rmtree(out_dir)`，数据为空时落保留 schema 的空
  `data.parquet`。
- `storage.write_incremental(old_lf, inc_df, dt_expr, pkeys, out_dir, gran, dt_col,
  sym_expr=None, sort_cols=None)` —— **分区桶增量重写**（见 §3.2）。
- `storage.write_incremental_flat(out_path, inc_df, dt_expr, keys, sym_expr=None,
  sort_cols=None)` —— **flat 增量合并**（见 §3.3），panel/fieldset/factor/tester
  四资产共用同一实现。
- `storage.row_count / disk_files / detect_layout / partition_of / signature /
  footer / diff_files / columns_union / to_expr / prune_files / calc_stats /
  calc_storage` —— 元数据、对账、裁剪、统计工具。

## 2. 全量物化方法

### 2.1 统一流程

```
1. assert_ready：BFS 校验上游依赖链全部 valid（未就绪抛 DependencyError）
2. 构建视图（lazy）：panel=join 链；fieldset=panel + 字段；factor=sample 视图 +
   公式；tester=sample + factor 视图
3. sort([时间键, 标的键])：物化存储时间优先（桶内同序）
4. collect() 一次：rows 计数与写盘共用，不重复求值
5. storage.write_all(df, out_dir, pkeys, gran, dt_col, clean=True)
   —— clean=True 清空旧目录，避免新数据缺失的陈旧桶残留（phantom 行）
6. resolve 收口 + 铸版本
```

### 2.2 各资产全量要点

- **panel**：`_panel_lazy(svc, name, live=True)` 强制实时 join（不能读物化重写
  自身）；index 为左表，成员表按 `left_join`（等值）或 asof（默认 backward 就近），
  String 日期先 cast Date 做 asof 再 cast 回 String；`where` 只引用左表列时下推
  到 join 前。
- **fieldset**：`engine.scan(base, keys, fields)` 一次算齐全部已校验字段；只物化
  keys + 字段列。
- **factor**：`_factor_compute` 先**列投影**（keys + 公式引用列）再 collect（宽表
  panel 下避免全列物化）→ `engine.field` 算公式列（校验逐行）→ hstack 索引 +
  因子列 → `engine.transform` 施加 pipeline 算子链；`factor update --all` 批量
  场景走「共享视图 + 分别物化」三阶段（§4.5）。
- **tester**：`_tester_build` 只 collect 测试必需列 + keys + 公式引用列的投影视图，
  复用 `_factor_compute(view_df=...)` 避免重复 join；`prepare_factor_data` 生成
  分位/前向收益等测试列。
- **stat**（独立计算资产，非物化链）：`stat scan` 对目标（table 实时 lazy /
  panel 实时 join 视图）逐分区 `calc_stats(...).collect()` 后 `write_file`——
  结果小（组数 × 14 列），用 in-memory 引擎（流式 sink_parquet 的 group_by 哈希
  表单分区峰值 ~8GB 且跨分区不释放内存，1852 万行实测第 8 个分区即 OOM）。

## 3. 增量处理方法

### 3.1 增量范围的来源：图事件积累

- 源头（table/index）update 对账出差异后，`_change_events` 生成**范围化事件**
  （upsert/delete，带 `datetime_scope=[lo, hi]` 与 `symbol_scope`=变化标的集合）
  写日志并置脏下游。
- 上游 update 消费事件会写入自身 `version_list`（resolve 语义）；下游
  `svc._upstream_scope(node)` 按出边 `required_version` 水位取**直接依赖未消费
  事件的并集**：datetime 取 [min, max] 区间，symbols 为变化标的并集（None=全集）。
  **沿链收集，不找最上游 table/index。**
- `symbol_scope` 提取：index 的 hive 分区键 `<symbol_col>=<v>` 直取分区值，否则
  读变化文件该列 distinct；removed 文件取不到 → None（全集）。

### 3.2 分区布局增量：write_incremental

时间桶粒度（yearly/monthly/daily）粗于增量区间（天级），直接删桶会丢掉桶内
未变化的行；且增量新日期可能与旧数据**同桶**。因此：

```
1. 增量行 inc 补 part 列（dt_col 前缀切片）→ 得 inc_parts（增量所在桶）
2. 受影响桶 affected = 旧数据命中「区间(× 标的)」行的桶 ∪ inc_parts
3. keep = 旧数据中 ~命中 且 part ∈ affected 的行（惰性过滤：只读 part 列判定、
   行级裁剪后才 collect）
4. 删除 affected 桶目录 → merged = keep + inc（vertical_relaxed 合并）
5. 可选 sort([时间, 标的]) → write_all 重写 affected 桶
sym_expr 给出时（事件带 symbol_scope）命中判定收窄到变化标的，未变化标的行不重算
```

读旧数据用 `scan(out_dir, exclude=())`（保留 part 列）；受影响桶判定**只读 part 列**、
keep 行级裁剪后才 collect——大表增量不整表读入内存。

### 3.3 flat 布局增量：write_incremental_flat

```
1. keep = scan_parquet(out_path) 过滤 ~(dt_expr [& sym_expr]) 的行（lazy 裁剪）
2. df = concat([keep, inc]).unique(subset=keys, keep="last") 按键去重保留新值
3. 可选 sort → write_parquet 写回单文件
```

panel/fieldset/factor/tester 四个资产的 flat 增量分支**统一走这一个实现**。

### 3.4 滚动窗口的范围展开（_expand_scope）

字段/公式/测试的窗口语义使「输入 [lo, hi] 变化」实际影响输出更大的区间，
增量重算区间按窗口展开，own_event 也带展开后的范围供下游继续增量：

| 资产 | 窗口来源 | 展开方向 |
|---|---|---|
| fieldset | 字段 `window_size`（回看 w） | 前向：输出受影响 [lo, hi+w-1] |
| factor | feature（公式定义）`window_size` | 前向：同上 |
| tester | d{no} 前向收益窗口 | 后向：重算区间 [lo-(max_no-1), hi] |

非 ISO 日期/解析失败原样返回；窗口展开只作用于时间维度，symbol 原样透传。

### 3.5 各资产增量步骤

- **panel**：`_upstream_scope` → dt_expr + sym_expr → `_panel_lazy(where=...)`
  （左表列下推）只算变化区间 × 标的的 join 行 → collect → 分区走
  `write_incremental` / flat 走 `write_incremental_flat`。
- **fieldset**：先 `_sync_fieldset_derives_all` 血缘对账（历史字段缺边/引用变化
  自愈，幂等不置脏）→ 按最大字段 window_size 前向展开区间 →
  `engine.scan(base, keys, fields)` 只算区间字段 → 写回。
- **factor**：`_factor_plan`（纯图内元数据，不触计算）判幂等/区间（feature 窗口
  展开）；`_factor_compute(dt_range=..., symbols=...)` 只算区间 × 标的 →
  `_factor_write` 写回 + resolve（own_event 带展开后范围供 tester 增量）。
- **tester**：按 max(periods)-1 后向展开 → `_tester_build(dt_range=..., symbols=...)`
  只构造区间测试数据 → 写回。

### 3.6 增量语义保证（与 PartitionBy 的交互）

- 增量合并 **不传 clean**：已按受影响桶精确删除，不能动未受影响桶；
- 全量分支 `clean=True`：处理「无范围删除事件（removed 文件）→ 全量重写」时
  PartitionBy 只写数据里存在的桶、不删缺失旧桶的 phantom 残留；
- `include_key=True` 保证 part 列恒为 String（若 False，hive 目录值被推断为
  Int64，与增量数据 String 类型不一致，`is_in` 过滤失败）。

## 4. 各资产处理详解

### 4.1 table / index —— 源头：重扫对账（无物化）

`update` = 信任磁盘、对账图登记：

1. `disk_files(root)` 列目录 → 与指纹表（catalog.db 普通表）`diff_files` 对比；
2. **无差异 → 不 bump 版本**（幂等）；有差异 → 逐文件读 footer（复用未变文件的
   旧 row_count/schema/stats）→ 指纹替换 + 列对账（`columns_union`）+ 签名更新；
3. 非首次变化 → `_change_events` 生成范围化事件 + `notify_change`（铸版本 +
   下游置脏）。

- table 无 symbol_col，事件恒无 symbol_scope（None=全集）；
- index 的 `(symbol_col, datetime_col)` 组合唯一性**只在 add 登记时校验**；
  update 是重扫对账（信任磁盘现状），跳过全表 unique（2000 万行 ~6s 的省）;
- index add 默认 yearly 且数据跨多年 → 报告带 `partition_hint`（建议 monthly/daily）。

### 4.2 panel —— 物化 join 视图

- 全量：`_panel_lazy(live=True)` 实时 join → collect → `write_all(clean=True)`；
- 增量：按「时间 × 标的」区间只重算受影响行（§3.5）；
- 读取：物化且 curated → `scan(root)`（剔除 part 列）；下游沿链
  `_panel_lazy`：curated 读物化 parquet，否则实时 join。

### 4.3 fieldset —— 物化公式字段

- 字段 `window_size` 前向展开增量区间；own_event 带展开后范围供下游；
- 字段血缘对账 `_sync_fieldset_derives_all` 随 update 全量自愈；
- 读取：已物化 → 物化字段与 panel 视图 left join（fields_only 时只读物化字段）。

### 4.4 sample —— 实时视图（无物化）

- update = `assert_ready` + 视图可构造（collect 一次行数）+ resolve 铸版本
  （`mark_materialized=False`）；
- 视图 = fieldset 视图 ∩ 指定 index 的 `(symbol, datetime)` 键集合（semi join，
  键列名不同时按位置映射）。

### 4.5 factor —— 因子物化（单因子 / --all 批量）

- 单因子：`_factor_scan_one` → plan（幂等/区间）→ compute → write；
- `--all` 批量三阶段：
  1. **计划**：`_factor_plan` 逐因子（纯图内元数据）；
  2. **共享计算**：`_factor_batch_compute` 按 sample 分组——每组只构建一次视图、
     一次 collect（列投影 = keys + 组内公式引用列并集）、按引擎一次
     `FactorEngine.fields` 算齐全部因子列（同公式共享一列），再按各自增量范围
     精确过滤 + 施加各自 pipeline；
  3. **分别物化**：逐因子 `_factor_write`（与单因子同语义）。

### 4.6 tester —— 测试数据集（资产链末端）

- 键列从 factor/sample keys 推断（index 的 symbol_col/datetime_col 可自定义）；
- 增量按 max(periods)-1 **后向**展开（d{no} 前向收益窗口，t 时刻输出用到
  t..t+no-1 的 returns）；
- 读取：物化 + curated → 读物化；未物化 → 报错提示先 update。

### 4.7 stat —— 独立统计资产

- 与上述增量链无关；产物 `stat/<type>/<name>/<kind>/<part>.parquet`
  （`all` + 索引列分组：panel=keys，table=非工具列）；
- `--partition <p>[,<p>...]` 按需只算指定分区（大表全量分区内存/耗时线性放大，
  实测 1852 万行单分区 ~16s、全部 23 分区在 32GB 机器不可行，建议按需）；
- scan 成功后登记图内 `Stat` 节点 + `(Stat)-[:DEPENDS]->目标` 边（登记镜像，
  物理文件仍是唯一数据源）。

## 5. 读取与一致性

- **get 三态**：已物化（curated）→ 读物化 parquet（对外剔除 part 列）；本应物化
  但未物化 → 报错提示先 `<type> update <name>`；sample/feature 恒实时。
- **curated 判定**：`materialized` 且 `extra.dependency_hash == 当前签名 hash`
  （panel = 上游版本 + joins + keys；fieldset = panel 版本 + 字段公式/窗口 +
  engine；factor = feature/sample 版本 + engine/pipeline/factor_col；tester =
  factor hash + returns/groupby/marketcap/factor_col/spec）。
- **读前快检**（`_ensure_fresh`，table/index 读前）：签名一致则继续，不一致自动重扫
  对账；未登记目录隐式注册。
- **沿链复用**：下游经 `svc._panel_lazy` / `_fieldset_view_lf` / `_sample_view_lf` /
  `_factor_compute` 取上游——上游 curated 则读物化，否则实时构造。

## 6. 命令入口与实测

```
<type> update <name>                        # 单资产（增量或全量，自动判定）
<type> update --all                         # 批量（index/table/factor/tester）
<type> update --resync                      # 强制全量
graph update --node <type:name> | --all     # 级联：目标 + 下游闭包（拓扑序）
stat scan <type> <name> [--partition ...]   # 独立统计（按需分区）
```

实测（真实数据 1852 万行 × 22 列，index 默认 yearly 37 桶）：

- 源头 update 幂等 0s；panel 全量物化 ~19s（PartitionBy 分区写出）；
- 全链增量（加 12 行 → `graph update --all`）16.2s，行数精确 +12；
- 无范围删除事件 → 全量重写会清陈旧桶（phantom 修复后 37 桶、行数精确）；
- stat 单分区 16s（全量 23 分区 ~22 分钟，`--partition all,date,sym` 34-55s）。