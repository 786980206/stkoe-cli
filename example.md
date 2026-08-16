# example.md — stkoe 全流程演练（v0.7.1）

> 把当前业务流程从「mock 造数」到「因子测试」用一条可复制粘贴的命令串起来。
> 所有命令经 `uv run -m stkoe ...` 走 Execute 同步分发，与 gRPC `Execute` 行为完全一致。
>
> 前置：`uv sync` 已装好依赖；命令在仓库根目录执行。

## 本版语义速览（v0.7.1 物化分区）

- **update 主推**：`update` 为 V3 语义名，源头 update=重扫对账，
  物化资产 update=校验+落盘；上游就绪检查（全链 valid）不通过会报错提示先 update 上游
- **物化按 index 的 `materialize_partition` 时间桶分区**：panel/fieldset/factor/test 统一
  继承其 index 的物化粒度（`yearly` 默认 / `monthly` / `daily`），落盘
  `part=<YYYY>[/<YYYY-MM>[/<YYYY-MM-DD>]]/data.parquet`（文件内保留 part 列；与 index
  物理是否分区无关）
- **对外剔除 part 列**：get/视图读取走 `_scan_materialized`，返回列集合与实时视图一致
- **get 三态**：已物化且 curated 读物化；本应物化但未物化 → 报错提示先
  `<type> update <name>`；sample/feature 恒实时
- **增量物化**：上游变化 → 沿链 update 只重算受影响时间桶（桶粒度粗于增量区间时保留
  桶内区间外旧行合并写回，不丢数据）；上游无变化时 update 幂等跳过
- **样本池 = fieldset ∩ index**：`sample add <name> <fieldset> <index>`——样本池是
  fieldset 视图按指定 index 的 (symbol, datetime) 键集合裁剪的动态产物（semi join，
  不再支持公式过滤）

## 0. 准备：独立数据目录 + mock 造数

先建一个独立数据目录，避免污染 `~/.stkoe`（默认数据目录）。

```bash
# 0.1 用独立配置指向演示数据目录（写入 ./stkoe.json）
export STKOE_CONFIG=./stkoe.example.json
uv run -m stkoe config set --data-dir ./example-data

# 0.2 mock 造数：生成两张演示 parquet 源表（默认 300 只 × 500 个交易日 = 15 万行）
uv run -m stkoe mock demo
```

> 说明：stkoe 只「发现」磁盘上的 parquet（`index add` / `table add`），不会替你生成数据；
> `stkoe mock demo` 用 polars 造了两张演示表，**index 表写到 `index/index`**（index 资产
> 独立目录），m1 写到 `table/m1`；500 个交易日从 2024-01-01 起，**跨 2024/2025 两个自然年**
> ——正好用来演示 yearly 时间桶分区（`part=2024` + `part=2025`）。也可用
> `--n-syms/--n-days` 调整规模；单表可用 `uv run -m stkoe mock gen <name> --kind <kind>`
> 参数化生成（`--kind index/m1/tdcal/common/feature/klday`，`--n-syms/--n-days/--start/--end/--seed` 可选）。

## 1. 发现源头资产

```bash
uv run -m stkoe index add index --symbol-col sym --datetime-col date   # 注册 index（发现 index/index；--materialize-partition 默认 yearly）
uv run -m stkoe table add m1                    # 注册 m1（发现 table/m1）
uv run -m stkoe index meta index       # index 元数据（含 symbol/datetime 列与 materialize_partition）
uv run -m stkoe index get index --where "date >= '2024-01-02'" --limit 5   # 谓词裁剪读取
```

## 2. 逻辑数据集（panel：join 视图，update 物化）

```bash
uv run -m stkoe panel add ds1 index m1                  # keys 由 index 推断 = sym+date；m1 为成员，缺省 asof join
uv run -m stkoe panel meta ds1                            # panel 元数据（join 后的列与 keys）
uv run -m stkoe panel update ds1                          # 物化 → panel/ds1/part=2024/ + part=2025/（yearly 桶）
uv run -m stkoe panel get ds1 --limit 5                   # 读物化（列不含 part，与实时视图一致）
uv run -m stkoe panel get ds1 --partition 2024 --limit 5  # 只读 2024 年桶（物化资产按 part 前缀匹配）
```

```bash
# 物化产物布局（文件内保留 part 列，对外剔除）
find example-data/panel/ds1 -name data.parquet
# → example-data/panel/ds1/part=2024/data.parquet
# → example-data/panel/ds1/part=2025/data.parquet
```

> get 三态：刚 `panel add` 未 update 时 `panel get` 会报错提示「先 panel update」；
> update 后上游再变化 → curated 失效回退实时，需再次 update。

### （可选）成员表 join 方式演示：asof / left

```bash
uv run -m stkoe mock gen m2 --kind m1 --n-syms 300 --n-days 500   # 第二张成员表（证券资料）
uv run -m stkoe table add m2
uv run -m stkoe panel add ds2 index m1:asof m2:left                # m1 asof 就近匹配、m2 left 精确等值
uv run -m stkoe panel meta ds2
```

## 3. 覆盖率统计（stat，对 panel）

```bash
uv run -m stkoe stat scan panel ds1                       # coverage：all + 每个索引列各一文件
uv run -m stkoe stat get panel ds1 --partition_by all       # 读全量分区
uv run -m stkoe stat meta panel ds1                         # 已扫描分区列表
```

## 4. 衍生指标集（fieldset：公式引擎 + 校验 + 物化）

```bash
uv run -m stkoe fieldset add fs1 --panel ds1              # 指标集挂到 panel ds1
uv run -m stkoe fieldset add fs1 x2 --formula "x * 2.0"     # 加指标 x2 = 2*x（validated=False）
uv run -m stkoe fieldset check fs1 x2                       # 校验（行数 == 源行数 → validated=True）
uv run -m stkoe fieldset update fs1                         # 物化 → fieldset/fs1/part=2024/ + 2025/（keys + x2）
uv run -m stkoe fieldset get fs1 --limit 5                  # panel 视图 + 已校验指标
```

## 5. 样本池（sample：fieldset 视图 ∩ 指定 index 键集合，无物化）

```bash
uv run -m stkoe mock gen idx2 --kind index --n-syms 300 --n-days 100   # 样本筛选参照 index（2024 年起前 100 个交易日）
uv run -m stkoe index add idx2 --symbol-col sym --datetime-col date
uv run -m stkoe sample add sp1 fs1 idx2          # 样本池 = fieldset 视图 ∩ idx2 的 (sym, date) 键集合（不再按公式过滤）
uv run -m stkoe sample check sp1                 # 过滤后含全部索引列且行数 > 0
uv run -m stkoe sample update sp1                # 传导就绪标记有效（无物化资产，供下游 factor 使用）
```

> 样本池只保留键存在于筛选 index 数据中的行（semi join）：本例取 idx2 的 100 个交易日
> 作为样本区间，sp1 ≈ 300 只 × 100 日（而非全量 500 日）。

## 6. 因子定义库（feature：命名公式，纯定义无物化）

```bash
uv run -m stkoe feature add ma5 --formula "x * 2.0" --unit "元"   # 命名公式（formula 必填）
uv run -m stkoe feature test ma5 --sample sp1               # 在样本池视图上即时求值
uv run -m stkoe feature update ma5                          # 传导就绪标记有效（纯定义资产，供下游 factor 使用）
```

## 7. 最终因子（factor：feature 公式 + pipeline 算子链 + 物化）

```bash
uv run -m stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
uv run -m stkoe factor check fac1                           # 计算成功 + 恰好 1 列因子列
uv run -m stkoe factor update fac1                          # 物化 → factor/fac1/part=2024/ + 2025/（上游就绪才可更新，幂等）
uv run -m stkoe factor get fac1 --limit 5                   # 读因子（样本索引 + 因子列，无 part）
uv run -m stkoe factor get fac1 --partition 2025 --limit 5  # 只读 2025 年桶
uv run -m stkoe factor meta fac1                            # partition_by=["part"]、partition_gran="yearly"
```

## 8. 因子测试数据集（test：要求 sample 视图含 date/sym/returns/groupby/marketcap 列）

```bash
uv run -m stkoe test add t1 --factor fac1 --returns r --groupby ic --marketcap fv
uv run -m stkoe test check t1                               # 构造成功 + 含必需列 + 行数 > 0
uv run -m stkoe test update t1                              # 物化 → factor_test/t1/part=2024/ + 2025/
uv run -m stkoe test get t1 --limit 5                       # 测试面板（d{no}/factor_quantile 等）
```

## 9. 增量更新演示（上游变化 → 沿链增量物化）

模拟上游新增 2026 年上半年数据（真实日更场景物理表按 date 分区时，事件范围精确到日、
只动受影响桶；本例单文件重写 → 事件范围=全表 → 全桶重算，但会开出新桶 `part=2026`）：

```bash
# 9.1 给 index 表追加 2026 年上半年交易日（覆盖写回同一文件）
uv run python -c "import polars as pl; from stkoe.mock.gen import index as g; df=g(n_syms=300,start='2026-01-01',end='2026-06-30'); pl.concat([pl.read_parquet('example-data/index/index/data.parquet'),df]).write_parquet('example-data/index/index/data.parquet')"

# 9.2 沿链逐级 update：源头对账 → 下游增量重算（只动受影响桶；置脏沿链依次恢复有效）
uv run -m stkoe index update index        # 检测文件变化 → 铸版本 + 全链下游置脏
uv run -m stkoe panel update ds1          # 增量：重算受影响时间桶（2024~2026 全范围）
uv run -m stkoe fieldset update fs1
uv run -m stkoe sample update sp1         # 无物化资产：传导就绪标记有效
uv run -m stkoe feature update ma5        # 同上
uv run -m stkoe factor update fac1
uv run -m stkoe test update t1

# 9.3 观察新桶出现、旧桶仍在（yearly：part=2024 / 2025 / 2026）
find example-data/factor/fac1 -name data.parquet
# → example-data/factor/fac1/part=2024/data.parquet
# → example-data/factor/fac1/part=2025/data.parquet
# → example-data/factor/fac1/part=2026/data.parquet
```

> 幂等：上游无变化时再跑一遍 update 会跳过（依赖签名/水位不变）；`--resync` 可强制全量重建。

## 10. 因子测试器（stat 集成，最终测试）

```bash
# 单因子 IC 测试（单位置参数简写 → test 目标）
uv run -m stkoe stat scan t1 --kind ic
# 分组收益 / 分位换手率 / 自相关 / 因子加权多空 等测试器（显式 test 目标）
uv run -m stkoe stat scan test t1 --kind bucket_returns
uv run -m stkoe stat scan test t1 --kind bucket_turnover
uv run -m stkoe stat scan test t1 --kind autocorrelation
uv run -m stkoe stat scan test t1 --kind factor_returns
uv run -m stkoe stat scan test t1 --kind coverage
# 读取某个测试产物（--partition_by <output>）
uv run -m stkoe stat get t1 --kind ic --partition_by ic_d1
```

产物落在 `stat/test/t1/<kind>/<output>.parquet`（`ic_d{no}` / `rtn_date` / `fr_d{no}` 等）。

## 11.（可选）任务版后台路径 + 血缘可视化

CLI 子命令走 Execute（同步流式）；长任务（物化/统计）经 gRPC `SubmitTask`（`s:...`）后台执行。

```bash
# 起 gRPC 服务（另一个终端）
uv run -m stkoe serve

# 用单文件 REPL 客户端演示任务版：mock 造数 + 后台物化 + 统计
uv run python gclient.py
stkoe> s:mock                        # 任务版示例：5 步进度 + 日志 + 落盘结果
stkoe> s:mock demo                   # 任务版 mock 造数（写 index/index + table/m1）
stkoe> s:mock gen mytable --kind klday --n-syms 20   # 任务版参数化生成
stkoe> s:test update t1              # 后台物化测试数据集（订阅到终态）
stkoe> s:stat scan t1 --kind ic      # 后台跑 IC 测试器（单位置简写同样可用）
stkoe> t:<task_id>                   # 回放订阅某任务事件流
```

血缘图（本案例的完整链）与可视化：

```bash
uv run -m stkoe graph lineage                      # Cytoscape elements JSON（全图）
uv run -m stkoe graph nodes                        # 节点摘要（中心节点选择器用）
uv run -m stkoe graph stats                        # 节点/边统计
# 浏览器可视化（Cytoscape.js 独立页）：
python tools/graph-viewer/export.py example-data/catalog.db --output example-data/graph.json
# 然后 python -m http.server 打开 tools/graph-viewer/index.html 或拖入 JSON
```

本案例血缘链：`index/index + table/m1 → panel:ds1 → fieldset:fs1 → sample:sp1（+ index:idx2
筛选参照）→ factor:fac1 → tester:t1`。

## 清理

```bash
# 删除演示数据目录与临时配置
Remove-Item -Recurse -Force example-data, stkoe.example.json   # PowerShell
rm -rf example-data stkoe.example.json                          # bash
```

> 注：`stkoe.example.json` 是 `STKOE_CONFIG` 指向的临时配置；`stkoe.json`（仓库根）为共享配置，
> 若此前存在会优先被读取，演示时请留意 `config show` 的 `config_file` 指向。
