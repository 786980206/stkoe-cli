# example.md — stkoe 全流程演练

> 把当前业务流程从「mock 造数」到「因子测试」用一条可复制粘贴的命令串起来。
> 所有命令经 `uv run -m stkoe ...` 走 Execute 同步分发，与 gRPC `Execute` 行为完全一致。
>
> 前置：`uv sync` 已装好依赖；命令在仓库根目录执行。

## 0. 准备：独立数据目录 + mock 造数

先建一个独立数据目录，避免污染 `~/.stkoe`（默认数据目录）。

```bash
# 0.1 用独立配置指向演示数据目录（写入 ./stkoe.json）
export STKOE_CONFIG=./stkoe.example.json
uv run -m stkoe config set --data-dir ./example-data

# 0.2 mock 造数：生成两张演示 parquet 源表到 example-data/tables/
uv run -m stkoe mock demo
```

> 说明：stkoe 只「发现」磁盘上的 parquet（`table add`），不会替你生成数据；
> `stkoe mock demo` 用 polars 造了两张演示表（默认 **300 只股票 × 500 个交易日 = 15 万行**）
> 到配置数据目录的 `tables/`，可接着用 `table add` 登记；也可用 `--n-syms/--n-days`
> 调整规模。单表可用 `uv run -m stkoe mock gen <name> --kind <kind>` 参数化生成
> （`--kind index/m1/tdcal/common/feature/klday`，`--n-syms/--n-days/--start/--end/--seed` 可选）。

## 1. 发现表资产

```bash
uv run -m stkoe table add index --type index   # 注册 index（发现 data.parquet，标记为 index 类型表）
uv run -m stkoe table add m1                    # 注册 m1
uv run -m stkoe table list             # 已注册表清单
uv run -m stkoe table meta index       # 表元数据（含列信息）
uv run -m stkoe table get index --where "date >= '2024-01-02'" --limit 5   # 谓词裁剪读取
```

## 2. 逻辑数据集（add 只注册，scan 才物化）

```bash
uv run -m stkoe dataset add ds1 index m1 --keys sym,date   # 注册数据集（index 为主表且必须 type=index，m1 为成员）
uv run -m stkoe dataset scan ds1                            # 物化：join 后落盘 datasets/ds1/
uv run -m stkoe dataset meta ds1                            # 数据集元数据（含 join 后的列）
uv run -m stkoe dataset get ds1 --limit 5                   # 读数据集（curated 读物化 parquet）
```

## 3. 覆盖率统计（stat）

```bash
uv run -m stkoe stat scan dataset ds1                       # coverage：all + 每个索引列各一文件
uv run -m stkoe stat get dataset ds1 --partition_by all     # 读全量分区
uv run -m stkoe stat meta dataset ds1                       # 已扫描分区列表
```

## 4. 衍生指标集（fieldset：公式引擎 + 校验 + 物化）

```bash
uv run -m stkoe fieldset add fs1 --dataset ds1              # 指标集挂到 ds1
uv run -m stkoe fieldset add fs1 x2 --formula "x * 2.0"     # 加指标 x2 = 2*x（validated=False）
uv run -m stkoe fieldset check fs1 x2                       # 校验（行数 == 源行数 → validated=True）
uv run -m stkoe fieldset scan fs1                           # 物化 keys + 已校验指标
```

## 5. 样本池（sample：基于 dataset_with_fieldset 的过滤，无物化）

```bash
uv run -m stkoe sample add sp1 --dataset ds1 --formula "(date >= '2024-01-02') & (x > 1.0)"
uv run -m stkoe sample check sp1                            # 过滤后含全部索引列且行数 > 0
```

## 6. 因子定义库（feature：命名公式，纯定义无物化）

```bash
uv run -m stkoe feature add ma5 --formula "x * 2.0" --unit "元"   # 命名公式（formula 必填）
uv run -m stkoe feature test ma5 --sample sp1               # 在样本池视图上即时求值
```

## 7. 最终因子（factor：feature 公式 + pipeline 算子链 + 物化）

```bash
uv run -m stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
uv run -m stkoe factor check fac1                           # 计算成功 + 恰好 1 列因子列
uv run -m stkoe factor scan fac1                            # 物化 factors/fac1/（镜像源 dataset 布局）
uv run -m stkoe factor get fac1 --limit 5                   # 读因子（样本索引 + 因子列）
```

## 8. 因子测试数据集（test：要求 sample 视图含 date/sym/returns/groupby/marketcap 列）

```bash
uv run -m stkoe test add t1 --factor fac1 --returns r --groupby ic --marketcap fv
uv run -m stkoe test check t1                               # 构造成功 + 含必需列 + 行数 > 0
uv run -m stkoe test scan t1                                # 物化 factor_tests/t1/data.parquet
uv run -m stkoe test get t1 --limit 5                       # 测试面板（d{no}/factor_quantile 等）
```

## 9. 因子测试器（stat 集成，最终测试）

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

产物落在 `stats/test/t1/<kind>/<output>.parquet`（`ic_d{no}` / `rtn_date` / `fr_d{no}` 等）。

## 10.（可选）后台任务路径 + mock 示例

CLI 子命令走 Execute（同步流式）；长任务（物化/统计）经 gRPC `SubmitTask`（`s:...`）后台执行。

```bash
# 起 gRPC 服务（另一个终端）
uv run -m stkoe serve

# 用单文件 REPL 客户端演示任务版：mock 造数 + 后台物化
uv run -m python gclient.py
stkoe> s:mock                        # 任务版示例：5 步进度 + 日志 + 落盘结果
stkoe> s:mock demo                   # 任务版 mock 造数（写 tables/index + tables/m1）
stkoe> s:mock gen mytable --kind klday --n-syms 20   # 任务版参数化生成
stkoe> s:test scan t1                # 后台物化测试数据集（订阅到终态）
stkoe> s:stat scan t1 --kind ic      # 后台跑 IC 测试器（单位置简写同样可用）
stkoe> t:<task_id>                   # 回放订阅某任务事件流
```

## 清理

```bash
# 删除演示数据目录与临时配置
Remove-Item -Recurse -Force example-data, stkoe.example.json   # PowerShell
rm -rf example-data stkoe.example.json                          # bash
```

> 注：`stkoe.example.json` 是 `STKOE_CONFIG` 指向的临时配置；`stkoe.json`（仓库根）为共享配置，
> 若此前存在会优先被读取，演示时请留意 `config show` 的 `config_file` 指向。
