#!/usr/bin/env bash
# example.md 全流程一键汇总（§0–§11 非交互部分）
# 用法：仓库根执行 bash <本脚本>（需先 uv sync）；幂等可重跑
set -euo pipefail

# ========== §0 准备：独立数据目录 + mock 造数 ==========
rm -rf example-data stkoe.example.json
export STKOE_CONFIG=./stkoe.example.json
uv run -m stkoe config set --data-dir ./example-data
uv run -m stkoe mock demo

# ========== §1 发现源头资产 ==========
uv run -m stkoe index add index --symbol-col sym --datetime-col date
uv run -m stkoe table add m1
uv run -m stkoe index meta index
uv run -m stkoe index get index --where "date >= '2024-01-02'" --limit 5

# ========== §2 逻辑数据集（panel：join 视图，update 物化）==========
uv run -m stkoe panel add ds1 index m1
uv run -m stkoe panel meta ds1
uv run -m stkoe panel update ds1
uv run -m stkoe panel get ds1 --limit 5
uv run -m stkoe panel get ds1 --partition 2024 --limit 5
find example-data/panel/ds1 -name data.parquet   # → part=2024/ + part=2025/

# ========== §3 覆盖率统计（stat，对 panel）==========
uv run -m stkoe stat scan panel ds1
uv run -m stkoe stat get panel ds1 --partition_by all
uv run -m stkoe stat meta panel ds1

# ========== §4 衍生指标集（fieldset）==========
uv run -m stkoe fieldset add fs1 --panel ds1
uv run -m stkoe fieldset add fs1 x2 --formula "x * 2.0"
uv run -m stkoe fieldset check fs1 x2
uv run -m stkoe fieldset update fs1
uv run -m stkoe fieldset get fs1 --limit 5

# ========== §5 样本池（sample：fieldset ∩ index 键集合）==========
uv run -m stkoe mock gen idx2 --kind index --n-syms 300 --n-days 100
uv run -m stkoe index add idx2 --symbol-col sym --datetime-col date
uv run -m stkoe sample add sp1 fs1 idx2
uv run -m stkoe sample check sp1
uv run -m stkoe sample update sp1

# ========== §6 因子定义库（feature）==========
uv run -m stkoe feature add ma5 --formula "x * 2.0" --unit "元"
uv run -m stkoe feature test ma5 --sample sp1
uv run -m stkoe feature update ma5

# ========== §7 最终因子（factor）==========
uv run -m stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
uv run -m stkoe factor check fac1
uv run -m stkoe factor update fac1
uv run -m stkoe factor get fac1 --limit 5
uv run -m stkoe factor get fac1 --partition 2025 --limit 5
uv run -m stkoe factor meta fac1

# ========== §8 因子测试数据集（tester）==========
uv run -m stkoe tester add t1 --factor fac1 --returns r --groupby ic --marketcap fv
uv run -m stkoe tester check t1
uv run -m stkoe tester update t1
uv run -m stkoe tester get t1 --limit 5

# ========== §9 增量更新演示（上游变化 → 沿链增量物化）==========
uv run python -c "import polars as pl; from stkoe.mock.gen import index as g; df=g(n_syms=300,start='2026-01-01',end='2026-06-30'); pl.concat([pl.read_parquet('example-data/index/index/data.parquet'),df]).write_parquet('example-data/index/index/data.parquet')"
uv run -m stkoe index update index        # 检测文件变化 → 铸版本 + 全链下游置脏
uv run -m stkoe panel update ds1          # 增量：重算受影响时间桶
uv run -m stkoe fieldset update fs1
uv run -m stkoe sample update sp1
uv run -m stkoe feature update ma5
uv run -m stkoe factor update fac1
uv run -m stkoe tester update t1
find example-data/panel/ds1 -name data.parquet      # → part=2024/ + 2025/ + 2026/（factor/tester
                                                    #   样本键集合限 idx2 2024 年，仍只有 part=2024）
# 等价一条命令（README §6.13 沿链级联 update）：uv run -m stkoe graph update --all

# ========== §10 因子测试器（stat 集成，最终测试）==========
uv run -m stkoe stat scan t1 --kind ic
uv run -m stkoe stat scan tester t1 --kind bucket_returns
uv run -m stkoe stat scan tester t1 --kind bucket_turnover
uv run -m stkoe stat scan tester t1 --kind autocorrelation
uv run -m stkoe stat scan tester t1 --kind factor_returns
uv run -m stkoe stat scan tester t1 --kind coverage
uv run -m stkoe stat get t1 --kind ic --partition_by ic_d1

# ========== §11 血缘可视化（graph；serve/gclient 交互部分另开终端）==========
uv run -m stkoe graph lineage
uv run -m stkoe graph nodes
uv run -m stkoe graph stats
uv run -m stkoe graph columns --node fieldset:fs1
uv run -m stkoe graph lineage --columns
uv run -m stkoe graph lineage --column fieldset:fs1.x2

echo "== 完成：产物保留在 example-data/；清理：rm -rf example-data stkoe.example.json"