# stkoe-cli 任务进度（2026-08-16 凌晨会话存档）

> 本文件是会话断点存档，供新会话恢复上下文。完整背景见 `AGENTS.md`（近期变更记录
> 最上方两条）+ `graph-v3-gap.md`（P0/P1/P2 全部标 ✅）。

## 当前状态（一页总结）

- **V3.0 graph 重构全部完成**：table/index/panel/fieldset/sample/feature/factor/test 基于
  graph（`src/stkoe/graph/service.py` GraphService），Execute/任务版/CLI 三路径共用一套实现。
- **P0/P1/P2 全部落地**（graph-v3-gap.md 已标 ✅）：
  - P0：物理 diff → 范围化事件（datetime 区间 + delete 事件）；factor/test 增量物化
  - P1：panel/sample/feature update 统一走 `graph.resolve` 收口；panel/fieldset 物化
  - P2：沿链增量物化（分区镜像 index + 删区间重算/合并写回）、get 三态、version_list
    裁剪、index 唯一性校验、fieldset 变更 self_invalidate 联动、`store._walk` 逐层批量
    Cypher 重写
- **测试全绿**：`tests/` 全量 **194 passed**（约 60s）。
- **Git 最新提交**：`af71801 docs(graph): graph-v3-gap.md 标记 P2 全部落地`；
  前两个提交 `fc46b8e`（P2 主体）、`98a0e57`（P2 收口修复）。

## 用户核心设计约束（必须遵守）

1. **scan → update 语义**：上游传导就绪检查（`assert_ready` BFS 全链 valid）；源头不齐失败
2. **下游物化分区镜像 index**：`_index_partition_keys(node)` 沿链 Cypher 找 index 取其分区键；
   分区写保留分区列 + hive 目录（`key=value/data.parquet`），读 `hive_partitioning=True`
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

## 关键技术结论（实测）

- **graphqlite**：变长路径 `-[:DEPENDS*1..N]->` 可用但 `length(p)` 对多跳恒返回 1（不可靠）；
  批量 `MATCH ... WHERE a.id IN $ids` 逐层拿下一层+边属性（可靠，用于 `_walk`）
- **Python 3.13 `isolation_level=''`**（legacy）：`GraphStore.execute` txn() 外 DML 必须立即
  commit（否则 close 回滚，指纹残留 bug 的根因）
- **幂等 materialized 读位置**：`resolve` 把 materialized 放节点顶层而非 extra → 幂等判断
  用 `node.get("materialized") or extra.get("materialized")`（曾 bug：恒 False 永不幂等）
- polars：`is_between` 需 `pl.lit()` 包裹；多文件 dtype 不一致用 `vertical_relaxed`
- asof join：String 日期先 cast Date 再 `join_asof`，结果 cast 回 String（触发 UserWarning 无碍）

## 已提交（本次会话）

| 提交 | 内容 |
|---|---|
| `98a0e57` | fix: test 增量物化漏写盘（flat 分支算完 df 未写回）+ factor/test 幂等 materialized 判断修正；测试适配 get 三态 |
| `fc46b8e` | feat: 沿链增量物化 + get 三态 + Cypher 批量血缘（P2 主体：_walk 重写/version_list 裁剪/self_invalidate/分区镜像） |
| `af71801` | docs: graph-v3-gap.md 标 P2 全部落地 |

## 剩余待办（下一步从这里继续）

### 1. 冗余清理（用户明确指示过）
- **V2.0 死代码 controller**：`src/stkoe/{table,dataset,fieldset,sample,feature,factor,
  factor_test}/controller.py` 是 V2.0 残留（业务已在 GraphService 一份）。**注意**：
  `V2.0/tests/` 的死代码测试仍从这些路径导入（`.venv/Scripts/python.exe -m pytest
  V2.0/tests/test_table.py -q` 可单独跑）——清理前需确认 V2.0/tests 是否保留（AGENTS.md
  说 V2.0/tests 是归档基线，默认不收集；若删除 controller 需同步处理 V2.0/tests 导入）
- `graph/handlers.py` 的 handler 形态（v3.0-def.py 形态示例）与 service 层并存，评估是否保留
- 运行服务 PID 5144（`-m stkoe serve`，21:32 启动）加载旧代码，portal 要新功能需重启

### 2. graph-v3-gap.md 剩余可选 P2
- `symbol_scope` 提取（读数据页，P2 可选——当前 datetime 区间已够用）
- stat 是否纳入图资产（G9：stat 目前在 catalog 外，血缘图看不到 stat 节点——设计决策，
  需问用户或按"不进图"保持）
- ModelNode 未实现（G10，后续规划）

### 3. 持续优化循环（用户指示：早 8 点前循环"优化-实现"，每项提交文档+Git）
- 结构清晰 / 容错 / 数据处理性能三个方向
- 每次优化必须：更新 AGENTS.md 变更记录（最上方）+ 相关 md + Git commit（中文 message 写明改动）

## 命令速查

```bash
# 测试（沙箱内 uv 不可用，用预建 .venv）
.venv/Scripts/python.exe -m pytest tests -q -W ignore::UserWarning        # 全量 ~60s，194 例
.venv/Scripts/python.exe -m pytest tests/test_graph_service.py -q         # 快速验证 graph service
# 单跑 V2.0 死代码测试（controller 删除前先确认）
.venv/Scripts/python.exe -m pytest V2.0/tests/test_table.py -q
# git 提交（PowerShell 5 不支持 &&，用 ; 分隔）
git -C D:\proj\stkoe\stkoe-cli add <files>; git -C D:\proj\stkoe\stkoe-cli commit -m "..."
```

## 已知 flaky
- `test_task_*` / `test_subscribe_live_events` / `test_task_delete`：max_seq 已防御修复
  （并发 fetchone None → 回退 0），但偶发；重跑即可

## 环境
- 仓库：`D:\proj\stkoe\stkoe-cli`；V2.0 备份区在 `V2.0/`（不要改其下代码）
- 服务 PID 5144 运行旧代码；data_dir 默认 `~/.stkoe`（dispatch 已 expanduser 处理）
- goal：持续优化到早 8 点或事项做完（goal-095097b8-...，max 8 轮）
