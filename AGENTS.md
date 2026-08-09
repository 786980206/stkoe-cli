# AGENTS.md - stkoe 项目开发指南

本文档为在本仓库（stkoe，自 DataCenter 仓库拆分出的独立项目）中工作的 AI 代理提供
项目信息、代码风格规范、架构说明与演进记录。

## 项目概述

stkoe 是从 DataCenter 仓库拆分出的独立量化研究项目（对应原 `stkoe/` 子目录，当前为 v0.4.0 功能状态），包含四大模块：

- **`data/`**：数据管理框架（本指南重点，最近在迭代）
  - `table`：只读观察者 + sniff 元数据同步（已重构完成，v0.2.0）
  - `dataset`：索引表+多表 join 逻辑数据集，自动物化/增量/分区（v0.3.0 完成）
  - `field`：指标管理（catalog 注册：dataset/formula/display_name，依赖 field→dataset）
  - `mock`：参数化演示数据生成
  - `plugins/`：数据插件（`wsdata` 实时接入、`mock`）
- **`factor/`**：因子研究框架（`core` 构建器、`zoo` 因子库、`operators`、`testers` 测试器）
- **`portal/`**：Panel 可视化门户（因子测试报告查看器等）
- **`barra/`**：Barra 风格因子

## 目录结构

```
stkoe/
├── pyproject.toml         # version 0.2.0；requires-python >=3.13
├── src/stkoe/
│   ├── __main__.py        # CLI 入口 + REPL（prompt_toolkit，Tab 补全）
│   ├── data/              # 数据管理框架
│   │   ├── __init__.py    # configure/get_root/catalog/init + table API 导出
│   │   ├── table.py       # table 业务：sniff/describe/status/schema/select/create/drop/rename/update
│   │   ├── util.py        # 通用能力：FileInfo/footer/layout/signature/diff
│   │   ├── query.py       # 谓词解析 + 文件级裁剪（to_expr/prune_files）
│   │   ├── task.py        # 任务管理：同步/后台执行 + 日志/进度/暂停/取消
│   │   ├── settings.py    # 配置：data_path + ignore_cols
│   │   ├── cli.py         # typer：table/config/mock/task/dataset/stat 子命令
│   │   ├── mock.py        # 参数化数据生成 + write + write_demo
│   │   ├── grpc/         # gRPC 服务：stkoe.proto + 生成 stub + server.py（端口 9569，config 可改）
│   │   ├── catalog/       # db.py(SQLite schema)/spec.py(dataclass)/access.py(行访问)/json.py
│   │   ├── dataset.py     # dataset：scan/create/sniff/materialize/select + 增量/自动分区（产物直接写 datasets/<name>/）
│   │   ├── stat.py        # stat：dataset 统计物化（产物在 stats/<name>/，catalog type='stat'，依赖登记 stkoe_depends）
│   │   ├── field.py       # field 业务：add/list/meta/rename/del + test_code/materialize（catalog 注册）
│   │   └── plugins/       # wsdata.py / mock.py
│   ├── factor/            # 因子框架（core/zoo/operators/testers）
│   ├── portal/            # Panel 门户
│   └── barra/
└── tests/                 # pytest（conftest 提供 make_df/write_single/write_hive）
```

## 构建/测试命令

### 包管理
```bash
uv sync                 # 安装依赖（项目用 uv，不用 pip）
uv add <package>        # 添加依赖
uv add --optional dev <package>  # 添加开发依赖（pytest 等）
```
注意：`wsdata` 为本地库（DataCenter/wslib），经 pyproject 的 `[tool.uv.sources]` 指向
`E:/DataCenter/wslib`，不走 PyPI；该路径与本机环境耦合，勿改为公共依赖。

### 运行代码
```bash
uv run python -m stkoe                    # 交互式 REPL
uv run python -m stkoe table sniff demo   # 单次命令
uv run python -m stkoe table list         # 列出已注册表
```

### 测试
```bash
# 标准方式
uv run pytest -q                          # 仓库根目录直接运行

# 本机开发环境（Windows 无 3.13，用预建 venv）：
#   venv = %TEMP%\opencode\stkoe-venv（Python 3.10 + system-site-packages，polars 1.32.0）
$vpy = "$env:TEMP\opencode\stkoe-venv\Scripts\python.exe"
$env:PYTHONPATH = 'D:\proj\stkoe-cli\src'
& $vpy -m pytest tests -q
```

## 代码风格与通用约定

（自 DataCenter 根仓库 AGENTS.md 整合，仅保留适用于本项目的通用规范）

### Python 版本与环境
- Python >= 3.13（本仓库）；虚拟环境使用 `.venv`。

### 导入顺序
```python
# 1. 标准库
import os
import datetime
from pathlib import Path
from typing import Optional, Iterable

# 2. 第三方库
import polars as pl
import pandas as pd
import typer

# 3. 项目内部模块
from stkoe.data.util import signature
```

### 命名约定
- **文件/模块**：`snake_case`；**类名**：`PascalCase`；**函数**：`snake_case`
- **常量**：`UPPER_SNAKE_CASE`；**私有方法**：`_prefix`（如 `_ensure_fresh`、`_partition_plan`）
- **Polars 表达式变量**：简短的 `snake_case`（如 `x`、`y`、`expr`）

### 类型注解
- 使用 Python 3.10+ 类型语法（`X | None` 而非 `Optional[X]`）
- dataclass 字段必须写类型注解；抽象方法必须有返回类型注解

### 代码组织
- **dataclass**：优先 `@dataclass(frozen=True)` 定义不可变数据结构（如 `ColumnMeta`、`DatasetMeta`、`FileInfo`）
- **错误处理**：显式抛带提示的异常（如 `DatasetExistsError`），用 loguru 记日志：
```python
from loguru import logger
logger.error(f"出错: {e}")
```

### 文档字符串
- 使用中文编写文档字符串（与代码风格保持一致）；说明参数、返回值与用法。

### 其他注意事项
- **包管理器**：只用 uv，不用 pip。
- **数据处理**：Polars 是主要库，优先 Polars API；可配合 `polynx` 扩展。
- **日志**：使用 `loguru`。

## 数据框架（data/）设计不变式

这些是不可违背的规则，新增/修改功能时必须保持：

1. **表数据只读**：用户用外部工具更新数据，框架绝不写/删用户数据文件
   （`drop` 只删 catalog 登记与元数据，`with_data` 仅为 API 对称）。
2. **sniff 幂等**：无差异时重复 sniff 不 bump version；version 单调递增。
3. **隐式注册**：sniff 发现未注册目录自动注册，INSERT 已置 `version=1`，**不再重复 bump**。
4. **读路径不碰数据**：Query/select 只 stat + 读 catalog（file_stats），不读 footer、不 collect；
   `schema()` 例外（读 footer 元数据，不读数据页）。
5. **读前快检**：`_ensure_fresh`（stat 签名比对）在**已注册表**上触发，不一致自动 sniff；未注册表不隐式报告。
6. **布局/分区键自动识别**：`detect_layout()`（SINGLE/FLAT/HIVE），不提供手动指定
   （create/update 的 `partition_by` 参数已移除）。
7. **catalog 表名统一 `stkoe_` 前缀**：`stkoe_objects`/`stkoe_data_files`/`stkoe_file_stats`/`stkoe_tasks`。
8. **工具字段**：`ignore_cols`（默认 `optime`），`select(exclude_tool=True)` 时剔除。
9. **rename 同步目录**：`tables/old → tables/new` 移动文件夹 + 更新 display_name（未自定义时）与 updated_at。

## catalog 结构（SQLite，data_root/catalog.db）

| 表 | 说明 |
|---|---|
| `stkoe_objects` | 对象统一表：type(table/dataset/field/stat) + name + version + signature + meta(JSON)；**身份为 (type,name) 复合唯一** |
| `stkoe_data_files` | 对象下文件清单：rel_path(相对表根) + partition_path + size/mtime_ns + schema |
| `stkoe_file_stats` | 列统计：dtype/min/max/null_count（裁剪依据） |
| `stkoe_depends` | 资源依赖图：obj(type,name) → dep(type,name) + detail(JSON)；供触发/级联（stat→dataset、dataset→table） |
| `stkoe_tasks` | 任务登记：task_id/type/object_ref/status/progress/stage/error/created_at/updated_at/finished_at |
| `stkoe_task_logs` | 任务日志：task_id + 单调 seq + ts/level/message（增量拉取） |

任务状态机：`submitted -> running <-> paused -> succeeded|failed|cancelled`；
日志/进度经 `TaskControl` 写入，`flush(conn)` 分区边界批量落盘；后台任务用独立连接（`catalog().new_conn()`），
WAL 多连接读写分离；pause/stop 协作式（`ctl.check()` 在分区边界检查）。

## dataset 设计（v0.3.0）

- **注册**：catalog type='dataset'，meta 存 `DatasetMeta`（index_table/tables/keys/columns/partition_by/
  partition_gran/materialized/partition_deps/dependency_hash…）；列带 `source_table/source_field` 映射；
  依赖登记 `stkoe_depends`：dataset → 成员 table（detail 记 join keys）。
- **join 键由 index 表定义**：keys 缺省 = index 表的**全部列**；`keys=` 可显式指定 index 列的子集。
  每个键必须在所有成员表都存在，缺列明确报错（不让 join 键静默退化导致结果膨胀）。
- **接口对齐 table**：`scan/create/describe/list/update/drop/rename/status/schema/partitions/select/sniff/
  sniff_all/materialize`；`data.dataset.*` 命名空间（顶层 `list`/`create` 等仍属 table，避免遮蔽）。
- **物化**：产物为框架自持派生数据，直接写 `datasets/<name>/`（**无 `.materialized/` 嵌套层级**；
  `part=<v>/data.parquet` 分区 或 `data.parquet` flat）；`create` 默认后台自动物化（REPL/daemon 语义），
  CLI 可用 `--sync`/`--no-materialize`；已存在时**不加 `--force` 直接报错**
  （`DatasetExistsError`，明确提示用 `--force`，避免用户误以为新定义生效），
  `--force` 覆盖重建（删旧定义 + 清空 `datasets/<name>/`，再注册新定义）。
- **统计解耦**：统计不再属 dataset，独立 `stat` 模块（见下），产物在 `stats/<name>/` 与数据隔离。
- **增量**：`partition_deps` 记录每分区喂给源文件（id 集）的 `rel_path|size|mtime_ns` 签名；
  sniff 只重算失配分区；identity 镜像用 index `partition_path` 精确对齐（分区列不在文件内，不能按统计裁剪）。
- **自动分区**：`_partition_plan` 优先镜像 index HIVE 时间分区键（identity）→ 否则行数≥1M 且有时间键时按
  跨度/行数选 year/month/date（目标 50 万行/分区）→ 否则 flat。
- **幂等**：无变更重复 sniff/materialize 不 bump version（首次物化也不 bump，对齐表隐式注册语义）。
- **select 不触发物化**：物化完成（无 running 任务）走 `scan_parquet(hive_partitioning=True)`，否则实时 join；
  `partition=` 过滤用 `part` 列（hive 推断成 Date/Int，统一 cast String 前缀匹配）。
- **默认阈值**：分区最小 1M 行、目标 50 万行/分区、后台线程池 4（task.py MAX_WORKERS）。

## stat 设计（v0.4.0）

- **定位**：dataset 统计的独立物化视图，与 dataset 本质解耦（dataset 只管数据，统计归 stat）。
- **命令形态**：`stat create/select/sniff/list/describe/status/drop/rename <dataset>`；
  同名 stat 对象以 dataset 名为键，与 dataset 对象经 `(type,name)` 复合唯一共存。
- **存储**：`stats/<name>/group=<all|col>/stats.parquet`（无 meta.json，有效性在 catalog object meta）；
  `--all` = "all" + 逐索引列全部分组；`--group-col` 指定单列分组。
- **catalog**：type='stat'，meta 记 `{dataset, groups: {g: {data_key, computed_at}}}`；
  依赖边 stat → dataset（stkoe_depends），dataset rename/drop 级联 stat。
- **缓存语义**：默认读缓存，`--refresh` 强制重算；有效性键 `data_key` 复用 `dataset.data_key(name)`
  （物化态=depends_hash，未物化=当前源签名）；未预计算时 select 惰性计算并落盘 + 注册。
- **sniff**：幂等重算 data_key 失配的分组；data_key 变化才 bump version。

关键点：
- 签名 `signature()` = sha256(sorted `rel_path|size|mtime_ns`)，**相对表根**，故 rename 目录不影响签名。
- 数据根目录优先级：`STKOE_LOCAL_DATA` 环境变量 > 配置 `data_path` > `~/.stkoe`。
- 配置查找：`STKOE_CONFIG` 环境变量 > `./stkoe.json` > `~/.stkoe.json`；本地 `stkoe.json` 已 gitignore。

## 模块职责（新增公共能力放哪）

- **util.py**：文件/布局/footer/签名/差异（与对象类型无关，`__all__` 已声明）。
- **query.py**：谓词解析 + 文件级裁剪（table.select 复用；dataset.select 复用）。
- **task.py**：任务模型（`run_task` 同步/后台；`TaskControl` 日志/进度/暂停/取消；`task_list/stop/pause/resume/log`）。
- **catalog/access.py**：catalog 行访问（get_object/insert_object/update_object_meta/replace_data_files/
  add_dep/set_deps/clear_deps/deps_of/dependents/rename_dep/rename_obj…）。
- **table.py**：只留 table 业务逻辑与表级聚合（`_describe_row` 等）。
- **dataset.py**：dataset 业务（scan/物化/增量），复用 util/task/query/access，不把公共逻辑写回业务模块。
- **stat.py**：stat 业务（create/select/sniff/status/drop/rename + 级联 `_drop_cascade`/`_rename_cascade`），
  只依赖 dataset 的公共 API（`data_key`/`describe`/`select`），不触碰 dataset 私有实现。
- 原则：field 迁移时优先复用 util/task/query/access，不把公共逻辑写回业务模块。

## 演进记录

### v0.4.1（维护：动词统一 + 级联/依赖修复）
- **CLI 动词统一**：table/dataset/stat 三组命令统一为 `add/list/meta/get/scan/del/set/rename`，
  废弃旧 `create/sniff/describe/update/drop/select` 形态（`describe` 留为 dataset.meta 兼容别名）；
  `--all/--json/--force/--refresh/--background` 全量参数化；`table col <name> --json` 列清单。
- **级联语义修正**：`_ensure_fresh`（读路径自动 sniff）不再级联下游（`cascade=False`），
  只有显式 `table scan` 才触发 dataset/stat 重算——修复物化期间自触发导致的 dataset 版本多跳。
- **依赖修复**：dataset 依赖登记补上 **index 表** 边（原来只登记非 index 成员表），
  `table del/rename` 的依赖保护对 index 表生效。
- **stat 输出对齐**：分组统计首列以**分组列名**命名（如 `sym`/`date`，非固定 `group`）；
  field 顺序按源表列序（数值/字符串/时间列各自内部保持原序）。
- 测试：全量 105 用例 `uv run pytest -q` 全绿；test_cli 补 REPL 顺序隔离（autouse 恢复同步默认）。

### v0.4.4（portal 对接：gRPC 面扩展 + 数据层加固）
- **Execute 新动词**（供 portal 桥调用）：`table candidates`（候选表）、`dataset validate`（配置校验）、
  `dataset get --extra`（字段快照 extra，如 category）、`stat get/add`（统计物化/写入，`--group`/`--refresh`）、
  `field create/test/test-code/rename/set`（test-code 为不落盘公式测试，返回 test report；set 走 JSON 参数）、
  `version`（版本号）。
- **Select 扩展**：分页（limit/offset）、过滤（filters 谓词）、排序（sort）、total（行数，分页用）。
- **RunTask 流式**：`(cmd, args)` 形态长任务经 TaskEvent 流返回（log/progress/result/done/error），
  portal tasks.rs 直接消费；`dataset scan/materialize` 分支返回前端契约 payload
  （`materialized_payload(name, elapsed_ms)`：datasetId/columns/rows/dataFile/elapsedMs）。
- **Health / --reload**：Health RPC 供 portal 状态灯；`server run --reload` 开发热重载。
- **真实 bug 修复（live E2E 抓出）**：
  1. join 键时区不一致崩溃 — `dataset._align_keys()`：`_view_lf` 各成员表 join 键统一 cast
     为无时区同精度（`optime` 曾出现 `[μs]` vs `[μs,UTC]` 混合）；回归 `test_join_keys_tz_mismatch`。
  2. RunTask/Execute 结果 JSON 序列化不支持 `pl.Date/Decimal` — `server._dumps`（datetime/date/time
     → isoformat、Decimal→float），替换两处 `json.dumps`。
  3. dataset 物化任务缺 portal 契约 — 见上 `materialized_payload`。
- **数据层**：dataset del 支持 `--force`、rename；query 过滤/排序；spec extra 字段。
- 测试：全量 135 用例 `uv run pytest -q` 全绿（新增 test_grpc 2 + test_dataset 1，行序无关断言）。

### v0.4.3（gRPC 交互服务）
- **gRPC 服务**：`src/stkoe/grpc/`（stkoe.proto + 生成 stub + server.py）；默认端口 9569，
  `config set --grpc-port` 可改（StkoeConfig.grpc_port）；REPL 启动时同步后台启动
  （绑定失败降级不阻断），退出自动停止；独立前台运行 `stkoe server`。
- **接口约定**：小数据量（table/dataset/stat/field 的 list/meta、config show、task list、version）
  走 Execute RPC → JSON；表格数据走 Select RPC → 完整 Arrow IPC 帧（bytes ipc + schema_json，
  polars/pyarrow 直接读）；type 空则自动探测（先 dataset 后 table）；业务错误放响应体
  （code/error 字段），不用传输层状态。只绑定 127.0.0.1。
- **依赖**：grpcio（运行时）+ grpcio-tools（dev）；protoc 生成后需手动把 stkoe_pb2_grpc.py
  顶部 `import stkoe_pb2` 改为 `from . import stkoe_pb2`。
- 测试：全量 116 用例 `uv run pytest -q` 全绿（新增 tests/test_grpc.py 7 用例）。

### v0.4.2（field 迁移 + factor/barra/portal 导入修复）
- **field 迁移完成**：遗留 YAML 实现（依赖已删的 `ResponseData`/`SYS_COLS`）替换为 catalog 注册
  模式（`stkoe/data/field.py`）：type='field'，meta 存 `FieldMeta`（dataset/formula/display_name/…），
  依赖边 field→dataset（stkoe_depends）；接口 `add/list/meta/rename/del` + CLI `field` 子命令
  + REPL 补全；字段不物化（formula 存根），fields/ 目录仅作存档保留。
- **factor/barra/portal 导入修复**：nulloperator/btop 相对导入层级修正；`FeatureSpec` 补
  `direction` 字段（zoo 因子方向规范，log_market_cap 已按标准 `fac_*` 形态重写）；
  `barra/__init__.py` 拆 WSData 为惰性导入（高层模块不硬依赖 wsdata）；portal viewers
  统一指向 `models/factor_test_result`；`portal/pages/factor.py` 调试残留改惰性加载+降级提示。
- **测试**：全量 109 用例 `uv run pytest -q` 全绿（新增 tests/test_field.py 4 用例）。

### v0.4.0（目标 `83d962f`，tag `v0.2.0`）
- **模块化重构**：table.py（原 613 行）公共逻辑抽离为 `util.py`/`task.py`/`query.py`/`catalog/access.py`，只留 table 业务。
- **配置系统**：`settings.py`（原 `config.py` 改名，规避遮蔽 `data.config()`）＋ `stkoe config show/set` CLI。
- **工具字段简化**：`sys_cols`/`tool_cols` 统一为 `ignore_cols`（支持多个）；`ColumnMeta.is_tool`。
- **CLI**：`table`/`config`/`mock` 三个 typer 子命令；REPL Tab 补全（顶层命令→子命令→表名）。
- **mock**：`mock gen <name> --kind klday|tdcal|feature|common` 参数化生成 + write 写盘+sniff。
- **catalog 表加 `stkoe_` 前缀**；`create --all` 批量注册未注册且有数据目录；`rename` 同步移动目录；`partition_by` 手动参数移除（自动识别）。

### v0.1.0
- 原始 table 模块（单文件），含 sniff/update/select 骨架、Dagster asset 草案、omnibox 集成。

### 开发中（v0.3.0/v0.4.0 规划）
- **task 异步化（已完成）**：`run_task(background=True)` 线程池后台执行；日志表 `stkoe_task_logs`（seq 增量拉取）；
  `progress/stage` 分区边界 flush；pause/resume/stop 协作式；`stkoe task list|stop|pause|resume|log|clean` CLI + REPL 补全；
  `task stop --all` 停止所有运行/暂停任务并等待收尾后清理完成态任务（`task_clean` 删除 succeeded/failed/cancelled，日志级联删除）。
- **dataset 重设计（已完成）**：接口对齐 table；`sniff/materialize` 增量重物化、`partition_deps` 源文件级依赖签名、
  自动分区（镜像 index HIVE → 数据量+时间键 year/month/date → flat）、后台自动物化；
  `select` 不触发物化只按完成状态选路径；**物化产物直接写 `datasets/<name>/`（去掉 `.materialized/` 层级）**；
  遗留 `field.py` 待替换。
- **stat 解耦（已完成）**：统计从 dataset 拆出为独立 `stat` 模块（`stat create/select/sniff/list/describe/status/drop/rename`），
  产物写 `stats/<name>/group=<all|col>/stats.parquet`（`--all` 覆盖 "all"+逐索引列），catalog type='stat'（`(type,name)` 复合唯一身份），
  默认读缓存 + `--refresh` 重算（有效性键 `data_key` 复用 `dataset.data_key()`），未预计算时 select 惰性计算；
  dataset rename/drop 经 `stkoe_depends` 级联 stat。
- **stkoe_depends 依赖图（已完成）**：`stkoe_depends` 表登记资源依赖（dataset→table、stat→dataset），
  access.py 提供 add_dep/set_deps/clear_deps/deps_of/dependents/rename_dep/rename_obj，供后续触发/级联。

### 待办 / 已知缺口
- field 已随 v0.4.2 迁移完成（catalog 注册 + test_code/materialize）；无重大遗留模块。
- `test_task.py` 中 pause/resume/cancel 相关用例对时序敏感，全量并行跑偶发 flaky（单文件跑稳定）。
- portal 对接面按需扩展（Execute 动词 / RunTask 分支），见 v0.4.4。
