# stkoe

stkoe 数据服务（gRPC）：管理**表 / 数据集 / 衍生指标 / 样本池 / 因子 / 统计**等数据资产，
当前正在进行 **V3.0 图数据库血缘重构**（graphqlite 嵌入式图库，记录资产间血缘关系）。

- **V2.0 基线**：gRPC 服务 + SQLite catalog + polars 计算，代码已备份至 `V2.0/`
- **V3.0 已落地**：`src/stkoe/graph/` 图模块（节点/边/版本/事件响应）+ `GraphService`
  （table/index/panel/fieldset/sample/feature/factor/test 三路径统一，catalog.db 废弃），
  详见 [`graph-design.md`](graph-design.md)

## 当前功能（做了什么）

### V3.0 数据资产（已全面切 graph）

| 模块 | 能力 |
|---|---|
| `table` | 注册/读取/删除本地 parquet 表、列元数据、`list --candidate`（登记/版本/依赖走 graph） |
| `index` | 独立索引资产主体（symbol/datetime 列），table 恒 type="table" |
| `panel` | 逻辑数据集（原 dataset，index 表 + 成员表 + keys），实时 join 视图；dataset 旧别名转发 |
| `fieldset` | 衍生指标集（公式引擎 polars 插件制），check 校验写回 validated |
| `sample` | 样本池（fieldset 视图过滤产物，无物化，实时构造） |
| `feature` | 因子定义库（命名公式，纯定义），`feature test` 在样本视图即时求值 |
| `factor` | 最终因子（feature 公式 + sample 视图 + pipeline 算子链），可物化、幂等 |
| `test` | 因子测试数据集（factor_test）+ 六类测试器（stat 集成） |
| `stat` | 覆盖率 / 存续统计（storage），输出 parquet 产物 |
| `mock` | 演示数据生成（`stkoe mock demo`/`gen`） |
| `task` | 后台任务框架（SubmitTask/SubscribeTask/TaskControl，协作式取消） |

### V3.0 图实现（已落地，三路径统一走 GraphService）

- **`graph` 模块**（graphqlite 嵌入式图数据库，Cypher）：
  - 节点（10 类资产，label=类型，id=`<type>:<name>`）+ DEPENDS 边（依赖方→被依赖方，
    带 `required_version` 消费水位 + role/join）
  - **版本 = 高精度时间戳**（`time.time_ns()`）+ `version_list` 事件日志
  - **事件响应**：`notify_change`（上游变化→下游置脏）→ `resolve`/`resolve_all`
    （拓扑重算：积累事件合并→storage 钩子→版本递增+边水位对齐），成环报错
  - 删除约束：无下游才可删（force 绕过）；血缘链
    `table/index → panel → fieldset → sample → factor`
- **GraphService**（`graph/service.py`）：table/index/panel/fieldset/sample/feature/factor/test
  统一服务——登记/依赖/版本进 graph；物理指纹表（stkoe_data_files/stkoe_file_stats）
  迁入 catalog.db 普通表；factor/test 物化落盘 `factor/`、`factor_test/`（幂等）；
  Execute（dispatch）与 SubmitTask（任务版 handler）三路径对齐
- **`graph` 命令（gRPC Execute，JSON 返回）**：`e:graph lineage [--node][--depth]` /
  `e:graph nodes [--type]` / `e:graph stats`
- **可视化**：`tools/graph-viewer/`（Cytoscape.js 独立页）+ **portal 前端右上角
  "血缘关系"抽屉**（Tauri 经 gRPC 拉取渲染）

## 接下来要做什么（路线图）

1. **panel 物化**：panel scan 落盘 + index 唯一性校验等物理细节
2. **V2.0 清理**：任务版 table/dataset handler 切 graph、V2.0 controller 死代码评估
3. **列级血缘**：DEPENDS 边 detail 的字段映射升级为独立列节点图
   （`(column) -[:DERIVES]-> (column)`）
4. **版本/事件沉淀**：version_list 过期裁剪、事件合并的跨依赖精确并集
5. **图算法能力**：graphqlite 内置算法（PageRank/中心性/连通分量）用于资产重要性分析
6. **测试**：图模块更多边界用例 + gRPC 全链路回归

## 环境要求

- Python >= 3.13；包管理用 [uv](https://docs.astral.sh/uv/)
- 依赖：graphqlite / grpcio / orjson / polars / pyarrow（dev：grpcio-tools / pytest）

## 安装与运行

```bash
uv sync                 # 安装依赖
uv run stkoe serve      # 前台运行 gRPC 服务（默认 127.0.0.1:9569）
uv run pytest -q        # 全量测试
```

## 配置（stkoe.json）

- 查找优先级：`STKOE_CONFIG` > `./stkoe.json` > `~/.stkoe/stkoe.json`
- 已知键：`grpc-host`（默认 127.0.0.1）、`grpc-port`（9569）、`data-dir`（~/.stkoe）；任意键进 extra

```bash
uv run stkoe config show | set --<key> <value> ...
```

## CLI / gRPC 命令

请求统一为 `<source> <action> <args...>`；CLI 子命令与 gRPC Execute 同一分发：

```bash
uv run stkoe table list | meta demo | add demo | set demo --display_name 演示表 | col demo sym --unit 元
uv run stkoe dataset add ds1 index m1 --keys sym,date | scan ds1 | get ds1 | meta ds1
uv run stkoe fieldset add fs1 --dataset ds1 | fieldset add fs1 ma5 --formula "price.rolling_mean(5)"
uv run stkoe sample add sp1 --dataset ds1 --formula "(date>='2026-01-01')"
uv run stkoe feature add ma5 --formula "price.rolling_mean(5)"
uv run stkoe factor add fac1 --feature ma5 --sample sp1 --pipeline "nothing()"
uv run stkoe test add t1 --factor fac1 --returns r --groupby ic --marketcap fv
uv run stkoe stat scan table demo | stat scan dataset ds1 | stat scan t1 --kind ic
uv run stkoe mock demo                          # 生成演示 parquet（需 table add 注册）
uv run stkoe task list                          # 后台任务列表

# V3.0 血缘图（JSON 返回，供 portal 血缘模块 / 脚本使用）
uv run stkoe graph lineage                      # 全图（Cytoscape elements payload）
uv run stkoe graph lineage --node panel:ds1 --depth 3   # 指定节点上下游子图
uv run stkoe graph nodes --type panel           # 节点摘要（中心节点选择器）
uv run stkoe graph stats                        # 节点/边统计
```

## 血缘可视化

- **portal 前端**：右上角"血缘关系"按钮 → 右侧抽屉 / 展开完整页面（Cytoscape.js）
- **独立工具**：`tools/graph-viewer/`（导出 JSON + 静态页，离线可用）
- **数据源**：`<data-dir>/catalog.db`（graphqlite）；`graph lineage` 返回
  Cytoscape elements payload（详见 api.md §3.13）

## gRPC 协议

见 `src/stkoe/grpc/stkoe.proto`：Execute（流式 DataHeader + JsonData/ArrowTable）/
SubmitTask / SubscribeTask / TaskControl / Health。`(source, action)` 全量命令表见
[`api.md`](api.md)。

## 测试

```bash
uv run pytest -q        # 全量 271 用例（graph 模块 48 例 + gRPC/资产模块）
```

## 目录结构

```
src/stkoe/
├── cli.py / args.py / jsonutil.py / logutil.py / settings.py
├── grpc/               # stkoe.proto + 编译产物 + dispatch.py（Execute 分发）+ server.py
├── table/ dataset/ fieldset/ sample/ feature/ factor/ factor_test/ stat/ mock/
├── graph/              # V3.0 资产血缘图（graphqlite）
│   ├── model.py        # DataChangeEvent / AssetMeta / DependencyEdge / 列元数据
│   ├── store.py        # GraphStore：节点/边 CRUD + BFS 血缘遍历 + txn 事务
│   ├── export.py       # build_payload / node_summaries（→ Cytoscape elements JSON）
│   ├── events.py       # 事件合并（并集/交集）与积累（required_version 水位线）
│   ├── controller.py   # GraphController：CRUD + 依赖约束 + notify_change/resolve(_all)
│   ├── handlers.py     # v3.0-def.py 形态的资产 Handler
│   ├── version.py      # 高精度时间戳版本号
│   └── errors.py
└── task/               # 后台任务框架
tools/graph-viewer/     # Cytoscape.js 血缘可视化（独立页）
V2.0/                   # V2.0 全量备份（重构基线，勿改）
v1.0/                   # 旧版参考实现（v0.5.1）
```

## 文档索引

- [`api.md`](api.md)：对外 API 全量文档（gRPC/Execute/SubmitTask/CLI/配置/存储布局）
- [`graph-design.md`](graph-design.md)：V3.0 图设计（节点/边/版本/事件模型）
- [`example.md`](example.md)：全流程演练（mock 造数 → 因子测试）
- `AGENTS.md`：开发指南（目录结构/架构要点/变更记录）
