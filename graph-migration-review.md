# V2 → V3 迁移评审：handler 示例整合冲突与待完善项

> 背景：V3 设计阶段提供了 v3.0-def.py 形态的 Handler（graph/handlers.py，classmethod +
> controller 参数，含 add/get/meta/set/delete/check/notify_change/on_change/materialize 等
> 钩子）；随后把 V2.0 的 table/dataset/fieldset/sample/feature/factor/test 实现整合进
> GraphService（graph/service.py）。本文梳理两层设计碰在一起产生的冲突、不合理点与
> 正确性问题，作为后续完善清单。严重度：🔴 影响正确性 / 🟠 设计冲突 / 🟡 一致性瑕疵。

---

## 一、🔴 正确性问题（当前就存在的 bug）

### 1. 失效传播断裂 → 上游变化不会让 factor/test 物化重建（数据过期）

- `GraphController._propagate_stale` 只做 BFS 下游**置脏**（`valid=False, materialized=False`），
  **不 bump 下游版本**（controller.py `_mark_stale`）。
- 但 GraphService 的物化幂等签名**完全依赖上游版本**：
  - `_factor_hash` = `feature.version` + `sample.version` + engine/pipeline/factor_col
  - `_test_hash` = `_factor_hash(factor)` + spec + 测试列名
- 传播链：table/index 物理变化 → `notify_change`（**表自身铸版本** + panel/fieldset/sample/factor
  置脏）→ `sample.version` **不变** → `_factor_hash` 不变 → `factor scan` 幂等判定"无变化"跳过
  → **物化数据是旧的**。fieldset 定义变化（set/add_field/check）同理。
- `fieldset_scan` 走了 `graph.resolve`（有积累事件才 bump），但 factor/test 的 scan 不 resolve，
  且 sample/feature 的版本在纯置脏路径下永远不会被 bump。
- **修法方向**（选一）：
  a) `_propagate_stale` 置脏时连 bump 版本（需避免 version_list 膨胀，可只 bump 不记事件）；
  b) 物化 hash 加入"脏标记"或事件水位（`required_version` 消费水位，见设计 §4.2）；
  c) `factor_scan`/`test_scan` 前置 `resolve` 上游链（当前只有 fieldset_scan 这么做）。

### 2. meta 形态双轨 → V2.0 客户端（portal）按旧形态解析会失败

- V2.0：各资产 `*Meta.to_dict()`（SampleMeta 有顶层 `dataset/engine/formula/keys/columns` 等）。
- graph 版：`sample_meta`/`feature_meta` 等直接返回 `graph._meta(node)` = AssetMeta 扁平形态
  （`type/name/version/version_list/valid/...` + 专属字段塞进 `data`，且 id 带 `"fieldset:fs1"`
  前缀）。
- 同一"资产 meta"在 `table/index/panel/fieldset` 是 service 手工 dict（对齐 V2.0），在
  `sample/feature/factor/test` 是 AssetMeta 或半手工 dict——**同一产品两种形态**。
- portal 的 `sample/feature` 列表/详情按 V2.0 字段解析会拿不到数据。
- **修法方向**：统一对外 meta = V2.0 形态 dict（service 手工拼装），AssetMeta 只作内部载体；
  需要一份各资产 meta 的规范化（对齐 api.md §3.7 已承诺的字段）。

### 3. catalog.db 同名双结构（新旧 schema 混杂，无迁移工具）

- V2.0 controller 与 GraphService **都写 `<data_dir>/catalog.db`，但 schema 完全不同**：
  V2.0 建 `stkoe_objects/stkoe_depends/stkoe_data_files/stkoe_file_stats`（普通表）；
  GraphService 建 graphqlite 扩展表 + 自己的指纹表。同一文件里两张"catalog"。
- 旧数据（V2.0 入库）在 stkoe_objects 表里，graphqlite 打开读不到图 → 前端血缘空白
  （用户已实际遇到）；物理指纹表虽同名但结构也不同。
- **修法方向**：提供一次性迁移脚本（V2.0 catalog → 新结构：遍历 stkoe_objects 重建图节点/
  边 + 指纹）；或至少在文档明示"旧数据需 `table add --all` 重新发现入库"；长期删掉 V2.0
  catalog 读写路径（见 §四-13）。

---

## 二、🟠 设计冲突（V3 handler 示例 vs V2 整合）

### 4. 三层职责重叠：graph/handlers.py ↔ graph/service.py ↔ V2.0 controller

- `graph/handlers.py`（V3 示例形态）只被 service 用来做**登记**（add/delete/set/meta/col），
  其 `get`/`check`/`materialize`/`on_change` 钩子在 service 里没被复用（service 自实现
  get/check/scan/物化）。
- `graph/service.py` 是实际的对外服务层；V2.0 controller（src/stkoe/{table,dataset,...}/
  controller.py）是完整功能的第三套实现（catalog 版），新代码路径已完全不引用它，但它仍
  完整存在并被 tests 大量用例覆盖。
- **后果**：同一资产"登记在 handlers、服务在 service、旧实现躺在 controller"；改动要三处
  对齐心智；V2.0 controller 与新代码并存易误用。
- **修法方向**：明确分层——handlers 收窄为"节点/边账本定义"（或直接并入 service 方法），
  service 为唯一对外实现；V2.0 controller 降级为文档参考或移入 `V2.0/`（测试同步迁移）。

### 5. 事件响应机制（notify_change/resolve/accumulated/stale）只驱动了局部

- GraphController 的事件流设计（铸版本 + version_list 事件日志 + 出边 required_version 水位
  + 积累合并 + resolve 拓扑重算）是 V3 的核心，但**实际服务路径几乎不消费它**：
  - 只有 table/index 物理变化走 `notify_change`；`fieldset_scan` 走 `resolve`；
  - 读取（_fieldset_view_lf/_sample_view_lf/_factor_view_lf）与 factor/test 物化**不驱动**
    resolve/accumulated，`stale`（失效待重算）节点从不被自动重算；
  - `version_list` 事件日志累积后无裁剪，版本是纳秒时间戳 + 事件 dict，长期运行会膨胀。
- **后果**：图的状态（valid/materialized/version）与读取/物化结果脱节；§1 的过期 bug 即由此
  而来。
- **修法方向**：要么把"读取前 resolve 依赖链 / 物化前消费积累事件"接入 service 各路径
  （对齐 V3 设计），要么**明确降级**：事件机制只服务血缘展示与版本审计，正确性由
  service 的 hash/幂等独立保证（但需先解决 §1 的传播）。

### 6. 物化语义三套并存；panel/fieldset 物化能力退化

- V2.0：`dataset scan`/`fieldset scan` 真实落盘（datasets/、fieldsets/）+ curated 读。
- graph：panel **无物化**（每次实时 join）；`fieldset_scan` 只 `resolve` 标记
  `materialized=True`，**无物理产物**（返回 fields_count/rows 但不落盘）。
- factor/test 有物化（新实现，落盘 factors/、factor_tests/）。
- **后果**：大 panel 反复实时 join 无加速；`fieldset scan` 名不副实（没落盘却报 materialized）；
  V2.0 的"物化产物加速读取"能力丢失。
- **修法方向**：补齐 panel/fieldset 物化（scan 落盘 + curated 读，镜像 V2.0 行为）或改命令
  语义（fieldset scan → 校验/标记，不承诺落盘，文档/返回字段同步）。

### 7. graph/handlers.py 的 `get` 返回节点原始属性，与对外 get 语义冲突

- handlers 的 `get(cls, ctrl, name)` 返回 `ctrl.get`（节点元数据 dict），而 service/dispatch 的
  `get` 返回 ArrowTable（真实数据）+ meta。同名方法两种语义，容易误导后续开发。

---

## 三、🟡 接口/一致性瑕疵

### 8. 错误类型不统一

- `service._require_node` 对**所有**资产（panel/fieldset/sample/factor/tester...）抛
  `TableNotFoundError`（"panel not registered: xxx" 报 TableNotFoundError 语义错位）；
  graph.controller 抛 `AssetNotFoundError`；V2.0 各有专属 `*NotFoundError`。dispatch 层错误
  经 CommandError 转 DataHeader。对外错误体系三套。

### 9. 三路径覆盖不齐

- `index`/`panel` 无 SubmitTask 任务版（只有 Execute + CLI）；
- `graph` source（lineage/nodes/stats）只有 Execute，CLI 没有 `stkoe graph` 子命令
  （`stkoe graph nodes` 报"未知命令"）；
- `dataset` 任务版转发 panel 但 Execute 的 `e:dataset add --materialize` 静默忽略该 flag
  （panel 无物化，参数吞掉不报错）。

### 10. 返回字段/列元数据在部分资产仍是简化形态

- 上轮已补 table/index/panel 的列全键（unit/formula/source_table/source_field），但
  `sample`/`factor`/`test` 的 meta columns 仍是 `{name, data_type}` 简化；
- `files` 键名当前是 `partition`，V2.0 是 `partition_path`（api.md 已按 partition 写，需定
  一）；
- `fieldset_scan`/`factor_scan`/`test_scan` 的返回字段与 V2.0 的 ScanReport 形态不同
  （缺 version_before/after 等，见 §2-6）。

### 11. 版本号对外形态变化（int → 纳秒时间戳）

- V2.0 递增 int（portal 可能按比较/展示）；V3 是 `time.time_ns()`。前端/客户端若按 int 语义
  （如 == 1）会失效——需在 portal 侧同步确认。

---

## 四、维护性

### 12. V2.0 controller 死代码 + 测试双轨

- `src/stkoe/{table,dataset,fieldset,sample,feature,factor,factor_test}/controller.py` 已无
  新代码路径引用，但保留在 src/ 且 tests 大量用例仍测它们（catalog 版回归）——维护成本翻倍、
  读者易混淆"哪套是当前实现"。
- **建议**：新实现稳定后，把 V2.0 controller 与其专属测试整体移入 `V2.0/`（或标记
  `@pytest.mark.skip`），src/ 只保留 graph 一套。

### 13. graph/export.py 与 graph/service.py 的图读取重复

- `graph lineage/nodes/stats`（dispatch `_graph_store` + export.build_payload）直接开
  catalog.db 读图，而 GraphService 内部也持有 GraphStore——两处打开同一文件（只读场景 OK，
  但连接管理/命名回退逻辑分散在两处，后续加写路径易漏）。

---

## 五、建议的完善路线（按优先级）

1. **修 §1 失效传播**（factor/test 物化过期）——正确性优先；推荐 hash 中纳入
   "依赖链脏/有效"状态或 scan 前 resolve 上游。
2. **统一对外 meta 形态**（§2）——服务层全资产输出 V2.0 形态 dict，portal 兼容；
   同步 api.md §3.7。
3. **决策物化策略**（§6）：panel/fieldset 是否补物化；`fieldset scan` 语义定死。
4. **收敛实现层**（§4/§12）：service 为唯一实现，handlers 收窄，V2.0 controller 移出 src/。
5. **错误体系统一**（§8）：按资产类型抛专属 NotFound/ExistsError（或统一 AssetNotFound 并
   在 message 带类型）。
6. **补齐三路径**（§9）：index/panel 任务版、CLI graph 子命令、`--materialize` 显式报错。
7. **迁移工具**（§3）：V2.0 catalog → 新结构一键迁移脚本。
8. **事件机制收口**（§5/§11）：确定版本/事件在对外语义中的角色，version_list 裁剪。
