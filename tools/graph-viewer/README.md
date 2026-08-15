# stkoe 资产血缘图查看器（Cytoscape.js）

把 graphqlite 图数据库里的**资产血缘关系**导出为 JSON，并用
[Cytoscape.js](https://js.cytoscape.org/) 在浏览器里交互式探索：
类型着色、点击看详情、选中节点高亮上下游、聚焦子图、搜索定位、多布局切换。

## 用法

```bash
# 1. 导出图数据（全图）
python tools/graph-viewer/export.py <data-dir>/graph.db --output tools/graph-viewer/graph-data.json

#    或只导出某个节点的上下游子图（深度可选）
python tools/graph-viewer/export.py <data-dir>/graph.db --node panel:ds1 --depth 3 --output tools/graph-viewer/graph-data.json

# 2. 启动静态服务
python -m http.server 8080 --directory tools/graph-viewer

# 3. 浏览器打开
#    http://127.0.0.1:8080/              （默认加载 graph-data.json）
#    http://127.0.0.1:8080/?data=xxx.json（指定其他数据文件）
```

> 直接双击 `index.html`（file://）打开时浏览器禁止 fetch，会提示把 JSON 拖进页面；
> 推荐用上面的静态服务方式。

## export.py 选项

| 选项 | 说明 |
|---|---|
| `<db>` | graphqlite 图数据库路径（位置参数，必填） |
| `--node <type:name>` | 只导出该节点及其上下游子图（默认全图） |
| `--depth N` | 上下游血缘深度（默认不限） |
| `--output <path>` | 输出文件（默认 `graph-data.json`） |
| `--pretty` | 美化 JSON |
| `--no-meta` | 节点不带全量 meta（详情面板字段变少、文件更小） |

## 页面交互

- **单击节点**：右侧详情面板（类型/名称/版本/有效态/物化态/上游数/下游数/类型专属键/版本事件数）
- **选中节点**：自动高亮——琥珀色 = **上游**（依赖链）、青色 = **下游**（影响链），无关节点淡化
- **聚焦子图**：仅显示选中节点的上下游子图（可配合逐层探索）
- **搜索**：按名称/展示名/id 匹配，回车定位并淡化其余
- **图例**：点击类型可整体隐藏/显示该类资产（源头 table/index 为八角形）
- **布局**：dagre（层级，推荐）/ cose（力导向）/ breadthfirst / concentric / grid

## 数据格式（Cytoscape elements）

```json
{
  "graph": { "exported_at": "...", "source_db": "...", "center": null,
             "node_count": 7, "edge_count": 6, "types": ["table", "..."] },
  "elements": {
    "nodes": [{ "data": { "id": "table:index", "type": "table", "name": "index",
                          "label": "index", "version": 1755232000000000000,
                          "valid": true, "materialized": false, "meta": { "...": "..." } } }],
    "edges": [{ "data": { "id": "panel:ds1->table:m1", "source": "panel:ds1",
                          "target": "table:m1", "role": "member", "join": "left_join",
                          "required_version": 1755232000000000000 } }]
  }
}
```

- 节点 `id` = `"<type>:<name>"`；`type` 决定颜色与形状；`meta` 为全量资产属性（详情面板用）
- 边方向 = 依赖方向（依赖方 → 被依赖方）；`role` 表示角色（index/member/panel/fieldset/
  sample/feature/factor/tester…），`join` 仅 table → panel 边带（left_join/asof_join）

## 目录

```
tools/graph-viewer/
├── export.py          # 图数据 → Cytoscape elements JSON
├── index.html         # 可视化页面（Cytoscape.js，交互探索）
├── graph-data.json    # 导出产物（运行 export.py 生成，不入库）
└── vendor/            # 本地化前端库（离线可用，许可见 VENDOR.md）
    ├── cytoscape.min.js        # 3.34.1 (MIT)
    ├── dagre.min.js            # 0.8.5  (MIT)
    └── cytoscape-dagre.min.js  # 4.0.0  (MIT)
```
