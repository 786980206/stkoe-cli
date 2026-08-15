# vendor 前端库（本地化）

本目录前端库自 npm 镜像（npmmirror）下载，离线可用，不依赖外网 CDN：

| 文件 | 包 | 版本 | 许可证 |
|---|---|---|---|
| `cytoscape.min.js` | [cytoscape](https://github.com/cytoscape/cytoscape.js) | 3.34.1 | MIT |
| `dagre.min.js` | [dagre](https://github.com/dagrejs/dagre) | 0.8.5 | MIT |
| `cytoscape-dagre.min.js` | [cytoscape-dagre](https://github.com/cytoscape/cytoscape-dagre) | 4.0.0 | MIT |

三个库均为 MIT 许可，各自的 LICENSE 随上游包分发，可按需替换为
`https://cdn.jsdelivr.net/npm/<pkg>@<ver>/...` 等 CDN 版本（需联网）。
