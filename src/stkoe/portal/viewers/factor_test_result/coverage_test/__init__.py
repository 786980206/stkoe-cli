"""覆盖率分析页面 - 因子覆盖率趋势与核心指标"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 覆盖率分析

### 指标说明

| 指标 | 含义 | 理想值 |
|------|------|--------|
| **SF2S** | 因子样本覆盖率 = SFNo / SNo | 越高越好 |
| **X2S** | 因子样本剔除率 = XNo / SNo | 越低越好 |
| **F2T** | 因子数据覆盖率 = FNo / TNo | 越高越好 |
| **S2T** | 样本标的覆盖率 = SNo / TNo | 越高越好 |

### 核心指标

**SF2S(rolling_window)**：因子样本覆盖率的 rolling_window 日滚动均值，是判断因子是否可用的关键指标。

### 说明

- **SFNo**：截面有效样本数（因子值不为空且通过样本筛选）
- **FNo**：截面有效因子数据数（因子值不为空）
- **SNo**：截面样本数（通过样本筛选条件）
- **XNo**：截面剔除样本数（因子值为空导致从 SNo 中剔除）
- **TNo**：截面全部标的数
"""


class CoverageView:
    """覆盖率视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        # --- 结果展示区域 ---
        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        # --- 组合区域 ---
        self.toolbar = pn.Row(
            pn.HSpacer(),
            self.view_button,
            self.data_button,
            scroll=True,
            sizing_mode="stretch_width",
            styles={"margin": "0 5px 0 0"},
        )
        self.split_content = Split(
            self.graph_result,
            self.data_result,
            collapsed=None,
            expanded_sizes=(50, 50),
            sizing_mode="stretch_both",
            styles={"border-top": "1px solid var(--panel-border-color)"},
            gutter_size=1,
        )

        self.layout = pn.Column(
            self.toolbar,
            self.split_content,
            styles={"height": "calc(100vh - 73px)"},
        )


class CoverageController:
    """覆盖率控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: CoverageView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        view = self.view

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        coverage = self.model.coverage_test
        plot = coverage.plot_cvg_date()
        data = coverage.cvg_date

        view.graph_result[:] = [
            (
                plot.opts.get("plot").kwargs.get("title"),
                pn.panel(plot, sizing_mode="stretch_both"),
            )
        ]
        view.data_result[:] = [
            pn.pane.Markdown("**聚合统计**"),
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class CoverageViewer:
    """覆盖率分析页面入口"""

    def __init__(self, model: FactorTestResultModel, page_root=None) -> None:
        if model.coverage_test is None:
            self.view = None
            self.controller = None
            self.layout = pn.pane.Markdown(
                "## 无可用覆盖率测试结果\n\n请将 CoverageTestResults.pkl 放入 `results/` 目录"
            )
        else:
            self.view = CoverageView(model)
            self.controller = CoverageController(model, self.view)
            self.layout = self.view.layout
