"""覆盖率分析页面 - 因子覆盖率趋势与核心指标"""

import panel as pn
from panel_splitjs import Split
from ...models.factor_test_result import FactorTestResultModel as FactorTestModel
from ..base2 import build_standard_layout, toggle_split, make_loading

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
    """覆盖率视图 - 控制工具栏 + 图表展示 + 数据表格 + 右侧说明栏"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

        # --- 控制输入组件 ---
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.sidebar_button = pn.widgets.Button(name="侧栏", color="primary")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        # --- 结果展示区域 ---
        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        # --- 组合区域 ---
        self.toolbar = pn.Row(
            self.view_button,
            self.sidebar_button,
            self.data_button,
            scroll=True,
            sizing_mode="stretch_width",
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
        self.layout = self._build()

    def _build(self):
        main_content = pn.Column(
            self.toolbar,
            self.split_content,
            styles={"height": "calc(100vh - 27px)"},
        )
        right_sidebar = pn.Tabs(
            (
                "测试",
                pn.Column(
                    pn.Accordion(
                        ("回测参数", pn.pane.JSON(self._spec_json)),
                        sizing_mode="stretch_both",
                    ),
                    sizing_mode="stretch_both",
                ),
            ),
            (
                "说明",
                pn.Column(
                    pn.pane.Markdown(desc, sizing_mode="stretch_width"),
                    sizing_mode="stretch_both",
                    scroll=True,
                ),
            ),
            sizing_mode="stretch_both",
            styles={"height": "calc(100vh - 27px)"},
        )
        return Split(
            main_content,
            right_sidebar,
            collapsed=None,
            sizes=(80, 20),
            expanded_sizes=(80, 20),
            sizing_mode="stretch_both",
            gutter_size=1,
        )


class CoverageController:
    """覆盖率控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestModel, view: CoverageView) -> None:
        self._model = model
        self._view = view
        self._bind_events()

    def _bind_events(self) -> None:
        view = self._view
        view.view_button.on_click(self._on_view_click)
        view.sidebar_button.on_click(
            lambda _: self._toggle_split(view.layout)
        )
        view.data_button.on_click(
            lambda _: self._toggle_split(view.split_content)
        )

    @staticmethod
    def _toggle_split(splitter: Split) -> None:
        splitter.collapsed = None if splitter.collapsed == 1 else 1

    def _on_view_click(self, event) -> None:
        view = self._view

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        coverage = self._model.coverage_test
        plot = coverage.plot_cvg_date()
        data = coverage.cvg_date

        view.graph_result[:] = [
            (
                plot.opts.get("plot").kwargs.get("title"),
                pn.panel(plot, sizing_mode="stretch_both"),
            )
        ]
        view.data_result[:] = [
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class CoverageViewer:
    """覆盖率分析页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestModel) -> None:
        if model.coverage_test is None:
            self._view = None
            self._controller = None
        else:
            self._view = CoverageView(model.get_spec_json("coverage_test"))
            self._controller = CoverageController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用覆盖率测试结果\n\n请将 CoverageTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
