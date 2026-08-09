"""自相关系数页面 - 因子原始/Rank自相关系数分析"""

import panel as pn
from panel_splitjs import Split
from ...models.factor_test_result import FactorTestResultModel as FactorTestModel
from ..base2 import build_standard_layout, toggle_split, make_loading

desc = """## 自相关系数

衡量因子值在时间序列上的持续性（惯性）。

### 1. 原始自相关

$$AC(d_n) = Corr(factor_t, factor_{t-n})$$

### 2. Rank自相关

$$RankAC(d_n) = SpearmanCorr(factor_t, factor_{t-n})$$

### 指标解读

- 自相关系数越高，说明因子值变化越缓慢（低换手）
- 绿色虚线 (0.7) 为参考阈值
- 不同持有期($$d_n$$)的自相关系数可对比观察
"""


class AutoCorrView:
    """自相关系数视图"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

        # --- 控制输入组件 ---
        self.ac_type_input = pn.widgets.Select(
            value="Rank自相关",
            options=["原始自相关", "Rank自相关"],
            width=150,
        )
        self.ma1_input = pn.widgets.IntInput(value=22, width=120, name="短期MA")
        self.ma2_input = pn.widgets.IntInput(value=252, width=120, name="长期MA")
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.linked_axes_switch = pn.widgets.Switch(name="轴联动")
        self.sidebar_button = pn.widgets.Button(name="侧栏", color="primary")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        # --- 结果展示区域 ---
        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        # --- 组合区域 ---
        self.toolbar = pn.Row(
            self.ac_type_input,
            self.ma1_input,
            self.ma2_input,
            self.view_button,
            self.linked_axes_switch,
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


class AutoCorrController:
    """自相关系数控制器"""

    def __init__(self, model: FactorTestModel, view: AutoCorrView) -> None:
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
        ac_type = view.ac_type_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        ac = self._model.auto_correlation
        if ac_type == "原始自相关":
            plots = list(ac.plot_ac_date())
            data = ac.ac_date
        else:
            plots = list(ac.plot_rank_ac_date())
            data = ac.rank_ac_date

        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        view.data_result[:] = [
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class AutoCorrViewer:
    """自相关系数页面入口"""

    def __init__(self, model: FactorTestModel) -> None:
        if model.auto_correlation is None:
            self._view = None
            self._controller = None
        else:
            self._view = AutoCorrView(model.get_spec_json("auto_correlation"))
            self._controller = AutoCorrController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用自相关测试结果\n\n请将 AutoCorrelationTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
