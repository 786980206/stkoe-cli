"""IC分布页面 - 原始IC、分组调整IC、RankIC、分组调整RankIC直方图分布"""

import panel as pn
from panel_splitjs import Split
from ...models.factor_test_result import FactorTestResultModel as FactorTestModel
from ..base2 import build_standard_layout, toggle_split, make_loading

desc = """## IC分布

展示IC值的频率分布直方图，用于判断IC分布是否接近正态分布。

- **绿色虚线**：IC均值
- IC均值 > 0 表示因子有正向预测能力
- IC均值 < 0 表示因子有反向预测能力
- IC分布越集中，说明因子越稳定
"""

IC_HIST_TYPES = [
    ("原始IC分布", "ic"),
    ("分组调整IC分布", "gic"),
    ("Rank IC分布", "rank_ic"),
    ("分组调整Rank IC分布", "rank_gic"),
]


class ICHistView:
    """IC分布视图"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

        self.ic_type_input = pn.widgets.Select(
            value="原始IC分布",
            options=[label for label, _ in IC_HIST_TYPES],
            width=150,
        )
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.linked_axes_switch = pn.widgets.Switch(name="轴联动")
        self.sidebar_button = pn.widgets.Button(name="侧栏", color="primary")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        self.toolbar = pn.Row(
            self.ic_type_input,
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


class ICHistController:
    """IC分布控制器"""

    def __init__(self, model: FactorTestModel, view: ICHistView) -> None:
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
        ic_type_label = view.ic_type_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        ic_key = next(k for label, k in IC_HIST_TYPES if label == ic_type_label)
        ic = self._model.ic_test

        plot_fn = getattr(ic, f"plot_{ic_key}_date_hist")
        plots = list(plot_fn())

        # 获取原始数据用于表格展示
        data = getattr(ic, f"{ic_key}_date")

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


class ICHistViewer:
    """IC分布页面入口"""

    def __init__(self, model: FactorTestModel) -> None:
        if model.ic_test is None:
            self._view = None
            self._controller = None
        else:
            self._view = ICHistView(model.get_spec_json("ic_test"))
            self._controller = ICHistController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
