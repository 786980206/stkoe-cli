"""IC月度页面 - 原始IC、分组调整IC、RankIC、分组调整RankIC月度热力图"""

import panel as pn
from panel_splitjs import Split
from ...models.factor_test import FactorTestModel
from ..base2 import build_standard_layout, toggle_split, make_loading

desc = """## IC月度

展示每月的IC均值热力图，用于观察IC的时变特征。

- 颜色越红表示IC > 0（因子正向预测能力）
- 颜色越蓝表示IC < 0（因子反向预测能力）
- 行为年份，列为持有期
"""

IC_MONTH_TYPES = [
    ("原始IC月度", "ic"),
    ("分组调整IC月度", "gic"),
    ("Rank IC月度", "rank_ic"),
    ("分组调整Rank IC月度", "rank_gic"),
]


class ICMonthView:
    """IC月度视图"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

        self.ic_type_input = pn.widgets.Select(
            value="原始IC月度",
            options=[label for label, _ in IC_MONTH_TYPES],
            width=150,
        )
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.sidebar_button = pn.widgets.Button(name="侧栏", color="primary")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        self.toolbar = pn.Row(
            self.ic_type_input,
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


class ICMonthController:
    """IC月度控制器"""

    def __init__(self, model: FactorTestModel, view: ICMonthView) -> None:
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

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        ic_key = next(k for label, k in IC_MONTH_TYPES if label == ic_type_label)
        ic = self._model.ic_test

        plot_fn = getattr(ic, f"plot_{ic_key}_month")
        calc_fn = getattr(ic, f"calc_{ic_key}_month")

        plot = plot_fn()
        data = calc_fn()

        view.graph_result[:] = [
            (
                plot.opts.get("plot").kwargs.get("title"),
                pn.panel(plot, sizing_mode="stretch_width"),
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


class ICMonthViewer:
    """IC月度页面入口"""

    def __init__(self, model: FactorTestModel) -> None:
        if model.ic_test is None:
            self._view = None
            self._controller = None
        else:
            self._view = ICMonthView(model.get_spec_json("ic_test"))
            self._controller = ICMonthController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
