"""IC分布页面 - 原始IC、分组调整IC、RankIC、分组调整RankIC直方图分布"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

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
    """IC分布视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        self.ic_type_input = pn.widgets.Select(
            value="原始IC分布",
            options=[label for label, _ in IC_HIST_TYPES],
            width=150,
        )
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.linked_axes_switch = pn.widgets.Switch(name="轴联动")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        self.toolbar = pn.Row(
            self.ic_type_input,
            self.view_button,
            pn.HSpacer(),
            self.linked_axes_switch,
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


class ICHistController:
    """IC分布控制器"""

    def __init__(self, model: FactorTestResultModel, view: ICHistView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        view = self.view
        ic_type_label = view.ic_type_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        ic_key = next(k for label, k in IC_HIST_TYPES if label == ic_type_label)
        ic = self.model.ic_test

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
            pn.pane.Markdown("**聚合统计**"),
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class ICHistViewer:
    """IC分布页面入口"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.ic_test is None:
            self.view = None
            self.controller = None
        else:
            self.view = ICHistView(model)
            self.controller = ICHistController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return self.view.layout
