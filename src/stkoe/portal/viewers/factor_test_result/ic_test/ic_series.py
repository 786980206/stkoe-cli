"""IC序列页面 - 原始IC、分组调整IC、RankIC、分组调整RankIC时间序列"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## IC序列

### 1. 原始IC

$$IC(d_n) = Corr(factor, d_n)$$

因子值与未来n日收益的Pearson相关系数。

### 2. 分组调整IC

$$GIC(d_n) = Corr(factor, d_n - E(d_n|group))$$

在行业分组内去均值后的收益与因子的相关系数。

### 3. Rank IC

$$RankIC(d_n) = SpearmanCorr(factor, d_n)$$

因子值与未来n日收益的Spearman秩相关系数。

### 4. 分组调整Rank IC

$$RankGIC(d_n) = SpearmanCorr(factor, d_n - E(d_n|group))$$

行业分组调整后的Spearman秩相关系数。
"""

IC_TYPES = [
    ("原始IC", "ic"),
    ("分组调整IC", "gic"),
    ("Rank IC", "rank_ic"),
    ("分组调整Rank IC", "rank_gic"),
]


class ICSeriesView:
    """IC序列视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.ic_type_input = pn.widgets.Select(
            value="原始IC",
            options=[label for label, _ in IC_TYPES],
            width=150,
        )
        self.ma_input = pn.widgets.IntInput(value=22, width=120, name="短期MA")
        self.ma2_input = pn.widgets.IntInput(value=251, width=120, name="长期MA")
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.linked_axes_switch = pn.widgets.Switch(name="轴联动")
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        # --- 结果展示区域 ---
        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        # --- 组合区域 ---
        self.toolbar = pn.Row(
            self.ic_type_input,
            self.ma_input,
            self.ma2_input,
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


class ICSeriesController:
    """IC序列控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: ICSeriesView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        view = self.view
        ic_type_label = view.ic_type_input.value
        ma = view.ma_input.value
        ma2 = view.ma2_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 获取 IC 类型对应的 key
        ic_key = next(k for label, k in IC_TYPES if label == ic_type_label)
        ic = self.model.ic_test

        # 调用对应的 plot 和 calc 方法
        plot_fn = getattr(ic, f"plot_{ic_key}_date")
        calc_fn = getattr(ic, f"{ic_key}_date")

        plots = list(plot_fn(ma=ma, ma2=ma2))
        data = calc_fn

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


class ICSeriesViewer:
    """IC序列页面入口"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.ic_test is None:
            self.view = None
            self.controller = None
        else:
            self.view = ICSeriesView(model)
            self.controller = ICSeriesController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return self.view.layout
