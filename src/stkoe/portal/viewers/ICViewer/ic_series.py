"""IC序列页面 - 原始IC、分组调整IC、RankIC、分组调整RankIC时间序列"""

import panel as pn
from panel_splitjs import Split
from ...models.factor_test import FactorTestModel
from ..base2 import build_standard_layout, toggle_split, make_loading

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
    """IC序列视图 - 控制工具栏 + 图表展示 + 数据表格 + 右侧说明栏"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

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
        self.sidebar_button = pn.widgets.Button(name="侧栏", color="primary")
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


class ICSeriesController:
    """IC序列控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestModel, view: ICSeriesView) -> None:
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
        ma = view.ma_input.value
        ma2 = view.ma2_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 获取 IC 类型对应的 key
        ic_key = next(k for label, k in IC_TYPES if label == ic_type_label)
        ic = self._model.ic_test

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

    def __init__(self, model: FactorTestModel) -> None:
        if model.ic_test is None:
            self._view = None
            self._controller = None
        else:
            self._view = ICSeriesView(model.get_spec_json("ic_test"))
            self._controller = ICSeriesController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
