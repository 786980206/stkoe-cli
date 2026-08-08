"""分组换手率页面 - 因子分层换手率分析"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 分组换手率

衡量相邻两个日期之间，各分箱内股票组成的变化程度。

### 计算方法

对每个分箱 q，计算相邻日期的持仓重叠率：

$$TR(d_n, q) = 1 - \\frac{|Holdings_t \\cap Holdings_{t+n}|}{|Holdings_t|}$$

### 指标解读

- 换手率越低，说明因子分层越稳定
- 换手率过高会增加交易成本
- 不同持有期(d_n)的换手率可对比观察
"""


class TurnoverView:
    """分组换手率视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
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


class TurnoverController:
    """分组换手率控制器"""

    def __init__(self, model: FactorTestResultModel, view: TurnoverView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        view = self.view
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        turnover = self.model.bucket_turnover
        plots = list(turnover.plot_tr_date())
        data = turnover.tr_date

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


class TurnoverViewer:
    """分组换手率页面入口"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.bucket_turnover is None:
            self.view = None
            self.controller = None
        else:
            self.view = TurnoverView(model)
            self.controller = TurnoverController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用换手率测试结果\n\n请将 BucketTurnoverTestResults.pkl 放入 `results/` 目录"
            )
        return self.view.layout
