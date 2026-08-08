"""收益价差页面 - 因子分层收益价差分析"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel


desc = """## 收益价差

### 1. 多空价差序列

取最大分箱 $q_{max}$ 与最小分箱 $q_1$，计算各滞后期的逐日价差：

$$\\Delta d_n = E(d_n)_{q_{max}} - E(d_n)_{q_1}$$

### 2. 标准误

假设两分箱独立：

$$SE(\\Delta d_n) = \\sqrt{SE(d_n)_{q_{max}}^2 + SE(d_n)_{q_1}^2}$$

> 注：严格计算应减去协方差项 $-2 \\cdot Cov(d_n)_{q_{max}, q_1}$

### 3. 图表要素

- **折线**：$\\Delta d_n$ 逐日价差序列
- **置信带**：$\\Delta d_n \\pm 3 \\cdot SE(\\Delta d_n)$
- **MA 均线**：$MA(\\Delta d_n, w) = \\frac{1}{w}\\sum_{i=0}^{w-1} \\Delta d_{n,t-i}$
- **零线**：参考基准线
"""

class RtnDiffView:
    """收益价差视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.condition_input = pn.widgets.TextInput(
            placeholder="输入筛选条件, 如: '2020-01-01<=date<=2020-12-31'"
        )
        self.ma_input = pn.widgets.IntInput(value=20, width=120)
        self.view_button = pn.widgets.Button(name="查看", color="primary")
        self.linked_axes_switch = pn.widgets.Switch(name="轴联动")
        self.single_plot_switch = pn.widgets.Switch(name="单图展示", value=True)
        self.data_button = pn.widgets.Button(name="数据", color="primary")

        # --- 结果展示区域 ---
        self.graph_result = pn.Accordion(
            desc, scroll=True, sizing_mode="stretch_both", toggle=True, active=[0]
        )
        self.data_result = pn.Column(sizing_mode="stretch_both")

        # --- 组合区域 ---
        self.toolbar = pn.Row(
            self.condition_input,
            self.ma_input,
            self.view_button,
            pn.HSpacer(),
            self.linked_axes_switch,
            self.single_plot_switch,
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


class RtnDiffController:
    """收益价差控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: RtnDiffView) -> None:
        self.model = model
        self.view = view

        # 单图展示开关控制 Accordion 展开模式
        view.graph_result.toggle = pn.bind(
            lambda single: single is not False, view.single_plot_switch
        )
        view.view_button.on_click(self._onview_click)

        # 数据面板切换
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    @staticmethod
    def _compute(bucket, condition: str | None, ma: int):
        """根据条件调用对应 plot/calc 方法。

        Returns:
            (plots, data): plots 为图表列表, data 为 Polars DataFrame
        """
        if condition:
            plots = bucket.plot_rtn_diff_by_condition(condition, ma=ma)
            data = bucket.calc_rtn_diff_by_condition(condition)
        else:
            plots = bucket.plot_rtn_diff_by_condition(ma=ma)
            data = bucket.calc_rtn_diff_all()
        return plots, data

    def _onview_click(self, event) -> None:
        """点击「查看」后查询数据并更新图表/表格"""
        view = self.view
        condition = view.condition_input.value or None
        ma = view.ma_input.value
        linked_axes = view.linked_axes_switch.value

        # 显示加载动画
        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 根据条件调用计算方法
        bucket = self.model.bucket_returns
        plots, data = self._compute(bucket, condition, ma)

        # 渲染图表到 Accordion
        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        # 渲染数据表格
        exclude_cols = {"year", "month", "date", "factor_quantile"}
        columns_config = {
            col: {"number_format": {"style": "percent", "minimumFractionDigits": 2, "maximumFractionDigits": 2}}
            for col in data.columns
            if col not in exclude_cols
        }
        view.data_result[:] = [
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                columns_config=columns_config,
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class RtnDiffViewer:
    """收益价差页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.bucket_returns is None:
            self.view = None
            self.controller = None
        else:
            self.view = RtnDiffView(model)
            self.controller = RtnDiffController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
            )
        return self.view.layout
