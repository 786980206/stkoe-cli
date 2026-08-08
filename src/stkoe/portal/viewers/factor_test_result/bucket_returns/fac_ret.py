"""因子收益页面 - 因子累计收益分析"""

import holoviews as hv
import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 因子收益

### 1. 多空投资组合

取最大分箱 $q_{max}$ 与最小分箱 $q_1$，计算逐日因子收益序列：

$$FR(d_n) = CR(\\Delta d_n) - 1$$

其中 $CR(\\Delta d_n)$ 为多空组合的累计净值。

### 2. 交错子序列法

采用与累计收益相同的交错子序列方法，消除持仓期重叠：

$$NV(d_n)@t_i = \\prod_{k=0}^{T/n-1} \\left( E(d_n)_{q_{max}} - E(d_n)_{q_1} + 1 \\right)$$

### 3. 平均净值

$$FR(d_n, t) = \\frac{1}{n} \\sum_{i=1}^{n} NV(d_n, t)@t_i - 1$$
"""


class FacRetView:
    """因子收益视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.stat_input = pn.widgets.Select(
            value="整体统计",
            options=["整体统计", "按年统计"],
            width=100,
        )
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
            self.stat_input,
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


class FacRetController:
    """因子收益控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: FacRetView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)

        # 数据面板切换
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        """点击「查看」后查询数据并更新图表/表格"""
        view = self.view
        stat_mode = view.stat_input.value
        linked_axes = view.linked_axes_switch.value

        # 显示加载动画
        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 根据统计模式调用计算方法
        bucket = self.model.bucket_returns
        plots, data = self._compute(bucket, stat_mode)

        # 渲染图表到 Accordion
        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        # 渲染数据表格
        exclude_cols = {"year", "date"}
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

    @staticmethod
    def _compute(bucket, stat_mode: str):
        """根据统计模式调用对应 plot/calc 方法。

        Returns:
            (plots, data): plots 为图表列表, data 为 Polars DataFrame
        """
        method_map = {
            "整体统计": "all",
            "按年统计": "by_year",
        }
        method_suffix = method_map.get(stat_mode)
        if method_suffix is None:
            raise ValueError(f"未知统计模式: {stat_mode}")

        plot_fn = getattr(bucket, f"plot_fac_rets_{method_suffix}")
        calc_fn = getattr(bucket, f"calc_fac_rets_{method_suffix}")

        plot = plot_fn()
        data = calc_fn()
        # by_year 返回 Layout，需转为列表
        plots = list(plot) if isinstance(plot, hv.Layout) else [plot]
        return plots, data


class FacRetViewer:
    """因子收益页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.bucket_returns is None:
            self.view = None
            self.controller = None
        else:
            self.view = FacRetView(model)
            self.controller = FacRetController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
            )
        return self.view.layout
