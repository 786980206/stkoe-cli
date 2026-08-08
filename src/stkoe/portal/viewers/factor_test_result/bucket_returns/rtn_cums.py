"""收益累计页面 - 因子分层累计收益分析"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 累计收益

### 1. 分箱累计收益

对每个分箱 q 和滞后期 $d_n$，计算该分箱的累计净值曲线。

由于 $E(d_n)$ 本身是 n 日收益的均值，直接连乘会导致持仓期重叠。采用交错子序列方法：

### 2. 交错子序列法

按间隔 n 拆分为 n 条不重叠的子序列（起始点 $t_1$ 至 $t_n$）：

$$NV(d_n, q)@t_i = \\prod_{k=0}^{T/n-1} \\left( E(d_n, q, t_i + k \\cdot n) + 1 \\right) - 1$$

### 3. 平均净值

将 n 条子序列前值填充（forward fill）后，逐日取平均：

$$CR(d_n, q, t) = \\frac{1}{n} \\sum_{i=1}^{n} NV(d_n, q, t)@t_i$$

### 4. 图表

每个滞后期 $d_n$ 对应一张图，图中包含全部 N 个分箱的 $CR(d_n, q)$ 累计净值曲线。
"""

class RtnCumsView:
    """收益累计视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.return_type_input = pn.widgets.Select(
            options=["原始收益", "超额收益"], width=100
        )
        self.stat_input = pn.widgets.Select(
            value="整体统计",
            options=["整体统计", "按年统计"],
            width=100,
        )
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
            self.stat_input,
            self.return_type_input,
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


class RtnCumsController:
    """收益累计控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: RtnCumsView) -> None:
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
    def _compute(bucket, return_type: str, stat_mode: str):
        """根据收益类型和统计模式调用对应 plot/calc 方法。

        Returns:
            (plots, data): plots 为图表列表, data 为 Polars DataFrame
        """
        prefix = "rtn" if return_type == "原始收益" else "exr"
        method_map = {
            "整体统计": "all",
            "按年统计": "by_year",
        }
        method_suffix = method_map.get(stat_mode)
        if method_suffix is None:
            raise ValueError(f"未知统计模式: {stat_mode}")

        plot_fn = getattr(bucket, f"plot_{prefix}_cums_{method_suffix}")
        calc_fn = getattr(bucket, f"calc_{prefix}_cums_{method_suffix}")

        plots = plot_fn()
        data = calc_fn()
        return plots, data

    def _onview_click(self, event) -> None:
        """点击「查看」后查询数据并更新图表/表格"""
        view = self.view
        return_type = view.return_type_input.value
        stat_mode = view.stat_input.value
        linked_axes = view.linked_axes_switch.value

        # 显示加载动画
        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 根据收益类型和统计模式分发到具体计算方法
        bucket = self.model.bucket_returns
        plots, data = self._compute(bucket, return_type, stat_mode)

        # 渲染图表到 Accordion
        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        # 渲染数据表格
        exclude_cols = {"year", "date", "factor_quantile"}
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


class RtnCumsViewer:
    """收益累计页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.bucket_returns is None:
            self.view = None
            self.controller = None
        else:
            self.view = RtnCumsView(model)
            self.controller = RtnCumsController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
            )
        return self.view.layout
