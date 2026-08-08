"""收益统计页面 - 因子分层收益统计分析与可视化"""

import polars as pl
import panel as pn
from holoviews.streams import Tap
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 收益统计

### 1. 分箱收益

将因子值按分位数分为 N 个箱（quantile），计算每个箱内个股在未来第 n 日收益的截面均值：

$$E(d_n) = \\frac{1}{|S_q|} \\sum_{i \\in S_q} d_{n,i}$$

$$SE(d_n) = \\frac{std(d_n)}{\\sqrt{N}}$$

其中 $S_q$ 为第 q 个分箱的股票集合，$d_{n,i}$ 为个股 i 的 n 日收益。

### 2. 统计量

对各交易日的 $E(d_n)$ 按维度聚合：

- **整体统计**：$mean(E(d_n))$, $SE = std(E(d_n)) / \\sqrt{T}$
- **按年/月统计**：在对应时间窗口内聚合

### 3. 超额收益

$$d_n^{exr} = d_n - \\overline{d_n}$$

其中 $\\overline{d_n} = \\frac{1}{N}\\sum d_n$ 为当日截面均值，再对 $d_n^{exr}$ 做同样的分箱统计。
"""

class RtnStatView:
    """收益统计视图 - 控制工具栏 + 图表展示 + 数据表格 + 右侧说明栏"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.stat_input = pn.widgets.Select(
            value="整体统计",
            options=["整体统计", "按年统计", "按月统计"],
            width=100,
        )
        self.year_input = pn.widgets.Select(options=model.get_bucket_returns_years(), width=100)
        self.return_type_input = pn.widgets.Select(
            options=["原始收益", "超额收益"], width=100
        )
        self.condition_input = pn.widgets.TextInput(
            placeholder="输入筛选条件, 如: '2020-01-01<=date<=2020-12-31'"
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
            self.year_input,
            self.condition_input,
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
        

class RtnStatController:
    """收益统计控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: RtnStatView) -> None:
        self.model = model
        self.view = view

        # 按月统计时显示年份选择器
        view.year_input.visible = pn.bind(
            lambda stat: stat == "按月统计", view.stat_input
        )
        # 单图展示开关控制 Accordion 展开模式
        view.graph_result.toggle = pn.bind(
            lambda single: single is not False, view.single_plot_switch
        )
        view.view_button.on_click(self._onview_click)

        # 侧栏 / 数据面板 切换
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )
        

    def _onview_click(self, event) -> None:
        """点击「查看」后查询数据并更新图表/表格"""
        view = self.view
        stat_mode = view.stat_input.value
        return_type = view.return_type_input.value
        year = view.year_input.value if stat_mode == "按月统计" else None
        linked_axes = view.linked_axes_switch.value

        # 显示加载动画
        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 根据收益类型和统计模式分发到具体计算方法
        bucket = self.model.bucket_returns
        plots, data = self._compute(bucket, stat_mode, return_type, year)

        # 渲染图表到 Accordion
        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        # 渲染数据表格（聚合统计）
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


    @staticmethod
    def _compute(bucket, stat_mode: str, return_type: str, year: int | None):
        """根据统计模式和收益类型，调用对应 plot/calc 方法。

        Returns:
            (plots, data): plots 为图表列表, data 为 Polars DataFrame
        """
        prefix = "rtn" if return_type == "原始收益" else "exr"
        method_map = {
            "整体统计": "by_condition",
            "按年统计": "by_year",
            "按月统计": "by_month",
        }
        method_suffix = method_map.get(stat_mode)
        if method_suffix is None:
            raise ValueError(f"未知统计模式: {stat_mode}")

        plot_fn = getattr(bucket, f"plot_{prefix}_stat_{method_suffix}")
        calc_fn = getattr(bucket, f"calc_{prefix}_stat_{method_suffix}")
        kwargs = {"year": year} if year is not None else {}

        plots = plot_fn(**kwargs)
        # by_condition 返回单个图，by_year/by_month 返回 Layout（可迭代）
        if method_suffix == "by_condition":
            plots = [plots]
        data = calc_fn(**kwargs)
        return plots, data


class RtnStatViewer:
    """收益统计页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.view = RtnStatView(model=model)
        self.controller = RtnStatController(model, self.view)

    @property
    def layout(self):
        return self.view.layout
