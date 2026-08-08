"""行业分组收益统计页面 - 按行业分组的因子收益统计、价差、累计收益分析"""

import polars as pl
import holoviews as hv
import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 行业分组收益统计

基于行业分组（`group`）的因子回测结果，分别展示各行业的收益统计、收益价差和累计收益。

### 1. 分组收益统计

在每个行业内部，按因子分位数分组计算截面收益均值与标准误：

$$E(d_n | group) = \\frac{1}{|S_q|} \\sum_{i \\in S_q} d_{n,i}$$

### 2. 收益价差

取最大/最小分箱，计算行业内的多空价差序列：

$$\\Delta d_n = E(d_n)_{q_{max}} - E(d_n)_{q_1}$$

### 3. 累计收益

采用交错子序列方法，计算各分箱的累计净值曲线。
"""


class GbrStatView:
    """行业分组收益视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc
        groups = sorted(model.bucket_returns.gbr_date["group"].unique().to_list())

        # --- 控制输入组件 ---
        self.stat_input = pn.widgets.Select(
            value="整体统计",
            options=["整体统计", "按年统计", "收益价差", "累计收益"],
            width=100,
        )
        self.year_input = pn.widgets.Select(options=model.get_bucket_returns_years(), width=100)
        self.group_input = pn.widgets.MultiSelect(
            value=groups[:1], options=groups, width=200, size=5
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
            self.year_input,
            self.group_input,
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


class GbrStatController:
    """行业分组收益控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestResultModel, view: GbrStatView) -> None:
        self.model = model
        self.view = view

        # 按年统计时显示年份选择器
        view.year_input.visible = pn.bind(
            lambda stat: stat == "按年统计", view.stat_input
        )
        view.view_button.on_click(self._onview_click)

        # 数据面板切换
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        """点击「查看」后查询数据并更新图表/表格"""
        view = self.view
        stat_mode = view.stat_input.value
        year = view.year_input.value if stat_mode == "按年统计" else None
        selected_groups = view.group_input.value
        linked_axes = view.linked_axes_switch.value

        # 显示加载动画
        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 根据统计模式调用计算方法
        bucket = self.model.bucket_returns
        plots, data = self._compute(bucket, stat_mode, year, selected_groups)

        # 渲染图表到 Accordion
        view.graph_result[:] = [
            (
                p.opts.get("plot").kwargs.get("title"),
                pn.panel(p, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
            for p in plots
        ]
        # 渲染数据表格
        exclude_cols = {"year", "month", "date", "group", "factor_quantile"}
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
    def _compute(bucket, stat_mode: str, year: int | None, selected_groups: list[str]):
        """根据统计模式调用对应 plot/calc 方法，仅生成选中行业的图表。

        Returns:
            (plots, data): plots 为图表列表, data 为 Polars DataFrame
        """
        from ....factor.testers.returns import plot_rtn_stat, plot_rtn_diff, plot_rtn_cums

        plots = []

        if stat_mode == "整体统计":
            data = bucket.calc_gbr_stat_all()
            data = data.filter(pl.col("group").is_in(selected_groups))
            for g in selected_groups:
                gd = data.filter(pl.col("group") == g)
                if gd.height > 0:
                    plots.append(plot_rtn_stat(gd, title=f"{g} - 整体统计"))

        elif stat_mode == "按年统计":
            data = bucket.calc_gbr_stat_by_year()
            data = data.filter(pl.col("year") == year).filter(pl.col("group").is_in(selected_groups))
            for g in selected_groups:
                gd = data.filter(pl.col("group") == g)
                if gd.height > 0:
                    plots.append(plot_rtn_stat(gd, title=f"{g} - {year}年"))

        elif stat_mode == "收益价差":
            data = bucket.calc_gbr_diff_all()
            data = data.filter(pl.col("group").is_in(selected_groups))
            for g in selected_groups:
                gd = data.filter(pl.col("group") == g)
                if gd.height > 0:
                    for dno in bucket.spec.periods:
                        plots.append(plot_rtn_diff(gd, dno=dno, title=f"{g} - 收益价差(d{dno})"))

        elif stat_mode == "累计收益":
            data = bucket.calc_gbr_cums_all()
            data = data.filter(pl.col("group").is_in(selected_groups))
            for g in selected_groups:
                gd = data.filter(pl.col("group") == g)
                if gd.height > 0:
                    for dno in bucket.spec.periods:
                        plots.append(plot_rtn_cums(gd, dno=dno, title=f"{g} - 累计收益(d{dno})"))
        else:
            raise ValueError(f"未知统计模式: {stat_mode}")

        return plots, data


class GbrStatViewer:
    """行业分组收益页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        if model.bucket_returns is None:
            self.view = None
            self.controller = None
        else:
            self.view = GbrStatView(model)
            self.controller = GbrStatController(model, self.view)

    @property
    def layout(self):
        if self.view is None:
            return pn.pane.Markdown(
                "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
            )
        return self.view.layout
