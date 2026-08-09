"""截面回归模型分析页面 - 模型t值、R2、Delta R2"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

desc = """## 截面回归模型

通过三个递进模型检验因子的 alpha 属性。

### 模型设定

| 模型 | 回归方程 |
|------|----------|
| m0 | $Rex \\sim 1 + factor$ |
| m1 | $Rex \\sim 1 + factor + beta$ |
| m2 | $Rex \\sim 1 + factor + beta + size$ |

### 核心指标

- **ΔR2(m1)**：加入市场因子 beta 后 R2 的增量（滚动均值）
- **ΔR2(m2)**：再加入规模因子 size 后 R2 的增量（滚动均值）
- **Prop(|t|>2)**：因子 t 值绝对值大于 2 的比例（滚动均值）

### 指标解读

- ΔR2(m1) > 0 且显著：因子具有独立于市场因子的解释力
- ΔR2(m2) > 0 且显著：因子具有独立于市场和规模因子的 alpha 属性
- Prop(|t|>2) 越高，因子越显著
"""


class CSRegModelView:
    """截面回归模型视图 - 控制工具栏 + 图表展示 + 数据表格"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.desc = desc

        # --- 控制输入组件 ---
        self.stat_input = pn.widgets.Select(
            value="Delta R2",
            options=["模型统计", "Delta R2"],
            width=150,
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


class CSRegModelController:
    """截面回归模型控制器"""

    def __init__(self, model: FactorTestResultModel, view: CSRegModelView) -> None:
        self.model = model
        self.view = view

        view.view_button.on_click(self._onview_click)
        view.data_button.on_click(
            lambda _: setattr(view.split_content, 'collapsed', None if view.split_content.collapsed == 1 else 1)
        )

    def _onview_click(self, event) -> None:
        view = self.view
        stat_mode = view.stat_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        model = self.model.cs_reg_model
        data = model.stat_date

        if stat_mode == "模型统计":
            # 展示 t 值和 R2 时间序列
            cols = ["date", "t(m0)", "t(m1)", "t(m2)", "R2(m0)", "R2(m1)", "R2(m2)"]
            plot_data = data.select(cols)
            # 生成 t 值和 R2 的折线图
            import holoviews as hv
            from ....factor.testers.base import NumeralTickFormatter

            t_plot = plot_data.hvplot.line(
                x="date",
                y=["t(m0)", "t(m1)", "t(m2)"],
                group_label="模型",
                title="因子 t 值序列",
                hover="vline",
                hover_cols="all",
            )
            r2_plot = plot_data.hvplot.line(
                x="date",
                y=["R2(m0)", "R2(m1)", "R2(m2)"],
                group_label="模型",
                title="模型 R2 序列",
                yformatter=NumeralTickFormatter(format="0.0000"),
                hover="vline",
                hover_cols="all",
            )
            plots = [t_plot, r2_plot]
            table_data = data
        else:
            # Delta R2
            core = model.calc_core_index()
            import holoviews as hv
            from ....factor.testers.base import NumeralTickFormatter

            delta_cols = [c for c in core.columns if c != "date"]
            delta_plot = core.hvplot.line(
                x="date",
                y=delta_cols,
                group_label="指标",
                title="Delta R2 与显著性比例",
                yformatter=NumeralTickFormatter(format="0.0000"),
                hover="vline",
                hover_cols="all",
            )
            plots = [delta_plot]
            table_data = core

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
                table_data.to_pandas(),
                columns=list(table_data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class CSRegModelViewer:
    """截面回归模型页面入口"""

    def __init__(self, model: FactorTestResultModel, page_root=None) -> None:
        if model.cs_reg_model is None:
            self.view = None
            self.controller = None
            self.layout = pn.pane.Markdown(
                "## 无可用截面回归模型结果\n\n请将 CSRegModelTestResults.pkl 放入 `results/` 目录"
            )
        else:
            self.view = CSRegModelView(model)
            self.controller = CSRegModelController(model, self.view)
            self.layout = self.view.layout
