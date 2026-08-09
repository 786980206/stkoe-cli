"""因子收益展示 - 12种因子收益加权方案的累计收益分析"""

import asyncio
import holoviews as hv
import panel as pn
from panel_splitjs import Split
from ...models.factor_test_result import FactorTestResultModel as FactorTestModel

desc = r"""## 因子收益

展示12种不同加权方案的因子组合日度收益及累计收益序列。

**核心逻辑：** 每日根据因子值构建投资组合，通过 $$w \times d_n$$ 加权得到当日因子收益 $$FR(d_n)$$，再通过重叠平均法计算累计收益序列（holidays平均法，详见 `_calc_factor_returns` 中 `cum_prod` + `pivot` 逻辑）。

**权重 W 与方向 D 的设计维度：**

| 维度 | 选项 | 说明 |
|------|------|------|
| 行业调整 | 未经调整 / 行业中性 | 行业中性指权重在行业内独立计算（`over("date","group")`） |
| 加权方式 | 因子加权 / 个股等权 / 多空等权 | 权重是否正比于因子值 |
| 多空方向 | 多空中性 / 原始方向 | 多空分界线：均值或中位数 |

### 6种基础加权方案

假设截面有4只股票 $$factor = [-1, 1, 3, 9]$$：

---

**① 因子加权多空中性** | $$[W=F-\bar{F}; D=\pm(F-\bar{F})]$$

- 方向: 因子值超过均值的做多，低于均值的做空
- 权重: $$w = F - E(F)$$，再 $$w = w / \sum(|w|)$$
- 示例: $$w = -4/12, -2/12, 0, 6/12$$

---

**② 多空等权多空中性** | $$[W=-1/N,+1/M; D=\pm(F-\bar{F})]$$

- 方向: 因子值超过均值的做多，低于均值的做空
- 权重: 多空内部等权，$$w = \text{sign}(F - E(F)) / \text{count}(\text{sign})$$
- 示例: $$w = -1/4, -1/4, 0, 2/4$$

---

**③ 因子加权原始方向** | $$[W=F; D=\pm(F)]$$

- 方向: 因子值为正的做多，为负的做空（无中性化约束）
- 权重: $$w = F$$，再 $$w = w / \sum(|w|)$$
- 示例: $$w = -1/14, 1/14, 3/14, 9/14$$

---

**④ 个股等权原始方向** | $$[W=\pm 1/N; D=\pm(F)]$$

- 方向: 因子值为正的做多，为负的做空
- 权重: 全截面等权，$$w = \text{sign}(F) / \sum(|\text{sign}(F)|)$$
- 示例: $$w = -1/4, 1/4, 1/4, 1/4$$

---

**⑤ 个股等权多空中性** | $$[W=\pm 1/N; D=\pm(F-MED(F))]$$

- 方向: 因子值超过中位数的做多，低于中位数的做空
- 权重: 全截面等权，$$w = \text{sign}(F - MED(F)) / \sum(|\text{sign}(F - MED(F))|)$$
- 示例: $$w = -1/4, -1/4, 1/4, 1/4$$

---

**⑥ 多空等权原始方向** | $$[W=-1/N,+1/M; D=\pm(F)]$$

- 方向: 因子值为正的做多，为负的做空
- 权重: 多空内部等权，$$w = \text{sign}(F) / \text{count}(\text{sign})$$
- 示例: $$w = -3/6, 1/6, 1/6, 1/6$$

---

### 12种组合说明

以上6种乘以「未经调整 / 行业中性」得到完整的12种加权方案，分别对应 `FactorReturnsTestResults` 中的12对 `{name}日度收益` / `{name}累计收益`：

| # | 名称 | 行业调整 | 权重公式 |
|---|------|----------|----------|
| 1 | 因子加权多空中性 | 否 | $W=F-\bar{F}; D=\pm(F-\bar{F})$ |
| 2 | 多空等权多空中性 | 否 | $W=-1/N,+1/M; D=\pm(F-\bar{F})$ |
| 3 | 因子加权原始方向 | 否 | $W=F; D=\pm(F)$ |
| 4 | 个股等权原始方向 | 否 | $W=\pm 1/N; D=\pm(F)$ |
| 5 | 个股等权多空中性 | 否 | $W=\pm 1/N; D=\pm(F-MED(F))$ |
| 6 | 多空等权原始方向 | 否 | $W=-1/N,+1/M; D=\pm(F)$ |
| 7 | 行业中性因子加权多空中性 | 是 | $W=F-\bar{F}; D=\pm(F-\bar{F})$ |
| 8 | 行业中性多空等权多空中性 | 是 | $W=-1/N,+1/M; D=\pm(F-\bar{F})$ |
| 9 | 行业中性因子加权原始方向 | 是 | $W=F; D=\pm(F)$ |
| 10 | 行业中性个股等权原始方向 | 是 | $W=\pm 1/N; D=\pm(F)$ |
| 11 | 行业中性个股等权多空中性 | 是 | $W=\pm 1/N; D=\pm(F-MED(F))$ |
| 12 | 行业中性多空等权原始方向 | 是 | $W=-1/N,+1/M; D=\pm(F)$ |

> 行业中性：权重在行业内独立计算（`group_adjust=True`），消除行业暴露的影响。
"""

FACTOR_RET_TYPES = [
    ("行业中性因子加权多空中性", "行业中性因子加权多空中性累计收益"),
    ("行业中性因子加权原始方向", "行业中性因子加权原始方向累计收益"),
    ("因子加权多空中性", "因子加权多空中性累计收益"),
    ("因子加权原始方向", "因子加权原始方向累计收益"),
    ("行业中性多空等权多空中性", "行业中性多空等权多空中性累计收益"),
    ("行业中性多空等权原始方向", "行业中性多空等权原始方向累计收益"),
    ("多空等权多空中性", "多空等权多空中性累计收益"),
    ("多空等权原始方向", "多空等权原始方向累计收益"),
    ("行业中性个股等权多空中性", "行业中性个股等权多空中性累计收益"),
    ("行业中性个股等权原始方向", "行业中性个股等权原始方向累计收益"),
    ("个股等权多空中性", "个股等权多空中性累计收益"),
    ("个股等权原始方向", "个股等权原始方向累计收益"),
]


class FactorReturnsView:
    """因子收益视图 - 条件筛选 + 因子收益图表/数据展示"""

    def __init__(self, spec_json: bytes) -> None:
        self._spec_json = spec_json

        # --- 控制输入组件 ---
        self.ret_type_input = pn.widgets.Select(
            value="行业中性因子加权多空中性",
            options=[label for label, _ in FACTOR_RET_TYPES],
            width=250,
        )
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
            self.ret_type_input,
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


class FactorReturnsController:
    """因子收益控制器 - 处理用户交互与数据加载"""

    def __init__(self, model: FactorTestModel, view: FactorReturnsView) -> None:
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
        ret_type_label = view.ret_type_input.value
        linked_axes = view.linked_axes_switch.value

        loading = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
        view.graph_result[:] = [loading]
        view.data_result[:] = [loading]

        # 获取对应的属性名
        attr_name = next(name for label, name in FACTOR_RET_TYPES if label == ret_type_label)
        fr = self._model.factor_returns
        data = getattr(fr, attr_name)

        from ....factor.testers.returns import plot_fac_rets
        plot = plot_fac_rets(data, title=ret_type_label)

        view.graph_result[:] = [
            (
                plot.opts.get("plot").kwargs.get("title"),
                pn.panel(plot, sizing_mode="stretch_both", linked_axes=linked_axes),
            )
        ]
        view.data_result[:] = [
            pn.pane.Perspective(
                data.to_pandas(),
                columns=list(data.columns),
                sizing_mode="stretch_both",
                settings=False,
            )
        ]


class FactorReturnsViewer:
    """因子收益页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestModel) -> None:
        if model.factor_returns is None:
            self._view = None
            self._controller = None
        else:
            self._view = FactorReturnsView(model.get_spec_json("factor_returns"))
            self._controller = FactorReturnsController(model, self._view)

    @property
    def layout(self):
        if self._view is None:
            return pn.pane.Markdown(
                "## 无可用因子收益结果\n\n请将 FactorReturnsTestResults.pkl 放入 `results/` 目录"
            )
        return self._view.layout
