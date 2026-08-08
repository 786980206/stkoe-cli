"""IC分析主页面 - IC序列、IC分布、IC累计、IC月度热力图"""

import panel as pn
from .ic_series import ICSeriesViewer
from .ic_hist import ICHistViewer
from .ic_cums import ICCumsViewer
from .ic_month import ICMonthViewer
from ...models.factor_test import FactorTestModel


class ICView:
    """IC分析视图 - 以 Tabs 组织所有子分析页"""

    def __init__(self, model: FactorTestModel) -> None:
        self._model = model
        self._ic_series = ICSeriesViewer(model)
        self._ic_hist = ICHistViewer(model)
        self._ic_cums = ICCumsViewer(model)
        self._ic_month = ICMonthViewer(model)

    @property
    def layout(self):
        """构建 IC 分析 Tabs 布局"""
        if self._model.ic_test is None:
            return pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        return pn.Tabs(
            ("IC序列", self._ic_series.layout),
            ("IC分布", self._ic_hist.layout),
            ("IC累计", self._ic_cums.layout),
            ("IC月度", self._ic_month.layout),
            sizing_mode="stretch_both",
        )


class ICController:
    """IC分析控制器（预留给未来子页面间交互逻辑）"""

    def __init__(self, model: FactorTestModel, view: ICView) -> None:
        self._model = model
        self._view = view


class ICViewer:
    """IC分析页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestModel) -> None:
        self._view = ICView(model)
        self._controller = ICController(model, self._view)

    @property
    def layout(self):
        """返回 Panel 组件树"""
        return self._view.layout
