"""稳定性分析主页面 - 分组换手率与因子自相关系数"""

import panel as pn
from .turnover import TurnoverViewer
from .autocorr import AutoCorrViewer
from ...models.factor_test_result import FactorTestResultModel as FactorTestModel


class StabilityView:
    """稳定性分析视图 - 以 Tabs 组织所有子分析页"""

    def __init__(self, model: FactorTestModel) -> None:
        self._model = model
        self._turnover = TurnoverViewer(model)
        self._autocorr = AutoCorrViewer(model)

    @property
    def layout(self):
        """构建稳定性分析 Tabs 布局"""
        has_turnover = self._model.bucket_turnover is not None
        has_autocorr = self._model.auto_correlation is not None

        if not has_turnover and not has_autocorr:
            return pn.pane.Markdown(
                "## 无可用稳定性测试结果\n\n请将 BucketTurnoverTestResults.pkl 和 AutoCorrelationTestResults.pkl 放入 `results/` 目录"
            )
        tabs = []
        if has_turnover:
            tabs.append(("分组换手率", self._turnover.layout))
        if has_autocorr:
            tabs.append(("自相关系数", self._autocorr.layout))
        return pn.Tabs(*tabs, sizing_mode="stretch_both")


class StabilityController:
    """稳定性分析控制器"""

    def __init__(self, model: FactorTestModel, view: StabilityView) -> None:
        self._model = model
        self._view = view


class StabilityViewer:
    """稳定性分析页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestModel) -> None:
        self._view = StabilityView(model)
        self._controller = StabilityController(model, self._view)

    @property
    def layout(self):
        return self._view.layout
