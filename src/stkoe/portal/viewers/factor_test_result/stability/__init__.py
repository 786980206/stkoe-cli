"""稳定性分析主页面 - 分组换手率与因子自相关系数"""

import panel as pn
from .turnover import TurnoverViewer
from .autocorr import AutoCorrViewer
from stkoe.portal.models.factor_test_result import FactorTestResultModel


class StabilityView:
    """稳定性分析视图 - 以 Tabs 组织所有子分析页"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.turnover = TurnoverViewer(model)
        self.autocorr = AutoCorrViewer(model)

        has_turnover = model.bucket_turnover is not None
        has_autocorr = model.auto_correlation is not None

        if not has_turnover and not has_autocorr:
            self.tabs = None
            self.layout = pn.pane.Markdown(
                "## 无可用稳定性测试结果\n\n请将 BucketTurnoverTestResults.pkl 和 AutoCorrelationTestResults.pkl 放入 `results/` 目录"
            )
        else:
            tabs = []
            if has_turnover:
                tabs.append(("分组换手率", self.turnover.layout))
            if has_autocorr:
                tabs.append(("自相关系数", self.autocorr.layout))
            self.tabs = pn.Tabs(*tabs, sizing_mode="stretch_both")
            self.layout = self.tabs


class StabilityController:
    """稳定性分析控制器 - Tab 切换更新 page_root.current_page"""

    def __init__(self, model: FactorTestResultModel, view: StabilityView, page_root=None) -> None:
        self.model = model
        self.view = view
        self.page_root = page_root

        if view.tabs is not None:
            view.tabs.param.watch(self._on_tab_change, 'active')

    def _on_tab_change(self, event=None) -> None:
        idx = self.view.tabs.active
        sub_views = []
        if self.model.bucket_turnover is not None:
            sub_views.append(self.view.turnover)
        if self.model.auto_correlation is not None:
            sub_views.append(self.view.autocorr)
        if idx is not None and 0 <= idx < len(sub_views) and self.page_root is not None:
            v = sub_views[idx]
            sub_view = getattr(v, 'view', None)
            if sub_view is not None:
                self.page_root.set_current_page(sub_view)


class StabilityViewer:
    """稳定性分析页面入口"""

    def __init__(self, model: FactorTestResultModel, page_root=None) -> None:
        self.view = StabilityView(model)
        self.controller = StabilityController(model, self.view, page_root)
