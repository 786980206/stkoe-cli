"""IC分析主页面 - IC序列、IC分布、IC累计、IC月度热力图"""

import panel as pn
from .ic_series import ICSeriesViewer
from .ic_hist import ICHistViewer
from .ic_cums import ICCumsViewer
from .ic_month import ICMonthViewer
from stkoe.portal.models.factor_test_result import FactorTestResultModel


class ICView:
    """IC分析视图 - 以 Tabs 组织所有子分析页"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.ic_series = ICSeriesViewer(model)
        self.ic_hist = ICHistViewer(model)
        self.ic_cums = ICCumsViewer(model)
        self.ic_month = ICMonthViewer(model)

        if model.ic_test is None:
            self.tabs = None
            self.layout = pn.pane.Markdown(
                "## 无可用 IC 测试结果\n\n请将 ICTestResults.pkl 放入 `results/` 目录"
            )
        else:
            self.tabs = pn.Tabs(
                ("IC序列", self.ic_series.layout),
                ("IC分布", self.ic_hist.layout),
                ("IC累计", self.ic_cums.layout),
                ("IC月度", self.ic_month.layout),
                sizing_mode="stretch_both",
            )
            self.layout = self.tabs


class ICController:
    """IC分析控制器 - Tab 切换更新 page_root.current_page"""

    def __init__(self, model: FactorTestResultModel, view: ICView, page_root=None) -> None:
        self.model = model
        self.view = view
        self.page_root = page_root

        if view.tabs is not None:
            view.tabs.param.watch(self._on_tab_change, 'active')

    def _on_tab_change(self, event=None) -> None:
        idx = self.view.tabs.active
        sub_views = [self.view.ic_series, self.view.ic_hist, self.view.ic_cums, self.view.ic_month]
        if idx is not None and 0 <= idx < len(sub_views) and self.page_root is not None:
            v = sub_views[idx]
            sub_view = getattr(v, 'view', None)
            if sub_view is not None:
                self.page_root.set_current_page(sub_view)


class ICViewer:
    """IC分析页面入口"""

    def __init__(self, model: FactorTestResultModel, page_root=None) -> None:
        self.view = ICView(model)
        self.controller = ICController(model, self.view, page_root)
