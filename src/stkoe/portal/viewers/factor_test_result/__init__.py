"""因子回测结果分析门户 - 多页面导航"""

import panel as pn
from stkoe.portal.config import DEFAULT_RESULT_ID
from stkoe.portal.models.factor_test_result import FactorTestResultModel
from .bucket_returns import BucketReturnsViewer
from .factor_returns import FacRetViewer
from .ic_test import ICViewer
from .coverage_test import CoverageViewer
from .stability import StabilityViewer
from .cs_reg_model import CSRegModelViewer


@pn.cache
def _get_model(result_id: str = DEFAULT_RESULT_ID) -> FactorTestResultModel:
    return FactorTestResultModel(result_id)


class FactorTestResultView:
    """左侧导航 + 内容区（右侧栏由 BaseLayoutView 管理）"""

    def __init__(self, result_id: str = DEFAULT_RESULT_ID, page_root=None) -> None:
        self.model = _get_model(result_id)
        self.page_root = page_root

        self.available_pages = self._get_available_pages()
        options = list(self.available_pages.keys())

        self.nav = pn.widgets.ToggleGroup(
            label='ToggleGroup', options=options,
            behavior="radio", orientation="vertical",
            value=options[0] if self.available_pages else None,
            styles={"margin": "0px"},
        )
        self.navbar = pn.Column(
            self.nav, styles={"border-right": "1px solid #ccc"},
            sizing_mode="stretch_height",
        )
        first_page = self._create_page(options[0])
        self.content = pn.Column(first_page, sizing_mode="stretch_both")
        self.layout = pn.Column(
            pn.Row(
                pn.Column(self.navbar, sizing_mode="stretch_height"),
                self.content,
                sizing_mode="stretch_both",
            ),
            sizing_mode="stretch_both",
        )

    def _get_available_pages(self) -> dict[str, type]:
        available_pages: dict[str, type] = {}
        available_pages["分层收益"] = BucketReturnsViewer
        if self.model.factor_returns is not None:
            available_pages["因子收益"] = FacRetViewer
        if self.model.ic_test is not None:
            available_pages["IC分析"] = ICViewer
        if self.model.coverage_test is not None:
            available_pages["覆盖率"] = CoverageViewer
        if self.model.bucket_turnover is not None or self.model.auto_correlation is not None:
            available_pages["稳定性"] = StabilityViewer
        if self.model.cs_reg_model is not None:
            available_pages["截面回归"] = CSRegModelViewer
        return available_pages

    def _create_page(self, name: str) -> pn.viewable.Viewable:
        viewer_cls = self.available_pages.get(name)
        if viewer_cls is None:
            return pn.pane.Markdown(f"## 未知页面: {name}")

        viewer = viewer_cls(self.model, page_root=self.page_root)
        self._current_viewer = viewer

        if self.page_root is not None:
            view = getattr(viewer, 'view', None)
            if view is not None:
                self.page_root.set_current_page(view)

        return viewer.layout


class FactorTestResultController:
    """导航切换控制器"""

    def __init__(self, view: FactorTestResultView) -> None:
        self.view = view
        self.view.nav.param.watch(self._on_nav_change, 'value')

    def _on_nav_change(self, event):
        if event.new is not None:
            self.view.content[:] = [self.view._create_page(event.new)]


class FactorTestResultViewer:
    """分层回测页面入口"""

    def __init__(self, result_id: str = DEFAULT_RESULT_ID, page_root=None) -> None:
        self.view = FactorTestResultView(result_id=result_id, page_root=page_root)
        self.controller = FactorTestResultController(self.view)
        self.layout = self.view.layout
