"""分层回测主页面 - 收益统计、收益价差、收益累计、因子收益、行业分组收益子选项卡"""

import panel as pn
from .rtn_stat import RtnStatViewer
from .rtn_diff import RtnDiffViewer
from .rtn_cums import RtnCumsViewer
from .fac_ret import FacRetViewer
from .gbr_stat import GbrStatViewer
from stkoe.portal.models.factor_test_result import FactorTestResultModel


class BucketReturnsView:
    """分层回测视图 - 以 Tabs 组织所有子分析页"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        self.rtn_stat = RtnStatViewer(model)
        self.rtn_diff = RtnDiffViewer(model)
        self.rtn_cums = RtnCumsViewer(model)
        self.fac_ret = FacRetViewer(model)
        self.gbr_stat = GbrStatViewer(model)

        self.tabs = pn.Tabs(
            ("收益统计", self.rtn_stat.layout),
            ("收益价差", self.rtn_diff.layout),
            ("收益累计", self.rtn_cums.layout),
            ("因子收益", self.fac_ret.layout),
            ("行业分组收益", self.gbr_stat.layout),
            sizing_mode="stretch_both",
        )
        self.layout = self.tabs


class BucketReturnsController:
    """Tab 切换控制器 - 更新 page_root.current_page"""

    def __init__(self, model: FactorTestResultModel, view: BucketReturnsView, page_root=None) -> None:
        self.model = model
        self.view = view
        self.page_root = page_root
        self.sub_views = [
            getattr(view.rtn_stat, 'view', None),
            getattr(view.rtn_diff, 'view', None),
            getattr(view.rtn_cums, 'view', None),
            getattr(view.fac_ret, 'view', None),
            getattr(view.gbr_stat, 'view', None),
        ]
        view.tabs.param.watch(self._on_tab_change, 'active')

    def _on_tab_change(self, event=None):
        if self.page_root is None:
            return
        idx = self.view.tabs.active
        if idx is not None and 0 <= idx < len(self.sub_views):
            v = self.sub_views[idx]
            if v is not None:
                self.page_root.set_current_page(v)


class BucketReturnsViewer:
    """分层回测页面入口"""

    def __init__(self, model: FactorTestResultModel, page_root=None) -> None:
        self.view = BucketReturnsView(model)
        self.controller = BucketReturnsController(model, self.view, page_root)
        self.layout = self.view.layout
