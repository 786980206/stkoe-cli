"""概览页面 - 回测配置摘要 + 整体收益统计概览"""

import panel as pn
from panel_splitjs import Split
from stkoe.portal.models.factor_test_result import FactorTestResultModel

class OverviewView:
    """概览视图 - 自动展示配置摘要与整体统计"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.model = model
        left = pn.Column()
        right = pn.Column()
        self.layout = Split(
            left,
            right,
            sizes=(40, 60),
            min_size=(300, 300),
            sizing_mode="stretch_both",
            gutter_size=1,
        )


class OverviewController:
    """概览控制器（预留给未来扩展）"""

    def __init__(self, model: FactorTestResultModel, view: OverviewView) -> None:
        self.model = model
        self.view = view


class OverviewViewer:
    """概览页面入口 - 组合 View + Controller"""

    def __init__(self, model: FactorTestResultModel) -> None:
        self.view = OverviewView(model)
        self.controller = OverviewController(model, self.view)

    @property
    def layout(self):
        return self.view.layout
