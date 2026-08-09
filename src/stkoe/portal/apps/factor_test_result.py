"""回测结果展示模块 - 通过 URL 参数 ?result_id=xxx 切换回测结果"""

import sys
from pathlib import Path


import panel as pn
from panel_splitjs import Split
from stkoe.portal.theme.quant import Quant

pn.extension(defer_load=True, loading_indicator=True)
pn.extension("perspective")
pn.extension("gridstack")
pn.config.design = Quant

from stkoe.portal.config import DEFAULT_RESULT_ID
from stkoe.portal.models.factor_test_result import FactorTestResultModel as FactorTestModel
from stkoe.portal.viewers.factor_test_result.bucket_returns import BucketReturnsViewer
from stkoe.portal.viewers.FactorReturnsViewer import FactorReturnsViewer
from stkoe.portal.viewers.ICViewer import ICViewer
from stkoe.portal.viewers.CoverageViewer import CoverageViewer
from stkoe.portal.viewers.StabilityViewer import StabilityViewer
from stkoe.portal.viewers.CSRegModelViewer import CSRegModelViewer

from stkoe.portal.viewers.chatbot import _MockChatBot


@pn.cache
def _get_model(result_id: str = DEFAULT_RESULT_ID) -> FactorTestModel:
    """按 result_id 获取模型实例（pn.cache 进程级缓存，多用户共享）"""
    return FactorTestModel(result_id)


def build_app(result_id: str):
    """根据 result_id 构建回测结果展示 Tabs"""
    model = _get_model(result_id)
    container = pn.Column()

    tabs = []
    # 分层收益（核心）
    if model.bucket_returns is not None:
        tabs.append(("分层收益", BucketReturnsViewer(model).layout))
    # 因子收益
    if model.factor_returns is not None:
        tabs.append(("因子收益", FactorReturnsViewer(model).layout))
    # IC分析
    if model.ic_test is not None:
        tabs.append(("IC分析", ICViewer(model).layout))
    # 覆盖率
    if model.coverage_test is not None:
        tabs.append(("覆盖率", CoverageViewer(model).layout))
    # 稳定性（换手率 + 自相关）
    if model.bucket_turnover is not None or model.auto_correlation is not None:
        tabs.append(("稳定性", StabilityViewer(model).layout))
    # 截面回归模型
    if model.cs_reg_model is not None:
        tabs.append(("截面回归", CSRegModelViewer(model).layout))

    if not tabs:
        tabs.append(("提示", pn.pane.Markdown(
            "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
        )))

    def _load_content(event=None):
        """延迟加载：首次渲染时才读取数据和构建 UI"""
        if len(container) > 0:
            return  # 已加载

        model = _get_model(result_id)
        new_tabs = []

        if model.bucket_returns is not None:
            new_tabs.append(("分层收益", BucketReturnsViewer(model).layout))
        if model.factor_returns is not None:
            new_tabs.append(("因子收益", FactorReturnsViewer(model).layout))
        if model.ic_test is not None:
            new_tabs.append(("IC分析", ICViewer(model).layout))
        if model.coverage_test is not None:
            new_tabs.append(("覆盖率", CoverageViewer(model).layout))
        if model.bucket_turnover is not None or model.auto_correlation is not None:
            new_tabs.append(("稳定性", StabilityViewer(model).layout))
        if model.cs_reg_model is not None:
            new_tabs.append(("截面回归", CSRegModelViewer(model).layout))

        if not new_tabs:
            new_tabs.append(("提示", pn.pane.Markdown(
                "## 无可用回测结果\n\n请将回测结果放入 `results/` 目录"
            )))

        container[:] = [
            Split(
                pn.Tabs(
                    *new_tabs,
                    tabs_location="left",
                    sizing_mode="stretch_both",
                ),
                _MockChatBot().layout,
                sizes=(80, 20),
                min_size=(300, 300),
                sizing_mode="stretch_both",
                gutter_size=1,
            )

        ]

    container.param.watch(_load_content, "visible")
    # 触发首次构建
    _load_content()
    return container


def create_app():
    """创建回测结果展示应用（供 main.py 导入 / 独立 panel serve 使用）"""
    location = pn.state.location
    container = pn.Column()

    def update_layout(event=None) -> None:
        """监听 URL 参数变化，重新加载对应回测结果"""
        result_id = DEFAULT_RESULT_ID
        if location and location.search:
            for part in location.search.lstrip("?").split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k == "result_id":
                        result_id = v
        container[:] = [build_app(result_id)]

    if location:
        location.param.watch(update_layout, "search")
    update_layout()
    return container


if __name__ == "__main__":
    create_app().servable()
