# """主入口 - 多模块导航，整合所有自营应用"""

import panel as pn
import pandas as pd
pn.extension(defer_load=True, loading_indicator=True)
pn.extension("perspective")
pn.extension("gridstack")
pn.extension("mathjax")
pn.extension('tabulator')
from stkoe.portal.theme.quant import Quant
pn.config.design = Quant

from stkoe.portal.viewers.base import BaseLayoutViewer
from stkoe.portal.viewers.factor_test_result import FactorTestResultViewer

page = BaseLayoutViewer()
ftr = FactorTestResultViewer(page_root=page.view)
data = pd.DataFrame([{"记录ID":"202601013030","因子名称":"fac_hbeta","开始时间":"2025-01-01","结束时间":"2026-12-31"}] * 100)
# page.view.content[:] = [pn.Row(pn.widgets.Tabulator( data, groupby=["因子名称"], hidden_columns=["因子名称","index"], sizing_mode="stretch_height", width=200,theme='semantic-ui', show_index=False, configuration={'headerVisible': False}), pn.Column(ftr.layout, sizing_mode="stretch_both"))]
page.view.content[:] = [pn.Row(pn.widgets.Tabulator( data, sizing_mode="stretch_height", width=500,theme='semantic-ui', show_index=False, disabled=True), pn.Column(ftr.layout, sizing_mode="stretch_both"))]
page.layout.servable()