"""因子实验页：加载本地因子收益 pickle 并展示累计收益（数据缺失时优雅降级）"""
import pickle
from pathlib import Path

import panel as pn

_RET_PKL = Path(r"E:/DataCenter/wslib/src/wsdata/der/factor_zoo/ret.pkl")


def _load_ret():
    if not _RET_PKL.exists():
        return None
    with _RET_PKL.open("rb") as f:
        return pickle.load(f)


def build():
    """构建因子实验页布局；本机实验数据缺失时返回提示占位"""
    ret = _load_ret()
    if ret is None:
        return pn.pane.Markdown(f"未找到因子实验数据：`{_RET_PKL}`（跳过因子页）")
    return pn.Column(ret.plot_rtn_cums_all(), scroll=True)