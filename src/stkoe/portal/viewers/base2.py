"""通用视图/控制器基类 - 消除各 Viewer 中重复的布局构建和交互逻辑"""

from __future__ import annotations

import panel as pn
from panel_splitjs import Split


def build_standard_layout(
    toolbar: pn.Row,
    graph_result: pn.Accordion,
    data_result: pn.Column,
    spec_json: bytes,
    desc: str,
    *,
    sizes: tuple[int, int] = (80, 20),
) -> tuple[Split, Split]:
    """构建标准的三栏布局：工具栏 → 图表/数据分栏 → 右侧说明栏

    所有 Viewer 页面共享此布局结构，仅工具栏内容和描述文案不同。

    Args:
        toolbar: 顶部工具栏组件
        graph_result: 图表展示 Accordion
        data_result: 数据表格展示 Column
        spec_json: 回测参数 JSON（右侧栏展示）
        desc: 页面说明 Markdown 文本（右侧栏展示）
        sizes: 主区域与右侧栏的比例，默认 (80, 20)

    Returns:
        (layout, split_content): layout 为完整页面布局, split_content 为图表/数据分栏（用于切换折叠）
    """
    split_content = Split(
        graph_result,
        data_result,
        collapsed=None,
        expanded_sizes=(50, 50),
        sizing_mode="stretch_both",
        styles={"border-top": "1px solid var(--panel-border-color)"},
        gutter_size=1,
    )

    main_content = pn.Column(
        toolbar,
        split_content,
        styles={"height": "calc(100vh - 73px)"},
    )

    layout = main_content

    return layout, split_content


def toggle_split(splitter: Split) -> None:
    """切换 Split 组件的折叠/展开状态"""
    splitter.collapsed = None if splitter.collapsed == 1 else 1


def make_loading() -> pn.indicators.LoadingSpinner:
    """创建加载动画组件"""
    return pn.indicators.LoadingSpinner(value=True, width=25, height=25)


def rasterize_plots(plots: list, **kwargs) -> list:
    """对 HoloViews 图表列表应用 datashader 光栅化，降低浏览器渲染负载

    对每个 Overlay/NdOverlay 应用 rasterize 操作，将矢量图形转为像素图像。
    光栅化在服务端完成，浏览器只接收图像而非原始数据点。

    Args:
        plots: HoloViews 图表列表（Overlay, NdOverlay 等）
        **kwargs: 传递给 rasterize 的额外参数

    Returns:
        光栅化后的图表列表，渲染失败时回退到原始图表
    """
    from holoviews.operation.datashader import rasterize

    rasterized = []
    for p in plots:
        try:
            rasterized.append(rasterize(p, precompute=True, dynamic=False, **kwargs))
        except Exception:
            rasterized.append(p)
    return rasterized
