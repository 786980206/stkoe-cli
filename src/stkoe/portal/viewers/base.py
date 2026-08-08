import panel as pn
import panel as pn
from panel_splitjs import Split
from stkoe.portal.viewers.utils import toggle_split
from stkoe.portal.viewers.chatbot import _MockChatBot

class BaseLayoutView:
    """基础页面框架"""

    def __init__(self) -> None:
        # 页面布局
        self.menu = pn.Row(
            pn.widgets.MenuButton(label="🧏🏻‍♂️ 因子管理",  items=["新建因子","因子列表"], width=100, color='light', styles={"margin":"0px"},align='center',split=False),
            pn.widgets.MenuButton(label="🧏🏻‍♂️ 因子测试",  items=["因子回测","回测结果"], width=100, color='light', styles={"margin":"0px"},align='center'),
            pn.widgets.MenuButton(label="🧏🏻‍♂️ 模型研究",  items=["新建模型","模型测试"], width=100, color='light', styles={"margin":"0px"},align='center'),
            align='center',
        )
        self.chatbox = pn.widgets.ButtonIcon(icon="message-chatbot", active_icon="message-off", size="14px",styles={"margin":"0px"},align='center')
        self.tools = pn.Row(
            pn.widgets.ToggleIcon(icon="settings", size="14px",styles={"margin":"0px"},align='center'),
            pn.widgets.ToggleIcon(icon="menu", size="14px",styles={"margin":"0px"},align='center'),
            self.chatbox,
            align='center',
        ) 
        self.header = pn.Row(
            pn.pane.HTML("🌏",  styles={"margin":"0px 10px 0px 10px","font-size":"14px"}, align='center'),
            self.menu,
            pn.HSpacer(),
            self.tools,
            height=25, 
            sizing_mode="stretch_width",
        )

        # 全局 current_page 跟踪 & 说明侧栏
        self.current_page = None
        self.desc_pane = pn.pane.Markdown(
            "## 页面说明\n\n当前页面暂无说明",
            sizing_mode="stretch_width",
        )
        self.right_sidebar = pn.Tabs(
            ("助手",_MockChatBot().layout),
            ("测试","123"),
            ("说明", pn.Column(self.desc_pane, sizing_mode="stretch_both", scroll=True)),
            sizing_mode="stretch_both",
        )
        self.footer = pn.Row(
            pn.pane.HTML("🌏",  styles={"margin":"0px 10px 0px 10px","font-size":"14px"}, align='center'),
            pn.HSpacer(),
            self.tools,
            height=20, 
            sizing_mode="stretch_width",
        )
        self.content = pn.Column(sizing_mode="stretch_both")
        self.body = Split(
            self.content,
            self.right_sidebar,
            collapsed=None,
            sizes=(85, 15),
            # min_size=(300,300),
            expanded_sizes=(85, 15),
            sizing_mode="stretch_both",
            styles={"border-top": "1px solid var(--panel-border-color)", "border-bottom": "1px solid var(--panel-border-color)"},
            gutter_size=1,
        )
        self.layout = pn.Column(self.header, self.body, self.footer)

    def set_current_page(self, view) -> None:
        """由内容页调用，更新当前页面 View 及侧栏说明"""
        self.current_page = view
        if view is not None and hasattr(view, 'desc'):
            self.desc_pane.object = view.desc
        else:
            self.desc_pane.object = "## 页面说明\n\n当前页面暂无说明"


class BaseLayoutController:
    """分层回测控制器（预留给未来子页面间交互逻辑）"""

    def __init__(self, view: BaseLayoutView) -> None:
        self.view = view

        # 收起展开侧边栏聊天框
        self.view.chatbox.on_click(lambda _: toggle_split(self.view.body))


class BaseLayoutViewer:
    """分层回测页面入口 - 组合 View + Controller"""

    def __init__(self) -> None:
        self.view = BaseLayoutView()
        self.controller = BaseLayoutController(self.view)

    @property
    def layout(self):
        """返回 Panel 组件树"""
        return self.view.layout
