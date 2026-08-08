import asyncio

import panel as pn
import param

pn.extension()


class AutoGrowTextArea(pn.reactive.ReactiveHTML):
    """自动增高输入框"""

    value = param.String(default="")
    submit_trigger = param.Integer(default=0)

    _template = """
    <textarea id="textarea" placeholder="请输入问题..."></textarea>
    """

    _scripts = {
        "render": """
            function autoHeight() {
                textarea.style.height = 'auto';
                textarea.style.height = textarea.scrollHeight + 'px';
            }

            autoHeight();

            textarea.addEventListener('input', () => {
                data.value = textarea.value;
                autoHeight();
            });

            textarea.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' && event.ctrlKey) {
                    event.preventDefault();
                    data.submit_trigger = data.submit_trigger + 1;
                }
            });

            // 同步 data.value → textarea.value（Python 侧清空输入时生效）
            let prev = data.value;
            function sync() {
                if (data.value !== prev) {
                    prev = data.value;
                    if (textarea.value !== data.value) {
                        textarea.value = data.value;
                        autoHeight();
                    }
                }
                requestAnimationFrame(sync);
            }
            requestAnimationFrame(sync);
        """
    }

    _stylesheets = ["""
    :host {
        width: 100%;
    }
    textarea {
        width: 100%;
        min-height: 44px;
        max-height: 180px;

        resize: none;
        overflow-y: auto;

        border: none;
        outline: none;

        background: transparent;

        font-size: 12px;
        line-height: 1.6;

        padding: 0;
        margin: 0;

        font-family: inherit;

        box-sizing: border-box;
    }
    """]


class _MockChatBot:

    _MOCK_RESPONSES = {
        "收益": (
            "当前回测的整体累计收益为 **12.5%**，年化收益约 **8.3%**，最大回撤 **-5.2%**。\n\n"
            "各分位组表现：\n"
            "- Q1（最低组）：-2.1%\n"
            "- Q5（最高组）：18.7%\n"
            "- 多头-空头收益差：20.8%"
        ),
        "风险": (
            "风险指标概览：\n\n"
            "- **年化波动率**：15.8%\n"
            "- **夏普比率**：0.72\n"
            "- **最大回撤**：-5.2%\n"
            "- **最大回撤期间**：2024-01 ~ 2024-02\n"
            "- **收益风险比**：1.24"
        ),
        "因子": (
            "当前回测使用的因子：\n\n"
            "1. **动量因子** — IC 均值 0.06，ICIR 1.2\n"
            "2. **价值因子** — IC 均值 0.04，ICIR 0.8\n"
            "3. **质量因子** — IC 均值 0.05，ICIR 0.9\n\n"
            "其中动量因子表现最佳。"
        ),
        "分组": (
            "| 分组 | 累计收益 |\n"
            "|------|---------|\n"
            "| Q1 | -2.1% |\n"
            "| Q2 | 3.5% |\n"
            "| Q3 | 7.8% |\n"
            "| Q4 | 12.3% |\n"
            "| Q5 | 18.7% |\n\n"
            "分组单调性良好。"
        ),
    }

    _DEFAULT_RESPONSE = """
您好！我是 **回测分析助手** 🧠

您可以问我：

- 📈 收益分析
- ⚠️ 风险分析
- 🔬 因子分析
- 📊 分组分析

请在下方输入您的问题。
"""

    def __init__(self):

        # =====================
        # Toolbar
        # =====================

        self._new_chat_btn = pn.widgets.ButtonIcon(icon="pencil-plus", size="14px",styles={"margin":"0px"},align='center')
        self._clear_btn = pn.widgets.ButtonIcon(icon="trash", size="14px",styles={"margin":"0px"},align='center')

        self._new_chat_btn.on_click(self._on_new_chat)
        self._clear_btn.on_click(self._on_clear_chat)

        self._toolbar = pn.Row(
            pn.layout.HSpacer(),
            self._new_chat_btn,
            self._clear_btn,
            sizing_mode="stretch_width",
            styles={
                "border-bottom": "1px solid var(--border-color)",
                "min-height": "27px",
                "align-items": "center",
            },
        )

        # =====================
        # Chat Feed
        # =====================

        self._feed = pn.chat.ChatFeed(
            sizing_mode="stretch_both",
            auto_scroll_limit=200,
            styles={
                "font-size": "12px",
            },            
        )

        # =====================
        # Composer
        # =====================

        self._input = AutoGrowTextArea(sizing_mode="stretch_width")

        self._model = pn.widgets.Select(
            options=[
                "Mock",
                "DeepSeek",
                "GPT-4o",
                "Claude",
            ],
            value="Mock",
            width=110,
            height=34,
            styles={"margin": "0px","border-radius":"10px"},
        )

        self._send_btn = pn.widgets.Button(
            name="↑",
            button_type="primary",
            width=34,
            height=34,
            align="center",
            styles={"margin": "0px","border-radius":"10px"},
        )

        self._send_btn.on_click(self._on_send)

        self._input.param.watch(
            lambda event: self._on_send(None),
            "submit_trigger",
        )

        self._composer = pn.Column(
            self._input,
            pn.Row(
                self._model,
                pn.layout.HSpacer(),
                self._send_btn,
                sizing_mode="stretch_width",
                align="center",
                height=25,
            ),
            sizing_mode="stretch_width",
            styles={
                "padding": "8px 12px",
                "background": "white",
                "border": "1px solid var(--border-color)",
                "border-radius": "12px",
                "margin": "8px",
            }
        )

        # =====================
        # Layout
        # =====================

        self._layout = pn.Column(
            self._toolbar,
            self._feed,
            self._composer,
            sizing_mode="stretch_both",
        )

        self._feed.send(
            self._DEFAULT_RESPONSE,
            user="AI助手",
            respond=False,
        )

    # ==========================================================
    # Send
    # ==========================================================

    def _on_send(self, _):

        text = self._input.value.strip()

        if not text:
            return

        self._feed.send(
            text,
            user="Stkoe",
            respond=False,
        )

        self._input.value = ""

        asyncio.create_task(
            self._stream_response(text)
        )

    # ==========================================================
    # Toolbar
    # ==========================================================

    def _on_new_chat(self, event):
        """新建对话 - 清空并重新发送欢迎消息"""
        self._feed.clear()
        self._feed.send(
            self._DEFAULT_RESPONSE,
            user="AI助手",
            respond=False,
        )

    def _on_clear_chat(self, event):
        """清空对话"""
        self._feed.clear()

    # ==========================================================
    # Mock LLM
    # ==========================================================

    async def _stream_response(self, text: str):

        response = self._match_response(text)

        message = self._feed.send(
            "",
            user="AI助手",
            respond=False,
        )

        streamed = ""

        for ch in response:

            streamed += ch

            try:
                message.object = streamed
            except Exception:
                pass

            await asyncio.sleep(0.015)

    def _match_response(self, text: str) -> str:

        for keyword, response in self._MOCK_RESPONSES.items():
            if keyword in text:
                return response

        return self._DEFAULT_RESPONSE

    # ==========================================================
    # Public
    # ==========================================================

    @property
    def layout(self):
        return self._layout