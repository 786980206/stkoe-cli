"""回测执行模块 - 待开发"""



import panel as pn


def create_app():
    """创建回测执行应用（供 main.py 导入 / 独立 panel serve 使用）"""
    return pn.pane.Markdown("# 回测执行\n\n功能开发中...")


if __name__ == "__main__":
    create_app().servable()
