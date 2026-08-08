from panel_splitjs import Split

def toggle_split(splitter: Split) -> None:
    """切换 Split 组件的折叠/展开状态"""
    splitter.collapsed = None if splitter.collapsed == 1 else 1