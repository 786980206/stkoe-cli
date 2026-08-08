import panel as pn
pn.extension('perspective')


class PerspectiveTable(object):
    def __init__(self, data):
        self.component = pn.pane.Perspective(
            data, 
            sizing_mode="stretch_width",
            plugin="datagrid",           # 表格视图
            settings=False,              # 隐藏左上角三点配置菜单
            height=600,
            title="Table"
    )

