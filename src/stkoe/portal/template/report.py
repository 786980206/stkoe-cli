import panel as pn
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(Path(__file__).parent))
jinja_template = env.get_template('report.html')

css = Path(Path(__file__).parent  / "report.css").read_text(encoding='utf-8')

class ReportPageTemplate:
    css = css

    def __init__(self, header=None, sidebar=None, main=None, toc=None, *args, **kwargs):
        self.page = pn.Template(jinja_template)
        self.page.add_variable("css", self.css)
        self.page.add_panel("header", header)

        # 左侧导航区域：嵌入 sidebar
        sidebar = pn.pane.HTML("""    
            <div class="nav-section">
                <div class="nav-section-title">导航菜单</div>
                <a href="#" class="nav-link">概述</a>
                <a href="#" class="nav-link">快速开始</a>
                <a href="#" class="nav-link">安装</a>            
            </div>
        """,css_classes=["sidebar-nav"], stylesheets=[self.css])

        # 右侧目录区域
        if toc is not None: toc = "\n".join([f'''<a href="#{anchor}" class="toc-item" style="padding-left:{8+(level-1)*14}px">{title}</a>''' for level,title,anchor in toc])
        toc = pn.pane.HTML(f"""
            <div class="toc-title">本页内容</div>
            { toc }
            <a href="#" class="toc-item" style="padding-left:8px">回到顶部</a>
            <div style="margin-top: 2rem; border-top: 1px solid #edf2f7; padding-top: 1rem;">
                <div class="toc-item" style="border-left: none; padding-left: 0;">📘 编辑于 GitHub</div>
            </div>
        """,css_classes=["right-toc"],stylesheets=[self.css])                    

        # 构建页面主体
        content = pn.Row(sidebar, main, toc, css_classes=["layout-container"],stylesheets=[self.css])        
        self.page.add_panel("content", content)


if __name__ == "__main__":
    tmpl = ReportPageTemplate().tmpl
    pn.serve(tmpl)