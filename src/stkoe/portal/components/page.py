import panel as pn
from panel_material_ui import Page,Tabs
from pathlib import Path
import frontmatter
import os,re
from typing import Dict,Any
from .markdown import Markdown
from ..template.report import ReportPageTemplate

pn.extension()


class Page:

    def __init__(self,md_file_path,root_dir):
        self.parse_md_file(md_file_path,root_dir)
        self.content=Markdown(self.content_raw,self.vars,css=ReportPageTemplate.css)

        pages=[
            ("Introduction","/docs/introduction"),
            ("Syntax","/docs/syntax"),
            ("Components","/docs/components"),
        ]
        self.page = ReportPageTemplate(main=self.content.component, toc=self.content.toc).page
        # self.page = self.content.component

    def parse_md_file(self,md_file_path:Path,root_dir:Path)->Dict[str,Any]:
        self.metadata,self.content_raw=frontmatter.parse(md_file_path.read_text(encoding="utf-8"))
        rel_path=str(md_file_path.relative_to(root_dir.resolve()).with_suffix(''))
        self.url=f"/{rel_path.replace(os.sep,'/')}"
        self.vars={}
        module_path=md_file_path.with_suffix('.py')
        if module_path.exists():exec(module_path.read_text(encoding="utf-8"),self.vars)