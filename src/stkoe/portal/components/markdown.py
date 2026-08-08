import re
import panel as pn

import re
import panel as pn


class Markdown:
    HEAD_RE=r'^(#{1,6})\s+(.+)$'

    def __init__(self,content:str,vars=None,css=None):
        self.css = css
        # print(css)
        self.vars = vars or {}
        self.toc = [] 
        self.content = self._build_content(content)
        self.component = self.content

    def _slugify(self,text):
        return re.sub(r'\W+','-',text.lower())
    
    def _process_headers(self,text):
        lines=[]
        for line in text.splitlines():
            m=re.match(self.HEAD_RE,line)
            if m:
                level=len(m.group(1))
                title=m.group(2)
                anchor=self._slugify(title)
                self.toc.append((level,title,anchor))
                line=f'{m.group(1)} <a id="{anchor}"></a>{title}'
            lines.append(line)
        return "\n".join(lines)
            
    def _build_content(self, content):
        """构造内容区域"""
        components=[]
        pos=0
        for match in re.finditer(r'\{\{\s*(\w+)\s*\}\}',content):
            if match.start()>pos: components.append( pn.pane.Markdown( self._process_headers(content[pos:match.start()]), css_classes=["main-content-item"],stylesheets=[self.css]))
            name=match.group(1)
            comp=self.vars.get(name)
            if comp is not None: components.append(comp.component if hasattr(comp,"component") else comp)
            pos=match.end()
        if pos<len(content):
            components.append( pn.pane.Markdown( self._process_headers(content[pos:]), css_classes=["main-content-item"], stylesheets=[self.css] ) )
        return pn.Column(*components,css_classes=["main-content"], stylesheets=[self.css]) 
