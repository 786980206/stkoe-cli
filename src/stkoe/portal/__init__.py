from .components.page import Page
from pathlib import Path
import frontmatter
import os
from pathlib import Path
from typing import List, Dict, Any
import panel as pn

def ScanPages(root_dir):
    # md files
    md_file_paths = list( root_dir.resolve().rglob('*.md') )
    print("扫描页面")
    pages = {md_file_path:Page(md_file_path, root_dir) for md_file_path in md_file_paths}
    return pages

