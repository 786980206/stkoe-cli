from panel.io.resources import CDN_DIST
from panel.viewable import Viewable
from panel.widgets.indicators import Number
from panel.theme.base import ( DarkTheme, DefaultTheme, Design, Inherit )
from pathlib import Path

class QuantDefaultTheme(DefaultTheme):
    ""

class QuantDarkTheme(DarkTheme):
    modifiers = {
        Number: {'default_color': 'white'}
    }

class Quant(Design):

    modifiers = {
        Viewable: {'stylesheets': [Inherit, f"{Path(__file__).with_suffix('.css')}"]}
    }

    _themes = {'dark': QuantDarkTheme,'default': QuantDefaultTheme}

if __name__ == '__main__':
    import panel as pn
    pn.config.design = Quant

    tb = pn.Row(
        pn.widgets.Select(options=['Biology', 'Chemistry', 'Physics'], width=100),
        pn.widgets.Select(options=['Biology', 'Chemistry', 'Physics'], width=100),
        pn.widgets.TextInput(value='Hello, world!', width=100),
    )
    content = pn.Tabs(
        ("test", tb ),
        ("test2", "哈哈哈")
    )
    pn.serve(content, port=9560)

