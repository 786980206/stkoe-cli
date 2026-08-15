from hvplot import polars
from bokeh.models import NumeralTickFormatter
import holoviews as hv
from bokeh.themes.theme import Theme

theme = Theme(json={
    "attrs": {
        "Title": {"align": "center", "text_font_size": "15px"},
        "Axis": {"axis_label_text_font_style": "normal"},
        "Legend": {"title_text_font_style": "normal"},
        "ColorBar": {"title_text_font_style": "normal"},
    }
})
hv.renderer("bokeh").theme = theme
hv.plotting.bokeh.plot.BokehPlot.autohide_toolbar = True
hv.plotting.bokeh.plot.BokehPlot.title = ""
hv.plotting.bokeh.plot.BokehPlot.ylabel = ""
hv.plotting.bokeh.plot.BokehPlot.xlabel = ""
hv.plotting.bokeh.element.ElementPlot.active_tools = []
hv.opts.defaults(
    hv.opts.Bars(show_grid=True),
    hv.opts.Curve(show_grid=True),
    hv.opts.Scatter(show_grid=True),
)
