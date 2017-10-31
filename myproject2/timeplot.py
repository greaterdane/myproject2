import numpy as np
import pandas as pd
import bokeh.palettes as colors
from bokeh.plotting import figure
from bokeh.layouts import row
from bokeh.models import ColumnDataSource, Circle, DatetimeTickFormatter, HoverTool, Legend, LinearAxis, Range1d
#from bokeh.embed import components

def timelineplot(numeric_data):
    fields = numeric_data.drop(['date', 'formadv_id'], axis = 1).columns
    fieldmap = dict(zip(fields, NUMERICDISPLAY))
    linecolors = dict(zip(fields, colors.Set1[4]))
    circlecolors = dict(zip(fields, colors.Pastel1[4]))
    
    y2_top = numeric_data[fields[1:]]\
        .stack().groupby(level = 1)\
        .agg('max').max()

    if y2_top == 'n/a':
        y2_top = 0
    
    fig = figure(x_axis_label = "Filing Date",
        title = "Investment Adviser Timeline",
        width = 1500,
        height = 800,
        toolbar_location="above",
        tools = "save, zoom_in, zoom_out, reset, pan")
        
    fig.extra_y_ranges = {"y2": Range1d(start = 0, end = y2_top + int(y2_top / 7))}
    fig.add_layout(LinearAxis(y_range_name= "y2"), 'right')
    data = {field : numeric_data[field].values for field in fields}
    data.update({'date' : numeric_data.date,})
    source = ColumnDataSource(data)
    legenditems = []
    
    for field in fields:
        kwds = dict(x = 'date', y = field, source = source)
        
        if 'number' in field:
            kwds['y_range_name'] = "y2"
        else:
            kwds.pop('y_range_name', '')
    
        l = fig.line(line_color = linecolors[field],
            line_width = 1.6,
            **kwds)
    
        c = fig.circle(line_width = 2,
            size = 8,
            fill_color = circlecolors[field],
            name = field,
            **kwds)
    
        valfmt = "@%s" % field
        if 'assets' in field:
            valfmt = "$@{assetsundermgmt}{0,0.00}"
    
        htool = HoverTool(
            tooltips = [
                ("Date: ", "@date{%F}"),
                ("%s: " % fieldmap[field], valfmt,)
                    ],
            renderers = [c],
            formatters = {
                'date' : 'datetime',
                'assetsundermanagement' : 'numeral'
                    },
            name = field,
            mode = 'vline')
    
        fig.add_tools(htool)
        legenditems.append((fieldmap[field], [l, c],))
    
    fig.legend.click_policy = "mute"
    fig.xaxis.formatter = DatetimeTickFormatter(years = ['%F'], months = ["%F"], days = ["%F"])
    fig.xaxis.axis_label_text_font_style = 'bold'
    fig.left[0].formatter.use_scientific = False
    legend = Legend(items = legenditems, location=(0.3, -25))
    fig.add_layout(legend, 'left')
    return fig
