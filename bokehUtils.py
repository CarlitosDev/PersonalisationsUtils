import pandas as pd
import numpy as np
import os

#from bkcharts import Chord
from bkcharts import Chord, Donut
from bokeh.io import show, output_file, export_png


def renderChordChart(df, dfSource, dfTarget, dfValue, dfTitle=''):
    chord_from_df = Chord(df, 
        source=dfSource, target=dfTarget, value=dfValue, 
        title=dfTitle)
    chord_from_df.plot_height = 1600
    chord_from_df.plot_width  = 1600
    output_file('tempChordChart.html')
    show(chord_from_df)
    return chord_from_df;

def renderDonutChart(df, labels, values, textFontSize, hoverText, dfTitle=''):
    donutChart = Donut(df, label=labels, values=values,
        text_font_size=textFontSize, hover_text=hoverText,
        title=dfTitle)
    donutChart.plot_height = 1600
    donutChart.plot_width  = 1600
    output_file('tempDonutChart.html')
    show(donutChart)
    return donutChart;


def exportFigure(bkFig, figPath='tester.png'):
    export_png(bkFig, figPath)


