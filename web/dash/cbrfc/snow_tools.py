from dash import html, dcc
import dash_bootstrap_components as dbc
from dash_extensions.javascript import Namespace, arrow_function

import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

from config import base_url, snow_course_stations, snow_pillow_stations, graph_config, tool_style, tabtitle_style, tabtitle_selected_style, popup_ts_style

# start to build maps
ns = Namespace('dashExtensions', 'default')


# draw snow course time series
def draw_course(staid, ptype):
    if staid in snow_course_stations:
        fcsv = f'{base_url}/data/cdec/snow_course/SWE_monthly_{staid}.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date')
        fcsv2 = f'{base_url}/data/{ptype}/sites/{staid}.csv'
        df2 = pd.read_csv(fcsv2, parse_dates=True, index_col='Date')
        fig_course = go.Figure()
        fig_course.add_trace(go.Scatter(x=df.index, y=df['SWE'], name='Snow Course SWE', mode='lines+markers', line=go.scatter.Line(color='black')))
        fig_course.add_trace(go.Scatter(x=df2.index, y=df2['SWE']/25.4, name='WRF-Hydro SWE', mode='lines', line=go.scatter.Line(color='magenta')))
        xrange = [df2.index[0].to_pydatetime()-timedelta(days=15), df2.index[-1].to_pydatetime()+timedelta(days=15)]
    else:
        fig_course = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': ''})
    fig_course.update_layout(margin=dict(l=15, r=15, t=15, b=5), xaxis_range=xrange,
                          plot_bgcolor='#eeeeee',
                          legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.99),
                          hovermode='x unified') #, font=dict(size=20))
    fig_course.update_yaxes(title='Snow Water Equivalent (in)')
    fig_course.update_traces(hovertemplate=None)
    return fig_course

# draw snow pillow time series
def draw_pillow(staid, ptype):
    if staid in snow_pillow_stations:
        fcsv = f'{base_url}/data/cdec/snow_pillow/SWE_daily_{staid}.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date')
        df.drop(df[(df['SWE']<-10)|(df['SWE']>200)].index, inplace=True)
        fcsv2 = f'{base_url}/data/{ptype}/sites/{staid}.csv'
        df2 = pd.read_csv(fcsv2, parse_dates=True, index_col='Date')
        fig_course = go.Figure()
        fig_course.add_trace(go.Scatter(x=df.index, y=df['SWE'], name='Snow Pillow SWE', mode='lines', line=go.scatter.Line(color='black')))
        fig_course.add_trace(go.Scatter(x=df2.index, y=df2['SWE']/25.4, name='WRF-Hydro SWE', mode='lines', line=go.scatter.Line(color='magenta')))
        xrange = [df2.index[0].to_pydatetime()-timedelta(days=15), df2.index[-1].to_pydatetime()+timedelta(days=15)]
    else:
        fig_course = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': ''})
    fig_course.update_layout(margin=dict(l=15, r=15, t=15, b=5), xaxis_range=xrange,
                          plot_bgcolor='#eeeeee',
                          legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.99),
                          hovermode='x unified') #, font=dict(size=20))
    fig_course.update_yaxes(title='Snow Water Equivalent (in)')
    fig_course.update_traces(hovertemplate=None)
    return fig_course

    
def get_snow_tools():

    ## pop-up window and its tabs/graphs
    fig_course_nrt   = draw_course('GRZ', 'nrt')
    fig_course_retro = draw_course('GRZ', 'retro')
    fig_pillow_nrt   = draw_pillow('RTL', 'nrt')
    fig_pillow_retro = draw_pillow('RTL', 'retro')
    graph_course_nrt   = dcc.Graph(id='snow-graph-course-nrt',   figure=fig_course_nrt,   style={'height': '360px'}, config=graph_config)
    graph_course_retro = dcc.Graph(id='snow-graph-course-retro', figure=fig_course_retro, style={'height': '360px'}, config=graph_config)
    graph_pillow_nrt   = dcc.Graph(id='snow-graph-pillow-nrt',   figure=fig_pillow_nrt,   style={'height': '360px'}, config=graph_config)
    graph_pillow_retro = dcc.Graph(id='snow-graph-pillow-retro', figure=fig_pillow_retro, style={'height': '360px'}, config=graph_config)

    tab_course_nrt   = dcc.Tab(label='Snow Course + WRF-Hydro NRT',           value='snow-course-nrt',   children=[dcc.Loading(id='loading-snow-course-nrt',   children=graph_course_nrt)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_course_retro = dcc.Tab(label='Snow Course + WRF-Hydro Retrospective', value='snow-course-retro', children=[dcc.Loading(id='loading-snow-course-retro', children=graph_course_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_pillow_nrt   = dcc.Tab(label='Snow Pillow + WRF-Hydro NRT',           value='snow-pillow-nrt',   children=[dcc.Loading(id='loading-snow-pillow-nrt',   children=graph_pillow_nrt)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_pillow_retro = dcc.Tab(label='Snow Pillow + WRF-Hydro Retrospective', value='snow-pillow-retro', children=[dcc.Loading(id='loading-snow-pillow-retro', children=graph_pillow_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs_course = dcc.Tabs([tab_course_nrt, tab_course_retro], id='course-popup-tabs', value='snow-course-nrt')
    popup_tabs_pillow = dcc.Tabs([tab_pillow_nrt, tab_pillow_retro], id='pillow-popup-tabs', value='snow-pillow-nrt')
    
    course_popup_plots = dbc.Offcanvas([popup_tabs_course],
        title='Snow Courses', placement='top', is_open=False, scrollable=True, id='course-popup-plots', style=popup_ts_style
    )
    pillow_popup_plots = dbc.Offcanvas([popup_tabs_pillow],
        title='Snow Pillows', placement='top', is_open=False, scrollable=True, id='pillow-popup-plots', style=popup_ts_style
    )

    return course_popup_plots, pillow_popup_plots
