from dash import html, dcc
import dash_bootstrap_components as dbc

from dash import html
from dash import dash_table

import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from glob import glob
import os

from config import cloud_url, fnf_stations, graph_config, tabtitle_style, tabtitle_selected_style, popup_ts_style

# flow retro figure
def draw_retro(staid):
    if staid in fnf_stations:
        fcsv = f'{cloud_url}/data/cbrfc/retro/combined/{staid}_monthly.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date')
        fig_retro = px.line(df, labels={'Date': '', 'value': 'Flow (kaf/mon)'})
        fig_retro = go.Figure()
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['FNF'],    name='Full Natural Flow', mode='lines+markers', line=go.scatter.Line(color='black', dash='dot')))
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qsim'],   name='Model-Simulated',   mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Plotly[0])))
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qsimbc'], name='CDF-matched',    mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Prism[7])))
    else:
        fig_retro = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': 'Flow (kaf/mon)'})
    fig_retro.update_layout(margin=dict(l=15, r=15, t=15, b=5),
                            plot_bgcolor='#eeeeee',
                            legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.99),
                            hovermode='x unified') #, font=dict(size=20))
    fig_retro.update_xaxes(range=['1979-10-01', '2024-09-30'])
    fig_retro.update_yaxes(title='Flow (kaf/mon)')
    fig_retro.update_traces(hovertemplate=None)
    return fig_retro
    
def get_site_tools():

    staid0     = '09236000'

    ## pop-up window and its tabs/graphs/tables

    fig_retro = draw_retro(staid0)

    graph_retro = dcc.Graph(id='graph-retro', figure=fig_retro, style={'height': '360px'}, config=graph_config)

    tab_retro = dcc.Tab(label='Retrospective',   value='retro', children=[dcc.Loading(id='loading-retro', children=graph_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs = dcc.Tabs([tab_retro], id='popup-tabs', value='retro')

    popup_plots = dbc.Offcanvas([popup_tabs],
        title='FNF Stations', placement='top', is_open=False, scrollable=True, id='popup-plots', style=popup_ts_style
    )

    return popup_plots


