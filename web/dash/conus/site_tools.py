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

from config import base_url, cloud_url, usgs_gages, graph_config, tabtitle_style, tabtitle_selected_style, popup_ts_style

# flow retro figure
def draw_retro(staid):
    sites_per_file = 20
    if staid in usgs_gages:
        ind = [i for i, value in enumerate(usgs_gages) if value == staid][0]
        fileno = '%03d' % (int(ind/sites_per_file))
        fcsv = f'{cloud_url}/data/conus/retro/combined/{fileno}_daily.csv.gz' #; print(fcsv)
        df_all = pd.read_csv(fcsv, parse_dates=True, compression='gzip', index_col='Date', dtype={'gage_id': str})
        df = df_all[df_all['gage_id']==staid]
        df.loc[df['Qsim'] < 0, 'Qsim'] = np.nan
        fig_retro = go.Figure()
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qobs'],   name='USGS Flow', mode='lines+markers', line=go.scatter.Line(color='black', dash='dot')))
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qsim'],   name='Model-Simulated',   mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Plotly[0])))
    else:
        fig_retro = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': 'Flow (cfs)'})
    fig_retro.update_layout(margin=dict(l=15, r=15, t=15, b=5),
                            plot_bgcolor='#eeeeee',
                            legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.99),
                            hovermode='x unified') #, font=dict(size=20))
    fig_retro.update_xaxes(range=['1979-10-01', '2024-09-30'])
    fig_retro.update_yaxes(title='Flow (cfs)')
    fig_retro.update_traces(hovertemplate=None)
    return fig_retro
    
# flow monitor/forecast figure
def draw_mofor(staid, fcst_type, fcst_t1, fcst_t2, fcst_update):
    #nens = len(glob(f'{base_url}/data/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/??'))
    nens = 45
    if staid in fnf_stations:
        fcsv = f'{base_url}/data/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date', usecols = ['Date']+['Ens%02d' % (i+1) for i in range(nens)]+['Avg', 'Exc50', 'Exc90', 'Exc10'])
        if fcst_t2.month>=7:
            df.drop(index=df.index[-1], axis=0, inplace=True)
        fcsv2 = f'{base_url}/data/nrt/combined/{staid}_monthly.csv'
        df2 = pd.read_csv(fcsv2, parse_dates=True, index_col='Date', usecols=['Date', 'FNF', 'Qsim', 'Qmatch'])
        fig_mofor = go.Figure()
        for e in range(1, nens+1):
            fig_mofor.add_trace(go.Scatter(x=df.index, y=df[f'Ens{e:02d}'], name=f'Ens{e:02d}', mode='lines+markers', line=go.scatter.Line(color='lightgray'), showlegend=False))
        fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Avg'],   name='Historical Average', mode='lines+markers', line=go.scatter.Line(color='black', width=3, dash='dash')))
        fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Exc50'], name='50% Exceedance', mode='lines+markers', line=go.scatter.Line(color=px.colors.qualitative.Plotly[2], width=4)))
        fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Exc90'], name='90% Exceedance', mode='lines+markers', line=go.scatter.Line(color=px.colors.qualitative.Plotly[4], width=2)))
        fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Exc10'], name='10% Exceedance', mode='lines+markers', line=go.scatter.Line(color=px.colors.qualitative.Plotly[3], width=2)))
        #linecolors = {'Ens%02d' % (i+1): 'lightgray' for i in range(42)}
        #linecolors.update({'Avg': 'black', 'Exc50': 'green', 'Exc90': 'red', 'Exc10': 'blue'})
        fig_mofor.add_trace(go.Scatter(x=df2.index, y=df2['FNF'], name='Full Natural Flow', mode='markers', marker=dict(symbol='square', size=12, color='black')))#, visible='legendonly'))
        fig_mofor.add_trace(go.Scatter(x=df2.index, y=df2['Qsim'], name='Model-Simulated',   mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Plotly[0])))#, visible='legendonly'))
        fig_mofor.add_trace(go.Scatter(x=df2.index, y=df2['Qmatch'], name='CDF-matched', mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Prism[7])))#, visible='legendonly'))
    else:
        fig_mofor = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': 'Flow (kaf/mon)'})
    fig_mofor.update_layout(margin=dict(l=15, r=15, t=15, b=5), plot_bgcolor='#eeeeee',
                            legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.99), hovermode='x unified')
    fig_mofor.update_yaxes(title='Flow (kaf/mon)')
    fig_mofor.update_traces(hovertemplate=None)
    return fig_mofor
    

def get_site_tools():

    df_system_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
    
    staid0     = '11460000'

    ## pop-up window and its tabs/graphs/tables

    fig_retro = draw_retro(staid0)
    #fig_mofor = draw_mofor(staid0, fcst_type0, fcst_t1, fcst_t2, tup_latest)

    graph_retro = dcc.Graph(id='graph-retro', figure=fig_retro, style={'height': '360px'}, config=graph_config)
    #graph_mofor = dcc.Graph(id='graph-mofor', figure=fig_mofor, style={'height': '360px'}, config=graph_config)

    tab_retro = dcc.Tab(label='Retrospective',   value='retro', children=[dcc.Loading(id='loading-retro', children=graph_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    #tab_mofor = dcc.Tab(label='NRT Monitor/Forecast',value='mofor', children=[dcc.Loading(id='loading-mofor', children=graph_mofor)], style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs = dcc.Tabs([tab_retro], id='popup-tabs', value='retro')

    popup_plots = dbc.Offcanvas([popup_tabs],
        title='USGS Gage', placement='top', is_open=False, scrollable=True, id='popup-plots', style=popup_ts_style
    )

    return popup_plots


