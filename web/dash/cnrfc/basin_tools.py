from dash import html, dcc
import dash_bootstrap_components as dbc
from dash_extensions.javascript import Namespace, arrow_function

import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

from config import cloud_url, fnf_stations, fnf_id_names, graph_config, tool_style, tabtitle_style, tabtitle_selected_style, popup_ts_style, fig_ts_style

# start to build maps
ns = Namespace('dashExtensions', 'default')

# draw system status chart
def draw_system_status():
    df_system_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
    fig_system_status = go.Figure()
    i = 1
    for datastream,datatime in df_system_status.items():
        if datastream not in ['Forcing Retro', 'WRF-Hydro Retro', 'Current']:
            fig_system_status.add_trace(go.Scatter(x=datatime, y=[i, i], name=datastream, 
                text=[datastream+' '+datetime.fromisoformat(datatime[0]).strftime('%-m/%-d %HZ'), datastream+' '+datetime.fromisoformat(datatime[1]).strftime('%-m/%-d %HZ')],
                                                   textposition='top center', line=dict(width=12), mode='lines+text'))
            i += 1
        if datastream=='Current':
            fig_system_status.add_trace(go.Scatter(x=datatime, y=[0.5, i-1], name=datastream, text=['Status Update @ '+datetime.fromisoformat(datatime[1]).strftime('%-m/%-d %HZ'), ''], line=dict(dash='dash'), mode='lines+text'))
            i += 1
    fig_system_status.update_traces(hovertemplate='%{x}')
    fig_system_status.update_layout(title='System Status', showlegend=False, xaxis_range=[datetime.utcnow()-timedelta(days=12), datetime.utcnow()+timedelta(days=10)],
                                    margin=dict(l=15, r=15, t=35, b=5), plot_bgcolor='#eeeeee', title_font_size=15,
                                    legend=dict(yanchor='top', y=0.99, xanchor='right', x=0.99, title='Datastream'))
    fig_system_status.update_xaxes(dtick='D', tickformat='%-m/%-d')
    fig_system_status.update_yaxes(visible=False)
    return fig_system_status

# draw basin average time series
def draw_basin_ts(staid, ptype):
    if staid in fnf_stations:
        fcsv = f'{cloud_url}/data/cnrfc/{ptype}/averaged/{staid}_daily.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date')
        fig_nrt = go.Figure()
        fig_nrt.add_trace(go.Bar(x=df.index, y=df['PREC'], name='Precipitation'))
        fig_nrt.add_trace(go.Scatter(x=df.index, y=df['T2D'], name='Air Temperature', mode='markers', line=go.scatter.Line(color='orange'), yaxis='y2'))
        fig_nrt.add_trace(go.Scatter(x=df.index, y=df['SWE'], name='Snow Water Equivalent', mode='lines', line=go.scatter.Line(color='magenta'), yaxis='y3'))
        fig_nrt.add_trace(go.Scatter(x=df.index, y=df['SMTOT']*100, name='Total Soil Moisture', mode='lines', line=go.scatter.Line(color='green'), yaxis='y4'))
        if ptype=='retro':
            xrange = [df.index[0].to_pydatetime()-timedelta(days=150), df.index[-1].to_pydatetime()+timedelta(days=150)]
        else:
            xrange = [df.index[0].to_pydatetime()-timedelta(days=10), df.index[-1].to_pydatetime()+timedelta(days=10)]
    else:
        fig_nrt = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': ''})
    fig_nrt.update_layout(margin=dict(l=15, r=15, t=15, b=5), xaxis_range=xrange,
                          plot_bgcolor='#eeeeee',
                          legend=dict(title='', bgcolor='rgba(255,255,255,0.7)', yanchor='top', y=0.99, xanchor='right', x=0.92),
                          hovermode='x unified',
xaxis=dict(domain=[0.07, 0.93]),
yaxis =dict(title=dict(text='Precipitation (mm/day)', font=dict(color='blue')), tickfont=dict(color='blue')),
yaxis2=dict(title=dict(text="Air Temperature (C)", font=dict(color='orange')), tickfont=dict(color='orange'), anchor='free', overlaying='y', side='left', position=0.03),
yaxis3=dict(title=dict(text="Snow Water Equivalent (mm)", font=dict(color='magenta')), tickfont=dict(color='magenta'), anchor='x', overlaying='y', side='right'),
yaxis4=dict(title=dict(text="Total Soil Moisture (%)", font=dict(color='green')), tickfont=dict(color='green'), anchor='free', overlaying='y', side='right', position=0.97),
                         ) #, font=dict(size=20))
    fig_nrt.update_traces(hovertemplate=None)
    return fig_nrt
    
def get_basin_tools():

    fig_system_status = draw_system_status()
    graph_system_status = dcc.Graph(id='graph-system_status', figure=fig_system_status, style={'height': '310px', 'padding-top': '10px'}, config=graph_config)

    # system status panel
    system_status_tab = html.Div([dcc.Loading(id='loading-system-status', children=graph_system_status)], style=tool_style)

    flow_instructions = 'Click on any blue dots (B-120 inflow points) and a pop-up window will show up with three tabs: (1) composite time series plots that include WRF-Hydro NRT simulations (before and after bias corrections), Full Natural Flow (FNF), and ensemble forecasts with exceedance levels, (2) ensemble forecast data table, (3) retrospective WRF-Hydro simulations and FNF.'
    snow_instructions = 'Click on the Map Layer Selection button on the upper right corner of the map region, then turn off the "Data" and "B-120 Basins" layers. Now you can click on any brown dots (Snow Courses) or orange dots (Snow Pillows) and a pup-up window will show up with two tabs in it: (1) the first tab compares WRF-Hydro NRT simulations to the site observations and (2) the second tab compares WRF-Hydro Retrospective simulations to the observations. NOTE: some sites too far away from B120 watersheds may not include model results.'
    sm_instructions   = 'Click on any basin polygon in the map region, and a pup-up window will appear with two tabs in it: (1) the first tab shows basin averaged time series of WRF-Hydro NRT simulations (Snow Water Equivalent and Total Soil Moisture) as well as forcing inputs (Precipitation and Temperature), and (2) the second tab shows the same basin averaged time series but for Retrospective WRF-Hydro simulations and forcing inputs. Caution: the Retrospective tab/figure contains a large amount of data and thus slow to load - a bit of patience needed.'
    
    basin_tools = html.Div(dcc.Tabs([
        dcc.Tab(html.Div([flow_instructions], style=tool_style), label='Streamflow',    value='tab-flow', style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab(html.Div([snow_instructions], style=tool_style), label='Snowpack',      value='tab-snow', style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab(html.Div([sm_instructions],   style=tool_style), label='Soil Moisture', value='tab-sm',   style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab([system_status_tab],  label='System Status', value='tab-status', style=tabtitle_style, selected_style=tabtitle_selected_style),
    ], value='tab-status'))

    ## pop-up window and its tabs/graphs
    staid0     = 'FTO'    
    fig_nrt    = draw_basin_ts(staid0, 'nrt')
    fig_retro  = draw_basin_ts(staid0, 'retro')
    graph_nrt  = dcc.Graph(id='basin-graph-nrt',   figure=fig_nrt,   style=fig_ts_style, config=graph_config)
    graph_retro= dcc.Graph(id='basin-graph-retro', figure=fig_retro, style=fig_ts_style, config=graph_config)

    tab_nrt   = dcc.Tab(label='NRT Monitor',  value='basin-nrt',   children=[dcc.Loading(id='loading-basin-nrt',  children=graph_nrt)],   style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_retro = dcc.Tab(label='Retrospective',value='basin-retro', children=[dcc.Loading(id='loading-basin-retro', children=graph_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs = dcc.Tabs([tab_nrt, tab_retro], id='basin-popup-tabs', value='basin-nrt')
    basin_popup_plots = dbc.Offcanvas([popup_tabs],
        title='B-120 Basin', placement='top', is_open=False, scrollable=True, id='basin-popup-plots', style=popup_ts_style
    )

    return basin_tools, basin_popup_plots
