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
import os, json

from config import cloud_url, domain_config, fnf_stations, fnf_id_names, graph_config, tabtitle_style, tabtitle_selected_style, popup_ts_style, fig_ts_style, geojson_basins, geojson_points

# flow retro figure
def draw_retro(staid):
    if staid in fnf_stations:
        fcsv = f'{cloud_url}/data/cnrfc/retro/combined/{staid}_monthly.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date')
        fig_retro = px.line(df, labels={'Date': '', 'value': 'Flow (kaf/mon)'})
        fig_retro = go.Figure()
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['FNF'],    name='Full Natural Flow', mode='lines+markers', line=go.scatter.Line(color='black', dash='dot')))
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qsim'],   name='Model-Simulated',   mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Plotly[0])))
        fig_retro.add_trace(go.Scatter(x=df.index, y=df['Qmatch'], name='CDF-matched',    mode='lines', line=go.scatter.Line(color=px.colors.qualitative.Prism[7])))
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
    
# flow monitor/forecast figure
def draw_mofor(staid, fcst_type, fcst_t1, fcst_t2, fcst_update):
    nens = 45
    if staid in fnf_stations:
        fcsv = f'{cloud_url}/data/cnrfc/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=True, index_col='Date', usecols = ['Date']+['Ens%02d' % (i+1) for i in range(nens)]+['Avg', 'Exc50', 'Exc90', 'Exc10'])
        if fcst_t2.month>=7:
            df.drop(index=df.index[-1], axis=0, inplace=True)
        fcsv2 = f'{cloud_url}/data/cnrfc/nrt/combined/{staid}_monthly.csv'
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
    

# forecast table
def draw_table(staid, staname, fcst_type, fcst_t1, fcst_t2, fcst_update):
    cols = ['Date', 'Exc50', 'Pav50', 'Exc90', 'Pav90', 'Exc10', 'Pav10', 'Avg']
    if staid in fnf_stations:
        fcsv = f'{cloud_url}/data/cnrfc/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=False, usecols=cols)
        df = df[cols]
        cols.remove('Date')
        df[cols] = np.rint(df[cols])
        df['Date'] = [ datetime.strptime(m, '%Y-%m-%d').strftime('%B %Y') for m in df['Date'] ]
        if fcst_t2.month>=7:
            df.iloc[-1, 0] = df.iloc[-1, 0].replace('July', 'April-July total')
    else:
        fcsv = f'{cloud_url}/data/cnrfc/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=False, usecols=cols)
        df = df[cols]
        df.drop(df.index, inplace=True)

    table_fcst = dash_table.DataTable(id='fcst-table',
                     #columns=[{'name': i, 'id': i} for i in df.columns],
                     columns=[{'name': [staname, 'Month'], 'id': 'Date'},
                              {'name': ['50%', 'KAF'], 'id': 'Exc50'}, {'name': ['50%', '%AVG'], 'id': 'Pav50'},
                              {'name': ['90%', 'KAF'], 'id': 'Exc90'}, {'name': ['90%', '%AVG'], 'id': 'Pav90'},
                              {'name': ['10%', 'KAF'], 'id': 'Exc10'}, {'name': ['10%', '%AVG'], 'id': 'Pav10'},
                              {'name': ['AVG', 'KAF'], 'id': 'Avg'}
                              ],
                     data=df.to_dict('records'),
                     style_data={'whiteSpace': 'normal', 'width': '300px'},
                     style_header={'backgroundColor': 'lightyellow', 'fontWeight': 'bold', 'textAlign': 'center'},
                     style_table={'padding-bottom': '0px'},
                     export_format='xlsx',
                     export_headers='display',
                     merge_duplicate_headers=True,
                     )

    table_note = html.Div('  [Note] 50%, 90%, 10%: exceedance levels within the forecast ensemble. AVG: month of year average during 1979-2020. %AVG: percentage of AVG. KAF: kilo-acre-feet.', id='table-note', style={'font-size': 'small'})
    return [table_fcst, table_note]

# forecast tables over all FNF stations
def draw_table_all(fcst_type, fcst_t1, fcst_t2, fcst_update):
    cnt = 0
    for staid,staname in fnf_id_names.items():
        cols = ['Date', 'Exc50', 'Pav50', 'Exc90', 'Pav90', 'Exc10', 'Pav10', 'Avg']
        fcsv = f'{cloud_url}/data/cnrfc/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=False, usecols=cols)
        df = df[cols]
        cols.remove('Date')
        df[cols] = np.rint(df[cols])
        df['Date'] = [ datetime.strptime(m, '%Y-%m-%d').strftime('%B %Y') for m in df['Date'] ]
        if fcst_t2.month>=7:
            df.iloc[-1, 0] = df.iloc[-1, 0].replace('July', 'April-July total')
        df.loc[-1] = ['' if i>0 else staname for i in range(df.shape[1])]
        df.index = df.index + 1  # shifting index
        df.sort_index(inplace=True)
        if cnt==0:
            df_all = df
        else:
            df_all = pd.concat([df_all, df], ignore_index=True)
        cnt += 1
    #df_all.drop(df_all.tail(1).index, inplace=True)
    table_fcst = dash_table.DataTable(id='fcst-table',
                     #columns=[{'name': i, 'id': i} for i in df.columns],
                     columns=[
                              #{'name': ['', 'Station (%d in total)' % cnt], 'id': 'Station'},
                              {'name': ['Station', 'Month'], 'id': 'Date'},
                              {'name': ['50%', 'KAF'], 'id': 'Exc50'}, {'name': ['50%', '%AVG'], 'id': 'Pav50'},
                              {'name': ['90%', 'KAF'], 'id': 'Exc90'}, {'name': ['90%', '%AVG'], 'id': 'Pav90'},
                              {'name': ['10%', 'KAF'], 'id': 'Exc10'}, {'name': ['10%', '%AVG'], 'id': 'Pav10'},
                              {'name': ['AVG', 'KAF'], 'id': 'Avg'}
                              ],
                     data=df_all.to_dict('records'),
                     style_data={'whiteSpace': 'normal', 'maxWidth': '400px'},
                     style_cell={'whiteSpace': 'normal', 'minWidth': '100px'},
                     style_header={'backgroundColor': 'lightyellow', 'fontWeight': 'bold', 'textAlign': 'center'},
                     style_table={'padding-bottom': '0px'},
                     export_format='xlsx',
                     export_headers='display',
                     merge_duplicate_headers=True,
                     page_size=df.shape[0],
                     page_current=0,
                     )

    return [table_fcst]

def draw_map(fcst_type, fcst_t1, fcst_t2, fcst_update):
    cnt = 0
    for staid,staname in fnf_id_names.items():
        cols = ['Date', 'Exc50', 'Pav50', 'Exc90', 'Pav90', 'Exc10', 'Pav10', 'Avg']
        fcsv = f'{cloud_url}/data/cnrfc/fcst/init{fcst_t1:%Y%m%d}_update{fcst_update:%Y%m%d}/basins/{fcst_type}/{staid}_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.csv'
        df = pd.read_csv(fcsv, parse_dates=False, usecols=cols, dtype={'Date': str})
        df = df.tail(1)
        df = df.rename(columns={'Date': 'StationID'})
        df.iloc[-1, 0] = staid
        [lon, lat] = [ geojson_points['features'][i]['geometry']['coordinates'] for i in range(len(geojson_points['features'])) if geojson_points['features'][i]['properties']['Station_ID']==staid ][0]
        df['lat'] = [lat]
        df['lon'] = [lon]
        if cnt==0:
            df_all = df
        else:
            df_all = pd.concat([df_all, df], ignore_index=True)
        cnt += 1
    fig_b = px.choropleth_map(df_all, featureidkey='properties.Station', geojson=geojson_basins, locations='StationID', color='Pav50',
                           color_continuous_scale='BrBG', range_color=(0, 200),
                           map_style='carto-positron', opacity=0.7,
                           zoom=5.2, center = {'lat': domain_config['center'][0]+0.35, 'lon': domain_config['center'][1]-2},
                          )
    fig_b.update_traces(hovertemplate='%{location}: %{z:.0f}%')
    fig_b.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, coloraxis_colorbar=dict(title='Median April-to-July forecast<br>(% of historical aveverage)', titleside="right"))
    fig_p = px.scatter_map(df_all, lat='lat', lon='lon', color='Exc50', size='Exc50', range_color=(0, 2500), map_style='carto-positron', opacity=1,
                           hover_name='StationID', hover_data={'lat': False, 'lon': False}, labels={'Exc50': 'Median forecast volume'},
                           color_continuous_scale='plotly3', size_max=20, zoom=5.2, center = {'lat': domain_config['center'][0]+0.35, 'lon': domain_config['center'][1]-2})
    fig_p.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, coloraxis_colorbar=dict(title='Median April-to-July forecast<br>(KAF)', titleside="right"))
    return [fig_b, fig_p]

def get_site_tools():

    df_system_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
    
    fcst_t1 = datetime.fromisoformat(df_system_status['ESP-WWRF Fcst'][0]).date()
    fcst_t2 = datetime.fromisoformat(df_system_status['ESP-WWRF Fcst'][1]).date()
    #print(fcst_t1, fcst_t2)
    fcst_type0 = 'cdfm'
    staid0     = 'FTO'
    staname0   = 'Feather River at Oroville'

    # find all forecasts in the current year
    if fcst_t1.month>=10:
        tup1 = datetime(fcst_t1.year, 12, 1)
        tup2 = datetime(fcst_t1.year+1, 7, 1)
    else:
        tup1 = datetime(fcst_t1.year-1, 12, 1)
        tup2 = datetime(fcst_t1.year, 7, 1)
    df_esp_wwrf_updates = pd.read_csv(f'{cloud_url}/data/cnrfc/esp_wwrf_updates.csv', parse_dates=True)
    dt_updates = pd.to_datetime(df_esp_wwrf_updates['Date']).to_list()
    dt_updates.sort()
    tup_latest = dt_updates[-1]
    #print(dt_updates)

    ## pop-up window and its tabs/graphs/tables

    fig_retro = draw_retro(staid0)
    fig_mofor = draw_mofor(staid0, fcst_type0, fcst_t1, fcst_t2, tup_latest)

    table_fcst = draw_table(staid0, staname0, fcst_type0, fcst_t1, fcst_t2, tup_latest)
    
    [fig_b, fig_p] = draw_map(fcst_type0, fcst_t1, fcst_t2, tup_latest)

    graph_retro = dcc.Graph(id='graph-retro', figure=fig_retro, style=fig_ts_style, config=graph_config)
    graph_mofor = dcc.Graph(id='graph-mofor', figure=fig_mofor, style=fig_ts_style, config=graph_config)
    div_table = html.Div(id='div-table', children=table_fcst, style={'padding': '0px 50px 30px 50px', 'maxHeight': '470px', 'overflowY': 'scroll'})
    graph_mapb  = dcc.Graph(id='graph-mapb', figure=fig_b, style={'height': '460px', 'width': '500px', 'padding-top': '0px', 'padding-bottom': '0px', 'margin': 'auto'}, config=graph_config)
    graph_mapp  = dcc.Graph(id='graph-mapp', figure=fig_p, style={'height': '460px', 'width': '500px', 'padding-top': '0px', 'padding-bottom': '0px', 'margin': 'auto'}, config=graph_config)
    div_maps    = dbc.Row([dbc.Col(graph_mapb), dbc.Col(graph_mapp)], justify='center')

    tab_retro = dcc.Tab(label='Retrospective',   value='retro', children=[dcc.Loading(id='loading-retro', children=graph_retro)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_mofor = dcc.Tab(label='NRT Monitor/Forecast',value='mofor', children=[dcc.Loading(id='loading-mofor', children=graph_mofor)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_table = dcc.Tab(label='Forecast Table',  value='table', children=[dcc.Loading(id='loading-table', children=div_table)],   style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_map   = dcc.Tab(label='Forecast Map',  value='map', children=[dcc.Loading(id='loading-map', children=div_maps)],   style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs = dcc.Tabs([tab_mofor, tab_table, tab_map, tab_retro], id='popup-tabs', value='mofor')

    slider_updates = dcc.Slider(min=0, max=203, step=None,
        marks={(dt-tup1).days: dt.strftime('%-m/%-d') for dt in dt_updates},
        value=(tup_latest-tup1).days, #.timetuple().tm_yday,
        id='slider_updates'
    )

    slider_text  = html.Div('Forecast Updated on:', style={'display': 'inline-block', 'font-weight': 'bold', 'vertical-align': 'top'})
    slider_block = html.Div(slider_updates, style={'width': '70%', 'display': 'inline-block'})

    radio_pp = dcc.RadioItems(
       options=[
           {'label': ' CDF Match', 'value': 'cdfm'},
           {'label': ' LSTM',      'value': 'lstm'},
       ],
       value='lstm', labelStyle={'padding-right': 10}, id='radio_pp'
    )

    radio_text  = html.Div('Post-Processing: ', style={'display': 'inline-block', 'font-weight': 'bold', 'vertical-align': 'top', 'padding-right': 10})
    radio_block = html.Div(radio_pp, style={'vertical-align': 'top', 'display': 'inline-block'})

    popup_plots = dbc.Offcanvas([slider_text, slider_block, radio_text, radio_block, popup_tabs],
        title='B-120 Forecast Point', placement='top', is_open=False, scrollable=True, id='popup-plots', style=popup_ts_style
    )

    return popup_plots


