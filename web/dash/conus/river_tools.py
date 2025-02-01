from dash import html, dcc
import dash_bootstrap_components as dbc

from dash import html
from dash import dash_table

import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import sqlite3
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

from config import base_url, graph_config, tabtitle_style, tabtitle_selected_style, popup_ts_style

    
# flow monitor/forecast figure
def draw_mofor_river_db(rivid):
    
    df_system_status = pd.read_csv(f'{base_url}/data/system_status.csv', parse_dates=True)
    
    moni_t2 = datetime.fromisoformat(df_system_status['WRF-Hydro NRT'][1]).date()
    if moni_t2.month>=10:
        moni_t1 = date(moni_t2.year, 1, 1)
    else:
        moni_t1 = date(moni_t2.year-1, 10, 1)
    fcst_t1 = datetime.fromisoformat(df_system_status['WWRF Fcst'][0]).date()
    fcst_t2 = datetime.fromisoformat(df_system_status['WWRF Fcst'][1]).date()
    
    if rivid != '':
        fig_mofor = go.Figure()
        
        fillcolors = ['sienna', 'orange', 'yellow', 'lightgreen', 'lightcyan', 'lightblue', 'mediumpurple']
        fillcolors.reverse()
        clim_t2 = fcst_t2 if fcst_t2>moni_t2 else moni_t2
        clim_t2 += timedelta(days=15)
        if clim_t2.month>=10:
            clim_t1 = date(clim_t2.year, 1, 1)
        else:
            clim_t1 = date(clim_t2.year-1, 10, 1)
        for i,pctl in enumerate([95, 90, 80, 50, 20, 10, 5]):
            fdb = f'{base_url}/data/nrt/rivers/CHRTOUT_{clim_t1:%Y%m}-{clim_t2:%Y%m}.daily.pctl{pctl:02d}.db'
            #print(fdb)
            conn = sqlite3.connect(fdb)
            df = pd.read_sql_query(f'SELECT * FROM streamflow WHERE [index]={rivid}', conn).T
            conn.close()
            df.drop(index=df.index[0], axis=0, inplace=True)
            num = df._get_numeric_data(); num[num<0] = 0
            df.rename(columns={0: 'Flow'}, inplace=True)
            tsname = f'  {pctl:d}<sup>th</sup>' if pctl<10 else f'{pctl:d}<sup>th</sup>'
            fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Flow'], name=tsname, line=dict(color=fillcolors[i]), fill='tozeroy', mode='lines'))
        
        fdb = f'{base_url}/data/nrt/rivers/CHRTOUT_{moni_t1:%Y%m}-{moni_t2:%Y%m}.daily.db'#; print(fdb)
        conn = sqlite3.connect(fdb)
        df = pd.read_sql_query(f'SELECT * FROM streamflow WHERE [index]={rivid}', conn).T
        conn.close()
        df.drop(index=df.index[0], axis=0, inplace=True)
        num = df._get_numeric_data(); num[num<0] = 0
        df.rename(columns={0: 'Flow'}, inplace=True)
        #fig_mofor.add_trace(go.Scatter(x=df['Date'], y=df['Flow'], name='Monitor', line=dict(color='blue'), mode='lines+markers'))
        df2 = df.tail(1)
        
        fdb = f'{base_url}/data/nrt/rivers/CHRTOUT_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.daily.db'#; print(fdb)
        conn = sqlite3.connect(fdb)
        dff = pd.read_sql_query(f'SELECT * FROM streamflow WHERE [index]={rivid}', conn).T
        conn.close()
        dff.drop(index=dff.index[0], axis=0, inplace=True)
        num = dff._get_numeric_data(); num[num<0] = 0
        dff.rename(columns={0: 'Flow'}, inplace=True)
        dff = pd.concat([df2, dff])#.reset_index(drop = True)
        fig_mofor.add_trace(go.Scatter(x=dff.index, y=dff['Flow'], name='Forecast', line=dict(color='magenta'), mode='lines+markers'))
        fig_mofor.add_trace(go.Scatter(x=df.index, y=df['Flow'], name='Monitor', line=dict(color='blue'), mode='lines+markers'))
        xrange = [datetime.strptime(df.index[0],'%Y-%m-%d')-timedelta(days=5), datetime.strptime(dff.index[-1],'%Y-%m-%d')+timedelta(days=35)]        
    else:
        fig_mofor = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': 'Flow (m^3/s)'})
        xrange = [2018, 2023]
    fig_mofor.update_layout(margin=dict(l=15, r=15, t=15, b=5), plot_bgcolor='#eeeeee', hovermode='x unified',
                            legend=dict(title=''),
                            xaxis_title=f'Forecast Initiated on {fcst_t1:%b %-d, %Y}', xaxis_range=xrange,
                            yaxis_title='Model Estimated Flow (m<sup>3</sup>/s, <b>uncorrected</b>)')
    fig_mofor.update_traces(hovertemplate=None)
    return fig_mofor

# Reverse ESP plots
def draw_rev_esp(rivid):
    
    fcst_t1 = datetime(2024, 11, 20)
    fcst_t2 = datetime(2024, 11, 27)
    
    if rivid != '':
        fig_rev_esp = go.Figure()
        
        fillcolors = ['sienna', 'orange', 'yellow', 'lightgreen', 'lightcyan', 'lightblue', 'mediumpurple']
        fillcolors.reverse()
        clim_t2 = fcst_t2
        clim_t2 += timedelta(days=15)
        if clim_t2.month>=10:
            clim_t1 = date(clim_t2.year, 1, 1)
        else:
            clim_t1 = date(clim_t2.year-1, 10, 1)
        for i,pctl in enumerate([95, 90, 80, 50, 20, 10, 5]):
            fdb = f'{base_url}/data/nrt/rivers/CHRTOUT_{clim_t1:%Y%m}-{clim_t2:%Y%m}.daily.pctl{pctl:02d}.db'
            #print(fdb)
            conn = sqlite3.connect(fdb)
            df = pd.read_sql_query(f'SELECT * FROM streamflow WHERE [index]={rivid}', conn).T
            conn.close()
            df.drop(index=df.index[0], axis=0, inplace=True)
            num = df._get_numeric_data(); num[num<0] = 0
            df.rename(columns={0: 'Flow'}, inplace=True)
            tsname = f'  {pctl:d}<sup>th</sup>' if pctl<10 else f'{pctl:d}<sup>th</sup>'
            fig_rev_esp.add_trace(go.Scatter(x=df.index, y=df['Flow'], name=tsname, line=dict(color=fillcolors[i]), fill='tozeroy', mode='lines'))

        for e in range(1, 47):
            fdb = f'{base_url}/data/fcst/rev_esp/{e:02d}/CHRTOUT_{fcst_t1:%Y%m%d}-{fcst_t2:%Y%m%d}.daily.db'
            conn = sqlite3.connect(fdb)
            dff = pd.read_sql_query(f'SELECT * FROM streamflow WHERE [index]={rivid}', conn).T
            dff.drop(index=dff.index[0], axis=0, inplace=True)
            conn.close()
            num = dff._get_numeric_data(); num[num<0] = 0
            dff.rename(columns={0: 'Flow'}, inplace=True)
            if e<46:
                lcolor = 'darkgray'
            else:
                lcolor = 'blue'
            fig_rev_esp.add_trace(go.Scatter(x=dff.index, y=dff['Flow'], name=f'Ens {e:02d}', line=dict(color=lcolor), mode='lines+markers'))
            
        xrange = [datetime.strptime(dff.index[0],'%Y-%m-%d')-timedelta(days=35), datetime.strptime(dff.index[-1],'%Y-%m-%d')+timedelta(days=35)]        
    else:
        fig_rev_esp = px.line(x=[2018, 2023], y=[0, 0], labels={'x': 'Data not available.', 'y': 'Flow (m^3/s)'})
        xrange = [2018, 2023]
    fig_rev_esp.update_layout(margin=dict(l=15, r=15, t=15, b=5), plot_bgcolor='#eeeeee', hovermode='x unified',
                            legend=dict(title=''),
                            xaxis_title=f'Reverse ESP Experiment Initiated on {fcst_t1:%b %-d, %Y}', xaxis_range=xrange,
                            yaxis_title='Model Estimated Flow (m<sup>3</sup>/s, <b>uncorrected</b>)')
    fig_rev_esp.update_traces(hovertemplate=None)
    return fig_rev_esp

def get_river_tools():
    
    fig_mofor_river = draw_mofor_river_db('342455')
    fig_rev_esp     = draw_rev_esp('342455')

    graph_mofor_river = dcc.Graph(id='graph-mofor-river', figure=fig_mofor_river, style={'height': '360px'}, config=graph_config)
    graph_rev_esp     = dcc.Graph(id='graph-rev-esp',     figure=fig_rev_esp,     style={'height': '360px'}, config=graph_config)

    tab_mofor_river = dcc.Tab(label='Monitor/Forecast',value='mofor-river',  children=[dcc.Loading(id='loading-mofor-river', children=graph_mofor_river)], style=tabtitle_style, selected_style=tabtitle_selected_style)
    tab_rev_esp     = dcc.Tab(label='Reverse ESP Experiment',value='rev-esp', children=[dcc.Loading(id='loading-rev-esp',    children=graph_rev_esp)],    style=tabtitle_style, selected_style=tabtitle_selected_style)

    popup_tabs_river = dcc.Tabs([tab_mofor_river, tab_rev_esp], id='popup-tabs-river', value='mofor-river')

    popup_plots_river = dbc.Offcanvas(
        [popup_tabs_river],
        title='WRF-Hydro Forecast Point', placement='top', is_open=False, scrollable=True, id='popup-plots-river',
        style=popup_ts_style
    )
    
    return popup_plots_river
