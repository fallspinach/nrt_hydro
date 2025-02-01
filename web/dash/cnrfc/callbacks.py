from main import app
from config import all_stations, fnf_stations

from dash.dependencies import ClientsideFunction, Input, Output, State
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from site_tools import draw_retro, draw_mofor, draw_table, draw_table_all, draw_map
from basin_tools import draw_basin_ts
from snow_tools import draw_course, draw_pillow
from river_tools import draw_mofor_river
from config import cloud_url
## Callbacks from here on

# callback to update data var in the title section
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_title_var'
    ),
    Output('title-var', 'children'),
    Input(component_id='data-sel',  component_property='value'),
    Input(component_id='met-vars',  component_property='value'),
    Input(component_id='hydro-vars', component_property='value')
)

# callback to update data date in the title section
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_title_date'
    ),
    Output('title-date', 'children'),
    Input('datepicker', 'date')
)

# callback to update url of image overlay
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_img_url'
    ),
    Output('data-img', 'url'),
    Input('datepicker', 'date'),
    Input(component_id='data-sel',  component_property='value'),
    Input(component_id='met-vars',  component_property='value'),
    Input(component_id='hydro-vars', component_property='value')
)

# callback to update url of color bar
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_cbar'
    ),
    Output('data-cbar-img', 'src'),
    Input(component_id='data-sel',  component_property='value'),
    Input(component_id='met-vars',  component_property='value'),
    Input(component_id='hydro-vars', component_property='value')
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_cbar_visibility'
    ),
    Output('data-cbar', 'style'),
    Input(component_id='data-map-ol', component_property='checked')
)

# callback to update datepicker and slider on button clicks
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='update_date'
    ),
    Output('datepicker', 'date'),
    Input('button-forward-day',  'n_clicks_timestamp'),
    Input('button-backward-day', 'n_clicks_timestamp'),
    Input('button-forward-month',   'n_clicks_timestamp'),
    Input('button-backward-month',  'n_clicks_timestamp'),
    Input('datepicker', 'date'),
    Input('datepicker', 'min_date_allowed'),
    Input('datepicker', 'max_date_allowed')
)

# update system status periodically
@app.callback(Output(component_id='datepicker', component_property='max_date_allowed'),
              Input('interval-check_system', 'n_intervals'))
def update_system_status(basin):
    df_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
    return datetime.fromisoformat(df_status['WRF-Hydro NRT'][1]).date()

# callback to switch river vector sources according to zoom level
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='switch_river_vector'
    ),
    Output('nwm-rivers', 'url'),
    Output('nwm-rivers', 'zoomToBoundsOnClick'),
    Input('map-region', 'zoom')
)

# create/update historic time series graph in popup
@app.callback(Output(component_id='graph-retro', component_property='figure'),
              Output(component_id='graph-mofor', component_property='figure'),
              Output(component_id='div-table', component_property='children'),
              Output(component_id='graph-mapb', component_property='figure'),
              Output(component_id='graph-mapp', component_property='figure'),
              Output('popup-plots', 'is_open'),
              Output('popup-plots', 'title'),
              Input('b120-points', 'clickData'),
              Input('slider_updates', 'value'),
              Input('radio_pp', 'value'))
def update_flows(fcst_point, yday_update, pp):
    df_system_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
    fcst_t1 = datetime.fromisoformat(df_system_status['ESP-WWRF Fcst'][0]).date()
    fcst_t2 = datetime.fromisoformat(df_system_status['ESP-WWRF Fcst'][1]).date()
    if fcst_t1.month>=10:
        fcst_update = datetime(fcst_t1.year, 12, 1) + timedelta(days=yday_update)
    else:
        fcst_update = datetime(fcst_t1.year-1, 12, 1) + timedelta(days=yday_update)
    # re-derive fcst_t1 and fcst_t2 from fcst_update
    fcst_t1 = datetime(fcst_update.year, fcst_update.month, 1)
    fcst_t2 = fcst_t1 + relativedelta(months=6) - timedelta(days=1)
    if fcst_t1.month==1:
        fcst_t2 = fcst_t1 + relativedelta(months=7) - timedelta(days=1)
    elif fcst_t1.month==12:
        fcst_t2 = fcst_t1 + relativedelta(months=8) - timedelta(days=1)
    if fcst_point==None:
        staid = 'FTO'
        stain = 'FTO: Feather River at Oroville'
    else:
        staid = fcst_point['properties']['Station_ID']
        stain = fcst_point['properties']['tooltip']
    fcst_type = f'{pp}'
    fig_retro = draw_retro(staid)
    fig_mofor = draw_mofor(staid, fcst_type, fcst_t1, fcst_t2, fcst_update)
    if staid!='TNL':
        table_fcst = draw_table(staid, all_stations[staid], fcst_type, fcst_t1, fcst_t2, fcst_update)
    else:
        table_fcst = draw_table_all(fcst_type, fcst_t1, fcst_t2, fcst_update)
    [fig_mapb, fig_mapp] = draw_map(fcst_type, fcst_t1, fcst_t2, fcst_update)
    
    return [fig_retro, fig_mofor, table_fcst, fig_mapb, fig_mapp, True, stain]

# create/update historic time series graph in popup
@app.callback(Output(component_id='basin-graph-nrt',   component_property='figure'),
              Output(component_id='basin-graph-retro', component_property='figure'),
              Output('basin-popup-plots', 'is_open'),
              Output('basin-popup-plots', 'title'),
              Input('b120-watersheds', 'clickData'))
def update_basin(basin):
    if basin==None:
        staid = 'FTO'
        stain = 'FTO: Feather River at Oroville'
    else:
        staid = basin['properties']['Station']
        stain = basin['properties']['tooltip']
    fig_nrt   = draw_basin_ts(staid, 'nrt')
    fig_retro = draw_basin_ts(staid, 'retro')
    
    return [fig_nrt, fig_retro, True, stain]

# create/update snow course time series graph in popup
@app.callback(Output(component_id='snow-graph-course-nrt',   component_property='figure'),
              Output(component_id='snow-graph-course-retro', component_property='figure'),
              Output('course-popup-plots', 'is_open'),
              Output('course-popup-plots', 'title'),
              Input('b120-courses', 'clickData'))
def update_course(site):
    if site==None:
        staid = 'GRZ'
        stain = 'StationID: GRZ, Name: Grizzly Ridge, Elevation: 6900ft, Basin: Feather River'
    else:
        staid = site['properties']['STA']
        stain = site['properties']['tooltip']
    fig_course_nrt   = draw_course(staid, 'nrt')
    fig_course_retro = draw_course(staid, 'retro')
    
    return [fig_course_nrt, fig_course_retro, True, stain]

# create/update snow pillow time series graph in popup
@app.callback(Output(component_id='snow-graph-pillow-nrt',   component_property='figure'),
              Output(component_id='snow-graph-pillow-retro', component_property='figure'),
              Output('pillow-popup-plots', 'is_open'),
              Output('pillow-popup-plots', 'title'),
              Input('b120-pillows', 'clickData'))
def update_pillow(site):
    if site==None:
        staid = 'RTL'
        stain = 'StationID: RTL, Name: Rattlesnake, Elevation: 6210ft, Basin: Feather River'
    else:
        staid = site['properties']['STA']
        stain = site['properties']['tooltip']
    fig_pillow_nrt   = draw_pillow(staid, 'nrt')
    fig_pillow_retro = draw_pillow(staid, 'retro')
    
    return [fig_pillow_nrt, fig_pillow_retro, True, stain]

# create/update streamflow time series for rivers
@app.callback(Output(component_id='graph-mofor-river', component_property='figure'),
              Output('popup-plots-river', 'is_open'),
              Output('popup-plots-river', 'title'),
              Input('nwm-rivers', 'clickData'))
def update_flows_river(fcst_point):

    if 'feature_id' in fcst_point['properties']:
        rivid = fcst_point['properties']['feature_id']
        rivin = fcst_point['properties']['tooltip']
        pop = True
    else:
        rivid = ''
        rivin = ''
        pop = False
        
    fig_mofor_river = draw_mofor_river(rivid)
    
    return [fig_mofor_river, pop, rivin]


# callback to open the pop-up window for google doc
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='open_gdoc'
    ),
    Output('gdoc-popup', 'is_open'),
    Input('gdoc-button', 'n_clicks')
)

# callback to open the pop-up window for forcing doc
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='open_fdoc'
    ),
    Output('fdoc-popup', 'is_open'),
    Input('fdoc-button', 'n_clicks')
)
