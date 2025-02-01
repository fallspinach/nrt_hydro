from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash_extensions.javascript import Namespace, arrow_function

from datetime import date, datetime, timedelta
import pandas as pd

from config import cloud_url, map_tiles, domain_config, data_vars, tool_style, tabtitle_style, tabtitle_selected_style

from basin_tools  import get_basin_tools
from docs_links   import get_docs_links

def get_region_tools():
    
    df_system_status = pd.read_csv(f'{cloud_url}/data/system_status.csv', parse_dates=True)
 
    last_whnrt = datetime.fromisoformat(df_system_status['WRF-Hydro NRT'][1]).date()
    #last_whnrt = datetime.fromisoformat(df_system_status['WRF-Hydro Monitor'][1]).date()
    data_start = date(2024, 1, 1)

    # start to build maps
    ns = Namespace('dashExtensions', 'default')
    locator = dl.LocateControl(locateOptions={'enableHighAccuracy': True})

    # B-120 forecast points
    b120_points = dl.GeoJSON(url='assets/fnf_points_proj_tooltip_24.pbf', format='geobuf', id='b120-points',
                             options=dict(pointToLayer=ns('b120_ptl')), cluster=True, superClusterOptions=dict(radius=5),
                             hoverStyle=arrow_function(dict(weight=5, color='red', fillColor='red', dashArray='')),
                             hideout=dict(circleOptions=dict(fillOpacity=1, color='red', weight=2, radius=5), colorscale=['cyan'], colorProp='POINT_Y', min=0, max=100))
    # B-120 watersheds
    watershed_style = dict(weight=2, opacity=1, color='darkblue', fillOpacity=0)
    b120_watersheds = dl.GeoJSON(url='assets/fnf_watershed_proj_tooltip_24.pbf', format='geobuf', id='b120-watersheds',
                                 options=dict(style=ns('b120_style')), zoomToBoundsOnClick=False,
                                 hoverStyle=arrow_function(dict(weight=4, color='brown', dashArray='', fillOpacity=0)),
                                 hideout=dict(colorscale=['darkblue'], classes=[0], style=watershed_style, colorProp='Area_SqMi'))
    # HUC2
    huc_style = dict(weight=1, opacity=1, color='#202020', fillOpacity=0)
    huc_bound = dl.GeoJSON(url='assets/huc2_conus_0.5_tooltip.pbf', format='geobuf', id='huc-bound',
                           options=dict(style=ns('b120_style')), zoomToBoundsOnClick=True,
                           hoverStyle=arrow_function(dict(weight=3, color='red', dashArray='', fillOpacity=0.1)),
                           hideout=dict(colorscale=['black'], classes=[0], style=huc_style, colorProp='areasqkm'))
    # NWM rivers with stream_order>3 and simplified geometry
    river_style = dict(weight=1, opacity=1, color='green', fillOpacity=0)
    nwm_rivers = dl.GeoJSON(url='assets/nwm_reaches_cnrfc_order4plus_0d001_single_matched.pbf', format='geobuf', id='nwm-rivers',
                            options=dict(style=ns('river_style')), zoomToBoundsOnClick=True,
                            hoverStyle=arrow_function(dict(weight=4, color='orange', dashArray='', fillOpacity=0)),
                            hideout=dict(colorscale=['black'], classes=[0], style=river_style, colorProp='feature_id'))
    
    # B-120 snow courses
    b120_courses = dl.GeoJSON(url='assets/b120_snow_courses_tooltip.pbf', format='geobuf', id='b120-courses',
                             options=dict(pointToLayer=ns('b120_ptl')), cluster=False, superClusterOptions=dict(radius=5),
                             hoverStyle=arrow_function(dict(weight=5, color='red', fillColor='red', dashArray='')),
                             hideout=dict(circleOptions=dict(fillOpacity=1, color='white', weight=2, radius=3), colorscale=['brown'], colorProp='Elevation', min=0, max=20000))
    
    # B-120 snow pillows
    b120_pillows = dl.GeoJSON(url='assets/b120_snow_pillows_tooltip.pbf', format='geobuf', id='b120-pillows',
                             options=dict(pointToLayer=ns('b120_ptl')), cluster=False, superClusterOptions=dict(radius=5),
                             hoverStyle=arrow_function(dict(weight=5, color='red', fillColor='red', dashArray='')),
                             hideout=dict(circleOptions=dict(fillOpacity=1, color='white', weight=2, radius=3), colorscale=['orange'], colorProp='Elevation', min=0, max=20000))

    # image data overlay
    if last_whnrt.month>7 and last_whnrt.month<12:
        data_var_selected = 1
    else:
        data_var_selected = 1
    img_url  = last_whnrt.strftime(data_vars[data_var_selected]['url'])
    cbar_url = data_vars[data_var_selected]['cbar']
    data_map = dl.ImageOverlay(id='data-img', url=img_url, bounds=domain_config['bounds'], opacity=0.7)
    # color bar
    data_cbar = html.Div(html.Img(src=cbar_url, title='Color Bar', id='data-cbar-img'), id='data-cbar',
                         style={'position': 'absolute', 'left': '18px', 'top': '140px', 'z-index': '500'})

    layers_region = [dl.Overlay([data_map, data_cbar], id='data-map-ol',  name='Data',   checked=True),
                     dl.Overlay(huc_bound,      id='huc-ol', name='HUC', checked=True)]
                     #dl.Overlay(nwm_rivers,       id='rivers-ol', name='Rivers', checked=False),
                     #dl.Overlay(b120_watersheds,  id='basins-ol', name='B120 Basins', checked=True),
                     #dl.Overlay(b120_points,      id='sites-ol',  name='B120 Sites',  checked=True),
                     #dl.Overlay(b120_courses,     id='courses-ol',  name='Snow Course',  checked=True),
                     #dl.Overlay(b120_pillows,     id='pillows-ol',  name='Snow Pillows',  checked=True)]
                 
    # region map on the left
    map_region = dl.Map([map_tiles[1], locator, dl.LayersControl(layers_region)], id='map-region',
                        center=domain_config['center'], zoom=domain_config['zoom'],
                        style={'width': '100%', 'height': '100%', 'min-height': '780px', 'min-width': '700px', 'margin': '0px', 'display': 'block'})


    # met variable tab
    tab_style = tool_style.copy()
    tab_style.update({'min-height': '72px', 'padding-top': '5px', 'margin-bottom': '10px'})
    item_style = {'margin': '5px 10px 2px 10px'}
    met_vars = [{'label': v['label'], 'value': v['name']} for v in data_vars if v['cat']=='met']
    met_tab = html.Div(dcc.RadioItems(options=met_vars, value=met_vars[0]['value'], id='met-vars', inputStyle=item_style, inline=True), style=tab_style)

    # hydro variable tab
    hydro_vars = [{'label': v['label'], 'value': v['name']} for v in data_vars if v['cat']=='hydro']
    hydro_tab = html.Div(dcc.RadioItems(options=hydro_vars, value=hydro_vars[data_var_selected]['value'], id='hydro-vars', inputStyle=item_style, inline=True), style=tab_style)

    control_data_sel = html.Div(dcc.Tabs([
        dcc.Tab(met_tab,   label='Meteorology', value='met',   style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab(hydro_tab, label='Hydrology',   value='hydro', style=tabtitle_style, selected_style=tabtitle_selected_style),
    ], value='hydro', id='data-sel'))


    ## hour slider
    hour_marks = {}
    for i in range(24):
        hour_marks[i] = '%d' % i
    hour_marks[0] = '0z'
    
    slider_hour =  html.Div(
        dcc.Slider(
            id='slider-hour',
            min=0,
            max=23,
            step=1,
            marks=hour_marks,
            value=0,
            disabled=True,
        ),
        id='slider-hour-container',
        style={'padding-top': '10px'}
    )

    datepicker = dcc.DatePickerSingle(
        id='datepicker',
        display_format='YYYY-MM-DD',
        min_date_allowed=data_start,
        max_date_allowed=last_whnrt,
        initial_visible_month=data_start,
        date=last_whnrt,
        day_size=30,
    )

    interval_check_system = dcc.Interval(
        id='interval-check_system',
        interval=3600*1000, # in milliseconds
        n_intervals=0
    )

    ## buttons for forward and backward moves
    button_style = {'margin': '0px 2px 0px 2px'}
    button_backward_hour  = html.Button('<H', id='button-backward-hour',  n_clicks=0, style=button_style, disabled=True)
    button_backward_day   = html.Button('<D', id='button-backward-day',   n_clicks=0, style=button_style)
    button_backward_month = html.Button('<M', id='button-backward-month', n_clicks=0, style=button_style)
    button_forward_hour   = html.Button('H>', id='button-forward-hour',   n_clicks=0, style=button_style, disabled=True)
    button_forward_day    = html.Button('D>', id='button-forward-day',    n_clicks=0, style=button_style)
    button_forward_month  = html.Button('M>', id='button-forward-month',  n_clicks=0, style=button_style)

    ## figure title
    title_var  = html.Div(data_vars[data_var_selected]['label'], id='title-var',
                          style={'position': 'absolute', 'left': '50px',  'bottom': '25px', 'z-index': '500', 'font-size': 'medium'})
    title_date = html.Div(last_whnrt.strftime(' @ %Y-%m-%d '), id='title-date',
                          style={'position': 'absolute', 'left': '245px', 'bottom': '25px', 'z-index': '500', 'font-size': 'medium'})

    title_zone = html.Div([title_var, title_date], id='title-zone')

    button_open_popup  = html.Button('Open Time Series Window', id='button-open-popup',  n_clicks=0, style={'margin-top': '15px'})

    tab_stylec = tab_style.copy()
    tab_stylec.update({'text-align': 'center', 'padding-top': '15px'})
    # time step selection tab
    #timestep_tab = html.Div([button_backward_month, button_backward_day, button_backward_hour, datepicker, 
    #                         button_forward_hour, button_forward_day, button_forward_month, slider_hour, button_open_popup, title_var, title_date], style=tab_stylec)
    timestep_tab = html.Div([button_backward_month, button_backward_day, button_backward_hour, datepicker, interval_check_system,
                             button_forward_hour, button_forward_day, button_forward_month], style=tab_stylec)

    ## month slider
    month_marks = {}
    for i in range(12):
        month_marks[i] = '%d' % (i+1)
    month_marks = {0: 'Jan', 1: 'Feb', 2: 'Mar', 3: 'Apr', 4: 'May', 5: 'Jun', 6: 'Jul', 7: 'Aug', 8: 'Sep', 9: 'Oct', 10: 'Nov', 11: 'Dec'}
    
    slider_month =  html.Div(
        dcc.Slider(
            id='slider-month',
            min=0,
            max=11,
            step=1,
            marks=month_marks,
            value=0,
            disabled=True,
        ),
        id='slider-month-container',
        style={'padding-top': '10px'}
    )

    # monthly climatology selection tab
    #clim_tab = html.Div(['Select Month', slider_month], style=tab_stylec)
    clim_tab = html.Div([slider_month], style=tab_stylec)

    # tabs for time step selection
    control_time_sel = html.Div(dcc.Tabs([
        dcc.Tab(timestep_tab, label='Time Step',   value='timestep', style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab(clim_tab,     label='Climatology', value='clime',    style=tabtitle_style, selected_style=tabtitle_selected_style),
    ], value='timestep'))

    basin_tools,basin_popup_plots = get_basin_tools()
    gdoc_popup, fdoc_popup, docs_links = get_docs_links()

    side_content = html.Div([
                    dbc.Row(control_data_sel),
                    dbc.Row(control_time_sel),
                    dbc.Row(html.Div([
                        dbc.Button('More »', id='button-openmore', size='sm', color='secondary', n_clicks=0, style={'width': '70px'}),
                        dbc.Collapse([dbc.Row([basin_tools, basin_popup_plots])], #dbc.Row([docs_links, gdoc_popup])
                                     id='collapse-openmore', is_open=False)
                        ]))
                    ], style={'width': '400px'})

    # offcavas using dropdown menu
    side_canvas = dbc.DropdownMenu(label='', children=[side_content], direction='start', id='side-canvas', color='secondary', style={'position': 'absolute', 'top': '70px', 'right': '23px', 'z-index': '500', 'opacity': '0.9'})

    return map_region, title_var, title_date, side_canvas

