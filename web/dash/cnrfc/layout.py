from dash import html, dcc
import dash_bootstrap_components as dbc

from region_tools import get_region_tools
from site_tools   import get_site_tools
from basin_tools  import get_basin_tools
from snow_tools   import get_snow_tools
from river_tools  import get_river_tools
from docs_links   import get_docs_links

def get_layout():

    map_region, control_data_sel, control_time_sel = get_region_tools()
    popup_plots = get_site_tools()
    [basin_tools, basin_popup_plots] = get_basin_tools()
    [course_popup_plots, pillow_popup_plots] = get_snow_tools()
    [gdoc_popup, fdoc_popup, docs_links] = get_docs_links()
    popup_plots_river = get_river_tools()
    
    panel_layout = dbc.Container([
            dbc.Row([
                dbc.Col([html.Div([map_region, popup_plots, course_popup_plots, pillow_popup_plots, popup_plots_river])]),
                dbc.Col([
                    dbc.Row(control_data_sel),
                    dbc.Row(control_time_sel),
                    dbc.Row([basin_tools, basin_popup_plots]),
                    dbc.Row([docs_links, gdoc_popup, fdoc_popup])
                ], width=4)
            ])
        ], fluid=True,
        style={'width': '98%', 'min-width': '1000px', 'height': '100%', 'min-height': '800px'}
    )

    return panel_layout

