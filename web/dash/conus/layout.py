from dash import html, dcc
import dash_bootstrap_components as dbc

from region_tools import get_region_tools
#from site_tools   import get_site_tools
from basin_tools  import get_basin_tools
#from snow_tools   import get_snow_tools
#from river_tools  import get_river_tools
from docs_links   import get_docs_links

def get_layout():

    map_region, title_var, title_date, side_canvas = get_region_tools()
    #popup_plots = get_site_tools()
    #basin_tools = get_basin_tools()
    #[course_popup_plots, pillow_popup_plots] = get_snow_tools()
    #[gdoc_popup, fdoc_popup, docs_links] = get_docs_links()
    #popup_plots_river = get_river_tools()
    
    panel_layout = html.Div([map_region, title_var, title_date, side_canvas],
            style={'width': '100%', 'padding': '10px', 'min-width': '1000px', 'height': '100%', 'min-height': '800px'}
    )

    return panel_layout

