from dash import html, dcc
import dash_bootstrap_components as dbc
from dash_extensions.javascript import Namespace, arrow_function

from datetime import date, datetime, timedelta

from config import tool_style, tabtitle_style, tabtitle_selected_style

# start to build maps
ns = Namespace('dashExtensions', 'default')
    
def get_docs_links():

    gdoc = html.Iframe(src='https://docs.google.com/document/d/e/2PACX-1vRpsWbx0SGVU6PXPeVBkQdB9rG2AT5AixiVeoLCw5srjblHIYv1HzBDTmBL3hvaQsU69rkfUt0RyyM3/pub?embedded=true',
                       width='98%', height='98%')
    gdoc_popup = dbc.Offcanvas([gdoc],
        title='CW3E WRF-Hydro Environment Documentation', placement='top', is_open=False, scrollable=True, id='gdoc-popup',
        style={'opacity': '1', 'width': '900px', 'height': '100%', 'margin-top': '0px', 'margin-left': 'auto', 'margin-right': 'auto', 'font-size': 'smaller'}
    )
    fdoc = html.Iframe(src='https://docs.google.com/document/d/e/2PACX-1vShtg6sapWHonjKVLASFBa_AIfEP66SkrG6HDXuSK095rcYpDbRxSI05eadaTvwFIiEw6fE2VIjAAN2/pub?embedded=true',
                       width='98%', height='98%')
    fdoc_popup = dbc.Offcanvas([fdoc],
        title='CW3E Forcing Documentation', placement='top', is_open=False, scrollable=True, id='fdoc-popup',
        style={'opacity': '1', 'width': '900px', 'height': '100%', 'margin-top': '0px', 'margin-left': 'auto', 'margin-right': 'auto', 'font-size': 'smaller'}
    )
    
    gdoc_row = dbc.Row([
        dbc.Col([dbc.Button('System Doc', id='gdoc-button', size='sm', outline=True, color='primary', className='me-1', style={'margin': 'auto'})], width=4),
        dbc.Col(['Click "5. Web app (Dash) for interactive visualizations" under "Table of Contents".'])
    ], className='g-0', style={'font-size': 'small', 'margin-top': '10px'})
    fdoc_row = dbc.Row([
        dbc.Col([dbc.Button('Forcing Doc', id='fdoc-button', size='sm', outline=True, color='primary', className='me-1', style={'margin': 'auto'})], width=4),
        dbc.Col(['Google Doc for CW3E forcing.'])
    ], className='g-0', style={'font-size': 'small', 'margin-top': '5px'})
    
    scode_row = dbc.Row([
        dbc.Col([dbc.Button('System Source', href='https://github.com/fallspinach/nrt_hydro/', target='_blank', id='scode-button', size='sm', outline=True, color='primary', className='me-1', style={'margin': 'auto'})], width=4),
        dbc.Col(['System source code on GitHub.'])
    ], className='g-0', style={'font-size': 'small', 'margin-top': '5px'})
    wcode_row = dbc.Row([
        dbc.Col([dbc.Button('Web App Source', href='https://github.com/fallspinach/cw3e-water-panel/', target='_blank', id='wcode-button', size='sm', outline=True, color='primary', className='me-1', style={'margin': 'auto'})], width=4),
        dbc.Col(['Web app source code on GitHub.'])
    ], className='g-0', style={'font-size': 'small', 'margin-top': '5px'})

    docs = dbc.Stack([gdoc_row, fdoc_row, scode_row])
    
    tab_style = tool_style.copy()
    tab_style.update({'min-height': '180px', 'padding-top': '20px', 'text-align': 'center'})
    
    links = html.Div([''], style=tab_style)

    docs_links = html.Div(dcc.Tabs([
        dcc.Tab(docs,  label='Documentation', value='tab-docs',  style=tabtitle_style, selected_style=tabtitle_selected_style),
        dcc.Tab(links, label='Links',         value='tab-links', style=tabtitle_style, selected_style=tabtitle_selected_style),
    ], style={'margin-top': '10px'}, value='tab-docs'))

    return gdoc_popup, fdoc_popup, docs_links
