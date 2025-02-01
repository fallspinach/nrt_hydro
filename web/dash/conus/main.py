import os
import dash
import dash_bootstrap_components as dbc

from layout import get_layout


# some external things
external_stylesheets = [dbc.themes.BOOTSTRAP, 'https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css']
external_scripts     = ['https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js']  # js lib used for colors

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, external_scripts=external_scripts, prevent_initial_callbacks=True)

server = app.server

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>CONUS Water Panel (experimental & internal use only)</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

app.layout = get_layout

from callbacks import *

if __name__ == '__main__':
   app.run_server(host='0.0.0.0', port=8050, debug=True)
