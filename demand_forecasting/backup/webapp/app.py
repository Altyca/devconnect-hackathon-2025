from dash import Dash, dcc, callback, Input, Output, State, html
import dash_bootstrap_components as dbc
import dash
from dash.exceptions import PreventUpdate

_font = "https://fonts.googleapis.com/css2?family=Lato&display=swap"
app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME, _font],
	   suppress_callback_exceptions=True, prevent_initial_callbacks=True)
server = app.server

############################################################################################
# Import shared components
from assets.navbar import _nav
from assets.footer import _footer

############################################################################################
# App Layout
app.layout = dbc.Container([
	## Page Nav
	dbc.Row([
        dbc.Col([
            _nav
        ], width = 12)
    ]),

    ## Page Location to determine current URL
    dcc.Location(
         id = 'page-location'
    ),

	## Page content
    dbc.Row([
        dbc.Col([
             
            html.Div(children = [],
            className = 'container', id = 'app-filter-div'), # Same class as page content below

            ## Page Content
            dash.page_container,

            ## Footer
			_footer
               
	    ], className = 'page-content', width = 12),
    ]),
	
    dcc.Store(id='browser-memo', data=dict(), storage_type='session')
	
], fluid=True)

############################################################################################
# Callbacks


############################################################################################
# Run App
if __name__ == '__main__':
	app.run(debug=False, port='8855')