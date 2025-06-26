from dash import html
import dash_bootstrap_components as dbc

_footer = html.Div([
    dbc.Container([

        dbc.Row([
            dbc.Col([], width = 2),
            dbc.Col([html.Hr([], className = 'hr-footer')], width = 8),
            dbc.Col([], width = 2)
        ]),

        dbc.Row([
            dbc.Col([], width = 4),
            dbc.Col(['Hosted on Databricks, Created with Plotly Dash'], width = 4),
	        dbc.Col([], width = 4),
        ], className = 'footer-row')
        
    ], fluid=True)

], className = 'footer', id = 'footer-div')