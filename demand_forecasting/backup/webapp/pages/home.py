import dash
from dash import html, callback, dcc, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path='/', order = 1, name='Home', title='Databricks Hackaton | Home')

############################################################################################
# Import functions, settings
from assets.make_prediction import query_endpoint

############################################################################################
# Upload data
import pandas as pd
df_ = pd.read_parquet('assets/history_pd_v2.parquet')

############################################################################################
# Page layout
layout = dbc.Container([
    dbc.Row([],className='empty-row'),

    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
    ### ### ### ### ### Dashboard Embedding
    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
    dbc.Row([
        dbc.Col([
            html.H1("Sales Data - Actual",className='titles-h1')
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            html.Iframe(
               src="https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f05046c292165d9a2d8a273539fe4a?o=1444828305810485",
               className='dashboard-iframe')
        ], width=12)
    ]),
    dbc.Row([],className='empty-row'),

    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
    ### ### ### ### ### Model Serving Endpoint
    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
    dbc.Row([
        dbc.Col([
            html.H1("Generate Predictions",className='titles-h1')
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([], width = 4),
        dbc.Col([
            html.Button(children=["Query Model Serving",html.Br(),"Endpoint"], id='submit-button', n_clicks=0, className='my-button')
        ], className = 'button-col', width = 4),
        dbc.Col([], width = 4)
    ]),
    dbc.Row([],className='empty-row'),
    dbc.Row([
        dbc.Col([], width = 4),
        dbc.Col(id='response-alert-col', width = 4),
        dbc.Col([], width = 4)
    ]),
    dbc.Row([
        dbc.Col([], width = 4),
        dbc.Col(id='response-predictions', width = 4),
        dbc.Col([], width = 4)
    ]),
])

### PAGE CALLBACKS ###############################################################################################################
@callback(
    Output(component_id='response-alert-col', component_property='children'),
    Output(component_id='response-predictions', component_property='children'),
    Input(component_id='submit-button', component_property='n_clicks'))
def data_transform(button_clicks):
    _output = []
    if ctx.triggered_id == 'submit-button':
        # Query endpoint API
        _response_output = None
        try:
            _response = query_endpoint(df_)
            if _response.status_code == 200:
                _predictions = dict(_response.json())['predictions']
                alert_output_ = dbc.Alert(children=['Request Successful!'], color='success', class_name='alert-style')
                ## Convert predicted data to string
                predicted_data = []
                for p in _predictions:
                    predicted_data.append(f"ds: {str(p['ds'])}")
                    predicted_data.append(html.Br())
                    predicted_data.append(f"yhat: {str(p['yhat'])}")
                    #predicted_data += f"yhat_upper: {str(p['yhat_upper'])}"
                    #predicted_data += f"yhat_lower: {str(p['yhat_lower'])}"
                    predicted_data.append(html.Br())
                    predicted_data.append(html.Br())
                ## Nest content a div object
                _response_output = html.Div(
                    children = [html.H3("Predictions:",className='titles-h3'), html.P(predicted_data)],
                    className = 'prediction-div'
                )
            else:
                alert_output_ = dbc.Alert(children=['Request failed with status: '+str(_response.status_code)+', '+str(_response.text)],
                                    color='danger', class_name='alert-style-danger')
        except:
            alert_output_ = dbc.Alert(children=["Request couldn't be initiated"], color='danger', class_name='alert-style-danger')
    else:
        raise PreventUpdate
    return alert_output_, _response_output