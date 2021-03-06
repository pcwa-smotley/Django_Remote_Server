import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import json
import pandas as pd
df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_february_us_airport_traffic.csv')

app = dash.Dash()

app.layout = html.Div([
    html.Div(
        html.Pre(id='lasso', style={'overflowY': 'scroll', 'height': '100vh'}),
        className="three columns"
    ),

    html.Div(
        className="nine columns",
        children=dcc.Graph(
            id='graph',
            figure={
                'data': [{
                    'lat': df.lat, 'lon': df.long, 'type': 'scattermapbox'
                }],
                'layout': {
                    'mapbox': {
                        'accesstoken': (
                            "pk.eyJ1Ijoic21vdGxleSIsImEiOiJuZUVuMnBBIn0.xce7KmFLzFd9PZay3DjvAA"
                        )
                    },
                    'margin': {
                        'l': 0, 'r': 0, 'b': 0, 't': 0
                    },
                }
            }
        )
    )
], className="row")

external_css = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
for css in external_css:
    app.css.append_css({"external_url": css})



@app.callback(
    Output('lasso', 'children'),
    [Input('graph', 'selectedData')])
def display_data(selectedData):
    return json.dumps(selectedData, indent=2)


if __name__ == '__main__':
    app.run_server(debug=True)