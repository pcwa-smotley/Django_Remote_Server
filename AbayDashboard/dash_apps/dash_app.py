import os
from io import BytesIO
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from zipfile import ZipFile
import copy
import requests
from urllib.error import HTTPError
from datetime import datetime, timedelta, time
import pytz
import numpy as np
from django.contrib.staticfiles.storage import staticfiles_storage
from dash.dependencies import Input, Output, State
from plotly import graph_objs as go
from datetime import datetime as dt
from django_plotly_dash import DjangoDash
import dash_bootstrap_components as dbc
from htexpr import compile
import dash_daq as daq
from scipy import stats
import json
from .dash_abay_extras.layout import top_cards
from ..mailer import send_mail
import psutil

# ###To run the app on it's own (not in Django), you would do:
# app = dash.Dash()
# ###Then at the bottom you would do the following:
# if __name__ == '__main__':
#    app.run_server(debug=True)
### Then you'd just run python from the terminal > python dash_first.py
# app = DjangoDash('UberExample')

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width" }],
    external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css"],
    external_scripts=["https://code.jquery.com/jquery-3.5.1.min.js",
                      'https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/js/materialize.min.js',
                      ]
)

# app = dash.Dash()

class PiRequest:
    #
    # https://flows.pcwa.net/piwebapi/assetdatabases/D0vXCmerKddk-VtN6YtBmF5A8lsCue2JtEm2KAZ4UNRKIwQlVTSU5FU1NQSTJcT1BT/elements
    def __init__(self, db, meter_name, attribute):
        self.db = db  # Database (e.g. "Energy Marketing," "OPS")
        self.meter_name = meter_name  # R4, Afterbay, Ralston
        self.attribute = attribute  # Flow, Elevation, Lat, Lon, Storage, Elevation Setpoint, Gate 1 Position, Generation
        self.baseURL = 'https://flows.pcwa.net/piwebapi/attributes'
        self.meter_element_type = self.meter_element_type()  # Gauging Stations, Reservoirs, Generation Units
        self.url = self.url()
        self.data = self.grab_data()

    def url(self):
        try:
            if self.db == "Energy_Marketing":
                response = requests.get(
                    url="https://flows.pcwa.net/piwebapi/attributes",
                    params={
                        "path": f"\\\\BUSINESSPI2\\{self.db}\\Misc Tags|{self.attribute}",
                    },
                )
            else:
                response = requests.get(
                    url="https://flows.pcwa.net/piwebapi/attributes",
                    params={
                        "path": f"\\\\BUSINESSPI2\\{self.db}\\{self.meter_element_type}\\{self.meter_name}|{self.attribute}",
                        },
                )
            j = response.json()
            url_flow = j['Links']['InterpolatedData']
            return url_flow

        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            return None

    def grab_data(self):
        # Now that we have the url for the PI data, this request is for the actual data. We will
        # download data from the beginning of the water year to the current date. (We can't download data
        # past today's date, if we do we'll get an error.
        try:
            response = requests.get(
                url=self.url,
                params={"startTime": (datetime.utcnow() + timedelta(hours=-24)).strftime("%Y-%m-%dT%H:%M:00-00:00"),
                        "endTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00-00:00"),
                        "interval": "1m",
                        },
            )
            print(f'Response HTTP Status Code: {response.status_code} for {self.meter_name} | {self.attribute}')
            j = response.json()
            # We only want the "Items" object.
            return j["Items"]
        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            return None

    def meter_element_type(self):
        if not self.meter_name:
            return None
        if self.attribute == "Flow":
            return "Gauging Stations"
        if "Afterbay" in self.meter_name:
            return "Reservoirs"
        if "Middle Fork" or "Oxbow" in self.meter_name:
            return "Generation Units"

local = False
if local:
    app = dash.Dash(
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        external_scripts=["https://code.jquery.com/jquery-3.5.1.min.js",
                          ],
        suppress_callback_exceptions = True
    )
    server = app.server
    df = pd.read_csv("pcwa_locations.csv")
else:
    app = DjangoDash('dash_django', suppress_callback_exceptions=True, add_bootstrap_links=True)
    df = pd.read_csv(staticfiles_storage.path("data/pcwa_locations.csv"))  # make sure this is in the static dir

main_dir = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))
mapbox_access_token = "pk.eyJ1Ijoic21vdGxleSIsImEiOiJuZUVuMnBBIn0.xce7KmFLzFd9PZay3DjvAA"


def main(meters):
    # This will store the data for all the PI requests
    df_all, df_cnrfc = update_data(meters, None)

    # This is for the abay levels. We're just going to show the data on an hourly basis.
    df_hourly_resample = df_all.resample('60min', on='Timestamp').mean()

    # Since the data contain CNRFC data, which contains forecast data, remove all the nan values
    # which will basically give us a dataframe only with current PI data.

    locations_types = df.type.unique()
    mapbox_layers = {
        "SWE": "https://idpgis.ncep.noaa.gov/arcgis/services/NWS_Observations/NOHRSC_Snow_Analysis/"
                "MapServer/WmsServer?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image/"
                "png&TRANSPARENT=true&LAYERS=1&WIDTH=256&HEIGHT=256&CRS=EPSG:3857&STYLES=&BBOX={bbox-epsg-3857}",
        "RADAR": "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913/{z}/{x}/{y}.png",
        "SAT_TILES": "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}",
        "PRECIP_24": "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/q2-p24h-900913/{z}/{x}/{y}.png",
        "PRECIP_48": "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/q2-p48h-900913/{z}/{x}/{y}.png",
        "PRECIP_72": "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/q2-p72h-900913/{z}/{x}/{y}.png",
    }

# Layout for line graph plot.
    layout = dict(
        margin=dict(l=40, r=40, b=40, t=40),
        hovermode="closest",
        plot_bgcolor="#343a40",    # This hard codes the background. theme="plotly_dark" works if this line is removed.
        paper_bgcolor="#343a40",
        legend=dict(font=dict(size=10), orientation="h"),
        height=400
    )

    top_row_cards = top_cards(df_all, df_hourly_resample)

    app.layout = html.Main(
        className='content container',
        children=[
            top_row_cards,
            dbc.Row([html.Div(
                className="div-for-dropdown ml-2 col-sm-4 col-md-4 col-lg-4",
                children=[
                    dcc.DatePickerSingle(
                        id="date-picker",
                        min_date_allowed=dt(2018, 4, 1),
                        max_date_allowed=dt.now().date(),
                        initial_visible_month=dt.now().date(),
                        date=dt.now().date(),
                        display_format="MMMM D, YYYY",
                    )
                ],
            ),
                # Change to side-by-side for mobile layout
                html.Div(
                    className="div-for-dropdown ml-2 col-sm-4 col-md-4 col-lg-3",
                    children=[
                        # Dropdown for locations on map
                        dcc.Dropdown(
                            id="alert-dropdown",
                            options=[
                                {"label": i, "value": i}
                                for i in locations_types
                            ],
                            placeholder="Select meter",
                        )
                    ],
                ),
                html.Div(
                    className="div-for-dropdown col-sm-3 col-md-3 col-lg-3",
                    children=[
                        # Dropdown to select times
                        dcc.Dropdown(
                            id="mapbox_layer_dropdown",
                            options=[
                                {
                                    "label": key,
                                    "value": value,
                                }
                                for key, value in mapbox_layers.items()
                            ],
                            multi=True,
                            placeholder="Map Layers",
                        )
                    ],
                ),
            ], className="col-lg-6 col-md-12 col-sm-12 no-gutters"),
            dbc.Row([html.Div([dcc.Graph(id="map-graph")], className='col-sm-12 col-md-12 col-lg-6 mb-3 pr-md-2'),
                     html.Div(dcc.Loading(
                                id='loading_graph',
                                parent_className='loading_wrapper',
                                children=[html.Div([dcc.Graph(id="histogram")])],
                                type="circle",
                                ), className='col-sm-12 col-md-12 col-lg-6 mb-3 pr-md-2'
                            )
            ], className="no-gutters"),
            html.Div(id='dummy-output'),
            html.Div(id='dummy-output-timer', **{'data-pi_checker_running': "true"},),
            # This div will store our dataframe with all the PI data. It's a way for the
            # callbacks to share data
            html.Div(id='dummy-dataframe', style={'display':'none'}),
            html.Div(id='dummy-rfc-dataframe', style={'display': 'none'}),
            # This is a dummy id to allow the app to perform an update at a given interval
            dcc.Interval(
                id='interval-component',
                interval=1 * 60000,  # in milliseconds (60 seconds)
                #max_intervals=0,
                n_intervals=0
            ),
        ]
    )

    # A function to plot the main graph.
    def produce_individual(api_stn_name, rfc_json_data):
        try:
            point_row = df[df['id'] == api_stn_name].index[0]
            point = df.iloc[point_row]
        except:
            return None
        # Case where the id is a CNRFC forecast point.
        if "Fcst" in point['id']:
            # Read the cnrfc forecast from the dummy-div that is passed into this function
            df_cnrfc = pd.read_json(rfc_json_data, orient='index')

            # The id's are in a string format like "R4 Fcst". Split it up by the space to get the station.
            stn_number = point['id'].split(" ")[0]

            # Rename the columns to match the PI data. The "Value" column is the value to graph (the stn_number).
            df_meter = df_cnrfc.rename(columns={'GMT':'Timestamp',f"{stn_number}_fcst": "Value"})
        else:
            # If this isn't a forecast, get the data from a PI request.
            df_meter = pd.DataFrame.from_dict(PiRequest("OPS",point['id'],"Flow").data)

        # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
        df_meter.index = pd.to_datetime(df_meter.Timestamp).dt.tz_convert('US/Pacific')
        return df_meter

    # Callback does two things. 1) Automatically refresh the hidden dataframes (in html) for PI data and
    #                              the CNRFC data.
    #                           2) Check to see if the "pi_checker.py" program is still running. Note: this
    #                              updates the data-pi_checker_running component with a True/False, but the
    #                              clientside callback (below this callback) actually updates the html in
    #                              the sidebar, so the power-button icon turns red or green.
    @app.callback([Output(component_id='dummy-dataframe', component_property='children'),
                   Output(component_id='dummy-rfc-dataframe', component_property='children'),
                   Output(component_id='dummy-output-timer', component_property='data-pi_checker_running')
                   ],
                [Input('interval-component', 'n_intervals'),
                 Input('dummy-rfc-dataframe', 'children')])
    def get_new_data(value, current_rfc_df):
        # Part (1) in description above, update dataframes
        df_refresh, df_refresh_rfc = update_data(meters, current_rfc_df)

        # Part (2) in description above, check all processes running on system
        pi_process = False
        for p in psutil.process_iter():
            if "python" in p.name():            # Any process that is python
                for arg in p.cmdline():         # Check all python processes
                    if "pi_checker" in arg:     # If "pi_checker" is in any of those, program is running.
                        pi_process = True
        #if not pi_process:
            #send_mail("720*****","s*****@****.com","pi_checker.py is not running", "PI_CHEKER STATUS: OFF")
        return df_refresh.to_json(date_format='iso',orient='index'), \
               df_refresh_rfc.to_json(date_format='iso',orient='index'), pi_process



    @app.callback(Output("loading_output", "children"), [Input("map-graph", "hoverData")])
    def input_triggers_spinner(value):
        return


    # Updates the "Last Updated:" section in the html header.
    app.clientside_callback(
        """
        function(n_intervals, df, pi_checker_running){
            console.log(pi_checker_running)
            let df_obj = JSON.parse(df)
            let last_item = Object.keys(df_obj).length-1
            let last_timestring = df_obj[last_item]["Timestamp"]
            var parseTime = d3.utcParse("%Y-%m-%dT%H:%M:%S.%LZ")
            var last_timestamp = parseTime(last_timestring)
            var pretty_date = d3.timeFormat("%I:%M %p")
            $("#updated_time").text(pretty_date(last_timestamp))
            $("#pi_checker_status").attr('style',"color:red; text-shadow: 0px 0px 10px rgb(255 40 1 / 50%")
            if(pi_checker_running){
                $("#pi_checker_status").attr('style',"color:#8bff24; text-shadow: 0px 0px 10px rgb(40 255 1 / 50%")
                }
            return
        }
        """,
        Output(component_id='dummy-output-timer', component_property='children'),
        [Input('interval-component', 'n_intervals'), Input("dummy-dataframe","children"),
         Input("dummy-output-timer","data-pi_checker_running")]
    )

    @app.callback(
        Output(component_id="modal_header_text", component_property="children", ),
        [Input(component_id="open_modal", component_property="n_clicks"), Input(component_id="close", component_property="n_clicks")],
        [State(component_id="alert-dropdown", component_property="value")],
    )
    def show_meter_in_modal(n1, n2, value):
        if n1 or n2:
            return html.H5(f"Change Alarm Values For: {value}", className="modal-title")
        return html.H5(f"Change Alarm Values For: {value}", className="modal-title")


    #This callback does two things:
    # 1: Call back for adding / removing mapbox layer
    # 2: Callback for changing marker display.
    @app.callback(
        Output(component_id="map-graph", component_property="figure", ),
        [Input(component_id="alert-dropdown", component_property="value"),
         Input("mapbox_layer_dropdown", "value")],
    )
    def map_markers(marker_type_val, mapbox_overlay):
        fig = go.Figure()

        # MARKER SECTION
        if marker_type_val is None:
            markers = df.loc[df['type'] == 'Flow']
        else:
            markers = df.loc[df['type'] == marker_type_val]

        # This is for the large outer Ring
        fig.add_trace(go.Scattermapbox(
            lat=markers.lat,
            lon=markers.lon,
            mode='text+markers',  # A mode of "text" will just show hover
            marker=go.scattermapbox.Marker(
                size=17,
                color='rgb(38, 174, 38)',
                opacity=0.7
            ),
            text=markers.id,
            textfont=dict(
                family="sans serif",
                size=18,
                color="white"
            ),
            textposition="top center",
            hoverinfo='text'
        ))

        # This is for the small inner right.
        fig.add_trace(go.Scattermapbox(
            lat=markers.lat,
            lon=markers.lon,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=8,
                color='rgb(172, 242, 177)',
                opacity=0.7
            ),
            hoverinfo='text',
            customdata=markers.id,
            text=[i for i in markers.name],
        ))

        fig.update_layout(
            title='Meter Locations',
            hovermode='closest',
            margin=dict(l=10, r=10, b=10, t=40),
            plot_bgcolor="#F9F9F9",
            paper_bgcolor="#F9F9F9",
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=dict(
                    lat=39,
                    lon=-120.5
                ),
                pitch=0,
                zoom=9,
                style='dark'
            ),
        )

        # MAPBOX OVERLAY SECTION
        if mapbox_overlay is None or len(mapbox_overlay) == 0:
            fig.update_layout(
                mapbox_layers=[
                    {}
                ])
            return fig
        fig.update_layout(
            mapbox_layers=[
                {
                    "below": 'traces',
                    "sourcetype": "raster",
                    "sourceattribution": "United States Geological Survey",
                    "source": [
                        mapbox_overlay[0]
                    ]
                }
            ])

        return fig

    app.clientside_callback(
        """
        function(alarmHi, alarmLo, div_id){
            console.log(div_id, alarmHi, alarmLo)
            if(div_id=="R4"){
                console.log("INSIDE")
                $("#id_r4_hi").val(alarmHi)
                $("#id_r4_lo").val(alarmLo)
            }
            if(div_id=="Abay"){
                $("#id_abay_upper").val(alarmHi)
                $("#id_abay_lower").val(alarmLo)
            }
        }
        """,
        Output(component_id='dummy-output', component_property='children'),
        [Input("alarmInputHi", "value"),
        Input("alarmInputLow", "value"),
        Input("alert_id", "value")])


    # Main graph -> individual graph
    @app.callback(Output("histogram", "figure"),
                  [Input("map-graph", "clickData"), Input("dummy-rfc-dataframe","children")])
    def make_individual_figure(main_graph_click, rfc_json_data):
        layout_individual = copy.deepcopy(layout)

        if main_graph_click is None:
            main_graph_click = {
                "points": [
                    {"curveNumber": 1, "pointNumber": 6,
                     "text": "MIDDLE FORK AMERICAN RIVER NR FORESTHILL", "customdata":"R4"}
                ]
            }

        chosen = [point["customdata"] for point in main_graph_click["points"]]
        df_meter = produce_individual(chosen[0], rfc_json_data)

        if df_meter is None:
            annotation = dict(
                text="No data available",
                x=0.5,
                y=0.5,
                align="center",
                showarrow=False,
                xref="paper",
                yref="paper",
            )
            layout_individual["annotations"] = [annotation]
            data = []
        else:
            future_data = dict()
            if "Fcst" in chosen[0]:
                # The CNRFC's forecast contains both actual data and a forecast (they push actual values
                # into their forecast for some reason). So the idea here is to make two lines for the CNRFC data: One
                # line for the actuals and another line for the forecast.

                # Pandas dataframes where the index is time can be selected to include the data between, before, or
                # after a certain time / date. This says: any data in the CNRFC data after current time is a forecast.
                df_future = df_meter.loc[datetime.now(pytz.timezone('US/Pacific')):]
                future_data = dict(
                    type="scatter",
                    mode="lines",
                    name="Forecast Flow in CFS",
                    x=df_future.index,
                    y=df_future['Value'],
                    line=dict(shape="spline", smoothing=1, width=1, color="#d11406"),
                    marker=dict(symbol="diamond-open"),
                    hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                )
            # The default is to always plot at least one line, which will be this line.
            data = [
                dict(
                    type="scatter",
                    mode="lines",
                    name="Flow in CFS",
                    x=df_meter.index,
                    y=df_meter['Value'],
                    line=dict(shape="spline", smoothing=1, width=1, color="#1254b0"),
                    marker=dict(symbol="diamond-open"),
                    hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                ),
                future_data
            ]
            layout_individual["title"] = main_graph_click['points'][0]['text']
            layout_individual["template"] = "plotly_dark"

        figure = go.Figure(data=data, layout=layout_individual)
        return figure


    # Triggered from "def set_alert_options" since it will send over all the options for a given type. The default
    # value will be the first value in the list (e.g. [0]['value']
    @app.callback(
        Output('alert_id', 'value'),
        [Input('alert_id', 'options')])
    def set_alert_value(available_options):
        return available_options[0]['value']

    @app.callback(
        Output('oxbow_sparkline', 'figure'),
        [Input("oxbow_sparkline", "figure"),
         Input("dummy-dataframe","children"),
        Input("ox_switch_span", "n_clicks")]
    )
    def add_oxbow_forecast(figure, json_data, n_clicks):
        ctx = dash.callback_context

        # The ID triggering the callback
        toggle_id = ctx.triggered[0]['prop_id'].split('.')[0]
        df_full = pd.read_json(json_data, orient='index')
        df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')

        # Initial Load, don't show forecast
        if toggle_id == 'dummy-dataframe' and len(figure['data'])<2:
            figure['data'].append(
                dict(x=df_full["Timestamp"], y=df_full["Oxbow_Forecast"],
                              mode='lines',
                              visible=False,
                              hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>", ))
        if n_clicks % 2 and len(figure['data']) > 0:
            figure['data'][1]['visible'] = True
        else:
            figure['data'][1]['visible'] = False

        return figure


    @app.callback(
        [Output('r4sparkline','figure'), Output('r30sparkline','figure'),
         Output('cnrfc_timestamp_r4','children'), Output('cnrfc_timestamp_r4','style'),
         Output('cnrfc_timestamp_r30','children'), Output('cnrfc_timestamp_r30','style')],
        [Input("cnrfc_switch_span","n_clicks"), Input("r4sparkline", "figure"),
         Input("cnrfc_switch_span_r30","n_clicks"), Input("r30sparkline", "figure"),
         Input("dummy-dataframe","children"), Input("dummy-rfc-dataframe","children"),
         Input('cnrfc_timestamp_r4','children'),
         Input('interval-component', 'n_intervals')
         ],
    )
    def flow_sparklines(r4_n_clicks, r4figure, r30_n_clicks, r30figure,
                        json_data, rfc_json_data, cnrfc_timestamp, n_intervals):
        '''
        Purpose: This function will be triggered in one of two ways:
                 (1) Interval update: This will completely redraw the graph by ingesting the info in the dummy div.
                                      The function knows the request is not coming from a click.
                 (2) Click: The idea here is that the data are already loaded, therefore it's faster if the
                            graph isn't redrawn, but rather updated with the CNRFC line added or removed.
                            Not sure if this is actually faster or not.
        :param r4_n_clicks:     Number of clicks from the toggle (even = on, odd = off)
        :param r4figure:        Graph data for r4
        :param r30_n_clicks:    Number of clicks from toggle (even = on, odd = off)
        :param r30figure:       Graph data for r30
        :param json_data:       PI data in json format from the dummy-dataframe div
        :param rfc_json_data:   cnrfc data in json format from dummy-rfc-dataframe div
        :param cnrfc_timestamp  Issued date of CNRFC forecast.
        :param n_intervals:     number of times the interval has been triggered.
        :return:                Returns the r4 and r30 figure.
        '''
        ctx = dash.callback_context

        # The ID triggering the callback
        toggle_id = ctx.triggered[0]['prop_id'].split('.')[0]

        # Is this an initial load or an update sent by interval update
        if toggle_id == 'dummy-dataframe' or toggle_id == 'interval-component':      # Call back not hit by switch, redraw graph
            layout = dict(
                margin= dict(l=0, r=0, t=4, b=4, pad=0),
                xaxis= dict(
                    showline=False,
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                ),
                yaxis= dict(
                    showline=False,
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                ),
                autosize= True,
                paper_bgcolor= "rgba(0,0,0,0)",
                plot_bgcolor= "rgba(0,0,0,0)",
                showlegend= False,
            )

            # Get the data stored in the 'dummy-dataframe' div. This should already be loaded
            try:
                df_full = pd.read_json(json_data, orient='index')

                df_cnrfc = pd.read_json(rfc_json_data, orient='index')
            # If it's not loaded, reload it.
            except ValueError:
                print("DATA NOT FOUND, RELOADING. PLEASE CHECK WHY")
                df_full, df_cnrfc = update_data(meters, rfc_json_data)

            # Convert all datetimes to US/Pacific
            df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')
            df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_convert('US/Pacific')
            df_cnrfc.FORECAST_ISSUED = pd.to_datetime(df_cnrfc.FORECAST_ISSUED).dt.tz_convert('US/Pacific')

            cnrfc_timestamp = f" Fcst Issued: {(df_cnrfc['FORECAST_ISSUED'][1]).strftime('%a, %-d %b %-I %p')}"  # The entire FORECAST_ISSUED column is the same value.

            figR4 = go.Figure(
                {
                    "data": [
                        {
                            "x": df_full["Timestamp"],
                            "y": df_full["R4_Flow"],
                            "mode": "lines",
                            "name": "R4 Flow",
                            "line": {"color": "#f4d44d"},
                            "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                        }
                    ],
                    "layout": layout,
                }
            )
            figR30 = go.Figure(
                {
                    "data": [
                        {
                            "x": df_full["Timestamp"],
                            "y": df_full["R30_Flow"],
                            "mode": "lines",
                            "name": "R30 Flow",
                            "line": {"color": "#f4d44d"},
                            "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                        }
                    ],
                    "layout": layout,
                }
            )
            try:
                cnrfc_r4_visible = False
                cnrfc_r30_visible = False
                r4_timestamp_display = "none"
                r30_timestamp_display = "none"
                if r4_n_clicks % 2 and len(r4figure['data'])>0:
                    # Check to make sure the CNRFC data actually loaded. If it did, it will
                    # be trace1 in the data
                    cnrfc_r4_visible= True
                    r4_timestamp_display = "block"
                # R30 checks
                if r30_n_clicks % 2 and len(r30figure['data']) > 0:
                    # Check to make sure the CNRFC data actually loaded. If it did, it will
                    # be trace1 in the data
                    cnrfc_r30_visible = True
                    r30_timestamp_display = "block"
                figR4.add_scatter(x=df_cnrfc["GMT"], y=df_cnrfc["R4_fcst"], mode='lines',
                                  visible=cnrfc_r4_visible,
                                hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>", )
                figR30.add_scatter(x=df_cnrfc["GMT"], y=df_cnrfc["R30_fcst"], mode='lines',
                                   visible=cnrfc_r30_visible,
                                  hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>", )
            except:
                print("CNRFC data unavail")
                r4_timestamp_display = "none"
                r30_timestamp_display = "none"
            r4_timestamp_style = {"background-color": "gray",
                                   "position": "absolute",
                                   "left": "0px", "right": "0px",
                                   "bottom": "0px", "display": r4_timestamp_display}
            r30_timestamp_style = {"background-color": "gray",
                                   "position": "absolute",
                                   "left": "0px", "right": "0px",
                                   "bottom": "0px", "display":r30_timestamp_display}
            return figR4, figR30, cnrfc_timestamp, r4_timestamp_style, cnrfc_timestamp, r30_timestamp_style

        # Not initial load and triggered via user toggle switch. It's possible this entire section could be
        # removed and you could just redraw the graph just as quickly every time.
        toggle_id = ctx.triggered[0]['prop_id'].split('.')[0]
        r4_timestamp_display = "none"
        r30_timestamp_display = "none"
        try:
            # Toggle is on (since it starts "off", n_clicks will be an odd val when toggle is "on")
            # R4 check
            if r4_n_clicks % 2 and len(r4figure['data'])>0:
                # Check to make sure the CNRFC data actually loaded. If it did, it will
                # be trace1 in the data
                r4figure['data'][1]['visible']=True
                r4_timestamp_display = "block"
            # Toggle is off
            else:
                if len(r4figure['data'])>0:
                    r4figure['data'][1]['visible'] = False
                    r4_timestamp_display = "none"

            # R30 checks
            if r30_n_clicks % 2 and len(r30figure['data'])>0:
                # Check to make sure the CNRFC data actually loaded. If it did, it will
                # be trace1 in the data
                r30figure['data'][1]['visible']=True
                r30_timestamp_display = "block"
            # Toggle is off
            else:
                if len(r30figure['data'])>0:
                    r30figure['data'][1]['visible'] = False
                    r30_timestamp_display = "none"
            r4_timestamp_style = {"background-color": "gray",
                                  "position": "absolute",
                                  "left": "0px", "right": "0px",
                                  "bottom": "0px", "display": r4_timestamp_display}
            r30_timestamp_style = {"background-color": "gray",
                                   "position": "absolute",
                                   "left": "0px", "right": "0px",
                                   "bottom": "0px", "display": r30_timestamp_display}
            return r4figure, r30figure, cnrfc_timestamp, r4_timestamp_style, cnrfc_timestamp, r30_timestamp_style
        except IndexError:
            print("CNRFC plot not loaded")
            r4_timestamp_style = {"background-color": "gray",
                                  "position": "absolute",
                                  "left": "0px", "right": "0px",
                                  "bottom": "0px", "display": "none"}
            r30_timestamp_style = {"background-color": "gray",
                                   "position": "absolute",
                                   "left": "0px", "right": "0px",
                                   "bottom": "0px", "display": "none"}
            return r4figure, r30figure, cnrfc_timestamp, r4_timestamp_style, cnrfc_timestamp, r30_timestamp_style

    # Interval update for Abay graphs
    @app.callback(
        [Output('my-tank2', 'value'), Output('my-tank2', 'max'), Output("abay_bargraph", 'figure')],
        [Input("dummy-dataframe", "children"), Input("abay_bargraph", "figure"),
         Input('interval-component', 'n_intervals')],
    )
    def abay_graphs(json_data, figure, n_intervals):
        df_full = pd.read_json(json_data, orient='index')
        df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')
        value = round(df_full["Afterbay_Elevation"].iloc[-1], 1),
        max = df_full["Afterbay_Elevation_Setpoint"].values.max()

        df_full_hourly = df_full.resample('60min', on="Timestamp").mean()

        # Blue fill bar (elevation)
        figure['data'][0]['x'] = df_full_hourly.index[-10:].to_numpy()
        figure['data'][0]['y'] = df_full_hourly['Afterbay_Elevation'][-10:].to_numpy()

        # Empty Bar giving perception of range.
        figure['data'][1]['x'] = df_full_hourly.index[-10:].to_numpy()
        figure['data'][1]['y'] = df_full_hourly['Afterbay_Elevation_Setpoint'][-10:].to_numpy() - df_full_hourly["Afterbay_Elevation"][-10:].to_numpy()

        return value, max, figure


def update_data(meters, rfc_json_data):
    # This will store the data for all the PI requests
    df_all = pd.DataFrame()

    meters = [PiRequest("OPS", "R4", "Flow"), PiRequest("OPS", "R11", "Flow"),
              PiRequest("OPS", "R30", "Flow"), PiRequest("OPS", "Afterbay", "Elevation"),
              PiRequest("OPS", "Afterbay", "Elevation Setpoint"),
              PiRequest("OPS", "Oxbow", "Power"),
              PiRequest("Energy_Marketing", None, "GEN_MDFK_and_RA"),
              PiRequest("Energy_Marketing", None, "ADS_MDFK_and_RA"),
              PiRequest("Energy_Marketing", None, "ADS_Oxbow"),
              PiRequest("Energy_Marketing", None, "Oxbow_Forecast")
                ]
    for meter in meters:
        try:
            df_meter = pd.DataFrame.from_dict(meter.data)

            # If there was an error getting the data, you will have an empty dataframe, escape for loop
            if df_meter.empty:
                return None

            # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
            df_meter.Timestamp = pd.to_datetime(df_meter.Timestamp).dt.tz_convert('US/Pacific')
            df_meter.index = df_meter.Timestamp
            df_meter.index.names = ['index']

            # Remove any outliers or data spikes
            # df_meter = drop_numerical_outliers(df_meter, meter, z_thresh=3)

            # Rename the column (this was needed if we wanted to merge all the Value columns into a dataframe)
            renamed_col = (f"{meter.meter_name}_{meter.attribute}").replace(' ', '_')

            # For attributes in the Energy Marketing folder, the name is "None", so just use attribute
            if meter.meter_name is None:
                renamed_col = (f"{meter.attribute}").replace(' ', '_')
            df_meter.rename(columns={"Value": f"{renamed_col}"}, inplace=True)

            if df_all.empty:
                df_all = df_meter
            else:
                df_all = pd.merge(df_all, df_meter[["Timestamp", renamed_col]], on="Timestamp", how='outer')

        except ValueError:
            print('Pandas Dataframe May Be Empty')
            return None

    # The first time this code is hit, the div containing the data should not have the
    # CNRFC data in it. Therefore, we need to download it.
    if not rfc_json_data:
        ######################   CNRFC SECTION ######################################
        # Get the CNRFC Data. Note, we are putting this outside the PI request since
        # it's entirely possible these data are not avail. If it fails, it will just
        # skip over this portion and return a df without the CNRFC data
        today12z = datetime.now().strftime("%Y%m%d12")
        yesterday12z = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d12")
        file_dates = [yesterday12z, today12z]
        df_cnrfc_list = []
        most_recent_file = None
        for file in file_dates:
            try:
                df_cnrfc_list.append(pd.read_csv(f"https://www.cnrfc.noaa.gov/csv/{file}_american_csv_export.zip"))
                most_recent_file = file  # The date last file successfully pulled.
            except HTTPError as error:
                print(f'CNRFC HTTP Request failed {error} for {file}')

        # The last element in the list will be the most current forecast. Get that one.
        df_cnrfc = df_cnrfc_list[-1].copy()

        # Put the forecast issued time in the dataframe so we can refer to it later.
        df_cnrfc["FORECAST_ISSUED"] = pd.to_datetime(datetime.strptime(most_recent_file, "%Y%m%d%H"))

        # Drop first row (the header is two rows and the 2nd row gets put into row 1 of the df; delete it)
        df_cnrfc = df_cnrfc.iloc[1:]

        # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_localize('UTC').dt.tz_convert('US/Pacific')

        df_cnrfc.rename(columns={"MFAC1L": "R20_fcst", "RUFC1": "R30_fcst", "MFPC1": "R4_fcst", "MFAC1": "R11_fcst"}, inplace=True)
        df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]].apply(
            pd.to_numeric) * 1000
    else:
        df_cnrfc = pd.read_json(rfc_json_data, orient='index')
    ######################## END CNRFC ###########################################
    return df_all, df_cnrfc


def drop_numerical_outliers(df, meter, z_thresh):
    # Constrains will contain `True` or `False` depending on if it is a value below the threshold.
    # 1) For each column, first it computes the Z-score of each value in the column,
    #   relative to the column mean and standard deviation.
    # 2) Then is takes the absolute of Z-score because the direction does not matter,
    #   only if it is below the threshold.
    # 3) all(axis=1) ensures that for each row, all column satisfy the constraint.

    # Note: The z-score will return NAN if all values are exactly the same. Therefore,
    #       if all values are the same, return the original dataframe and consider the data valid, otherwise the
    #       data won't pass the z-score test and the data will be QC'ed out.
    u = df["Value"].to_numpy()
    if (u[0] == u).all(0):
        return df

    orig_size = df.shape[0]
    constrains = df.select_dtypes(include=[np.number]) \
        .apply(lambda x: np.abs(stats.zscore(x)) < z_thresh, result_type='reduce') \
        .all(axis=1)
    # Drop (inplace) values set to be rejected
    df.drop(df.index[~constrains], inplace=True)

    if df.shape[0] != orig_size:
        print(f"A total of {orig_size - df.shape[0]} data spikes detected in {meter.meter_name}. "
              f" The data have been removed")
    return df

if __name__ == '__main__':
    app.run_server(debug=True)


