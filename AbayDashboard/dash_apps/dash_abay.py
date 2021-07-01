import os
from io import BytesIO
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from zipfile import ZipFile
import copy
import requests
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta, time, timezone
import pytz
import numpy as np
from django.contrib.staticfiles.storage import staticfiles_storage
from dash.dependencies import Input, Output, State
from plotly import graph_objs as go
from datetime import datetime as dt
from django_plotly_dash import DjangoDash
import dash_bootstrap_components as dbc
import dash_daq as daq
from scipy import stats
import json
#from .dash_abay_extras.layout import top_cards, main_layout
from .dash_abay_extras.layout_new import top_cards, second_cards, main_layout
from ..mailer import send_mail
import psutil
import logging

# ###To run the app on it's own (not in Django), you would do:
# app = dash.Dash()
# ###Then at the bottom you would do the following:
# if __name__ == '__main__':
#    app.run_server(debug=True)
### Then you'd just run python from the terminal > python dash_first.py
# app = DjangoDash('UberExample')

# app = dash.Dash(
#     __name__, meta_tags=[{"name": "viewport", "content": "width=device-width" }],
#     external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css"],
#     external_scripts=["https://code.jquery.com/jquery-3.5.1.min.js",
#                       'https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/js/materialize.min.js',
#                       ]
# )
#
# # app = dash.Dash()

class PiRequest:
    #
    # https://flows.pcwa.net/piwebapi/assetdatabases/D0vXCmerKddk-VtN6YtBmF5A8lsCue2JtEm2KAZ4UNRKIwQlVTSU5FU1NQSTJcT1BT/elements
    def __init__(self, db, meter_name, attribute, forecast=False):
        self.db = db  # Database (e.g. "Energy Marketing," "OPS")
        self.meter_name = meter_name  # R4, Afterbay, Ralston
        self.attribute = attribute  # Flow, Elevation, Lat, Lon, Storage, Elevation Setpoint, Gate 1 Position, Generation
        self.baseURL = 'https://flows.pcwa.net/piwebapi/attributes'
        self.forecast = forecast
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
        end_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00-00:00")
        if self.forecast:
            end_time = (datetime.utcnow() + timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:00-00:00")
        try:
            response = requests.get(
                url=self.url,
                params={"startTime": (datetime.utcnow() + timedelta(hours=-24)).strftime("%Y-%m-%dT%H:%M:00-00:00"),
                        "endTime": end_time,
                        "interval": "1m",
                        },
            )
            print(f'Response HTTP Status Code: {response.status_code} for {self.meter_name} | {self.attribute}')
            j = response.json()
            # We only want the "Items" object.
            return j["Items"]
        except requests.exceptions.RequestException:
            logging.warning(f"HTTP Failed For {self.meter_name} | {self.attribute}")
            print('HTTP Request failed')
            return None

    def meter_element_type(self):
        if not self.meter_name:
            return None
        if self.attribute == "Flow":
            return "Gauging Stations"
        if "Afterbay" in self.meter_name or "Hell Hole" in self.meter_name:
            return "Reservoirs"
        if "Middle Fork" in self.meter_name or "Oxbow" in self.meter_name:
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
    logging.basicConfig(filename='abay_err.log', level=logging.DEBUG)
    logging.info(f"Starting: {datetime.now().strftime('%a, %d %b %p')}")
    # This will store the data for all the PI requests
    df_all, df_cnrfc = update_data(meters, None)

    # This is for the abay levels. We're just going to show the data on an hourly basis.
    #
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

    second_row_cards = second_cards(df_all, df_hourly_resample)

    app.layout = main_layout(top_row_cards, second_row_cards, locations_types)

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
         Input("dummy-rfc-dataframe", "children"),
        Input("ox_switch_span", "n_clicks")]
    )
    def add_oxbow_forecast(figure, json_data, rfc_json_data, n_clicks):
        ctx = dash.callback_context

        # The ID triggering the callback
        toggle_id = ctx.triggered[0]['prop_id'].split('.')[0]
        df_full = pd.read_json(json_data, orient='index')
        df_cnrfc = pd.read_json(rfc_json_data, orient='index')

        df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_convert('US/Pacific')
        #test = df_cnrfc[df_cnrfc["Oxbow_fcst"].notnull()]
        # Initial Load, don't show forecast
        if toggle_id == 'dummy-dataframe' and len(figure['data'])<2:
            figure['data'].append(
                dict(x=df_cnrfc["GMT"][df_cnrfc["Oxbow_fcst"].notnull()],
                     y=df_cnrfc["Oxbow_fcst"][df_cnrfc["Oxbow_fcst"].notnull()],
                              mode='lines',
                              visible=False,
                              hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>", ))
        if n_clicks % 2 and len(figure['data']) > 0:
            figure['data'][1]['visible'] = True
        else:
            figure['data'][1]['visible'] = False

        return figure

    @app.callback(
        Output('abay_sparkline', 'figure'),
        [Input("abay_sparkline", "figure"),
         Input("dummy-dataframe", "children"),
         Input("dummy-rfc-dataframe", "children"),
         Input("abay_switch_span", "n_clicks")]
    )
    def add_abay_forecast(figure, json_data, rfc_json_data, n_clicks):
        ctx = dash.callback_context

        # The ID triggering the callback
        toggle_id = ctx.triggered[0]['prop_id'].split('.')[0]
        df_full = pd.read_json(json_data, orient='index')
        df_cnrfc = pd.read_json(rfc_json_data, orient='index')

        df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_convert('US/Pacific')
        # test = df_cnrfc[df_cnrfc["Oxbow_fcst"].notnull()]
        # Initial Load, don't show forecast
        if toggle_id == 'dummy-dataframe' and len(figure['data']) < 2:
            figure['data'].append(
                dict(x=df_cnrfc["GMT"][df_cnrfc["Abay_Elev_Fcst"].notnull()],
                     y=df_cnrfc["Abay_Elev_Fcst"][df_cnrfc["Abay_Elev_Fcst"].notnull()],
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
        if toggle_id == 'dummy-dataframe' or toggle_id == 'interval-component':   # Call back not hit by switch, redraw graph
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
                logging.warning("The PI data or CNRFC data are not being stored in a DIV. Please check why")
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
                logging.warning("CNRFC Data are unavail")
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
            logging.warning("User tried to load CNRFC plot, but plot appears unavail")
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
        [Output('my-tank2', 'value'), Output('my-tank2', 'max'),
         Output("abay_bargraph", 'figure'), Output("abay_float_txt", 'children')],
        [Input("dummy-dataframe", "children"), Input("abay_bargraph", "figure"),
         Input('interval-component', 'n_intervals')],
    )
    def abay_graphs(json_data, figure, n_intervals):
        df_full = pd.read_json(json_data, orient='index')
        df_full.Timestamp = df_full['Timestamp'].dt.tz_convert('US/Pacific')
        value = round(df_full["Afterbay_Elevation"].iloc[-1], 1),
        max = df_full["Afterbay_Elevation_Setpoint"].values.max()
        abay_float = f" Float: {int(df_full['Afterbay_Elevation_Setpoint'].iloc[-1])}'"

        df_full_hourly = df_full.resample('60min', on="Timestamp").mean()

        # Blue fill bar (elevation)
        figure['data'][0]['x'] = df_full_hourly.index[-10:].to_numpy()
        figure['data'][0]['y'] = df_full_hourly['Afterbay_Elevation'][-10:].to_numpy()

        # Empty Bar giving perception of range.
        figure['data'][1]['x'] = df_full_hourly.index[-10:].to_numpy()
        figure['data'][1]['y'] = df_full_hourly['Afterbay_Elevation_Setpoint'][-10:].to_numpy() - df_full_hourly["Afterbay_Elevation"][-10:].to_numpy()

        return value, max, figure, abay_float

    logging.info(f"Done: {datetime.now().strftime('%a, %d %b %p')}")
    # Callback for Abay Forecast


def update_data(meters, rfc_json_data):
    # This will store the data for all the PI requests
    df_all = pd.DataFrame()

    meters = [PiRequest("OPS", "R4", "Flow"), PiRequest("OPS", "R11", "Flow"),
              PiRequest("OPS", "R30", "Flow"), PiRequest("OPS", "Afterbay", "Elevation"),
              PiRequest("OPS", "Afterbay", "Elevation Setpoint"),
              PiRequest("OPS", "Oxbow", "Power"), PiRequest("OPS","R5","Flow"),
              PiRequest("OPS","Hell Hole","Elevation"),
              PiRequest("Energy_Marketing", None, "GEN_MDFK_and_RA"),
              PiRequest("Energy_Marketing", None, "ADS_MDFK_and_RA"),
              PiRequest("Energy_Marketing", None, "ADS_Oxbow"),
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

        except ValueError as e:
            print('Pandas Dataframe May Be Empty')
            logging.warning(f"Updating PI data produced empty data frame. Error: {e}")
            return None

    # PMIN / PMAX Calculations
    const_a = 0.09      # Default is 0.0855.
    const_b = 0.135422  # Default is 0.138639
    try:
        df_all["Pmin1"] = const_a*(df_all["R4_Flow"]-df_all["R5_Flow"])
        df_all["Pmin2"] = (-0.14*(df_all["R4_Flow"]-df_all["R5_Flow"])*
                              ((df_all["Hell_Hole_Elevation"]-2536)/(4536-2536)))
        df_all["Pmin"] = df_all[["Pmin1","Pmin2"]].max(axis=1)

        df_all["Pmax1"] = ((const_a+const_b)/const_b)*(124+(const_a*df_all["R4_Flow"]-df_all["R5_Flow"]))
        df_all["Pmax2"] = ((const_a+const_b)/const_a)*(86-(const_b*df_all["R4_Flow"]-df_all["R5_Flow"]))

        df_all["Pmax"] = df_all[["Pmax1","Pmax2"]].min(axis=1)

        df_all.drop(["Pmin1","Pmin2", "Pmax1", "Pmax2"], axis=1, inplace=True)
    except ValueError as e:
        print("Can Not Calculate Pmin or Pmax")
        df_all[["Pmin", "Pmax"]] = np.nan
        logging.info(f"Unable to caluclate Pmin or Pmax {e}")

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
            except (HTTPError, URLError) as error:
                logging.warning(f'CNRFC HTTP Request failed {error} for {file}. Error code: {error}')
                print(f'CNRFC HTTP Request failed {error} for {file}')

        # The last element in the list will be the most current forecast. Get that one.
        df_cnrfc = df_cnrfc_list[-1].copy()

        # Case for failed download and empty dataframe
        if df_cnrfc.empty:
            df_cnrfc = pd.date_range(start=datetime.utcnow() - timedelta(hours=48),
                                     end= datetime.utcnow() + timedelta(hours=72), freq='H', normalize=True)
            df_cnrfc[["FORECAST_ISSUED", "R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = np.nan

        # Download was successful, continue
        else:
            # Put the forecast issued time in the dataframe so we can refer to it later.
            df_cnrfc["FORECAST_ISSUED"] = pd.to_datetime(datetime.strptime(most_recent_file, "%Y%m%d%H"))

            # Drop first row (the header is two rows and the 2nd row gets put into row 1 of the df; delete it)
            df_cnrfc = df_cnrfc.iloc[1:]

            # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
            df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_localize('UTC').dt.tz_convert('US/Pacific')

            df_cnrfc.rename(columns={"MFAC1L": "R20_fcst", "RUFC1": "R30_fcst", "MFPC1": "R4_fcst", "MFAC1": "R11_fcst"}, inplace=True)
            df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]].apply(
                pd.to_numeric) * 1000
    # Dataframe already exists in html
    else:
        df_cnrfc = pd.read_json(rfc_json_data, orient='index')
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_convert('US/Pacific')
    ######################## END CNRFC ###########################################

    # Add in the remainder of any forecast data (e.g. Oxbow Forecast, Abay Fcst) to the cnrfc dataframe
    if 'Oxbow_fcst' not in df_cnrfc:
        df_cnrfc = abay_forecast(df_cnrfc, df_all)
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


def abay_forecast(df, df_pi):
    # PMIN / PMAX Calculations
    const_a = 0.09  # Default is 0.0855.
    const_b = 0.135422  # Default is 0.138639

    ########## GET OXBOW GENERRATION FORECAST DATA ###################
    try:
        # Download the data for the Oxbow and MFPH Forecast (data start_time is -24 hours from now, end time is +72 hrs)
        pi_data_ox = PiRequest("OPS", "Oxbow", "Forecasted Generation", True)
        # pi_data_gen = PiRequest("Energy_Marketing", None, "MFRA_Forecast", True)

        df_fcst = pd.DataFrame.from_dict(pi_data_ox.data)

        # This will need to be changed to the following:
        # df_fcst["MFRA_fcst"] = pd.DataFrame.from_dict(pi_data_gen.data)['Value']

        df_fcst["MFRA_fcst"] = pd.DataFrame.from_dict(pi_data_ox.data)['Value']

        # For whatever reason, the data are of type "object", need to convert to float.
        df_fcst["MFRA_fcst"] = pd.to_numeric(df_fcst.MFRA_fcst, errors='coerce')

        # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
        df_fcst.Timestamp = pd.to_datetime(df_fcst.Timestamp).dt.tz_convert('US/Pacific')
        df_fcst.index = df_fcst.Timestamp
        df_fcst.index.names = ['index']

        # For whatever reason, the data are of type "object", need to convert to float.
        df_fcst["Value"] = pd.to_numeric(df_fcst.Value, errors='coerce')

        df_fcst.rename(columns={"Value": "Oxbow_fcst"}, inplace=True)

        # These columns can't be resampled to hourly (they contain strings), so remove them.
        df_fcst.drop(["Good", "Questionable", "Substituted", "UnitsAbbreviation"], axis=1, inplace=True)

        # Resample the forecast to hourly to match CNRFC time. If this is not done, the following merge will fail.
        # The label = right tells this that we want to use the last time in the mean as the label (i.e. hour ending)
        df_fcst = df_fcst.resample('60min', label='right').mean()

        # Merge the forecast to the CNRFC using the GMT column for the cnrfc and the index for the oxbow fcst data.
        df = pd.merge(df, df_fcst[["Oxbow_fcst", "MFRA_fcst"]], left_on="GMT", right_index=True, how='outer')

        # Calculate the Pmin and Pmax in the same manner as with the historical data.
        df["Pmin1"] = const_a * (df["R4_fcst"] - 26)
        df["Pmin2"] = (-0.14 * (df["R4_fcst"] - 26) * ((df_pi["Hell_Hole_Elevation"].iloc[-1] - 2536) / (4536 - 2536)))

        df["Pmin"] = df[["Pmin1", "Pmin2"]].max(axis=1)

        df["Pmax1"] = ((const_a + const_b) / const_b) * (
                    124 + (const_a * df["R4_fcst"] - df_pi["R5_Flow"].iloc[-1]))
        df["Pmax2"] = ((const_a + const_b) / const_a) * (
                    86 - (const_b * df["R4_fcst"] - df_pi["R5_Flow"].iloc[-1]))

        df["Pmax"] = df[["Pmax1", "Pmax2"]].min(axis=1)

        # Drop unnesessary columns.
        df.drop(["Pmin1", "Pmin2", "Pmax1", "Pmax2"], axis=1, inplace=True)
    except Exception as e:
        print(f"Could Not Find Metered Forecast Data (e.g. Oxbow Forecast): {e}")
        df["Oxbow_fcst"] = np.nan
        logging.warning(f"Could Not Find Metered Forecast Data (e.g. Oxbow Forecast). Error Message: {e}")
    ################### END OXBOW FORECAST ##############################

    # Default ratio of the contribution of total power that is going to Ralston.
    RAtoMF_ratio = 0.41

    # 1 cfs = 0.0826 acre feet per hour
    cfs_to_afh = 0.0826448

    CCS = False

    # The last reading in the df for the float set point
    float = df_pi["Afterbay_Elevation_Setpoint"].iloc[-1]

    #df_pi.set_index('Timestamp', inplace=True)
    #abay_inital = df_pi["Afterbay_Elevation"].truncate(before=(datetime.now(timezone.utc)-timedelta(hours=24)))

    # The PI data we retrieve goes back 24 hours. The initial elevation will give us a chance to test the expected
    # abay elevation vs the actual abay elevation. The abay_initial is our starting point.
    # Note: For resampled data over an hour, the label used for the timestamp is the first time stamp, but since
    #       we want hour ending, we want the last time to be used at the label (label = right).
    df_pi_hourly = df_pi.resample('60min', on='Timestamp', label='right').mean()

    # Get any observed values that have already occurred from the PI data.
    df_pi_hourly["RA_MW"] = np.minimum(86, df_pi_hourly["GEN_MDFK_and_RA"] * RAtoMF_ratio)
    df_pi_hourly["MF_MW"] = np.minimum(128, df_pi_hourly["GEN_MDFK_and_RA"] - df_pi_hourly['RA_MW'])

    # Elevation observed at the beginning of our dataset (24 hours ago). This serves as the starting
    # point for our forecast, so that we can see if it's trued up as we go forward in time.
    # Convert elevation to AF ==> y = 0.6334393x^2 - 1409.2226x + 783749
    df_pi_hourly["Abay_AF_Observed"] = (0.6334393 * (df_pi_hourly["Afterbay_Elevation"] ** 2)) - 1409.2226 * df_pi_hourly[
        "Afterbay_Elevation"] + 783749
    abay_inital_af = df_pi_hourly["Abay_AF_Observed"].iloc[0]
    df_pi_hourly["Abay_AF_Change_Observed"] = df_pi_hourly["Abay_AF_Observed"].diff()

    # Ralston's Max output is 86 MW; so we want smaller of the two.
    df["RA_MW"] = np.minimum(86, df["MFRA_fcst"] * RAtoMF_ratio)
    df["MF_MW"] = np.minimum(128, df["MFRA_fcst"]-df['RA_MW'])

    # This is so we can do the merge below (we need both df's to have the same column name). The goal is to overwrite
    # any "forecast" data for Oxbow with observed values. There is no point in keeping forecast values in.
    df_pi_hourly.rename(columns={"Oxbow_Power": "Oxbow_fcst"}, inplace=True)

    # This is a way to "update" the generation data with any observed data. First merge in any historical data.
    df = pd.merge(df, df_pi_hourly[["RA_MW", "MF_MW", "Oxbow_fcst", "Abay_AF_Observed", "Abay_AF_Change_Observed"]],
                  left_on="GMT", right_index=True, how='left')

    # Next, since we already have an RA_MF column, the merge will make a _x and _y. Just fill the original with
    # the new data (and any bad data will be nan) and store all that data as RA_MW.
    df["RA_MW"] = df['RA_MW_y'].fillna(df['RA_MW_x'])
    df["MF_MW"] = df['MF_MW_y'].fillna(df['MF_MW_x'])
    df["Oxbow_fcst"] = df['Oxbow_fcst_y'].fillna(df['Oxbow_fcst_x'])

    # We don't need the _y and _x, so drop them.
    df.drop(['RA_MW_y', 'RA_MW_x', 'MF_MW_y', 'MF_MW_x', 'Oxbow_fcst_x','Oxbow_fcst_y'], axis=1, inplace=True)

    # Conversion from MW to cfs ==> CFS @ Oxbow = MW * 163.73 + 83.956
    df["Oxbow_Outflow"] = (df["Oxbow_fcst"] * 163.73) + 83.956

    # R5 Valve never changes (at least not in the last 5 years in PI data)
    df["R5_Valve"] = 28

    # If CCS is on, we need to account for the fact that Ralston will run at least at the requirement for the Pmin.
    if CCS:
        #df["RA_MW"] = max(df["RA_MW"], min(86,((df["R4_fcst"]-df["R5_Valve"])/10)*RAtoMF_ratio))
        df["RA_MW"] = np.maximum(df["RA_MW"], df["Pmin"] * RAtoMF_ratio)

    # Polynomial best fits for conversions.
    df["RA_Inflow"] = (0.0005*(df["RA_MW"]**3))-(0.0423*(df["RA_MW"]**2))+(10.266*df["RA_MW"]) + 2.1879
    df["MF_Inflow"] = (0.0049 * (df["MF_MW"] ** 2)) + (6.2631 * df["MF_MW"]) + 18.4

    # The linear MW to CFS relationship above doesn't apply if Generation is 0 MW. In that case it's 0 (otherwise the
    # value would be 83.956 due to the y=mx+b above where y = b when x is zero, we need y to = 0 too).
    df.loc[df['MF_MW'] == 0, 'RA_Inflow'] = 0
    df.loc[df['RA_MW'] == 0, 'MF_Inflow'] = 0
    df.loc[df['Oxbow_fcst'] == 0, 'Oxbow_Outflow'] = 0

    # It helps to look at the PI Vision screen for this.
    # Ibay In: 1) Inflow from MFPH (the water that's powering MFPH)
    #          2) The water flowing in at R4
    # Ibay Out: 1) Valve above R5 (nearly always 28)         = 28
    #           2) Outflow through tunnel to power Ralston.  = RA_out (CAN BE INFLUENCED BY CCS MODE, I.E. R4)
    #           3) Spill                                     = (MF_IN - RA_OUT) + R4
    #
    #                                |   |
    #                                |   |
    #                                |   |
    #                          ___MFPH INFLOW____
    #                          |                |
    #     OUTFLOW (RA INFLOW)  |                |  R4 INFLOW
    #                    ------|            <---|--------
    #               <--- ------|                |--------
    #                          |                |
    #                           ---SPILL+R5----
    #                                |   |
    #                R20             |   |
    #                --------------- |   |
    #                ---------------------
    #
    #           Inflow into IBAY  = MF_GEN_TO_CFS (via day ahead forecast --> then converted to cfs) + R4 Inflow
    #             Inflow to ABAY  = RA_GEN_TO_CFS (either via DA fcst or R4 if CCS is on) + R20
    #        Where RA_GEN_TO_CFS  = MF_GEN_TO_CFS * 0.41
    #                         R20 = R20_RFC_FCST + SPILL + R5
    #                       SPILL = R4_RFC_Fcst + MAX(0,(MF_GEN_TO_CFS - RA_GEN_TO_CFS)) + R5
    #        THEREFORE:
    #        Inflow Into Abay = RA_GEN_TO_CFS + R20_RFC_FCST + R4_RFC_fcst + AX(0,(MF_GEN_TO_CFS - RA_GEN_TO_CFS)) + R5
    #
    #        CALCULATION ERRORS:
    #        The error between the forecast and the observed is usually fairly consistent on a 24 hour basis (e.g. in
    #        our abay tracker we have a +Fill -Drain adder that we can apply).
    #        In order to compensate for errors, we will calculate the Observed change in Acre Feet vs the forecast
    #        and convert this error to cfs. The average of this error will be added to the forecast to adjust for the
    #        observed error.
    #
    # Ibay In - Ibay Out = The spill that will eventually make it into Abay through R20.
    df["Ibay_Spill"] = np.maximum(0,(df["MF_Inflow"] - df["RA_Inflow"])) + df["R5_Valve"] + df['R4_fcst']

    # CNRFC is just forecasting natural flow, which I believe is just everything from Ibay down. Therefore, it should
    # always be too low and needs to account for any water getting released from IBAY.
    df["R20_fcst_adjusted"] = df["R20_fcst"] + df["Ibay_Spill"]

    df["Abay_Inflow"] = df["RA_Inflow"]+df["R20_fcst_adjusted"]+df["R30_fcst"]
    df["Abay_Outflow"] = df["Oxbow_Outflow"]

    df["Abay_AF_Change"] = (df["Abay_Inflow"]-df["Abay_Outflow"])*cfs_to_afh

    # Calculate the error by taking the value of the forecast - the value of the observed
    df["Abay_AF_Change_Error"] = df["Abay_AF_Change"] - df["Abay_AF_Change_Observed"]

    # Convert the AF error to CFS (this will be in case we want to graph the errors).
    df["Abay_CFS_Error"] = df["Abay_AF_Change_Error"] * (1/cfs_to_afh)

    # Normally, the errors over a 24 hour period are pretty consistent. So just average the error.
    cfs_error = df["Abay_CFS_Error"].mean()
    af_error = df["Abay_AF_Change_Error"].mean()

    # To get the AF elevation forecast, take the initial reading and apply the change. Also add in the error.
    first_valid = df["Abay_AF_Change"].first_valid_index()
    for i in range(first_valid, len(df)):
        if i == first_valid:
            df.loc[i, "Abay_AF_Fcst"] = abay_inital_af
        else:
            df.loc[i, "Abay_AF_Fcst"] = df.loc[i-1,"Abay_AF_Fcst"] + df.loc[i, "Abay_AF_Change"] - af_error

    # Change from AF to Elevation
    # y = -1.4663E-6x^2+0.019776718*x+1135.3
    df["Abay_Elev_Fcst"] = np.minimum(float, (-0.0000014663 *
                                             (df["Abay_AF_Fcst"] ** 2)+0.0197767158*df["Abay_AF_Fcst"]+1135.3))
    return df


if __name__ == 'AbayDashboard.dash_apps.dash_abay':
    # meters = [PiRequest("R4", "Flow"), PiRequest("R11", "Flow"),
    #           PiRequest("R30", "Flow"), PiRequest("Afterbay", "Elevation"),
    #           PiRequest("Afterbay", "Elevation Setpoint"),
    #           PiRequest("Middle Fork", "Power - (with Ralston)"),
    #           PiRequest("Oxbow", "Power")]
    main(None)


