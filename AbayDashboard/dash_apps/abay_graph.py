import dash
import dash_core_components as dcc
import dash_html_components as html
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wx.settings')
django.setup()
from django.contrib.staticfiles.storage import staticfiles_storage
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
import dash_daq as daq
import requests
import logging
from urllib.error import HTTPError, URLError
import numpy as np
from datetime import datetime, timedelta, timezone
from dash.dependencies import Input, Output, State
import dash_table
import base64
import io
import re
import math
from plotly import graph_objs as go
from plotly.subplots import make_subplots
from plotly.colors import n_colors, make_colorscale
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from scipy import stats


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

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css',
                        'https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css',
                        'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',
                        'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css']
external_javascript = ["https://code.jquery.com/jquery-3.5.1.min.js", "https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.bundle.min.js"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, external_scripts=external_javascript)


def main():
    df_pi, df_cnrfc = update_data(None, None)
    df_hourly_resample = df_pi.resample('60min', on='Timestamp', label='right').mean()
    df = pd.DataFrame({"Date": pd.date_range(start=datetime.utcnow() + timedelta(days=-1),
                                             end=datetime.utcnow() + timedelta(days=3), freq='H', normalize=True)})
    df["Hi_Spill"] = df_hourly_resample["Afterbay_Elevation_Setpoint"].iloc[-1]
    df["Hi_Alarm"] = df_hourly_resample["Afterbay_Elevation_Setpoint"].iloc[-1] - 0.5
    df["Hi_Caution"] = df_hourly_resample["Afterbay_Elevation_Setpoint"].iloc[-1] - 1.5
    df["Lo_Limit"] = 1168
    df["Lo_Alarm"] = df["Lo_Limit"] + 0.5
    df["Lo_Caution"] = df["Lo_Limit"] + 1.5

    fig = go.Figure()
    # Create and style traces
    fig.add_trace(go.Scatter(x=df_hourly_resample.index, y=df_hourly_resample['Afterbay_Elevation'],
                             name='Abay Elevation',
                             mode='lines+markers',
                             hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                             line=dict(color='#002699', width=4)))
    fig.add_trace(go.Scatter(x=df_cnrfc["GMT"][df_cnrfc["Abay_Elev_Fcst"].notnull()],
                     y=df_cnrfc["Abay_Elev_Fcst"][df_cnrfc["Abay_Elev_Fcst"].notnull()],
                             name='Abay Elevation_Fcst',
                             hovertemplate="%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                             mode='lines+markers',
                             line=dict(color='orange', width=4)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Hi_Spill"], name= 'Spill',
                             line=dict(color='red', width=2)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Hi_Alarm"], name='Alarrm',
                             line=dict(color='#ff3399', width=2, dash='dash')))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Hi_Caution"], name='Caution',
                             line=dict(color='#009900', width=2, dash='dot')))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Lo_Caution"], name='Caution',
                             line = dict(color='#009900', width=2, dash='dot')))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Lo_Limit"], name='Low Limit',
                             line=dict(color='red', width=2,)))

    cfs_error_arrow = 'fas fa fa-caret-down text-danger'
    abay_out_arrow = 'fas fa fa-caret-down text-danger'
    abay_in_arrow = 'fas fa fa-caret-up text-success'
    last_observed_row = df_cnrfc["Abay_AF_Observed"].last_valid_index()
    if df_cnrfc["Abay_CFS_Error"].mean() >= 0:
        cfs_error_arrow = 'fas fa fa-caret-up text-success'


    info_cards = dbc.Row([
        html.Div(children=[
            html.Div(children=[
                html.Div(children=[
                    'Afterbay Flow'
                ], className='text-uppercase text-tracked mb-2'),
                html.Div(children=[
                    html.I(className=abay_in_arrow),
                    html.Span(children=[int(df_cnrfc["Abay_Inflow"].iloc[last_observed_row])],
                              className="mx-1 text-monospace"),
                    "Abay Inflow"
                ]),
                html.Div(children=[
                    html.I(className=abay_out_arrow),
                    html.Span(children=[int(df_cnrfc["Abay_Outflow"].iloc[last_observed_row])],
                              className="mx-1 text-monospace"),
                    "Abay Outflow"
                ]),
                html.Div(children=[
                    html.I(className=cfs_error_arrow),
                    html.Span(children=[f"{int(df_cnrfc['Abay_CFS_Error'].mean())} CFS"],
                              className="mx-1 text-monospace"),
                    "Flow Error"
                ]),
                html.Span(children=['Next Change:'], className="small"),
                html.Span(children=['3.4 MW @ 3:00 p.m.'], className="mall text-monospace mx-auto"),
            ], className='d-flex flex-column p-3 m-3 bg-white shadow-sm '
                         'rounded animated flipInX delay-8'),
        ], className="col-sm-12 col-md-12 col-lg-4 mb-3 pr-md-2", style={"color":"black"})
    ], className='top-cards no-gutters')

    # Edit the layout
    fig.update_layout(height=500,
                      xaxis_title='Month',
                      yaxis_title='Elevation (ft)',
                      yaxis_range=[1167,1177],
                      hovermode="closest",
                      legend=dict(font=dict(size=10), orientation="h"),
                      # This hard codes the background. theme="plotly_dark" works if this line is removed.
                      margin=dict(
                          l=20,
                          r=20,
                          b=20,
                          t=20,
                          pad=4
                      ),)

    app.layout = html.Main(
        className='content container',
        children=[
            info_cards,
            dbc.Row([
                dbc.Col([
                    html.Div(id='container',
                      children=[
                          dcc.Graph(id="abay_chart",
                                    figure=fig)
                      ])
                ], width=6),
                html.Div(children=[
                    dcc.Dropdown(
                        id='abay-dropdown',
                        options=[
                            {'label': 'Abay Inflow', 'value': 'Abay_Inflow'},
                            {'label': 'Abay Outflow', 'value': 'Abay_Outflow'},
                            {'label': 'River Flows', 'value': 'River_Flows'},
                            {'label': 'River Flow Error', 'value': 'Abay_CFS_Error'},
                            {'label': 'Total Gen', 'value': 'Total_Gen'},
                            {'label': 'RA and MF', 'value': 'RAandMF'},
                            {'label': 'Abay AF Change (Obs)', 'value': 'Abay_AF_Change_Observed'},
                            {'label': 'Abay AF Change (Fcst)', 'value': 'Abay_AF_Change'},
                            {'label': 'Pmin/Pmax', 'value': 'Pmin_Pmax'},
                        ],
                        value=[],
                        multi=True
                    ),
                    dcc.Graph(id='table_graph',
                        figure=go.Figure(
                            data=[go.Table(
                                header=dict(
                                    values=["<b>Day</b>", "<b>HE</b>", "<b>Oxbow</b>", "<b>Abay Fcst</b>", "Abay Inflow", "Abay Outflow"],
                                    line_color='white', fill_color='gray',
                                    align='center', font=dict(color='black', size=12)
                                ),
                                cells=dict(
                                    values=[
                                        # Day of week
                                        (df_cnrfc["GMT"][df_cnrfc["Oxbow_fcst"].notnull()]).dt.strftime("%a"),
                                        # HE
                                        (df_cnrfc["GMT"][df_cnrfc["Oxbow_fcst"].notnull()]).dt.strftime("%H"),
                                        # Oxbow Forecast
                                        (df_cnrfc["Oxbow_fcst"][df_cnrfc["Oxbow_fcst"].notnull()]).round(1),
                                        # Abay Elev
                                        (df_cnrfc["Abay_Elev_Fcst"][df_cnrfc["Oxbow_fcst"].notnull()]).round(2),
                                        # Abay Inflow
                                        (df_cnrfc["Abay_Inflow"][df_cnrfc["Oxbow_fcst"].notnull()]).round(2),
                                        # Abay Outflow
                                        (df_cnrfc["Abay_Outflow"][df_cnrfc["Oxbow_fcst"].notnull()]).round(2),
                                    ],
                                    line_color=['black'],
                                    fill_color=[
                                        # Day of week
                                        conditional_cell_formating(
                                            (df_cnrfc["GMT"][df_cnrfc["Oxbow_fcst"].notnull()]),
                                            len(df_cnrfc["GMT"][df_cnrfc["Abay_AF_Observed"].notnull()].index),None),
                                        # HE
                                        conditional_cell_formating(
                                            (df_cnrfc["GMT"][df_cnrfc["Oxbow_fcst"].notnull()]),
                                            len(df_cnrfc["GMT"][df_cnrfc["Abay_AF_Observed"].notnull()].index),None),
                                        # Oxbow Forecast
                                        conditional_cell_formating(
                                            df_cnrfc["Oxbow_fcst"][df_cnrfc["Oxbow_fcst"].notnull()], 0, 6),
                                        # Abay Elev
                                        conditional_cell_formating(
                                            df_cnrfc["Abay_Elev_Fcst"][df_cnrfc["Oxbow_fcst"].notnull()],
                                            1168, 1175),
                                        # Abay Inflow
                                        conditional_cell_formating(
                                            df_cnrfc["Abay_Inflow"][df_cnrfc["Abay_Inflow"].notnull()],
                                            df_cnrfc["Abay_Inflow"].min(),
                                            df_cnrfc["Abay_Inflow"].max()),
                                        conditional_cell_formating(
                                            df_cnrfc["Abay_Outflow"][df_cnrfc["Abay_Outflow"].notnull()],
                                            df_cnrfc["Abay_Outflow"].min(),
                                            df_cnrfc["Abay_Outflow"].max()),
                                    ],
                                    align='center', font=dict(color='black', size=11)
                                )
                            )
                            ],
                            layout=dict(
                                margin=dict(l=0,r=0,b=0,t=0,pad=4)
                            )
                        ),
                    )
                ], className="ml-2 col-sm-5 col-md-5 col-lg-5")
            ]),
            html.Div(id='dummy_fcst_df',
                     children=df_cnrfc.to_json(date_format='iso',orient='index'),
                     style={'display': 'none'}),
    ])
    app.run_server(debug=True)


@app.callback(
    Output('table_graph', 'figure'),
    [Input('abay-dropdown', 'value'),
     Input('table_graph', 'figure'),
     Input('dummy_fcst_df','children')])
def update_output(selections,table, df_json):
    # Read the df from the dummy id.
    df = pd.read_json(df_json, orient='index')

    # Every time we pull the df in json format, it needs to be converted to the correct time format.
    df.GMT = pd.to_datetime(df.GMT).dt.tz_convert('US/Pacific')

    table_contents = {
        "Day":{
            "header": "<b>Day</b>",
            "df_column": "GMT",
            "df_vals": (df["GMT"][df["Oxbow_fcst"].notnull()]).dt.strftime("%a"),
            "color_hi": None,
            "color_low": None,
        },
        "Hour": {
            "header": "<b>HE</b>",
            "df_column": "GMT",
            "df_vals": (df["GMT"][df["Oxbow_fcst"].notnull()]).dt.strftime("%H"),
            "color_hi": None,
            "color_low": None,
        },
        "Oxbow": {
            "header": "<b>Oxbow</b>",
            "df_column": "Oxbow_fcst",
            "df_vals": (df["Oxbow_fcst"][df["Oxbow_fcst"].notnull()]).round(1),
            "color_hi": 6,
            "color_low": 0,
        },
        "MF_MW": {
            "header": "<b>MF MW</b>",
            "df_column": "MF_MW",
            "df_vals": (df["MF_MW"][df["MF_MW"].notnull()]).round(0),
            "color_hi": 124,
            "color_low": 0,
        },
        "RA_MW": {
            "header": "<b>RA MW</b>",
            "df_column": "RA_MW",
            "df_vals": (df["RA_MW"][df["RA_MW"].notnull()]).round(0),
            "color_hi": 86,
            "color_low": 0,
        },
        "Abay_Elev_Fcst": {
            "header": "<b>Abay Fcst</b>",
            "df_column": "Abay_Elev_Fcst",
            "df_vals": (df["Abay_Elev_Fcst"][df["Oxbow_fcst"].notnull()]).round(2),
            "color_hi": 1175,
            "color_low": 1168,
        },
        "Abay_Inflow": {
            "header": "<b>Abay Inflow</b>",
            "df_column": "Abay_Inflow",
            "df_vals": (df["Abay_Inflow"][df["Abay_Inflow"].notnull()]).round(0),
            "color_hi": df["Abay_Inflow"].max(),
            "color_low": df["Abay_Inflow"].min(),
        },
        "Abay_Outflow": {
            "header": "<b>Abay Outflow</b>",
            "df_column": "Abay_Outflow",
            "df_vals": (df["Abay_Outflow"][df["Abay_Outflow"].notnull()]).round(0),
            "color_hi": df["Abay_Outflow"].max(),
            "color_low": df["Abay_Outflow"].min(),
        },
        "R4_fcst": {
            "header": "<b>R4</b>",
            "df_column": "R4_fcst",
            "df_vals": (df["R4_fcst"][df["Oxbow_fcst"].notnull()]).round(0),
            "color_hi": (df["R4_fcst"][df["Oxbow_fcst"].notnull()]).max(),
            "color_low": (df["R4_fcst"][df["Oxbow_fcst"].notnull()]).min(),
        },
        "R20_fcst_adjusted": {
            "header": "<b>R20</b>",
            "df_column": "R20_fcst_adjusted",
            "df_vals": (df["R20_fcst_adjusted"][df["Oxbow_fcst"].notnull()]).round(0),
            "color_hi": (df["R20_fcst_adjusted"][df["Oxbow_fcst"].notnull()]).max(),
            "color_low": (df["R20_fcst_adjusted"][df["Oxbow_fcst"].notnull()]).min(),
        },
        "R30_fcst": {
            "header": "<b>R30</b>",
            "df_column": "R30_fcst",
            "df_vals": (df["R30_fcst"][df["Oxbow_fcst"].notnull()]).round(0),
            "color_hi": (df["R30_fcst"][df["Oxbow_fcst"].notnull()]).max(),
            "color_low": (df["R30_fcst"][df["Oxbow_fcst"].notnull()]).min(),
        },
        "Pmin": {
            "header": "<b>Pmin</b>",
            "df_column": "Pmin",
            "df_vals": (df["Pmin"][df["Oxbow_fcst"].notnull()]).round(1),
            "color_hi": (df["Pmin"][df["Oxbow_fcst"].notnull()]).max(),
            "color_low": (df["Pmin"][df["Oxbow_fcst"].notnull()]).min(),
        },
        "Pmax": {
            "header": "<b>Pmax</b>",
            "df_column": "Pmax",
            "df_vals": (df["Pmax"][df["Oxbow_fcst"].notnull()]).round(1),
            "color_hi": (df["Pmax"][df["Oxbow_fcst"].notnull()]).max(),
            "color_low": (df["Pmax"][df["Oxbow_fcst"].notnull()]).min(),
        },
        "Abay_CFS_Error": {
            "header": "<b>Error (cfs)</b>",
            "df_column": "Abay_CFS_Error",
            "df_vals": (df["Abay_CFS_Error"][df["Abay_CFS_Error"].notnull()]).round(0),
            "color_hi": (df["Abay_CFS_Error"][df["Abay_CFS_Error"].notnull()]).max(),
            "color_low": (df["Abay_CFS_Error"][df["Abay_CFS_Error"].notnull()]).min(),
        },
        "Total_Gen": {
            "header": "<b>RA+MF Gen</b>",
            "df_column": "Total_Gen",
            "df_vals": (df["RA_MW"][df["RA_MW"].notnull()]+(df["MF_MW"][df["MF_MW"].notnull()])).round(0),
            "color_hi": (df["RA_MW"][df["RA_MW"].notnull()]+(df["MF_MW"][df["MF_MW"].notnull()])).max(),
            "color_low": (df["RA_MW"][df["RA_MW"].notnull()]+(df["MF_MW"][df["MF_MW"].notnull()])).min(),
        },
    }

    default_columns = ["Day","Hour","Oxbow","Abay_Elev_Fcst"]
    if "River_Flows" in selections:
        selections.remove('River_Flows')
        selections.extend(['R4_fcst','R20_fcst_adjusted','R30_fcst'])
    if "Pmin_Pmax" in selections:
        selections.remove('Pmin_Pmax')
        selections.extend(['Pmin', 'Pmax'])
    if "RAandMF" in selections:
        selections.remove('RAandMF')
        selections.extend(['RA_MW', 'MF_MW'])

    default_columns.extend(selections)
    header_vals = []
    cell_vals = []
    cell_fill = []
    for column in default_columns:
        header_vals.append(table_contents[column]['header'])
        cell_vals.append(table_contents[column]['df_vals'])
        if column == "Day" or column == "Hour":
            cell_fill.append(conditional_cell_formating(df["GMT"][df["Oxbow_fcst"].notnull()],
                                                        len(df["GMT"][df["Abay_AF_Observed"].notnull()].index), None))
        else:
            cell_fill.append(conditional_cell_formating(table_contents[column]['df_vals'],
                                                        table_contents[column]['color_low'],
                                                        table_contents[column]['color_hi']))
    table['data'][0]['cells']['values'] = cell_vals
    table['data'][0]['cells']['fill']['color'] = cell_fill
    table['data'][0]['header']['values'] = header_vals
    return table


def conditional_cell_formating(df, dfmin, dfmax):
    # Adapted From https://stackoverflow.com/questions/50027959/scaling-normalizing-pandas-column
    # and https://plotly.com/python/table/

    if is_datetime(df):
        # If it's a datetime object, we are doing two things:
        # 1) At HE 00, color the cell gray
        #       This is done with two color bins, and a normalized range of 0, 23
        # 2) For any observed data, color the cells purple.
        #       This is done by sending the number rows in the dataframe where Abay_Elevation_Observed is not nan.

        color_bins = 2                  # The number of color bins (resolution of color scale)
        max_rgb = 'rgb(255, 255, 255)'  # Color for the highest values
        min_rgb = 'rgb(162, 162, 162)'  # Color for the lowest values

        # numpy array containing the colors, size of array based on # of color bins provided above.
        colors = n_colors(min_rgb, max_rgb, color_bins, colortype='rgb')
        a, b = 0, color_bins - 1
        nrng_lo, nrng_hi = 0, 23

        # We have two colors because we only have two bins. For all hours > 0, round up so the value is associated
        # with the white bin. For hour = 0, the value will be associated with the gray bin.
        rgb = np.array(colors)[np.ceil(((df.dt.hour - nrng_lo) / (nrng_hi - nrng_lo) * (b - a) + a)).astype(int)]

        # If we're passing a dfmin, then it will be the number rows that have observed data. Change the color
        # of all these rows to purple.
        if dfmin:
            rgb[:dfmin] = 'rgb(271,179,255)'    # Change all items in list before dfmin to a given color.
        return rgb
    color_bins = 21                 # The number of color bins (resolution of color scale)
    max_rgb = 'rgb(200, 0, 0)'      # Color for the highest values
    mid_rgb = 'rgb(255,255,255)'
    min_rgb = 'rgb(0, 200, 0)'      # Color for the lowest values

    # Mapping to three colors (low, mid, high) to make the color scale. If the number of color bins is an odd number,
    # round down in the low bins and round up to the high bins (so 21 color bins would give 10 to the
    # low and 11 to the high).
    colors_low = n_colors(min_rgb, mid_rgb, math.floor(color_bins/2), colortype='rgb')
    colors_high = n_colors(mid_rgb, max_rgb, math.ceil(color_bins/2), colortype='rgb')

    # numpy array containing the colors, size of array based on # of color bins provided above.
    colors = colors_low + colors_high

    # A normalized range that the data will fit into. So if oxbow goes from 0-6, a value of 3 will now
    # have a normalized value of 10 (half way between 0-21)
    a, b = 0, color_bins-1
    x, y = int(dfmin), int(dfmax)

    # In the case that a max / min limit is set (e.g. Abay min at 1168), but the forecast value goes below that level,
    # the normalized value will go outside the bounds of the color bins (e.g. -3). In this case, we need to set values
    # that go outside of the bins to the color for the max/min value (e.g. an abay level of 1177 would be the same
    # color as a value of 1175...red).
    df[df >= int(dfmax)] = int(dfmax)
    df[df <= int(dfmin)] = int(dfmin)
    rgb = np.array(colors)[((df - x) / (y - x) * (b - a) + a).astype(int)]
    return rgb


def generate_table(dataframe, max_rows=100):
    df_formated = dataframe.copy()
    df_formated["GMT"] = df_formated["GMT"].dt.strftime("%a %b %d")
    for column in df_formated.columns:
        if column != "GMT":
            df_formated[column]= round(df_formated[column],1)

    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in df_formated.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(df_formated.iloc[i][col]) for col in df_formated.columns
            ]) for i in range(min(len(df_formated), max_rows))
        ])
    ])


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

            # Remove any outliers or data spikes. Only do this for flows. If done for
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
    float_level = df_pi["Afterbay_Elevation_Setpoint"].iloc[-1]

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

    # This is so we can do the 88 below (we need both df's to have the same column name). The goal is to overwrite
    # any "forecast" data for Oxbow with observed values. There is no point in keeping forecast values in.
    df_pi_hourly.rename(columns={"Oxbow_Power": "Oxbow_fcst"}, inplace=True)

    # This is a way to "update" the generation data with any observed data. First merge in any historical data.
    df = pd.merge(df, df_pi_hourly[["RA_MW", "MF_MW", "Oxbow_fcst", "Abay_AF_Observed", "Abay_AF_Change_Observed"]],
                  left_on="GMT", right_index=True, how='left')

    # Next, since we already have an RA_MF column, the merge will make a _x and _y. Just fill the original with
    # the new data (and any bad data will be nan) and store all that data as RA_MW.
    # QUESTION: Why don't we overwrite the forecast flow data with the observed flow data?
    # ANS: If there is a bias developing in the CNRFC flow forecast, the errors will be captured and an average error
    #      will be applied to the forecast going forward. In many cases, the CNRFC data will match actual data.
    df["RA_MW"] = df['RA_MW_y'].fillna(df['RA_MW_x'])
    df["MF_MW"] = df['MF_MW_y'].fillna(df['MF_MW_x'])
    df["Oxbow_fcst"] = df['Oxbow_fcst_y'].fillna(df['Oxbow_fcst_x'])

    # We don't need the _y and _x, so drop them.
    df.drop(['RA_MW_y', 'RA_MW_x', 'MF_MW_y', 'MF_MW_x', 'Oxbow_fcst_x','Oxbow_fcst_y'], axis=1, inplace=True)

    # Conversion from MW to cfs ==> CFS @ Oxbow = MW * 163.73 + 83.956
    df["Abay_Outflow"] = (df["Oxbow_fcst"] * 163.73) + 83.956

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
    df.loc[df['Oxbow_fcst'] == 0, 'Abay_Outflow'] = 0

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
    #        Inflow Into Abay = RA_GEN_TO_CFS + R20_RFC_FCST + R4_RFC_fcst + MAX(0,(MF_GEN_TO_CFS - RA_GEN_TO_CFS)) + R5
    #
    #        CALCULATION ERRORS:
    #        The error between the forecast and the observed is usually fairly consistent on a 24 hour basis (e.g. in
    #        our abay tracker we have a +Fill -Drain adder that we can apply).
    #        In order to compensate for errors, we will calculate the Observed change in Acre Feet vs the forecast
    #        and convert this error to cfs. The average of this error will be added to the forecast to adjust for the
    #        observed error.
    #
    #        KNOWN ERRORS:
    #        Consider a single hour when generation goes from zero to max then back to zero at the end of the hour.
    #        Not all of the water will make it into Abay (as we are assuming), but rather, some of the water will
    #        move through during the first hour of gen, and some will move through after the generation goes back to 0.
    #                                   HE 1   HE 2   HE 3
    #        Tot Gen                    0 --> 210 --> 0
    #        Assumed Flow into Abay     0 --> ALL --> 0
    #        Actual Flow into Abay      0 --> 1/2 --> 1/2  (It's actually more like 1/3, 2/3)
    #

    # Ibay In - Ibay Out = The spill that will eventually make it into Abay through R20.
    df["Ibay_Spill"] = np.maximum(0,(df["MF_Inflow"] - df["RA_Inflow"])) + df["R5_Valve"] + df['R4_fcst']

    # CNRFC is just forecasting natural flow, which I believe is just everything from Ibay down. Therefore, it should
    # always be too low and needs to account for any water getting released from IBAY.
    df["R20_fcst_adjusted"] = df["R20_fcst"] + df["Ibay_Spill"]

    df["Abay_Inflow"] = df["RA_Inflow"]+df["R20_fcst_adjusted"]+df["R30_fcst"]

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
    df["Abay_Elev_Fcst"] = np.minimum(float_level, (-0.0000014663 *
                                             (df["Abay_AF_Fcst"] ** 2)+0.0197767158*df["Abay_AF_Fcst"]+1135.3))
    oxbow_automated(df, df_pi_hourly)
    return df


def oxbow_automated(df, df_pi):
    df.drop(['NFDC1','FMDC1','FMDC1O','BCTC1','LNLC1','RRGC1','HLLC1','HLLC1F','NMFC1',
             'MFAC1F','UNVC1F','ICHC1','AKYC1','AKYC1F','CBAC1','CBAC1F','FOLC1','FOLC1F',
             'UNVC1','SVCC1','MFPC1L','RBBC1F',	'RBBC1SPL','LNLC1F','RRGC1L','RRGC1F','RUFC1L',
             'SVCC1F','SVCC1L','HLLC1L','HLLC1SPL','CBAC1L'], axis=1, inplace=True)

    ################DEBUGGING###############################
    #df.GMT = pd.to_datetime(df.GMT).dt.tz_convert(None)
    #df.to_excel('dummy_data.xlsx')
    #df = pd.read_excel('dummy_data.xlsx')
    #df.GMT = pd.to_datetime(df.GMT).dt.tz_convert('US/Pacific')
    ################End Debug############################

    rafting = True
    # 1 cfs = 0.0826 acre feet per hour
    cfs_to_afh = 0.0826448
    abay_inital_af = df_pi["Abay_AF_Observed"].iloc[0]

    # The last reading in the df for the float set point
    float_level = df_pi["Afterbay_Elevation_Setpoint"].iloc[-1]

    # Absolute Minimum required by licence
    df["Abay_Requred_Release_CFS"] = 160
    if rafting:
        df.loc[df['GMT'].dt.hour.between(8, 11), 'Abay_Requred_Release_CFS'] = 1000

    # Flow into Abay over next 24 hours:
    abay_in = (df.loc[df['GMT'].dt.day.between(16, 16), 'Abay_Inflow']).sum() * cfs_to_afh
    abay_out = (df.loc[df['GMT'].dt.day.between(16, 16), 'Abay_Outflow']).sum() * cfs_to_afh

    # day_end_elev = abay_inital_af + (abay_in - abay_out)
    #
    target_elev = float_level - (float_level-1168)/2
    # # Elev to AF -> y=0.6334393556x^2 - 1409.2226152x + 783749
    target_af = 0.6334393556 * (target_elev ** 2) - 1409.2226152 * target_elev + 783749

    # To keep abay level
    df["Oxbow_fcst_suggested"] = ((df[["Abay_Inflow","Abay_Requred_Release_CFS"]].max(axis=1)*cfs_to_afh)-7.36)/13.425

    # Give an upper bound to Oxbow Generatioon.
    df["Oxbow_fcst_suggested"][df["Oxbow_fcst_suggested"] > 5.8] = 5.8

    df["Oxbow_fcst_suggested"] = oxbow_optimization(df, df_pi)
    # If the outflow is not high enough to meet the minimum flow requirements, we must increase oxbow to
    # meet the required flow where Oxbow Required MW = (Required CFS - 83.956) / 163.73
    # Note: This is bc y = 163.73x + 83.956 where y is CFS and x is MW (so MW = y - 83.956 / 163.73)
    # df.loc[df['Abay_Outflow'] <= df['Abay_Requred_Release_CFS'],
    #        'Oxbow_fcst_suggested'] = (df['Abay_Requred_Release_CFS'] - 83.956) / 163.73

    # Conversion from MW to cfs ==> CFS @ Oxbow = MW * 163.73 + 83.956
    df["Oxbow_Outflow_sug"] = (df["Oxbow_fcst_suggested"] * 163.73) + 83.956

    df["Abay_Outflow_sug"] = df["Oxbow_Outflow_sug"]

    df["Abay_AF_Change_sug"] = (df["Abay_Inflow"] - df["Abay_Outflow_sug"]) * cfs_to_afh

    # Calculate the error by taking the value of the forecast - the value of the observed
    df["Abay_AF_Change_Error_sug"] = df["Abay_AF_Change_sug"] - df["Abay_AF_Change_Observed"]

    # Convert the AF error to CFS (this will be in case we want to graph the errors).
    df["Abay_CFS_Error_sug"] = df["Abay_AF_Change_Error_sug"] * (1 / cfs_to_afh)

    # Normally, the errors over a 24 hour period are pretty consistent. So just average the error.
    cfs_error = df["Abay_CFS_Error_sug"].mean()
    af_error = df["Abay_AF_Change_Error_sug"].mean()

    # To get the AF elevation forecast, take the initial reading and apply the change. Also add in the error.
    first_valid = df["Abay_AF_Change_sug"].first_valid_index()
    for i in range(first_valid, len(df)):
        if i == first_valid:
            df.loc[i, "Abay_AF_Fcst_sug"] = abay_inital_af
        else:
            df.loc[i, "Abay_AF_Fcst_sug"] = df.loc[i - 1, "Abay_AF_Fcst_sug"] + df.loc[i, "Abay_AF_Change_sug"] - af_error

    # Change from AF to Elevation
    # y = -1.4663E-6x^2+0.019776718*x+1135.3
    df["Abay_Elev_Fcst_sug"] = np.minimum(float_level, (-0.0000014663 * (df["Abay_AF_Fcst_sug"] ** 2) + 0.0197767158 * df["Abay_AF_Fcst_sug"] + 1135.3))

    fig = go.Figure()
    #fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(x=df["GMT"][df["Abay_Elev_Fcst"].notnull()],
                             y=df["Oxbow_fcst_suggested"][df["Abay_Elev_Fcst"].notnull()], name="Oxbow MW"))
    fig.add_trace(go.Scatter(x=df["GMT"][df["Abay_Elev_Fcst"].notnull()],
                             y=df["Abay_Elev_Fcst_sug"][df["Abay_Elev_Fcst"].notnull()], name="Abay_Elev",
                             yaxis="y2"))
    fig.add_trace(go.Scatter(x=df["GMT"][df["Abay_Inflow"].notnull()],
                             y=df["Abay_Inflow"][df["Abay_Elev_Fcst"].notnull()], name="Inflow",
                             yaxis="y3"),)
    fig.add_trace(go.Scatter(x=df["GMT"][df["Abay_Inflow"].notnull()],
                             y=df["Abay_Outflow"][df["Abay_Elev_Fcst"].notnull()], name="Outflow",
                             yaxis="y3"), )

    fig.update_layout(
        yaxis=dict(
            title="Oxbow Gen (MW)",
            titlefont=dict(
                color="#1f77b4"
            ),
            tickfont=dict(
                color="#1f77b4"
            )
        ),
        yaxis2=dict(
        title="yaxis2 title",
        titlefont=dict(
            color="#ff7f0e"
        ),
        tickfont=dict(
            color="#ff7f0e"
        ),
        anchor="free",
        overlaying="y",
        side="left",
        position=0.15
        ),
        yaxis3=dict(
            title="yaxis3 title",
            titlefont=dict(
                color="#d62728"
            ),
            tickfont=dict(
                color="#d62728"
            ),
            anchor="x",
            overlaying="y",
            side="right"
        ),
    )
    fig.show()

    target_elev = 1174.5
    # Elev to AF -> y=0.6334393556x^2 - 1409.2226152x + 783749
    target_af = 0.6334393556*(target_elev**2) - 1409.2226152*target_elev + 783749

    hours_to_target = 6
    abay_inital_af = df_pi["Abay_AF_Observed"].iloc[0]

    # To keep abay level
    df["Oxbow_fcst_suggested"] = ((df["Abay_Inflow"]*cfs_to_afh)-7.36)/13.425


    # To get to a target.
    df["Oxbow_fcst_target"] = (((target_af-abay_inital_af)/hours_to_target)-7.361)/13.425



    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["GMT"], y=df["Oxbow_fcst_target"]))
    fig.show()
    # MW per Minute
    oxbow_ramp_rate = 3.8/90
    return

def oxbow_optimization(df, df_pi):
    cfs_to_afh = 0.0826448
    abay_inital_af = df_pi["Abay_AF_Observed"].iloc[0]

    # The last reading in the df for the float set point
    float_level = df_pi["Afterbay_Elevation_Setpoint"].iloc[-1]

    df["Oxbow_fcst_suggested"] =  df["Oxbow_fcst_suggested"].mean()
    # Conversion from MW to cfs ==> CFS @ Oxbow = MW * 163.73 + 83.956
    df["Oxbow_Outflow_sug"] = (df["Oxbow_fcst_suggested"] * 163.73) + 83.956

    df["Abay_Outflow_sug"] = df["Oxbow_Outflow_sug"]

    df["Abay_AF_Change_sug"] = (df["Abay_Inflow"] - df["Abay_Outflow_sug"]) * cfs_to_afh

    # Calculate the error by taking the value of the forecast - the value of the observed
    df["Abay_AF_Change_Error_sug"] = df["Abay_AF_Change_sug"] - df["Abay_AF_Change_Observed"]

    # Convert the AF error to CFS (this will be in case we want to graph the errors).
    df["Abay_CFS_Error_sug"] = df["Abay_AF_Change_Error_sug"] * (1 / cfs_to_afh)

    # Normally, the errors over a 24 hour period are pretty consistent. So just average the error.
    cfs_error = df["Abay_CFS_Error_sug"].mean()
    af_error = df["Abay_AF_Change_Error_sug"].mean()

    # To get the AF elevation forecast, take the initial reading and apply the change. Also add in the error.
    first_valid = df["Abay_AF_Change_sug"].first_valid_index()
    for i in range(first_valid, len(df)):
        if i == first_valid:
            df.loc[i, "Abay_AF_Fcst_sug"] = abay_inital_af
        else:
            df.loc[i, "Abay_AF_Fcst_sug"] = df.loc[i - 1, "Abay_AF_Fcst_sug"] + df.loc[
                i, "Abay_AF_Change_sug"] - af_error

    # Change from AF to Elevation
    # y = -1.4663E-6x^2+0.019776718*x+1135.3
    df["Abay_Elev_Fcst_sug"] = np.minimum(float_level, (
                -0.0000014663 * (df["Abay_AF_Fcst_sug"] ** 2) + 0.0197767158 * df["Abay_AF_Fcst_sug"] + 1135.3))
    if df["Abay_Elev_Fcst_sug"].min() > 1168 and df["Abay_Elev_Fcst_sug"].max() < float_level:
        return df["Oxbow_fcst_suggested"]



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
    main()