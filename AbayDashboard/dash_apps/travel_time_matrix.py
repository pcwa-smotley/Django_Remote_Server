import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
from django.contrib.staticfiles.storage import staticfiles_storage

from dash.dependencies import Input, Output
from plotly import graph_objs as go
from plotly.graph_objs import *
from datetime import datetime as dt
import requests
from datetime import datetime, timedelta
import logging
from django_plotly_dash import DjangoDash


############### CLASS OBJECT FOR CREATING PI DATA PULL **************************
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

    # Get the URL of the pi tag.
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
        # Now that we have the url for the PI data, this request is for the actual data.
        end_time = datetime.utcnow().strftime("%Y-%m-%dT%H:00:00-00:00")
        if self.forecast:
            end_time = (datetime.utcnow() + timedelta(hours=72)).strftime("%Y-%m-%dT%H:00:00-00:00")
        try:
            response = requests.get(
                url=self.url,
                params={"startTime": (datetime.utcnow() + timedelta(hours=-12)).strftime("%Y-%m-%dT%H:00:00-00:00"),
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

# ###To run the app on it's own (not in Django), you would do:
# app = dash.Dash()
# ###Then at the bottom you would do the following:
# if __name__ == '__main__':
#    app.run_server(debug=True)
### Then you'd just run python from the terminal > python dash_first.py
# app = DjangoDash('UberExample')
main_dir = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}],
    external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css"],
    external_scripts=["https://code.jquery.com/jquery-3.5.1.min.js",
                      'https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/js/materialize.min.js',
                      ]
)
server = app.server


# Plotly mapbox public token
mapbox_access_token = "pk.eyJ1Ijoic21vdGxleSIsImEiOiJuZUVuMnBBIn0.xce7KmFLzFd9PZay3DjvAA"

# Dictionary of important locations ******THESE LAT LONS ARE APPROXIMATE*************
list_of_locations = {
    "Oxbow_CFS":        {"lat":39.0031,"lon":-120.748534},
    "Fords":        {"lat":39.00677,"lon":-120.77341},  # ******THESE LAT LONS ARE APPROXIMATE*************
    "Ruck":          {"lat":38.95768,"lon":-120.85208}, # ******THESE LAT LONS ARE APPROXIMATE*************
    "Poverty":       {"lat":38.96407,"lon":-120.9368},  # ******THESE LAT LONS ARE APPROXIMATE*************
    "Mammoth":       {"lat":38.9303,"lon":-120.97505},  # ******THESE LAT LONS ARE APPROXIMATE*************
    "ARPS":          {"lat":38.927,"lon":-120.9907},    # ******THESE LAT LONS ARE APPROXIMATE*************
    "Oregon:":       {"lat":38.91729,"lon":-121.0144},  # ******THESE LAT LONS ARE APPROXIMATE*************
}
# A dict object containing the oxbow forecast from PI data
oxbow_fcst = PiRequest("OPS", "Oxbow", "Forecasted Generation", True)

# Create a dataframe from the dictionary object
df_fcst = pd.DataFrame.from_dict(oxbow_fcst.data)

# Drop useless columns.
df_fcst.drop(["Good", "Questionable", "Substituted", "UnitsAbbreviation"], axis=1, inplace=True)

# Convert the timestamp column from a string to a datetime object
df_fcst.Timestamp = pd.to_datetime(df_fcst.Timestamp).dt.tz_convert('US/Pacific')

# Rename the "Value" column to "Oxbow"
df_fcst.rename(columns={"Value": "Oxbow"}, inplace=True)

# Convert the Oxbow forecast MW to CFS where CFS = MW*163.73 + 83.956
df_fcst["Oxbow_CFS"] = (df_fcst["Oxbow"] * 163.73) + 83.956

# First value where the oxbow forecast is greater than 5.5, then go back 3 hours to assume a base flow. This is a
# terible way to do this in production.
base_flow_index = int(df_fcst[df_fcst["Oxbow_CFS"].gt(800)].index[0]) - (3*60)

# The value (in CFS) of the base flow at a given index #
base_flow = df_fcst["Oxbow_CFS"][base_flow_index]

# The time and width shift values based off of simple regression where:
# Time Table:
# tt_c = time table constant
# tt_x = time table coefficient.
#
# Time Table Width...Additional Minutes (increase width)
# tw_c = time width constant
# tw_x = time table coefficient
shift_table = {
    "Foresthill":   {"tt_c":0.52237, "tt_x":-0.00032, "tw_c": 0, "tw_x": 0},
    "Fords":        {"tt_c":4.1086, "tt_x":-0.0025, "tw_c": 25.848, "tw_x": -0.0232},
    "Ruck":         {"tt_c":5.7704, "tt_x":-0.0027, "tw_c": 144682, "tw_x": -1.3570},
    "Poverty":      {"tt_c":7.2653, "tt_x":-0.0034, "tw_c": 56509, "tw_x": -1.1350},
    "Mammoth":      {"tt_c":9.2797, "tt_x":-0.0042, "tw_c": 43204, "tw_x": -1.0450},
    "ARPS":         {"tt_c":11.292, "tt_x":-0.0050, "tw_c": 44377, "tw_x": -0.9650},
    "Oregon:":      {"tt_c":12.213, "tt_x":-0.0054, "tw_c": 44377, "tw_x": -0.9650},
}

# For each location downstream, take the original Oxbow forecast and:
# 1) shift (i.e. add minutes to) the forecast based off the linear regression above.
# 2) Expand the width of the pulse by:
#   a) finding the index number at the start of the pulse (where the CFS value > 1000 cfs)
#   b) creating an empty array of size "extra minutes"
#   c) filling the empty array with the appropriate value (should be the peak CFS, I used a static val of 1033)
#   d) inserting in the new array into the old array at the start of the pulse.
for location in shift_table:
    # 1) Linear regression to find the shift in minutes.
    shift_minutes = shift_table[location]["tt_x"] * base_flow + shift_table[location]["tt_c"] * 60
    df_fcst[location] = df_fcst["Oxbow_CFS"].shift(int(shift_minutes), axis=0)

    # 2) Find the number of minutes to expand the width of the pulse
    width_expansion = shift_table[location]["tw_c"] * (base_flow ** shift_table[location]["tw_x"])

    # The first location (Oxbow) will not need an expansion.
    if width_expansion > 0:
        # The start of the pulse is based on the poor programming method we used above to find the base_flow_index.
        pulse_start_index = int(shift_minutes + base_flow_index + 3 * 60)

        # Get an array of all the forecast values for this location. This will be the array we manipulate (expand) and
        # place back into the dataframe.
        inital_vals = df_fcst[location].values

        # An empty array of size n=width_expansion
        pulse_extra = np.empty(int(width_expansion))

        # Fill the empty array with the max value of the pulse (this code should be changed to get the actual max
        # value of the pulse rather than a static value.
        pulse_extra[:] = 1033

        # A new array where np.insert(main array, index point to initiate insert, additional array to insert)
        vals_width = np.insert(inital_vals, pulse_start_index, pulse_extra)

        # Fill in the dataframe with the new array. Note the new array will be larger than the dataframe, so just
        # truncate the additional values (i.e. len(df_fcst))
        df_fcst[location] = vals_width[0:len(df_fcst)]

        # Resample the minute data to hourly.
        df_fcst_hourly = df_fcst.resample('1H', on="Timestamp").mean()
        test = 5

# Initialize data frame
df1 = pd.read_csv(
    "../static/data/data1.csv"
)
df2 = pd.read_csv(
    "../static/data/data2.csv"
)
df3 = pd.read_csv(
    "../static/data/data3.csv"
)
#df2 = pd.read_csv(
#    staticfiles_storage.path("data/data2.csv"),
#)
#df3 = pd.read_csv(
#    staticfiles_storage.path("data/data3.csv"),
#)
df = pd.concat([df1, df2, df3], axis=0)
df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="%Y-%m-%d %H:%M")
df.index = df["Date/Time"]
df.drop("Date/Time", 1, inplace=True)
totalList = []
for month in df.groupby(df.index.month):
    dailyList = []
    for day in month[1].groupby(month[1].index.day):
        dailyList.append(day[1])
    totalList.append(dailyList)
totalList = np.array(totalList)

# Layout of Dash App
app.layout = html.Div(
    children=[
        html.Div(
            className="row",
            children=[
                # Column for user controls
                html.Div(
                    className="four columns div-user-controls",
                    children=[
                        html.Img(
                            className="logo", src=app.get_asset_url("dash-logo-new.png")
                        ),
                        html.H2("DASH - TRAVEL TIME MATRIX"),
                        html.P(
                            """Select different days using the date picker or by selecting
                            different time frames on the histogram."""
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                dcc.DatePickerSingle(
                                    id="date-picker",
                                    min_date_allowed=dt(2014, 4, 1),
                                    max_date_allowed=dt(2014, 9, 30),
                                    initial_visible_month=dt(2014, 4, 1),
                                    date=dt(2014, 4, 1).date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                            className="row",
                            children=[
                                html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        # Dropdown for locations on map
                                        dcc.Dropdown(
                                            id="location-dropdown",
                                            options=[
                                                {"label": i, "value": i}
                                                for i in list_of_locations
                                            ],
                                            placeholder="Select a location",
                                        )
                                    ],
                                ),
                                html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        # Dropdown to select times
                                        dcc.Dropdown(
                                            id="bar-selector",
                                            options=[
                                                {
                                                    "label": str(n) + ":00",
                                                    "value": str(n),
                                                }
                                                for n in range(24)
                                            ],
                                            multi=True,
                                            placeholder="Select certain hours",
                                        )
                                    ],
                                ),
                            ],
                        ),
                        html.P(id="total-rides"),
                        html.P(id="total-rides-selection"),
                        html.P(id="date-value"),
                        dcc.Markdown(
                            children=[
                                "Source: [PCWA Flows]"
                            ]
                        ),
                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="eight columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph"),
                        html.Div(
                            className="text-padding",
                            children=[
                                "Select any of the bars on the histogram to section data by time."
                            ],
                        ),
                        dcc.Graph(id="histogram"),
                    ],
                ),
            ],
        )
    ]
)

# Gets the amount of days in the specified month
# Index represents month (0 is April, 1 is May, ... etc.)
daysInMonth = [30, 31, 30, 31, 31, 30]

# Get index for the specified month in the dataframe
monthIndex = pd.Index(["Apr", "May", "June", "July", "Aug", "Sept"])

# Get the amount of rides per hour based on the time selected
# This also higlights the color of the histogram bars based on
# if the hours are selected
def get_selection(month, day, selection):
    xVal = []
    yVal = []
    xSelected = []
    colorVal = [
        "#F4EC15",
        "#DAF017",
        "#BBEC19",
        "#9DE81B",
        "#80E41D",
        "#66E01F",
        "#4CDC20",
        "#34D822",
        "#24D249",
        "#25D042",
        "#26CC58",
        "#28C86D",
        "#29C481",
        "#2AC093",
        "#2BBCA4",
        "#2BB5B8",
        "#2C99B4",
        "#2D7EB0",
        "#2D65AC",
        "#2E4EA4",
        "#2E38A4",
        "#3B2FA0",
        "#4E2F9C",
        "#603099",
    ]

    # Put selected times into a list of numbers xSelected
    xSelected.extend([int(x) for x in selection])

    for i in range(24):
        # If bar is selected then color it white
        if i in xSelected and len(xSelected) < 24:
            colorVal[i] = "#FFFFFF"
        xVal.append(i)
        # Get the number of rides at a particular time
        yVal.append(len(totalList[month][day][totalList[month][day].index.hour == i]))
    return [np.array(xVal), np.array(yVal), np.array(colorVal)]


# Selected Data in the Histogram updates the Values in the DatePicker
@app.callback(
    Output("bar-selector", "value"),
    [Input("histogram", "selectedData"), Input("histogram", "clickData")],
)
def update_bar_selector(value, clickData):
    holder = []
    if clickData:
        holder.append(str(int(clickData["points"][0]["x"])))
    if value:
        for x in value["points"]:
            holder.append(str(int(x["x"])))
    return list(set(holder))


# Clear Selected Data if Click Data is used
@app.callback(Output("histogram", "selectedData"), [Input("histogram", "clickData")])
def update_selected_data(clickData):
    if clickData:
        return {"points": []}


# Update the total number of rides Tag
@app.callback(Output("total-rides", "children"), [Input("date-picker", "date")])
def update_total_rides(datePicked):
    date_picked = dt.strptime(datePicked, "%Y-%m-%d")
    return "Total CFS: {:,d}".format(
        len(totalList[date_picked.month - 4][date_picked.day - 1])
    )


# Update the total number of rides in selected times
@app.callback(
    [Output("total-rides-selection", "children"), Output("date-value", "children")],
    [Input("date-picker", "date"), Input("bar-selector", "value")],
)
def update_total_rides_selection(datePicked, selection):
    firstOutput = ""

    if selection is not None or len(selection) != 0:
        date_picked = dt.strptime(datePicked, "%Y-%m-%d")
        totalInSelection = 0
        for x in selection:
            totalInSelection += len(
                totalList[date_picked.month - 4][date_picked.day - 1][
                    totalList[date_picked.month - 4][date_picked.day - 1].index.hour
                    == int(x)
                ]
            )
        firstOutput = "CFS at time: {:,d}".format(totalInSelection)

    if (
        datePicked is None
        or selection is None
        or len(selection) == 24
        or len(selection) == 0
    ):
        return firstOutput, (datePicked, " - showing hour(s): All")

    holder = sorted([int(x) for x in selection])

    if holder == list(range(min(holder), max(holder) + 1)):
        return (
            firstOutput,
            (
                datePicked,
                " - showing hour(s): ",
                holder[0],
                "-",
                holder[len(holder) - 1],
            ),
        )

    holder_to_string = ", ".join(str(x) for x in holder)
    return firstOutput, (datePicked, " - showing hour(s): ", holder_to_string)


# Update Histogram Figure based on Month, Day and Times Chosen
@app.callback(
    Output("histogram", "figure"),
    [Input("date-picker", "date"), Input("bar-selector", "value"), Input("map-graph", "clickData")]
)
def update_histogram(datePicked, selection, pointPicked):
    date_picked = dt.strptime(datePicked, "%Y-%m-%d")
    monthPicked = date_picked.month - 4
    dayPicked = date_picked.day - 1
    location = "Oxbow_CFS"
    if pointPicked:
        location = pointPicked['points'][0]['text']
    df = df_fcst_hourly.copy(deep=True)
    # Note, switched this to tomorrow
    today = (datetime.today() + timedelta(hours=24)).strftime('%Y-%m-%d')
    next_24_hrs = df[location].loc[today:today].round(1).values


    [xVal, yVal, colorVal] = get_selection(monthPicked, dayPicked, selection)
    yVal = next_24_hrs

    layout = go.Layout(
        bargap=0.01,
        bargroupgap=0,
        barmode="group",
        margin=go.layout.Margin(l=10, r=0, t=0, b=50),
        showlegend=False,
        plot_bgcolor="#323130",
        paper_bgcolor="#323130",
        dragmode="select",
        font=dict(color="white"),
        xaxis=dict(
            range=[-0.5, 23.5],
            showgrid=False,
            nticks=25,
            fixedrange=True,
            ticksuffix=":00",
        ),
        yaxis=dict(
            range=[0, max(yVal) + max(yVal) / 4],
            showticklabels=False,
            showgrid=False,
            fixedrange=True,
            rangemode="nonnegative",
            zeroline=False,
        ),
        annotations=[
            dict(
                x=xi,
                y=yi,
                text=str(yi),
                xanchor="center",
                yanchor="bottom",
                showarrow=False,
                font=dict(color="white"),
            )
            for xi, yi in zip(xVal, yVal)
        ],
    )

    return go.Figure(
        data=[
            go.Bar(x=xVal, y=yVal, marker=dict(color=colorVal), hoverinfo="x"),
            go.Scatter(
                opacity=0,
                x=xVal,
                y=yVal / 2,
                hoverinfo="none",
                mode="markers",
                marker=dict(color="rgb(66, 134, 244, 0)", symbol="square", size=40),
                visible=True,
            ),
        ],
        layout=layout,
    )


# Get the Coordinates of the chosen months, dates and times
def getLatLonColor(selectedData, month, day):
    listCoords = totalList[month][day]

    # No times selected, output all times for chosen month and date
    if selectedData is None or len(selectedData) == 0:
        return listCoords
    listStr = "listCoords["
    for time in selectedData:
        if selectedData.index(time) != len(selectedData) - 1:
            listStr += "(totalList[month][day].index.hour==" + str(int(time)) + ") | "
        else:
            listStr += "(totalList[month][day].index.hour==" + str(int(time)) + ")]"
    return eval(listStr)


# Update Map Graph based on date-picker, selected data on histogram and location dropdown
@app.callback(
    Output("map-graph", "figure"),
    [
        Input("date-picker", "date"),
        Input("bar-selector", "value"),
        Input("location-dropdown", "value"),
    ],
)
def update_graph(datePicked, selectedData, selectedLocation):
    zoom = 12.0
    latInitial =   39.0031
    lonInitial = -120.748534
    bearing = 0

    if selectedLocation:
        zoom = 15.0
        latInitial = list_of_locations[selectedLocation]["lat"]
        lonInitial = list_of_locations[selectedLocation]["lon"]


    date_picked = dt.strptime(datePicked, "%Y-%m-%d")
    monthPicked = date_picked.month - 4
    dayPicked = date_picked.day - 1
    listCoords = getLatLonColor(selectedData, monthPicked, dayPicked)

    return go.Figure(
        data=[
            # Data for all rides based on date and time
            Scattermapbox(
                lat=listCoords["Lat"],
                lon=listCoords["Lon"],
                mode="markers",
                hoverinfo="lat+lon+text",
                text=listCoords.index.hour,
                marker=dict(
                    showscale=True,
                    color=np.append(np.insert(listCoords.index.hour, 0, 0), 23),
                    opacity=0.5,
                    size=5,
                    colorscale=[
                        [0, "#F4EC15"],
                        [0.04167, "#DAF017"],
                        [0.0833, "#BBEC19"],
                        [0.125, "#9DE81B"],
                        [0.1667, "#80E41D"],
                        [0.2083, "#66E01F"],
                        [0.25, "#4CDC20"],
                        [0.292, "#34D822"],
                        [0.333, "#24D249"],
                        [0.375, "#25D042"],
                        [0.4167, "#26CC58"],
                        [0.4583, "#28C86D"],
                        [0.50, "#29C481"],
                        [0.54167, "#2AC093"],
                        [0.5833, "#2BBCA4"],
                        [1.0, "#613099"],
                    ],
                    colorbar=dict(
                        title="Time of<br>Day",
                        x=0.93,
                        xpad=0,
                        nticks=24,
                        tickfont=dict(color="#d8d8d8"),
                        titlefont=dict(color="#d8d8d8"),
                        thicknessmode="pixels",
                    ),
                ),
            ),
            # Plot of important locations on the map
            Scattermapbox(
                lat=[list_of_locations[i]["lat"] for i in list_of_locations],
                lon=[list_of_locations[i]["lon"] for i in list_of_locations],
                mode="markers",
                hoverinfo="text",
                text=[i for i in list_of_locations],
                marker=dict(size=10, color="#ffa0a0"),
            ),
        ],
        layout=Layout(
            autosize=True,
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                center=dict(lat=latInitial, lon=lonInitial),  # 40.7272  # -73.991251
                style="outdoors",
                bearing=bearing,
                zoom=zoom,
            ),
            updatemenus=[
                dict(
                    buttons=(
                        [
                            dict(
                                args=[
                                    {
                                        "mapbox.zoom": 12,
                                        "mapbox.center.lon": "-73.991251",
                                        "mapbox.center.lat": "40.7272",
                                        "mapbox.bearing": 0,
                                        "mapbox.style": "dark",
                                    }
                                ],
                                label="Reset Zoom",
                                method="relayout",
                            )
                        ]
                    ),
                    direction="left",
                    pad={"r": 0, "t": 0, "b": 0, "l": 0},
                    showactive=False,
                    type="buttons",
                    x=0.45,
                    y=0.02,
                    xanchor="left",
                    yanchor="bottom",
                    bgcolor="#323130",
                    borderwidth=1,
                    bordercolor="#6d6d6d",
                    font=dict(color="#FFFFFF"),
                )
            ],
        ),
    )


if __name__ == '__main__':
    app.run_server(debug=True)