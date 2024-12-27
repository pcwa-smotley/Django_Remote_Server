from dash import dcc, html
#import dash_core_components as dcc
#import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_daq as daq
from plotly import graph_objs as go
from datetime import datetime as dt

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


def main_layout(top_row_cards, second_row_cards, locations_types):
    main_html = html.Main(
        className='content container',
        children=[
            top_row_cards,
            second_row_cards,
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
    return main_html


def top_cards(df_all, df_hourly_resample, logging):
    logger = logging.getLogger(__name__)

    top_row = dbc.Row([
        # Top Card Columns.
        # Every card in this  column denoted by a classname of: col-md-6 col-xxl-3 mb-3 pr-md-2"
        # That stands for:
        # col-md-6: take up 6 grid spaces (50% of the row) for medium screens
        # col-lg-3: take up 3 columns (25% of the row) for large screens
        # mb-3: margin-bottom of 3: 1 rem (e.g. 16px if font-size is 16)
        # pr-md-2: padding-right for medium-size screens or larger is 2: 0.5 rem or 8px if fontsize is 16px
        html.Div([
            # ABAY CARD class of h-md-100 which means height on medium screens of 100%
            dbc.Card([
                dbc.CardHeader(f"Abay - "
                               f"Elev: {round(df_all['Afterbay_Elevation'].dropna().iloc[-1], 1)}"),
                # f" Updated: {(df_all['Timestamp'].iloc[-1]).strftime('%b %d, %H:%M %p')}"),
                # ABAY CARD BODY has ONE row and TWO columns.
                # The body class is: d-flex aligh-items-end
                # d-flex (display-flex): create a flexbox container and transform direct children elements into flex items.
                # align-items-end: center everything in this flex container from the bottom right corner.
                dbc.CardBody(
                    # THE ROW OF THIS CARD BODY HAS TWO COLUMNS, with class of flex-grow-1
                    # flex-grow-1: the rate at which this will grow relative to other rows...not sure this is needed.
                    dbc.Row([
                        html.Div(
                            children=[f" Float: {int(df_all['Afterbay_Elevation_Setpoint'].dropna().iloc[-1])}'"],
                            id="abay_float_txt",
                            style={"position": "absolute",
                                   "left": "25px",
                                   "top": "48px"}
                        ),
                        dbc.Col(
                            daq.Tank(
                                id='my-tank2',
                                className='dark-theme-control',
                                value=round(df_all["Afterbay_Elevation"].dropna().iloc[-1], 1),
                                min=1165,
                                height=110,  # Required! This is based off of a top-row height of 200px.
                                max=df_all["Afterbay_Elevation_Setpoint"].values.max(),
                                color='#2376f3',
                                units="",
                                showCurrentValue=True,
                            ), width=6,
                        ),
                        dbc.Col(
                            dcc.Graph(
                                className="sparkline_graph",
                                id="abay_bargraph",
                                style={"width": "100%", "height": "100%"},
                                config={
                                    "staticPlot": False,
                                    "editable": False,
                                    "displayModeBar": False,
                                },
                                figure={
                                    "data": [
                                        {
                                            "x": df_hourly_resample.index[-10:],
                                            "y": df_hourly_resample['Afterbay_Elevation'][-10:],
                                            "type": "bar",
                                            "name": "Elevation",
                                            "marker": {"color": '#2c7be5'},
                                            # "text":df_all['Afterbay_Elevation'][-10:],
                                            # "hoverinfo":"y",
                                            # The <extra></extra> gets rid of the y-axis label
                                            # see: https://plotly.com/python/reference/#bar-hovertext
                                            "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y}<extra></extra>",
                                            # "width":"1.5",
                                        },
                                        {
                                            "x": df_hourly_resample.index[-10:],
                                            "y": df_hourly_resample['Afterbay_Elevation_Setpoint'][-10:] -
                                                 df_hourly_resample["Afterbay_Elevation"][-10:],
                                            "type": "bar",
                                            "name": "Space",
                                            "marker": {"color": '#061325'},
                                            "hoverinfo": "skip",
                                            # "width": "1.5",
                                        },

                                    ],
                                    "layout": {
                                        "margin": dict(l=0, r=0, t=4, b=4, pad=0),
                                        "title": None,
                                        "showlegend": False,
                                        "yaxis": dict(
                                            title=None,
                                            titlefont_size=16,
                                            tickfont_size=14,
                                            showline=False,
                                            showgrid=False,
                                            zeroline=False,
                                            showticklabels=False,
                                            visible=False,
                                            range=[1168, df_all["Afterbay_Elevation_Setpoint"].values.max()]
                                        ),
                                        "xaxis": dict(
                                            showline=False,
                                            showgrid=False,
                                            zeroline=False,
                                            showticklabels=False,
                                            visible=False,  # numbers below
                                        ),
                                        "legend": None,
                                        "width": 140,
                                        "hovermode": "closest",
                                        "barmode": 'relative',
                                        "paper_bgcolor": 'rgba(0,0,0,0)',
                                        "plot_bgcolor": "rgba(0,0,0,0)",
                                        # "bargap": 0.05, # gap between bars of adjacent location coordinates.
                                        # "bargroupgap":0.05, # gap between bars of the same location coordinate.,
                                        "autosize": True,
                                    },
                                }
                            ), width="6", className='sparkline pl-0',
                        )
                    ]),
                    className='align-items-end mt-2'
                )
            ], color="dark", inverse=True, className="h-100")],
            className="col-sm-12 col-md-12 col-lg-4 mb-3 pr-md-2"
        ),
        html.Div([
            dbc.Card([
                dbc.CardHeader(
                    children=[
                        "R4 Flow",
                        html.Label(
                            id="cnrfc_switch",
                            n_clicks=0,
                            style={'float': 'right', 'margin-bottom': '0'},
                            children=[
                                dcc.Input(
                                    type="checkbox",
                                    id="cnrfc_toggle",
                                    className="c-switch-input",
                                    value="",
                                ),
                                html.Span(
                                    className="c-switch-slider",
                                    id="cnrfc_switch_span",
                                    n_clicks=0,
                                    **{'data-checked': "on",
                                       'data-unchecked': "off"},
                                ),
                            ],
                            className="c-switch c-switch-label c-switch-success c-switch-sm",
                        )]
                ),
                dbc.CardBody(
                    children=[
                        dbc.Row([
                            dbc.Col(
                                daq.LEDDisplay(
                                    size=20,
                                    value=int(df_all["R4_Flow"].dropna().iloc[-1]),
                                    color="#FF5E5E",
                                    backgroundColor="#343a40"
                                ),
                                width=3),
                            dbc.Col(
                                dcc.Loading(
                                    id='loading_r4_sparkline',
                                    type="circle",
                                    className='h-100',
                                    parent_className="loading_wrapper h-100",
                                    children=[
                                        dcc.Graph(
                                            id="r4sparkline",
                                            # Parent div of the figure must have a height or it will base height on
                                            # container.
                                            style={"height": 120},
                                            className="sparkline_graph",
                                            config={
                                                "staticPlot": False,
                                                "editable": False,
                                                "displayModeBar": False,
                                            },
                                            figure=go.Figure(),
                                        )]
                                ), width=9, className='sparkline'
                            )
                        ], className="h-100"),
                        html.Div(
                            children=[" Fcst Valid: "],
                            id="cnrfc_timestamp_r4",
                            style={"background-color": "gray",
                                   "position": "absolute",
                                   "left": "0px", "right": "0px",
                                   "bottom": "0px", "display":"none"}
                        )
                    ]
                ),
            ], color="dark", className="h-100", inverse=True)],
            className="col-sm-12 col-md-12 col-lg-4 mb-3 pr-md-2"
        ),
        html.Div([
            dbc.Card([
                dbc.CardHeader(children=[
                    "R30 Flow",
                    html.Label(
                        id="cnrfc_switch_r30",
                        n_clicks=0,
                        style={'float': 'right', 'margin-bottom': '0'},
                        children=[
                            dcc.Input(
                                type="checkbox",
                                id="cnrfc_toggle_r30",
                                className="c-switch-input",
                                value="",
                            ),
                            html.Span(
                                className="c-switch-slider",
                                id="cnrfc_switch_span_r30",
                                n_clicks=0,
                                **{'data-checked': "on",
                                   'data-unchecked': "off"},
                            ),
                        ],
                        className="c-switch c-switch-label c-switch-success c-switch-sm",
                    )]),
                dbc.CardBody(
                    children=[
                    dbc.Row([
                        dbc.Col(
                            daq.LEDDisplay(
                                size=20,
                                value=int(df_all["R30_Flow"].dropna().iloc[-1]),
                                color="#FF5E5E",
                                backgroundColor="#343a40"
                            ), width=3
                        ),
                        dbc.Col(dcc.Loading(
                                    id='loading_r30_sparkline',
                                    type="circle",
                                    className='h-100',
                                    parent_className="loading_wrapper h-100",
                                    loading_state={'is_loading': True},
                                    children=[
                            dcc.Graph(
                                className="sparkline_graph",
                                id="r30sparkline",
                                # Parent div of the figure must have a height or it will base height on
                                # container.
                                style={"height": 120},
                                config={
                                    "staticPlot": False,
                                    "editable": False,
                                    "displayModeBar": False,
                                },
                                figure=go.Figure(),
                            )]
                        ), width=9, className='sparkline'
                        )
                    ], className="h-100"),
                    html.Div(
                        children=[" Fcst Valid: "],
                        id="cnrfc_timestamp_r30",
                        style={"background-color": "gray",
                               "position": "absolute",
                               "left": "0px", "right": "0px",
                               "bottom": "0px", "display": "none"})
                    ]
                )
            ], color="dark", inverse=True, className='h-100')],
            className="col-sm-12 col-md-12 col-lg-4 mb-3 pr-md-2"
        )
    ],
        className='top-cards no-gutters'
    )
    return top_row


def second_cards(df_all, df_hourly_resample):
    row_two = dbc.Row([
        # DIV FOR CARD 1
        html.Div([
            dbc.Card([
                dbc.CardHeader(
                    children=[
                        # ROW FOR HEADERR
                        dbc.Row([
                            # HEADER ROW, COL 1 FOR TEXT
                            dbc.Col("Abay Forecast:",className="col-auto"),
                            # HEADER ROW, COL 2 FOR LED
                            dbc.Col(children=[
                                        daq.LEDDisplay(
                                        size=20,
                                        value=round(df_all["Afterbay_Elevation"].dropna().iloc[-1], 1),
                                        color="#FF5E5E",
                                        backgroundColor="#343a40"
                                        ),
                                    ], width=3),
                            # HEADER ROW, COL 3 FOR SWITCH
                            dbc.Col(
                                html.Label(
                                id="abay_fcst_switch",
                                n_clicks=0,
                                style={'float': 'right', 'margin-bottom': '0'},
                                children=[
                                    dcc.Input(
                                        type="checkbox",
                                        id="abay_toggle",
                                        className="c-switch-input",
                                        value="",
                                    ),
                                    html.Span(
                                        className="c-switch-slider",
                                        id="abay_switch_span",
                                        n_clicks=0,
                                        **{'data-checked': "on",
                                           'data-unchecked': "off"},
                                    ),
                                ],
                                className="c-switch c-switch-label c-switch-success c-switch-sm mr-auto",
                            ), className="mr-auto"), # END SWITCH
                        ]),  # END ROW
                    ]
                ),
                dbc.CardBody(
                            # Row Classname of "h-100" is critical here, since the plots will only take up space that's
                            # taken. But plotly plots are very stupid, for this to really work nicely, it's best to
                            # explicitly say the height of the object. If you don't, the first item that draws will take
                            # up the space in the column (so in this case, there are two graphs and the second one gets,
                            # squished just enough to make it noticable.
                            dbc.Row([
                                dbc.Col(
                                    dcc.Graph(
                                        className="sparkline_graph h-100",
                                        id="abay_sparkline",
                                        config={
                                            "staticPlot": False,
                                            "editable": False,
                                            "displayModeBar": False,
                                        },
                                        figure=go.Figure(
                                            {
                                                "data": [
                                                    {
                                                        "x": df_hourly_resample.index[-24:],
                                                        "y": df_hourly_resample['Afterbay_Elevation'][-24:],
                                                        "mode": "lines",
                                                        "name": "Obserrved",
                                                        "line": {"color": "#f4d44d"},
                                                        "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y:.1f}<extra></extra>",
                                                    }
                                                ],
                                                "layout": {
                                                    "margin": dict(l=0, r=0, t=4, b=4, pad=0),
                                                    "xaxis": dict(
                                                        showline=True,      # border
                                                        mirror=True,        # makes a box border
                                                        linecolor='gray',  # color of border
                                                        showgrid=False,
                                                        zeroline=True,
                                                        showticklabels=False,
                                                        gridcolor='black',
                                                    ),
                                                    "yaxis": dict(
                                                        showline=True,          # border
                                                        mirror=True,            # makes a box border
                                                        linecolor='gray',      # color of border
                                                        showgrid=True,
                                                        zeroline=True,
                                                        showticklabels=True,
                                                        gridcolor='black',
                                                        range=[1168, df_all["Afterbay_Elevation_Setpoint"].values.max()]
                                                    ),
                                                    "autosize": True,
                                                    "height": 120,  # px
                                                    "paper_bgcolor": "rgba(0,0,0,0)",
                                                    "plot_bgcolor": "rgba(0,0,0,0)",
                                                    "font_color": "white",
                                                    "showlegend": False,
                                                },
                                            }
                                        ),
                                    ), width=11, className='sparkline'),
                            ]), className='align-items-end mt-2')
            ], color="dark", inverse=True, className='h-100')],
            className="col-sm-12 col-md-12 col-lg-4 mb-4 pr-md-2"
        ),
        # DIV FOR CARD 2
        html.Div([
            dbc.Card([
                dbc.CardHeader(
                    children=[
                        # ROW FOR HEADERR
                        dbc.Row([
                            # HEADER ROW, COL 1 FOR TEXT
                            dbc.Col("Oxbow:", className="col-auto"),
                            # HEADER ROW, COL 2 FOR LED
                            dbc.Col(children=[
                                daq.LEDDisplay(
                                    size=20,
                                    value=round(df_all["Oxbow_Power"].dropna().iloc[-1], 1),
                                    color="#FF5E5E",
                                    backgroundColor="#343a40"
                                ),
                            ], width=3),
                            # HEADER ROW, COL 3 FOR SWITCH
                            dbc.Col(
                                html.Label(
                                    id="ox_fcst_switch",
                                    n_clicks=0,
                                    style={'float': 'right', 'margin-bottom': '0'},
                                    children=[
                                        dcc.Input(
                                            type="checkbox",
                                            id="oxbow_toggle",
                                            className="c-switch-input",
                                            value="",
                                        ),
                                        html.Span(
                                            className="c-switch-slider",
                                            id="ox_switch_span",
                                            n_clicks=0,
                                            **{'data-checked': "on",
                                               'data-unchecked': "off"},
                                        ),
                                    ],
                                    className="c-switch c-switch-label c-switch-success c-switch-sm mr-auto",
                                ), className="mr-auto"),  # END SWITCH
                        ]),  # END ROW
                    ]
                ),
                dbc.CardBody(
                    # Row Classname of "h-100" is critical here, since the plots will only take up space that's
                    # taken. But plotly plots are very stupid, for this to really work nicely, it's best to
                    # explicitly say the height of the object. If you don't, the first item that draws will take
                    # up the space in the column (so in this case, there are two graphs and the second one gets,
                    # squished just enough to make it noticable.
                    dbc.Row([
                        dbc.Col(
                            dcc.Graph(
                                className="sparkline_graph h-100",
                                id="oxbow_sparkline",
                                config={
                                    "staticPlot": False,
                                    "editable": False,
                                    "displayModeBar": False,
                                },
                                figure=go.Figure(
                                    {
                                        "data": [
                                            {
                                                "x": df_all["Timestamp"],
                                                "y": df_all["Oxbow_Power"],
                                                "mode": "lines",
                                                "name": "MFPH",
                                                "line": {"color": "#f4d44d"},
                                                "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y:.1f}<extra></extra>",
                                            }
                                        ],
                                        "layout": {
                                            "margin": dict(l=0, r=0, t=4, b=4, pad=0),
                                            "xaxis": dict(
                                                showline=True,  # border
                                                mirror=True,  # makes a box border
                                                linecolor='gray',  # color of border
                                                showgrid=False,
                                                zeroline=True,
                                                showticklabels=False,
                                                gridcolor='black',
                                            ),
                                            "yaxis": dict(
                                                showline=True,  # border
                                                mirror=True,  # makes a box border
                                                linecolor='gray',  # color of border
                                                showgrid=True,
                                                zeroline=True,
                                                showticklabels=True,
                                                gridcolor='black',
                                                range=[0, 6]
                                            ),
                                            "showlegend": False,
                                            "autosize": True,
                                            "height": 120,  # px
                                            "paper_bgcolor": "rgba(0,0,0,0)",
                                            "plot_bgcolor": "rgba(0,0,0,0)",
                                            "font_color": "white",
                                        },
                                    }
                                ),
                            ), width=11, className='sparkline'),
                    ]), className='align-items-end mt-2')
            ], color="dark", inverse=True, className='h-100')],
            className="col-sm-12 col-md-12 col-lg-4 mb-4 pr-md-2"
        ),
        # DIV FOR CARD 3
        html.Div([
            dbc.Card([
                dbc.CardHeader(
                    children=[
                        # ROW FOR HEADERR
                        dbc.Row([
                            # HEADER ROW, COL 1 FOR TEXT
                            dbc.Col("MFPH:", className="col-auto"),
                            # HEADER ROW, COL 2 FOR LED
                            dbc.Col(children=[
                                daq.LEDDisplay(
                                    size=20,
                                    value=round(df_all["GEN_MDFK_and_RA"].dropna().iloc[-1], 1),
                                    color="#FF5E5E",
                                    backgroundColor="#343a40"
                                ),
                            ], width=3),
                            # HEADER ROW, COL 3 FOR SWITCH
                            dbc.Col(
                                html.Label(
                                    id="mfph_fcst_switch",
                                    n_clicks=0,
                                    style={'float': 'right', 'margin-bottom': '0'},
                                    children=[
                                        dcc.Input(
                                            type="checkbox",
                                            id="mfph_toggle",
                                            className="c-switch-input",
                                            value="",
                                        ),
                                        html.Span(
                                            className="c-switch-slider",
                                            id="mfph_switch_span",
                                            n_clicks=0,
                                            **{'data-checked': "on",
                                               'data-unchecked': "off"},
                                        ),
                                    ],
                                    className="c-switch c-switch-label c-switch-success c-switch-sm mr-auto",
                                ), className="mr-auto"),  # END SWITCH
                        ]),  # END ROW
                    ]
                ),
                dbc.CardBody(
                    # Row Classname of "h-100" is critical here, since the plots will only take up space that's
                    # taken. But plotly plots are very stupid, for this to really work nicely, it's best to
                    # explicitly say the height of the object. If you don't, the first item that draws will take
                    # up the space in the column (so in this case, there are two graphs and the second one gets,
                    # squished just enough to make it noticable.
                    dbc.Row([
                        # Even though this is an html.Div, it's acting as a row. Program will crash if this is
                        # a dbc.Row instead of div
                        html.Div(id="PminPmaxRow", className='row',
                                    children=[
                                        # All columns are auto-sized.
                                        # Column 1 - autosize no margin on left hand side.
                                        html.Div(f" Pmin: ", className="col-auto ml-0 text-size-1"),
                                        # Column 2 (Pmin value)
                                        html.Div(round(df_all["Pmin"].dropna().iloc[-1], 1),
                                            className='col-auto badge badge-success text-size-1 text-monospace ml-0'),
                                        # Column 3 (Pmax text)
                                        html.Div(f" Pmax: ", className="col-auto ml-2 text-size-1"),
                                        # Col 4 (Pmax value)
                                        html.Div(round(df_all["Pmax"].dropna().iloc[-1], 1),
                                            className='col-auto badge badge-success text-size-1 text-monospace ml-0'),
                                    ]
                                 ),
                        html.Div(className="w-100"),
                        dbc.Col(
                            dcc.Graph(
                                className="sparkline_graph h-100",
                                config={
                                    "staticPlot": False,
                                    "editable": False,
                                    "displayModeBar": False,
                                },
                                figure=go.Figure(
                                    {
                                        "data": [
                                            {
                                                "x": df_all["Timestamp"],
                                                "y": df_all["GEN_MDFK_and_RA"],
                                                "mode": "lines",
                                                "name": "MFPH",
                                                "line": {"color": "#f4d44d"},
                                                "hovertemplate": "%{x|%b-%d <br> %I:%M %p} <br> %{y:.1f}<extra></extra>",
                                            }
                                        ],
                                        "layout": {
                                            "margin": dict(l=0, r=0, t=4, b=4, pad=0),
                                            "xaxis": dict(
                                                showline=True,  # border
                                                mirror=True,  # makes a box border
                                                linecolor='gray',  # color of border
                                                showgrid=False,
                                                zeroline=True,
                                                showticklabels=False,
                                                gridcolor='black',
                                            ),
                                            "yaxis": dict(
                                                showline=True,  # border
                                                mirror=True,  # makes a box border
                                                linecolor='gray',  # color of border
                                                showgrid=True,
                                                zeroline=True,
                                                showticklabels=True,
                                                gridcolor='black',
                                                range=[0, 220]
                                            ),
                                            "autosize": True,
                                            "height": 120,  # px
                                            "paper_bgcolor": "rgba(0,0,0,0)",
                                            "plot_bgcolor": "rgba(0,0,0,0)",
                                            "font_color": "white",
                                        },
                                    }
                                ),
                            ), width=11, className='sparkline'),
                    ], className='justify-content-center'), className='align-items-end')
            ], color="dark", inverse=True, className='h-100')],
            className="col-sm-12 col-md-12 col-lg-4 mb-4 pr-md-2"
        ),
    ],
        className='top-cards no-gutters'
    )
    return row_two