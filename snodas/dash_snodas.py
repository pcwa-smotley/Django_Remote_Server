'''
Author: Shane Motley
Creation Date: 2020.02.08
Purpose: Create two (2) graphs in both HTML and png form from Lake Spaulding Data.
        (1) A stacked bar graph with one bar being a frame of the average precip in a year
             and a second blue bar showing the total for this water year.
        (2) A cumulative line graph showing this year's total-to-date, last wy, wettest wy
             and driest wy, along with a filled area showing the average.
Method: All data obtained from CDEC, with two data pulls occurring on every run.
        (1) Daily precip data for the current water year.
        (2) Historical monthly data for the last 100 years.
'''


from plotly import graph_objs as go
import dash
import dash_html_components as html
import dash_core_components as dcc
import os
from calendar import monthrange
from plotly.subplots import make_subplots
from django.contrib.staticfiles.storage import staticfiles_storage
import requests
import pathlib
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from PIL import Image
from django_plotly_dash import DjangoDash

app = DjangoDash('plotly_snodas')

def create_plots():
    '''
    Perform the two data pulls from CDEC.
    Creates the following dataframes:
        df:             CDEC daily precip data for current water year.
        df_all_years:   Monthly precip data for the last 100 years.
    OUTPUT: Two html files and two static images.
    :return:
    '''
    cur_dir = os.getcwd()
    wy = datetime.today().year              # Water Year
    month_num = datetime.today().month + 2  # Current month number

    # The "AS-OF" date. Date of data pull
    data_pull_date = (datetime.today() - timedelta(days=1)).strftime("%b %d")

    # A date range spanning every day of the entire water year.
    idx = pd.date_range(f'10.01.{wy-1}', f'9.30.{wy}')
    if datetime.today().month >= 10:    # The water year will be year + 1 if it's Oct, Nov, or Dec
        wy = wy + 1
        month_num = month_num - 9
    try:
        # # Daily Data Pull
        # response = requests.get(
        #     url="https://cdec.water.ca.gov/dynamicapp/QueryDaily?s=LSP",
        #     params={"end": f"{wy}-10-1",
        #             "span": "1year"
        #             },
        # )
        # print('Response HTTP Status Code: {status_code}'.format(status_code=response.status_code))
        #
        # # Historical Data Pull
        # all_years = requests.get (
        #     url="https://cdec.water.ca.gov/dynamicapp/QueryWY",
        #     params={"Stations": "LSP",
        #             "span": "100+years",
        #             "SensorNums":2
        #             },
        # )
        # print('Response HTTP Status Code: {status_code}'.format(status_code=all_years.status_code))
        #
        # # Current data to-date
        # df = pd.read_html(response.content)[0]

        # For some reason, the dataframe extends to 10-01 of the following water year instead of 9-30, so
        # we need to remove the last row
        # df = df[:-1]

        df_hh = pd.read_excel(staticfiles_storage.path("data/Daily_Output.xlsx"), sheet_name='Hell_Hole_elv_bands',
                              header=1, engine='openpyxl')
        df_fm = pd.read_excel(staticfiles_storage.path("data/Daily_Output.xlsx"),
                              sheet_name='French_Meadows_elv_bands', header=1, engine='openpyxl')
        df_fm['Date'] = pd.to_datetime(df_fm['Date'])
        df_hh['Date'] = pd.to_datetime(df_hh['Date'])
        df = pd.merge(df_fm, df_hh, on="Date", how='outer')
        for column in df_hh:
            if column != 'Date':
                df[column] = df[f"{column}_x"] + df[f"{column}_y"]
                df.drop([f"{column}_x", f"{column}_y"], axis=1, inplace=True)

        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df_ave = df.groupby(df.index.strftime('2021-%m-%d')).mean()
        df_ave = df_ave[df_ave.index != "2021-02-29"]
        df_ave.index = pd.to_datetime(df_ave.index)
        df_dates = pd.DataFrame(pd.date_range('10-1-2020', '9-30-2021', freq='D'))
        df_dates.index=df_dates[df_dates.columns[0]]
        df_dates.columns = ["Date"]
        df_ave = pd.merge(df_dates, df_ave, left_on=[df_dates.index.month,df_dates.index.day],
                          right_on=[df_ave.index.month, df_ave.index.day], how='outer')

        df_ave.index = pd.to_datetime(df_ave["Date"])

        # df_ave['Date'] = df_ave.apply(lambda x: x["Date"].dt.year.replace('2021', '2020') if x['Date'].dt.month > 9 else x["Date"], axis=1)
        # df_ave.loc[df_ave['Date'].dt.month > 9, df_ave['Date'].dt.year] = 2020
        # Line chart info.
        lineChart = (go.Figure(data=[go.Scatter(
            x=(df_ave.index),
            y=df_ave['7.0-7.5'],
            name="15 Year Average",
            hovertext=df_ave['7.0-7.5'],
            hovertemplate="Average: %{y:.1f}\"<extra></extra>",
            fill='tozeroy',
            line=dict(width=0.5, color='rgb(111, 231, 219)'),
            marker_line_width=2,
            texttemplate='%{hovertext}',
            textposition="top center",
            textfont=dict(
                family="Arial",
                size=24,
                color="rgba(0, 0, 0)"
            )
        )
        ]
        )
        )

        df['Date'] = df.index
        for yr in range(2021,2022):
            visible = False
            if yr == 2004 or yr == 2021:
                if yr == 2004:
                    df_yr = df[(df['Date'] >= f"2004-1-1") & (df['Date'] <= f'{yr}-9-30')]
                if yr == 2021:
                    df_yr = df[(df['Date'] >= f"2020-10-1") & (df['Date'] <= f'{yr}-4-20')]
                    visible = True
            else:
                df_yr = df[(df['Date'] >= f"{yr-1}-10-1") & (df['Date'] <= f'{yr}-9-30')]

            # Last Year trace
            for column in df_yr.columns:
                if column != 'Date':
                    lineChart.add_trace(go.Scatter(x=df_ave["Date"],
                                                   y=df_yr[column],
                                                   mode='lines',
                                                   hovertemplate="AF at Elev: %{y}\"<extra></extra>",
                                                   name=f'WY {yr} between {column} ft',
                                                   visible="legendonly"),
                                                )

        # Update all aspects of the chart
        lineChart.update_layout(paper_bgcolor='rgb(255,255,255)',
                          plot_bgcolor="rgb(255,255,255)",
                          title=f"Total PCWA Basin SWE By Elevation Band {wy}",
                          showlegend=True,
                          hovermode="x unified",
                          legend=dict(
                              yanchor="top",
                              y=0.99,
                              xanchor="left",
                              x=0),
                          font=dict(
                              size=24,
                              color="black"
                          )
                          )

        #wfolder_path = pathlib.Path('G:/', 'Energy Marketing', 'Weather', 'Programs', 'Lake_Spaulding')
        wfolder_path = os.getcwd()

        #external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

        #app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
        app.layout = html.Div([html.H4("Choose one or more years"),
            dcc.Dropdown(
                id='wy_dropdown',
                options=[
                    {'label': 'WY 2021', 'value': '2021'},
                    {'label': 'WY 2020', 'value': '2020'},
                    {'label': 'WY 2019', 'value': '2019'},
                    {'label': 'WY 2018', 'value': '2018'},
                    {'label': 'WY 2017', 'value': '2017'},
                    {'label': 'WY 2016', 'value': '2016'},
                    {'label': 'WY 2015', 'value': '2015'},
                    {'label': 'WY 2014', 'value': '2014'},
                    {'label': 'WY 2013', 'value': '2013'},
                    {'label': 'WY 2012', 'value': '2012'},
                    {'label': 'WY 2011', 'value': '2011'},
                    {'label': 'WY 2010', 'value': '2010'},
                    {'label': 'WY 2009', 'value': '2009'},
                    {'label': 'WY 2008', 'value': '2008'},
                    {'label': 'WY 2007', 'value': '2007'},
                    {'label': 'WY 2006', 'value': '2006'},
                    {'label': 'WY 2005', 'value': '2005'},
                    {'label': 'WY 2004', 'value': '2004'},
                ],
                value=['2021'],
                multi=True
            ),
            html.Div(children=[
                dcc.Graph(figure=lineChart, id='wy_graph'),
            ],id='wy-output-container'),
            html.Div(id='slider-container',
            children=[html.H4("Slide To Choose Elevation Band"),
                      dcc.Slider(id='wy_slider',
                                 min=3000,
                                 max=7500,
                                 step=500,
                                 value=5000,
                                 marks={
                                     3000: {'label': '3-3.5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     3500: {'label': '3.5-4k ft',
                                            'style': {'color': '#77b0b1'}},
                                     4000: {'label': '4-4.5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     4500: {'label': '4.5-5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     5000: {'label': '5-5.5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     5500: {'label': '5.5-6k ft',
                                            'style': {'color': '#77b0b1'}},
                                     6000: {'label': '6-6.5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     6500: {'label': '6.5-6k ft',
                                            'style': {'color': '#77b0b1'}},
                                     7000: {'label': '7-7.5k ft',
                                            'style': {'color': '#77b0b1'}},
                                     7500: {'label': '7.5-8k ft',
                                            'style': {'color': '#77b0b1'}},
                                 }
                                 ),
                      ]),
        ])

        @app.callback(
            dash.dependencies.Output('wy_graph', 'figure'),
            [dash.dependencies.Input('wy_slider', 'value'),dash.dependencies.Input('wy_graph', 'figure'),
             dash.dependencies.Input('wy_dropdown', 'value')])
        def update_output(elev_slider, figure, wy):
            elev = dict({'3000': '3.0-3.5',
                    '3500': '3.5-4.0',
                    '4000': '4.0-4.5',
                    '4500': '4.5-5.0',
                    '5000': '5.0-5.5',
                    '5500': '5.5-6.0',
                    '6000': '6.0-6.5',
                    '6500': '6.5-7.0',
                    '7000': '7.0-7.5',
                    '7500': '7.5-8.0'})
            elev_col = elev[str(elev_slider)]
            lineChart = (go.Figure(data=[go.Scatter(
                            x=df_ave.index,
                            y=df_ave[elev_col],
                            name="15 Year Average",
                            hovertext=df_ave[elev_col],
                            hovertemplate="Average: %{y:.1f}\"<extra></extra>",
                            fill='tozeroy',
                            line=dict(width=0.5, color='rgb(111, 231, 219)'),
                            marker_line_width=2,
                            texttemplate='%{hovertext}',
                            textposition="top center",
                            textfont=dict(
                                family="Arial",
                                size=24,
                                color="rgba(0, 0, 0)"
                                    )
                                )]
                                )
                )
            for yr in wy:
                yr = int(yr)
                if yr == 2004 or yr == 2021:
                    if yr == 2004:
                        df_yr = df[(df['Date'] >= f"2004-1-1") & (df['Date'] <= f'{yr}-9-30')]
                    if yr == 2021:
                        #last_date = df['Date'].iloc[-1]
                        df_yr = df[(df['Date'] >= f"2020-10-1") & (df['Date'] <= f'{yr}-4-26')]
                else:
                    df_yr = df[(df['Date'] >= f"{yr-1}-10-1") & (df['Date'] <= f'{yr}-9-30')]
                lineChart.add_trace(go.Scatter(x=df_ave["Date"],
                                               y=df_yr[elev_col],
                                               mode='lines',
                                               hovertemplate="Total AF on %{x}: %{y}<extra></extra>",
                                               name=f'WY {yr} between {elev_col} ft'),
                                    )
            lineChart.update_layout(title=dict(
                                        text=f'<b>Total Acre Feet within {elev_col} ft elevation band</b>',
                                        x=0.5,
                                        font=dict(
                                            family="Arial",
                                            size=20,
                                            color='#000000'
                                        )
                                        ),
                                    height=500,
                                    yaxis_title="Total Acre Feet",)

            return lineChart

        #app.run_server(debug=True, use_reloader=False)  # Turn off reloader if inside Jupyter
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
        return None


# Press the green button in the gutter to run the script.
if __name__ == 'snodas.dash_snodas':
    create_plots()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/