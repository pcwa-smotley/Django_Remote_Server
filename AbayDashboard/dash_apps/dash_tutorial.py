import dash
import dash_core_components as dcc
import dash_html_components as html
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'AbayTracker.settings')
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
from plotly import graph_objs as go

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


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', 'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css']
external_javascript = ["https://code.jquery.com/jquery-3.5.1.min.js", "https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.bundle.min.js"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, external_scripts=external_javascript)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

# def abay_forecast(df, df_pi):
#     # PMIN / PMAX Calculations
#     const_a = 0.09  # Default is 0.0855.
#     const_b = 0.135422  # Default is 0.138639
#
#     ########## GET OXBOW GENERRATION FORECAST DATA ###################
#     try:
#         # Download the data for the Oxbow and MFPH Forecast (data start_time is -24 hours from now, end time is +72 hrs)
#         pi_data_ox = PiRequest("OPS", "Oxbow", "Forecasted Generation", True)
#         # pi_data_gen = PiRequest("Energy_Marketing", None, "MFRA_Forecast", True)
#
#         df_fcst = pd.DataFrame.from_dict(pi_data_ox.data)
#
#         # This will need to be changed to the following:
#         # df_fcst["MFRA_fcst"] = pd.DataFrame.from_dict(pi_data_gen.data)['Value']
#
#         df_fcst["MFRA_fcst"] = pd.DataFrame.from_dict(pi_data_ox.data)['Value']
#
#         # For whatever reason, the data are of type "object", need to convert to float.
#         df_fcst["MFRA_fcst"] = pd.to_numeric(df_fcst.MFRA_fcst, errors='coerce')
#
#         # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
#         df_fcst.Timestamp = pd.to_datetime(df_fcst.Timestamp).dt.tz_convert('US/Pacific')
#         df_fcst.index = df_fcst.Timestamp
#         df_fcst.index.names = ['index']
#
#         # For whatever reason, the data are of type "object", need to convert to float.
#         df_fcst["Value"] = pd.to_numeric(df_fcst.Value, errors='coerce')
#
#         df_fcst.rename(columns={"Value": "Oxbow_fcst"}, inplace=True)
#
#         # These columns can't be resampled to hourly (they contain strings), so remove them.
#         df_fcst.drop(["Good", "Questionable", "Substituted", "UnitsAbbreviation"], axis=1, inplace=True)
#
#         # Resample the forecast to hourly to match CNRFC time. If this is not done, the following merge will fail.
#         # The label = right tells this that we want to use the last time in the mean as the label (i.e. hour ending)
#         df_fcst = df_fcst.resample('60min', label='right').mean()
#
#         # Merge the forecast to the CNRFC using the GMT column for the cnrfc and the index for the oxbow fcst data.
#         df = pd.merge(df, df_fcst[["Oxbow_fcst", "MFRA_fcst"]], left_on="GMT", right_index=True, how='outer')
#
#         # Calculate the Pmin and Pmax in the same manner as with the historical data.
#         df["Pmin1"] = const_a * (df["R4_fcst"] - 26)
#         df["Pmin2"] = (-0.14 * (df["R4_fcst"] - 26) * ((df_pi["Hell_Hole_Elevation"].iloc[-1] - 2536) / (4536 - 2536)))
#
#         df["Pmin"] = df[["Pmin1", "Pmin2"]].max(axis=1)
#
#         df["Pmax1"] = ((const_a + const_b) / const_b) * (
#                     124 + (const_a * df["R4_fcst"] - df_pi["R5_Flow"].iloc[-1]))
#         df["Pmax2"] = ((const_a + const_b) / const_a) * (
#                     86 - (const_b * df["R4_fcst"] - df_pi["R5_Flow"].iloc[-1]))
#
#         df["Pmax"] = df[["Pmax1", "Pmax2"]].min(axis=1)
#
#         # Drop unnesessary columns.
#         df.drop(["Pmin1", "Pmin2", "Pmax1", "Pmax2"], axis=1, inplace=True)
#     except Exception as e:
#         print(f"Could Not Find Metered Forecast Data (e.g. Oxbow Forecast): {e}")
#         df["Oxbow_fcst"] = np.nan
#         logging.warning(f"Could Not Find Metered Forecast Data (e.g. Oxbow Forecast). Error Message: {e}")
#     ################### END OXBOW FORECAST ##############################
#
#     # Default ratio of the contribution of total power that is going to Ralston.
#     RAtoMF_ratio = 0.41
#
#     # 1 cfs = 0.0826 acre feet per hour
#     cfs_to_afh = 0.0826448
#
#     CCS = False
#
#     # The last reading in the df for the float set point
#     float = df_pi["Afterbay_Elevation_Setpoint"].iloc[-1]
#
#     #df_pi.set_index('Timestamp', inplace=True)
#     #abay_inital = df_pi["Afterbay_Elevation"].truncate(before=(datetime.now(timezone.utc)-timedelta(hours=24)))
#
#     # The PI data we retrieve goes back 24 hours. The initial elevation will give us a chance to test the expected
#     # abay elevation vs the actual abay elevation. The abay_initial is our starting point.
#     # Note: For resampled data over an hour, the label used for the timestamp is the first time stamp, but since
#     #       we want hour ending, we want the last time to be used at the label (label = right).
#     df_pi_hourly = df_pi.resample('60min', on='Timestamp', label='right').mean()
#
#     # Get any observed values that have already occurred from the PI data.
#     df_pi_hourly["RA_MW"] = np.minimum(86, df_pi_hourly["GEN_MDFK_and_RA"] * RAtoMF_ratio)
#     df_pi_hourly["MF_MW"] = np.minimum(128, df_pi_hourly["GEN_MDFK_and_RA"] - df_pi_hourly['RA_MW'])
#
#     # Elevation observed at the beginning of our dataset (24 hours ago). This serves as the starting
#     # point for our forecast, so that we can see if it's trued up as we go forward in time.
#     abay_inital_elev = df_pi_hourly["Afterbay_Elevation"].iloc[0]
#
#     # Convert elevation to AF ==> y = 0.6334393x^2 - 1409.2226x + 783749
#     abay_inital_af = (0.6334393*(abay_inital_elev**2))-1409.2226*abay_inital_elev+783749
#
#     # Ralston's Max output is 86 MW; so we want smaller of the two.
#     df["RA_MW"] = np.minimum(86, df["MFRA_fcst"] * RAtoMF_ratio)
#     df["MF_MW"] = np.minimum(128, df["MFRA_fcst"]-df['RA_MW'])
#
#     # This is so we can do the merge below (we need both df's to have the same column name). The goal is to overwrite
#     # any "forecast" data for Oxbow with observed values. There is no point in keeping forecast values in.
#     df_pi_hourly.rename(columns={"Oxbow_Power": "Oxbow_fcst"}, inplace=True)
#
#     # This is a way to "update" the generation data with any observed data. First merge in any historical data.
#     df = pd.merge(df, df_pi_hourly[["RA_MW", "MF_MW", "Oxbow_fcst"]],
#                   left_on="GMT", right_index=True, how='left')
#
#     # Next, since we already have an RA_MF column, the merge will make a _x and _y. Just fill the original with
#     # the new data (and any bad data will be nan) and store all that data as RA_MW.
#     df["RA_MW"] = df['RA_MW_y'].fillna(df['RA_MW_x'])
#     df["MF_MW"] = df['MF_MW_y'].fillna(df['MF_MW_x'])
#     df["Oxbow_fcst"] = df['Oxbow_fcst_y'].fillna(df['Oxbow_fcst_x'])
#
#     # We don't need the _y and _x, so drop them.
#     df.drop(['RA_MW_y', 'RA_MW_x', 'MF_MW_y', 'MF_MW_x', 'Oxbow_fcst_x','Oxbow_fcst_y'], axis=1, inplace=True)
#
#     # Conversion from MW to cfs ==> CFS @ Oxbow = MW * 163.73 + 83.956
#     df["Oxbow_Outflow"] = (df["Oxbow_fcst"] * 163.73) + 83.956
#
#     # R5 Valve never changes (at least not in the last 5 years in PI data)
#     df["R5_Valve"] = 28
#
#     # If CCS is on, we need to account for the fact that Ralston will run at least at the requirement for the Pmin.
#     if CCS:
#         #df["RA_MW"] = max(df["RA_MW"], min(86,((df["R4_fcst"]-df["R5_Valve"])/10)*RAtoMF_ratio))
#         df["RA_MW"] = np.maximum(df["RA_MW"], df["Pmin"] * RAtoMF_ratio)
#
#     # Polynomial best fits for conversions.
#     df["RA_Inflow"] = (0.005*(df["RA_MW"]**3))-(0.0423*(df["RA_MW"]**2))+(10.266*df["RA_MW"]) + 2.1879
#     df["MF_Inflow"] = (0.0049 * (df["MF_MW"] ** 2)) + (6.2631 * df["MF_MW"]) + 18.4
#
#     # The linear MW to CFS relationship above doesn't apply if Generation is 0 MW. In that case it's 0 (otherwise the
#     # value would be 83.956 due to the y=mx+b above where y = b when x is zero, we need y to = 0 too).
#     df.loc[df['MF_MW'] == 0, 'RA_Inflow'] = 0
#     df.loc[df['RA_MW'] == 0, 'MF_Inflow'] = 0
#     df.loc[df['Oxbow_fcst'] == 0, 'Oxbow_Outflow'] = 0
#
#     # It helps to look at the PI Vision screen for this.
#     # Ibay In: 1) Inflow from MFPH (the water that's powering MFPH)
#     #          2) The water flowing in at R4
#     # Ibay Out: 1) Valve above R5 (nearly always 28)         = 28
#     #           2) Outflow through tunnel to power Ralston.  = RA_out (CAN BE INFLUENCED BY CCS MODE, I.E. R4)
#     #           3) Spill                                     = (MF_IN - RA_OUT) + R4
#     #
#     #                                |   |
#     #                                |   |
#     #                                |   |
#     #                          ___MFPH INFLOW____
#     #                          |                |
#     #     OUTFLOW (RA INFLOW)  |                |  R4 INFLOW
#     #                    ------|            <---|--------
#     #               <--- ------|                |--------
#     #                          |                |
#     #                           ---SPILL+R5----
#     #                                |   |
#     #                R20             |   |
#     #                --------------- |   |
#     #                ---------------------
#     #
#     #           Inflow into IBAY  = MF_GEN_TO_CFS (via day ahead forecast --> then converted to cfs) + R4 Inflow
#     #             Inflow to ABAY  = RA_GEN_TO_CFS (either via DA fcst or R4 if CCS is on) + R20
#     #        Where RA_GEN_TO_CFS  = MF_GEN_TO_CFS * 0.41
#     #                         R20 = R20_RFC_FCST + SPILL + R5
#     #                       SPILL = R4_RFC_Fcst + MAX(0,(MF_GEN_TO_CFS - RA_GEN_TO_CFS)) + R5
#     #        THEREFORE:
#     #        Inflow Into Abay = RA_GEN_TO_CFS + R20_RFC_FCST + R4_RFC_fcst + AX(0,(MF_GEN_TO_CFS - RA_GEN_TO_CFS)) + R5
#     #
#     # Ibay In - Ibay Out = The spill that will eventually make it into Abay through R20.
#     df["Ibay_Spill"] = np.maximum(0,(df["MF_Inflow"] - df["RA_Inflow"])) + df["R5_Valve"] + df['R4_fcst']
#
#     # CNRFC is just forecasting natural flow, which I believe is just everything from Ibay down. Therefore, it should
#     # always be too low and needs to account for any water getting released from IBAY.
#     df["R20_fcst_adjusted"] = df["R20_fcst"] + df["Ibay_Spill"]
#
#     df["Abay_Inflow"] = df["RA_Inflow"]+df["R20_fcst_adjusted"]+df["R30_fcst"]
#     df["Abay_Outflow"] = df["Oxbow_Outflow"]
#
#     df["Abay_AF_Change"] = (df["Abay_Inflow"]-df["Abay_Outflow"])*cfs_to_afh
#
#     first_valid = df["Abay_AF_Change"].first_valid_index()
#     for i in range(first_valid, len(df)):
#         if i == first_valid:
#             df.loc[i, "Abay_AF_Fcst"] = abay_inital_af
#         else:
#             df.loc[i, "Abay_AF_Fcst"] = df.loc[i-1,"Abay_AF_Fcst"] + df.loc[i, "Abay_AF_Change"]
#
#     # y = -1.4663E-6x^2+0.019776718*x+1135.3
#     df["Abay_Elev_Fcst"] = np.minimum(float, (-0.0000014663 *
#                                              (df["Abay_AF_Fcst"] ** 2)+0.0197767158*df["Abay_AF_Fcst"]+1135.3))
#     return df
#
# def update_data(rfc_json_data):
#     # This will store the data for all the PI requests
#     df_all = pd.DataFrame()
#
#     meters = [PiRequest("OPS", "R4", "Flow"), PiRequest("OPS", "R11", "Flow"),
#               PiRequest("OPS", "R30", "Flow"), PiRequest("OPS", "Afterbay", "Elevation"),
#               PiRequest("OPS", "Afterbay", "Elevation Setpoint"),
#               PiRequest("OPS", "Oxbow", "Power"), PiRequest("OPS","R5","Flow"),
#               PiRequest("OPS","Hell Hole","Elevation"),
#               PiRequest("Energy_Marketing", None, "GEN_MDFK_and_RA"),
#               PiRequest("Energy_Marketing", None, "ADS_MDFK_and_RA"),
#               PiRequest("Energy_Marketing", None, "ADS_Oxbow"),
#                 ]
#     for meter in meters:
#         try:
#             df_meter = pd.DataFrame.from_dict(meter.data)
#
#             # If there was an error getting the data, you will have an empty dataframe, escape for loop
#             if df_meter.empty:
#                 return None
#
#             # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
#             df_meter.Timestamp = pd.to_datetime(df_meter.Timestamp).dt.tz_convert('US/Pacific')
#             df_meter.index = df_meter.Timestamp
#             df_meter.index.names = ['index']
#
#             # Remove any outliers or data spikes
#             # df_meter = drop_numerical_outliers(df_meter, meter, z_thresh=3)
#
#             # Rename the column (this was needed if we wanted to merge all the Value columns into a dataframe)
#             renamed_col = (f"{meter.meter_name}_{meter.attribute}").replace(' ', '_')
#
#             # For attributes in the Energy Marketing folder, the name is "None", so just use attribute
#             if meter.meter_name is None:
#                 renamed_col = (f"{meter.attribute}").replace(' ', '_')
#             df_meter.rename(columns={"Value": f"{renamed_col}"}, inplace=True)
#
#             if df_all.empty:
#                 df_all = df_meter
#             else:
#                 df_all = pd.merge(df_all, df_meter[["Timestamp", renamed_col]], on="Timestamp", how='outer')
#
#         except ValueError as e:
#             print('Pandas Dataframe May Be Empty')
#             logging.warning(f"Updating PI data produced empty data frame. Error: {e}")
#             return None
#
#     # PMIN / PMAX Calculations
#     const_a = 0.09      # Default is 0.0855.
#     const_b = 0.135422  # Default is 0.138639
#     try:
#         df_all["Pmin1"] = const_a*(df_all["R4_Flow"]-df_all["R5_Flow"])
#         df_all["Pmin2"] = (-0.14*(df_all["R4_Flow"]-df_all["R5_Flow"])*
#                               ((df_all["Hell_Hole_Elevation"]-2536)/(4536-2536)))
#         df_all["Pmin"] = df_all[["Pmin1","Pmin2"]].max(axis=1)
#
#         df_all["Pmax1"] = ((const_a+const_b)/const_b)*(124+(const_a*df_all["R4_Flow"]-df_all["R5_Flow"]))
#         df_all["Pmax2"] = ((const_a+const_b)/const_a)*(86-(const_b*df_all["R4_Flow"]-df_all["R5_Flow"]))
#
#         df_all["Pmax"] = df_all[["Pmax1","Pmax2"]].min(axis=1)
#
#         df_all.drop(["Pmin1","Pmin2", "Pmax1", "Pmax2"], axis=1, inplace=True)
#     except ValueError as e:
#         print("Can Not Calculate Pmin or Pmax")
#         df_all[["Pmin", "Pmax"]] = np.nan
#         logging.info(f"Unable to caluclate Pmin or Pmax {e}")
#
#     # The first time this code is hit, the div containing the data should not have the
#     # CNRFC data in it. Therefore, we need to download it.
#     if not rfc_json_data:
#         ######################   CNRFC SECTION ######################################
#         # Get the CNRFC Data. Note, we are putting this outside the PI request since
#         # it's entirely possible these data are not avail. If it fails, it will just
#         # skip over this portion and return a df without the CNRFC data
#         today12z = datetime.now().strftime("%Y%m%d12")
#         yesterday12z = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d12")
#         file_dates = [yesterday12z, today12z]
#         df_cnrfc_list = []
#         most_recent_file = None
#         for file in file_dates:
#             try:
#                 df_cnrfc_list.append(pd.read_csv(f"https://www.cnrfc.noaa.gov/csv/{file}_american_csv_export.zip"))
#                 most_recent_file = file  # The date last file successfully pulled.
#             except (HTTPError, URLError) as error:
#                 logging.warning(f'CNRFC HTTP Request failed {error} for {file}. Error code: {error}')
#                 print(f'CNRFC HTTP Request failed {error} for {file}')
#
#         # The last element in the list will be the most current forecast. Get that one.
#         df_cnrfc = df_cnrfc_list[-1].copy()
#
#         # Case for failed download and empty dataframe
#         if df_cnrfc.empty:
#             df_cnrfc = pd.date_range(start=datetime.utcnow() - timedelta(hours=48),
#                                      end= datetime.utcnow() + timedelta(hours=72), freq='H', normalize=True)
#             df_cnrfc[["FORECAST_ISSUED", "R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = np.nan
#
#         # Download was successful, continue
#         else:
#             # Put the forecast issued time in the dataframe so we can refer to it later.
#             df_cnrfc["FORECAST_ISSUED"] = pd.to_datetime(datetime.strptime(most_recent_file, "%Y%m%d%H"))
#
#             # Drop first row (the header is two rows and the 2nd row gets put into row 1 of the df; delete it)
#             df_cnrfc = df_cnrfc.iloc[1:]
#
#             # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
#             df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
#
#             df_cnrfc.rename(columns={"MFAC1L": "R20_fcst", "RUFC1": "R30_fcst", "MFPC1": "R4_fcst", "MFAC1": "R11_fcst"}, inplace=True)
#             df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]].apply(
#                 pd.to_numeric) * 1000
#     # Dataframe already exists in html
#     else:
#         df_cnrfc = pd.read_json(rfc_json_data, orient='index')
#         df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_convert('US/Pacific')
#     ######################## END CNRFC ###########################################
#
#     # Add in the remainder of any forecast data (e.g. Oxbow Forecast, Abay Fcst) to the cnrfc dataframe
#     if 'Oxbow_fcst' not in df_cnrfc:
#         df_cnrfc = abay_forecast(df_cnrfc, df_all)
#     return df_all, df_cnrfc

df_table = pd.read_csv('https://gist.githubusercontent.com/chriddyp/c78bf172206ce24f77d6363a2d754b59/raw/c353e8ef842413cae56ae3920b8fd78468aa4cb2/usa-agricultural-exports-2011.csv')
df_cycg = pd.read_excel(staticfiles_storage.path("data/CYCG_BIDS.xlsx"), engine='openpyxl', sheet_name=None)
# This will store the data for all the PI requests
#df_all, df_cnrfc = update_data(None)

# This is for the abay levels. We're just going to show the data on an hourly basis.
#
#df_hourly_resample = df_all.resample('60min', on='Timestamp').mean()


def generate_table(dataframe, max_rows=100):
    df_formated = dataframe.copy()
    df_formated["pretty_date"] = df_formated["Date"].dt.strftime("%a %b %d")

    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in df_formated[["pretty_date", "HE", "Self Schedule (mw)", "(MW1)", "(Price1)",
                                                        "DA_Price", "(MW1)_RT", "(Price1)_RT"]]])
        ),
        html.Tbody([
            html.Tr([
                html.Td(df_formated.iloc[i][col]) for col in df_formated[["pretty_date", "HE", "Self Schedule (mw)", "(MW1)", "(Price1)",
                                                        "DA_Price", "(MW1)_RT", "(Price1)_RT"]]
            ]) for i in range(min(len(df_formated), max_rows))
        ])
    ])


def get_prt():
    user = "pwca"
    password = "p9071p"

    # Generate a session with username and password info
    session = requests.Session()
    session.auth = (user, password)

    hostname = "https://www.prt-inc.com/forecast/Private/CaisAhdThNp15GenApnd/forecast.htm"
    response = requests.get(hostname, auth=(user, password))

    # The dataframe will contain several tables (i.e. multiple dataframes)
    df_prt = pd.read_html(response.text, header=0)

    # The website contains multiple tables, the first one is the load forecast.
    df_prt_load=df_prt[1]

    # The second table is the price forecast.
    df_prt_price=df_prt[2]

    # Remove the last two rows of the df rows for ("On-Peak and Off-Peak). Leaving this in will
    # cause an error when treating the Hour column as an integer.
    df_prt_price=df_prt_price.iloc[:-2]

    # Get the first date (day 0). We must do this because none of the columns have year info.
    # Therefore, we can't put "year = now().year" because it would fail if today is Dec 31st and the next
    # column was Jan 1st. Instead we will get day0 and let python add a date for every column.

    # The first column with date info is in column [2]
    first_date_column = df_prt_price.columns[2]

    # The columns are labeled as (e.g. Jun.1.Fri).
    # Group one: [A-Za-z]+ Match all letters and cases in any amount.
    #           \.? Match a period (\.), only one time (?)
    # Group two: \s Accept a space if needed and (for single digit) [0-9] Match all numbers, any number of times (+)
    match = re.match(r"([A-Za-z]+\.?)([\s0-9]+)", first_date_column, re.I)

    # Group 0 is the entire string, group 1 is the month, and group 2 is the day of the month.
    col_month = match.group(1)
    col_day = match.group(2)

    # Get the actual date into a datetime format.
    day0 = datetime.strptime(f"{col_month}{col_day}{datetime.now().year}","%b.%d%Y") - timedelta(days=1)

    # Rename each column, where ind will be the column number.
    for ind, column in enumerate(df_prt_price):
        # Don't change the first column, which is the hour column.
        if column != "Hour":
            col_date = day0+timedelta(days=(ind-1))
            if int(col_day) < 10:
                col_day = f"0{int(col_day)}"
            df_prt_price = df_prt_price.rename(columns={column: datetime.strftime(col_date, "%m-%d-%Y")})
    return df_prt_load, df_prt_price


def dataframe_creator(df_prt_price, df_cycg):
    """
    Purpose: Convert dataframes into a single dataframe with a datetime index.
            ==> Each dataframe has columns as dates and hours as rows. We need to convert these so that
                we have a single set of data that spans rows of datetime and contains multiple dates along the
                row axis. To do this, start with three empty dataframes that will append data to them for each day
                of data:
                1) df_DA will be the dataframe to hold all of the Day Ahead information.
                2) df_RT will hold all the Real Time information.
                3) df_prt_converted will hold all the PRT price info.
    :param df_prt_price: The PRT price info with columns being the dates and rows being the hour.
    :param df_cycg: A multi-sheet dataframe that is an absolute mess. This dataframe is from the excel spreadsheet
                    and contains both DA and RT data in a single sheet. We are only interested in pulling the
                    data for MFPH, and will basically slice the Dataframe by rows and columns to get the info.
    :return: A single dataframe with each row representing a specific hour ending date.
    """

    # The main dataframe that will be returned. It is a time series spanning 7 days (two days back, 5 forward).
    df = pd.DataFrame({"Date": pd.date_range(start=datetime.utcnow()+timedelta(days=-2),
                       end=datetime.utcnow()+timedelta(days=5), freq='H', normalize=True)})

    # Create a Date column that will help us during the merge process.
    df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize('UTC')
    df.index = df['Date']
    df.index.names = ['index']

    # Create the three empty dataframes to build data into in a datetime format.
    df_DA = pd.DataFrame()
    df_RT = pd.DataFrame()
    df_prt_converted = pd.DataFrame()

    # The CYCG data is a multisheet file (so multiple dataframes within the single dataframe). Loop through
    # each sheet, which represents a single day with both DA and RT data.
    for sheet in df_cycg:
        # Find the date column by first locating the "Operating Date" column. The actual date will be in the column
        # next to this one (so +1)
        date_col = df_cycg[sheet].columns.get_loc("Operating Date:") + 1

        # This is a string, so convert the date on the sheet to an actual date.
        sheet_date = datetime.strptime(df_cycg[sheet].columns[date_col], "%m-%d-%Y")

        # The use limit value is also on the sheet. It's in row 1, column 10.
        use_limit = df_cycg[sheet].iloc[1,10]
        df_cycg[sheet]["use_limit"] = use_limit

        # Rename headers and shift up. This is the only way to clean it up.
        df_cycg[sheet].columns = df_cycg[sheet].iloc[2]

        # The DA sheet for MFPH is from row 3 to 27, columns 1 to 16
        df_DA_day = df_cycg[sheet].iloc[3:27, 1:16]

        # The RT portion for MFPH is from row 32 to 56, columns 1 to 16
        df_RT_day = df_cycg[sheet].iloc[32:56, 1:16]

        # If there are no bids, the HE column will contain "HE 1" instead of just "1". Remove the "HE" portion.
        try:
            df_DA_day['HE'] = df_DA_day['HE'].str.replace(r"[a-zA-Z]",'')
            df_DA_day['HE'] = df_DA_day['HE'].astype(int)
        except AttributeError as e:
            print("Ok. No strings in HE column. Continue...")

        # If there are no bids, the HE column will contain "HE 1" instead of just "1". Remove the "HE" portion.
        try:
            df_RT_day['HE'] = df_RT_day['HE'].str.replace(r"[a-zA-Z]", '')
            df_RT_day['HE'] = df_RT_day['HE'].astype(int)
        except AttributeError as e:
            print("Ok. No strings in HE column. Continue...")

        # Add a column for the date info so we can do a proper merge.
        df_DA_day["operating_date"]= pd.to_datetime(sheet_date + pd.to_timedelta(df_DA_day['HE'], unit='h')).dt.tz_localize('UTC')
        df_RT_day["operating_date"]= pd.to_datetime(sheet_date + pd.to_timedelta(df_RT_day['HE'], unit='h')).dt.tz_localize('UTC')

        # The column names are the same for DA and RT in the spreadsheet, so rename all the RT columns.
        for column in df_RT_day.columns:
            df_RT_day.rename(columns={column: f"{column}_RT"}, inplace=True)

        # Initial fill of the dataframes
        if df_RT.empty:
            df_RT = df_RT_day
            df_DA = df_DA_day
        # If they are already filled, the column names will be the same in the next sheet, so we can append using
        # the concatinate function. This builds the dataframe into datetime dataframe.
        else:
            df_RT = pd.concat([df_RT, df_RT_day])
            df_DA = pd.concat([df_DA, df_DA_day])

    # Now that we have a DA and RT dataframe with rows in datetime format, merge them into the empty df.
    df = pd.merge(df,df_DA, left_on="Date", right_on="operating_date", how='left')
    df = pd.merge(df,df_RT, left_on="Date", right_on="operating_date_RT", how='left')

    # Portion for adding in the PRT price info into the dataframe
    for column in df_prt_price.columns:
        # The first column is "Hour". All others are a date (e.g. '3-1-2023'). If it's not a date, don't throw an error.
        try:
            # Convert to a datetime object.
            col_date = datetime.strptime(column, "%m-%d-%Y")

            # The hour column is a string, convert to int so we can make our datetime column.
            df_prt_price['Hour'] = df_prt_price['Hour'].astype(int)
            df_prt_price["Date_Valid"] = pd.to_datetime(col_date + pd.to_timedelta(df_prt_price['Hour'], unit='h')).dt.tz_localize('UTC')

            # To append this data into the other dataframe, we need the same name every time. This is a
            # trick to make a new column, and just copy the column of interest into it. That way we can reference
            # the "DA_Price" column and just append that into the df.
            df_prt_price["DA_Price"] = df_prt_price[column]

            # The first time this is hit, just copy the date and the price into into the new df
            if df_prt_converted.empty:
                df_prt_converted = df_prt_price[["DA_Price","Date_Valid"]]
            # Otherwise, keep appending to the df
            else:
                df_prt_converted = pd.concat([df_prt_converted, df_prt_price[["DA_Price","Date_Valid"]]])
        # Handle the case where the column wasn't a date.
        except ValueError as e:
            print(f"{e}: Can not convert column named {column} to datetime. Continuing to next column...")
    df = pd.merge(df, df_prt_converted, left_on="Date", right_on="Date_Valid", how='left')
    return df


df_prt_load, df_prt_price = get_prt()
df_all = dataframe_creator(df_prt_price, df_cycg)
#fig = px.line(df_cnrfc, x=df_cnrfc["GMT"][df_cnrfc["Abay_Elev_Fcst"].notnull()], y=df_cnrfc["Abay_Elev_Fcst"][df_cnrfc["Abay_Elev_Fcst"].notnull()])
fig = px.bar(df_prt_price, x=df_prt_price["Hour"], y=df_prt_price[df_prt_price.columns[2]], barmode="group")

app.layout = html.Main(
        children=[
            dbc.Row(children=[
                dbc.Col(
                dcc.Dropdown(
                    id="price_dropdown",
                    options=[
                        {'label': i, 'value': i} for i in df_prt_price.columns
                    ],
                ), width=6
                )
            ]),
            dbc.Row(children=[
            html.Div(children=[
            html.H1(children='Afterbay Tracker'),

            dcc.Graph(
                id='example-graph',
                figure=fig
            )], className="ml-2 col-sm-5 col-md-5 col-lg-5",
            ),
        html.Div(children=[
            html.H4(children='Abay Tracker'),
            generate_table(df_all)
            #generate_table(df_cnrfc[["GMT","Abay_Elev_Fcst","R4_fcst","R30_fcst","R20_fcst_adjusted"]][df_cnrfc["Abay_Elev_Fcst"].notnull()])
        ], className="ml-2 col-sm-5 col-md-5 col-lg-5")
        ]),
        html.Div([
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select Files')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                # Allow multiple files to be uploaded
                multiple=True
            ),
            html.Div(id='output-data-upload'),
        ])
    ])


def parse_excel(contents, filename, date):
    content_type, content_string = contents.split(',')
    main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded), sheet_name=None, engine='openpyxl')
            writer = pd.ExcelWriter(staticfiles_storage.path("data/CYCG_BIDS.xlsx"))
            for sheet_name in df:
                df[sheet_name].to_excel(writer, sheet_name)
            writer.save()
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    df_list = list(df)
    return html.Div([
        html.H5(filename),
        html.H6(datetime.fromtimestamp(date)),
        dash_table.DataTable(
            data=df[df_list[0]].to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df[df_list[0]].columns]
        ),

        html.Hr(),  # horizontal line

        # For debugging, display the raw contents provided by the web browser
        html.Div('Raw Content'),
        html.Pre(contents[0:200] + '...', style={
            'whiteSpace': 'pre-wrap',
            'wordBreak': 'break-all'
        })
    ])


@app.callback(Output('example-graph','figure'),
              [Input('price_dropdown', 'value'),
               Input('example-graph', 'figure')])
def update_graph(selection, figure):
    if selection is not None:
        df = pd.DataFrame()
        for sheet in df_cycg:
            test = df_cycg[sheet]
            date_col = df_cycg[sheet].columns.get_loc("Operating Date:") + 1
            date_val = df_cycg[sheet].columns[date_col]
            sheet_date = datetime.strptime(df_cycg[sheet].columns[date_col], "%m-%d-%Y")
            if date_val == selection:
                df_da = df
        return go.Figure(
            data=[
                go.Bar(x=df_prt_price["Hour"], y=df_prt_price[selection], hoverinfo="y"),
            ],
            layout=figure['layout']
        )
    return figure


@app.callback(Output('output-data-upload', 'children'),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_excel(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children



if __name__ == '__main__':
    app.run_server(debug=True)