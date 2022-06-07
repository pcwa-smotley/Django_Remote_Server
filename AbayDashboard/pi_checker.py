'''
Purpose: This program loops every x minutes to download and store PI data. It will issue an alert if a threshold is met.
    1) main(): a) Runs every 1 minute to download PI data and store last 24 hours in 1m resolution on sqlite server.
               b) Will execute alarm checker to see if a user specified threshold is met.
               c) If so send alerts will be executed to email out alerts

    2) get_cnrfc_data(): a) Runs every 30 minutes, will download cnrfc data from website and store it on our server.
                         b) Then it will initiate abay_forecast() and update the SQL forecast_data table.
'''
import os
import sys
import platform
from datetime import datetime, timedelta
import pandas as pd
import django
import logging
from io import StringIO
from urllib.error import HTTPError, URLError
import smtplib

if "Linux" in platform.platform(terse=True):
    sys.path.append("/var/www/wx/")
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wx.settings')
django.setup()
from django.db import connection, IntegrityError
from django.db.models import Q
from django.conf import settings
from AbayDashboard.models import AlertPrefs, Profile, User, Issued_Alarms, Recreation_Data, ForecastData, PiData
import requests
from timeloop import Timeloop
from scipy import stats
import numpy as np
from mailer import send_mail
import pytz
import sqlite3

__version__ = "1.0.1"
__author__ = "Shane Motley"
__copyright__ = "Copyright 2022, PCWA"

t1 = Timeloop()

class PiRequest:
    # https://flows.pcwa.net/piwebapi/assetdatabases/D0vXCmerKddk-VtN6YtBmF5A8lsCue2JtEm2KAZ4UNRKIwQlVTSU5FU1NQSTJcT1BT/elements
    def __init__(self, db, meter_name, attribute, monitor_for_alerts=True, forecast=False):
        self.db = db                                        # Database (e.g. "Energy Marketing," "OPS")
        self.meter_name = meter_name                        # R4, Afterbay, Ralston
        self.monitor_for_alerts = monitor_for_alerts        # Is this a meter that can trigger an alert
        self.attribute = attribute                          # Flow, Elevation, Lat, Lon, Storage, Elevation Setpoint, Gate 1 Position, Generation
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
            #print(f'Response HTTP Status Code: {response.status_code} for {self.meter_name} | {self.attribute}')
            j = response.json()
            # We only want the "Items" object.
            return j["Items"]
        except requests.exceptions.RequestException:
            logging.warning(f"HTTP Failed For {self.meter_name} | {self.attribute}")
            print(f"HTTP Failed For {self.meter_name} | {self.attribute}")
            return None

    def meter_element_type(self):
        if not self.meter_name:
            return "Energy Marketing Tag"
        if self.attribute == "Flow":
            return "Gauging Stations"
        if "Afterbay" in self.meter_name or "Hell Hole" in self.meter_name:
            return "Reservoirs"
        if "Middle Fork" in self.meter_name or "Oxbow" in self.meter_name:
            return "Generation Units"


@t1.job(interval=timedelta(minutes=1))
def main():
    print("Current Time in PDT is: ", datetime.now(tz=pytz.timezone('US/Pacific')).strftime("%a %H:%M:%S %p"))
    meters = [PiRequest("OPS", "R4", "Flow"), PiRequest("OPS", "R11", "Flow"),
              PiRequest("OPS", "R30", "Flow"), PiRequest("OPS", "Afterbay", "Elevation"),
              PiRequest("OPS", "Afterbay", "Elevation Setpoint"), PiRequest("OPS", "Oxbow", "Gov Setpoint"),
              PiRequest("OPS", "Oxbow", "Power"), PiRequest("OPS", "R5", "Flow", False, False),
              PiRequest("OPS", "Hell Hole", "Elevation", False, False),
              PiRequest("Energy_Marketing", "MFP_Total_Gen", "GEN_MDFK_and_RA"),
              PiRequest("Energy_Marketing", "MFP_ADS", "ADS_MDFK_and_RA"),
              PiRequest("Energy_Marketing", "Ox_ADS", "ADS_Oxbow"),
              ]
    df_all = pd.DataFrame()
    for meter in meters:
        # Now that we have the url for the PI data, this request is for the actual data. We will
        # download data from the beginning of the water year to the current date. (We can't download data
        # past today's date, if we do we'll get an error.
        try:
            df_meter = pd.DataFrame.from_dict(meter.data)

            # There is an issue where PI is reporting a dictionary of data for missing data points.
            df_meter["Value"] = df_meter["Value"].map(lambda x: np.nan if isinstance(x, dict) else x)

            # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
            df_meter.index = pd.to_datetime(df_meter.Timestamp)
            df_meter.index.names = ['index']

            # Remove any outliers or data spikes
            try:
                df_meter = drop_numerical_outliers(df_meter, meter, z_thresh=3)
            except ValueError as e:
                print("Unable to drop outliers", e)
            # Rename the column (this was needed if we wanted to merge all the Value columns into a dataframe)
            renamed_col = (f"{meter.meter_name}_{meter.attribute}").replace(' ', '_')

            # For attributes in the Energy Marketing folder, just use attribute
            if meter.db == 'Energy_Marketing':
                renamed_col = (f"{meter.attribute}").replace(' ', '_')

            df_meter.rename(columns={"Value": f"{renamed_col}"}, inplace=True)

            # This part is not longer needed.
            if df_all.empty:
                df_all = df_meter
            else:
                df_all = pd.merge(df_all, df_meter[["Timestamp", renamed_col]], on="Timestamp", how='outer')
                # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
                df_all.index = pd.to_datetime(df_all.Timestamp)
                df_all.index.names = ['index']

            # Check to see if a new alarm needed, only check over the 60 minutes.
            df_last_hour = df_all[df_all.index > df_all.index.max() - pd.Timedelta(hours=1)]
            if meter.monitor_for_alerts:
                alarm_checker(meter, df_last_hour, renamed_col)

        except (requests.exceptions.RequestException, KeyError):
            print('HTTP Request failed')
            return None

    # PMIN / PMAX Calculations
    const_a = 0.09  # Default is 0.0855.
    const_b = 0.135378  # Default is 0.138639
    try:
        df_all["Pmin1"] = const_a * (df_all["R4_Flow"] - df_all["R5_Flow"])
        df_all["Pmin2"] = (-0.14 * (df_all["R4_Flow"] - df_all["R5_Flow"]) *
                           ((df_all["Hell_Hole_Elevation"] - 2536) / (4536 - 2536)))
        df_all["Pmin"] = df_all[["Pmin1", "Pmin2"]].max(axis=1)

        df_all["Pmax1"] = ((const_a + const_b) / const_b) * 124 + (
                    const_a * (df_all["R4_Flow"] - df_all["R5_Flow"]))
        df_all["Pmax2"] = ((const_a + const_b) / const_a) * 86 - (const_b * (df_all["R4_Flow"] - df_all["R5_Flow"]))

        df_all["Pmax"] = df_all[["Pmax1", "Pmax2"]].min(axis=1)

        df_all.drop(["Pmin1", "Pmin2", "Pmax1", "Pmax2"], axis=1, inplace=True)
    except ValueError as e:
        print("Can Not Calculate Pmin or Pmax")
        df_all[["Pmin", "Pmax"]] = np.nan
        logging.info(f"Unable to caluclate Pmin or Pmax {e}")

    DB_PATH = settings.DATABASES['default']['NAME']
    CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
    df_all.drop(["Good", "Questionable", "Substituted", "Annotated"], axis=1, inplace=True)

    # A requirement of django is to have a column named ID or a primary key. ID will serve that purpose.
    df_all.reset_index(inplace=True, drop=True)
    df_all['id'] = df_all.index
    df_all.to_sql("pi_data", CONN, if_exists='replace', index=False)

    CONN.close()
    # Email / Text any alerts that may be needed.
    send_alerts()
    return


def alarm_checker(meter, df, column_name):
    # For flows and abay elevation, we have two alarms;
    # 1) For high values and 2) For low values. We need to check both.
    if meter.attribute == "Flow" or meter.attribute == "Elevation":
        # Using Q() operators allows us to create dynamic names.
        hi_Q = Q(**{f"{meter.meter_name.lower()}_hi__lt": df[column_name].dropna().values.max()})
        lo_Q = Q(**{f"{meter.meter_name.lower()}_lo__gt": df[column_name].dropna().values.min()})
        alert_hi = AlertPrefs.objects.filter(hi_Q)
        alert_lo = AlertPrefs.objects.filter(lo_Q)

        # ############### RESET ALARMS IF NEEDED ##############
        # Check for case were an alarm was issued for a given meter, but now the value is below the alert threshold.
        # This will reset the alarm, so that if the threshold is met again, it will go off.
        alarm_active_hi = Issued_Alarms.objects.filter(alarm_still_active=True,
                                                       alarm_trigger=f"{meter.meter_name.lower()}_hi")
        alarm_active_lo = Issued_Alarms.objects.filter(alarm_still_active=True,
                                                       alarm_trigger=f"{meter.meter_name.lower()}_lo")
        # Check all active alarms
        if alarm_active_hi.exists():
            # Loop through all active alarms
            for active_alarm in alarm_active_hi:
                # If the an alarm is active, but it is now not exceeding the threshold, reset the alarm.
                # Note: hi_Q contains 1 hour of data, so the entire hour must be below the threshold to reset the alarm.
                if not AlertPrefs.objects.filter(hi_Q, user_id=active_alarm.user_id).exists():
                    Issued_Alarms.objects.filter(id=active_alarm.id).update(alarm_still_active=False)

        # Check all active alarms
        if alarm_active_lo.exists():
            # Loop through all active alarms
            for active_alarm in alarm_active_lo:
                # If the an alarm is active, but it is now not exceeding the threshold, reset the alarm.
                # Note: lo_Q contains 1 hour of data, so the entire hour must be below the threshold to reset the alarm.
                if not AlertPrefs.objects.filter(lo_Q, user_id=active_alarm.user_id).exists():
                    Issued_Alarms.objects.filter(id=active_alarm.id).update(alarm_still_active=False)
        # ################## END RESET ###############

        if alert_hi.exists():
            update_alertDB(users=alert_hi,
                           alarm_trigger=f"{meter.meter_name.lower()}_hi",
                           trigger_value=df[column_name].values.max()
                           )

        if alert_lo.exists():
            update_alertDB(users=alert_lo,
                           alarm_trigger=f"{meter.meter_name.lower()}_lo",
                           trigger_value=df[column_name].values.min()
                           )

    # If the elevation setpoint changes, this code will alert all users. It's a simplified version of the other
    # alerts since there's no hi/lo and there's no threshold to hit; just a change > 0.5' in an hour.
    if meter.attribute == "Elevation Setpoint":
        # The float level will be the latest value obtained in the dataset.
        abay_float = df["Afterbay_Elevation_Setpoint"].loc[df["Afterbay_Elevation_Setpoint"].last_valid_index()]

        # Set initial value
        abay_float_changed = False

        # Check to see if the float has changed. If so, alert user.
        if abs(abay_float - df["Afterbay_Elevation_Setpoint"]
                .loc[df["Afterbay_Elevation_Setpoint"].first_valid_index()]) > 0.5:
            abay_float_changed = True

        #  If the float changed in the last hour, first check if the alarm is already active.
        alarm_active = Issued_Alarms.objects.filter(alarm_still_active=True,
                                                    alarm_trigger="Abay_Float_Change")

        # This is more basic than resetting the flow alarms. Here, we're just saying if the Abay float hasn't
        # changed, but there are still alarms that are active, reset all the alarms. That way, if the abay float
        # changes again, all users will be notified.
        if alarm_active.exists() and not abay_float_changed:
            alarm_active.update(alarm_still_active=False)

        # If the float has changed, we want to alert all users; so the users param will include everyone that has
        # set up an alert profile (basically everyone).
        if abay_float_changed:
            update_alertDB(users=AlertPrefs.objects.all(),
                           alarm_trigger="Afterbay_Float",
                           trigger_value=int(abay_float))

    if meter.attribute == "Power":
        rec_release(df)

    return


def rec_release(df):
    debugging = False

    # The maximum time (in minutes) a user can be alerted before a ramp up/down is needed. This param is used in
    # the following for loop, which will start checking if any user wants to be alerted up to 60 minutes
    # before the ramp.
    max_buffer = 60

    # The current setpoint
    oxbow_setpoint = df["Oxbow_Gov_Setpoint"].loc[df["Oxbow_Gov_Setpoint"].last_valid_index()]
    oxbow_current = df["Oxbow_Power"].loc[df["Oxbow_Power"].last_valid_index()]

    target_setpoint = 5.4

    # Oxbow Ramp Rate in MW/min
    oxrr = 0.0422

    ramp_times = Recreation_Data.objects.all()[0].ramp_times
    rec_start = ramp_times[0]
    rec_end = ramp_times[1]

    if debugging:
        tz = pytz.timezone('US/Pacific')
        rec_start = tz.localize(datetime(year=2021,month=7,day=3,hour=15,minute=0,second=0))
        rec_end = tz.localize(datetime(year=2021,month=7,day=3,hour=16,minute=0,second=0))
        oxbow_setpoint = 6.0
        oxbow_current = 0.4

    # If rec releases are being made today, ramp_times will exist.
    if ramp_times[0]:
        mw_to_ramp = target_setpoint - oxbow_current
        minutes_to_ramp = int(mw_to_ramp/oxrr)
        ramp_must_start = rec_start - timedelta(minutes=minutes_to_ramp)

        # The following code will check:
        # 1) Is the current time + minutes-to-ramp less than the rec release start time
        #       e.g. Its 7:02 am and it takes 60 minutes to ramp, then it will be 8:02 am when it reaches full release.
        #            if the rec release starts at 8:00, this would be too late.
        # 2) Is oxbow set to the desired setpoint.
        #       e.g. If it's past the time to start ramping, but Oxbow is already set to 6.0 MW, then we're good.
        # 3) Is it after the end of the rec release time. If so, rafting is done for the day and there's no point
        #    to check any other criteria.
        if datetime.now(pytz.timezone('US/Pacific')) + timedelta(minutes=minutes_to_ramp+max_buffer) > rec_start \
                and oxbow_setpoint < target_setpoint and datetime.now(pytz.timezone('US/Pacific')) < rec_end:

            # The Alert Database has a "trigger_value" field that holds numbers (not datetime). The field
            # stores a saved value that the user wanted to "trigger" the alarm. Since we're now dealing with times
            # we will pass the number of minutes needed until the ramp. Those minutes can then be subtracted from
            # the "trigger_time" field to give us the same answer (the time at which the user wanted to be alerted).
            tdelta = rec_start-(datetime.now(pytz.timezone('US/Pacific')) + timedelta(minutes=minutes_to_ramp))
            minutes_until_ramp_starts = int(tdelta.seconds / 60)

            # You are now late: ramp time has already passed. Show how many minutes late by providing negative minutes.
            # tdelta days is < 0 if it's past the time to start ramping.
            if tdelta.days < 0:
                tdelta = timedelta(
                    days=0,
                    seconds=(tdelta.days * 60 * 60 * 24 - tdelta.seconds),
                    microseconds=tdelta.microseconds
                )

                # provide minutes as a negative value to show we're already past the time to ramp.
                minutes_until_ramp_starts = int(-1*tdelta.seconds/60)

            # If the user's setting is greater than the number of minutes until ramping starts, alert the user.
            alert_rampup = AlertPrefs.objects.filter(rampup_oxbow__gt=minutes_until_ramp_starts)
            if alert_rampup.exists():
                update_alertDB(users=AlertPrefs.objects.all(),
                               alarm_trigger="rampup_oxbow",
                               trigger_value=int(minutes_until_ramp_starts))

        else:
            # No longer in ramping time criteria. If an alarm was triggered, reset the alarm so it will trigger again.
            rampup_active_alarms = Issued_Alarms.objects.filter(alarm_still_active=True,
                                                                alarm_trigger="rampup_oxbow")
            for rampup_active_alarm in rampup_active_alarms:
                Issued_Alarms.objects.filter(id=rampup_active_alarm.id).update(alarm_still_active=False)

        # If it's after the ramp down time the setpoint is still at the target setpoint, alert user.
        if datetime.now(pytz.timezone('US/Pacific')) + timedelta(minutes=max_buffer) > rec_end \
                and oxbow_setpoint >= target_setpoint:

            tdelta = rec_end - datetime.now(pytz.timezone('US/Pacific'))
            minutes_until_ramp_ends = int(tdelta.seconds / 60)

            # You are now late: ramp time has already passed. Show how many minutes late by providing negative minutes.
            # tdelta days is < 0 if it's past the time time start ramping.
            if tdelta.days < 0:
                tdelta = timedelta(
                    days=0,
                    seconds=(tdelta.days * 60 * 60 * 24 - tdelta.seconds),
                    microseconds=tdelta.microseconds
                )

                # provide minutes as a negative value to show we're already past the time to ramp.
                minutes_until_ramp_ends = int(-1 * tdelta.seconds / 60)
            alert_rampdown = AlertPrefs.objects.filter(rampdown_oxbow__gt=minutes_until_ramp_ends)
            if alert_rampdown.exists():
                update_alertDB(users=AlertPrefs.objects.all(),
                               alarm_trigger="rampdown_oxbow",
                               trigger_value=int(minutes_until_ramp_ends))

        # Else reset the alarm if one was triggered earlier.
        else:
            # No longer in ramping time criteria. If an alarm was triggered, reset the alarm so it will trigger again.
            rampdown_active_alarms = Issued_Alarms.objects.filter(alarm_still_active=True,
                                                                alarm_trigger="rampdown_oxbow")
            for rampdown_active_alarm in rampdown_active_alarms:
                Issued_Alarms.objects.filter(id=rampdown_active_alarm.id).update(alarm_still_active=False)
    return


def update_alertDB(users, alarm_trigger, trigger_value):
    """
    Purpose: Add alerts to the Issued Alarms table in the database. The goal here is to only add alerts to the
            database IF certain fields are unique together: user_id, name of alarm (e.g. r4_hi),
            alarm_setpoint (e.g. the value put into r4_hi to trigger alarm), alarm_still_active.
            This means that an alert for the same event could only be reissued IF the user changed the
            alarm setpoint in their AlertPrefs during the event (which a user might want to do).
    :param users (QuerySet): A queryset containing data from AlertPrefs where are values exceeded by the alarm_trigger.
                             If a user's threshold for an alert is met, the queryset will contain all the threshold
                             data from AlertPrefs for the user.
    :param alarm_trigger: The column name in AlertPrefs that triggered the alert (e.g. r4_hi, afterbay_lo)
    :param trigger_value: The value that exceeded the threshold
    :return:
    """
    for user in users:
        # Users don't have a setpoint for the Afterbay_Float alarm, since it is triggered on a change.
        if alarm_trigger == "Afterbay_Float":
            alarm_setpoint = trigger_value
        # Other values (like r4_hi) do have a threshold value for triggering an alarm.
        else:
            # For a given user, this will return the setpoint in AlertPrefs that the user set to trigger the alarm.
            alarm_setpoint = getattr(user, alarm_trigger)
        obj, created = Issued_Alarms.objects.get_or_create(
            user_id=user.user_id,
            alarm_trigger=alarm_trigger,
            alarm_setpoint=alarm_setpoint,  # unique in case someone changes
            alarm_still_active=True,
            defaults={
                'trigger_time': datetime.utcnow(),
                'trigger_value': trigger_value,
                'alarm_sent': False,
                'seen_on_website': False,
            },
        )
    return


def send_alerts():
    carrier_dict = {"AT&T": "@mms.att.net",
                    "Verizon": "@vzwpix.com",
                    "T-Mobile": "@tmomail.net",
                    "Sprint": "@pm.sprint.com"}
    need_to_send = Issued_Alarms.objects.filter(alarm_sent=False)
    if need_to_send.exists():
        for alert in need_to_send:
            user_profile = Profile.objects.get(user__id=alert.user_id)
            user_phone = user_profile.phone_number
            user_email = user_profile.user.email
            user_carrier = user_profile.phone_carrier
            mms = f"{user_phone}{carrier_dict[user_carrier]}"
            pretty_name = (alert.alarm_trigger.split('_')[0]).upper()
            email_body = f"{alert.alarm_trigger} triggered this alert \n" \
                         f"Current Value: {alert.trigger_value} \n" \
                         f"Your threshold: {alert.alarm_setpoint}"
            email_subject = f"PCWA Alarm For {pretty_name}"
            if "_oxbow" in alert.alarm_trigger:
                tz = pytz.timezone('US/Pacific')
                ramp_time = (alert.trigger_time + timedelta(minutes=alert.trigger_value)).astimezone(tz)
                email_body = f"Ramp Oxbow Now! \n" \
                             f"\nTime to Start Ramp: \n{ramp_time.strftime('%H:%M %p')} \n" \
                             f"\nYour settings: \nAlert {int(alert.alarm_setpoint)} min in advance." \
                             f"\n\nAlarm Created At: \n{datetime.now(pytz.timezone('US/Pacific')).strftime('%a %H:%M %p')} \n"
            send_mail(mms, user_email, email_body, email_subject)
            Issued_Alarms.objects.filter(id=alert.id).update(alarm_sent=True)
    return


def abay_forecast(df_cnrfc):
    # PMIN / PMAX Calculations
    const_a = 0.09  # Default is 0.0855.
    const_b = 0.135422  # Default is 0.138639

    # Most recent cnrfc forecast.
    # most_recent_cnrfc = ForecastData.objects.latest("FORECAST_ISSUED").FORECAST_ISSUED
    # df = pd.DataFrame.from_records(ForecastData.objects.all().values())
    most_recent_cnrfc = df_cnrfc['FORECAST_ISSUED'].max()
    df = df_cnrfc[(df_cnrfc.FORECAST_ISSUED == most_recent_cnrfc)].copy()

    # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
    df.GMT = pd.to_datetime(df.GMT)

    df_pi = pd.DataFrame.from_records(PiData.objects.all().values())


    ########## GET OXBOW GENERRATION FORECAST DATA ###################
    try:
        # Download the data for the Oxbow and MFPH Forecast (data start_time is -24 hours from now, end time is +72 hrs)
        pi_data_ox = PiRequest("OPS", "Oxbow", "Forecasted Generation", False, True)
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

        # There is an issue where PI is reporting a dictionary of data for missing data points.
        df_fcst["Value"] = df_fcst["Value"].map(lambda x: np.nan if isinstance(x, dict) else x)

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

    # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
    df.GMT = pd.to_datetime(df.GMT)

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


@t1.job(interval=timedelta(minutes=30))
def get_cnrfc_data():
    new_df = pd.DataFrame(columns = [f.name for f in ForecastData._meta.get_fields()])
    ######################   CNRFC SECTION ######################################
    # Get the CNRFC Data. Note, we are putting this outside the PI request since
    # it's entirely possible these data are not avail. If it fails, it will just
    # skip over this portion and return a df without the CNRFC data

    # Note, if a 00Z forecast is needed, the range() needs to be changed to range(-2,2,1) to get tomorrow's date.
    fcst_days = [(datetime.now() + timedelta(days=x)).strftime("%Y%m%d") for x in range(-2, 1, 1)]
    # This will hold the dates used in all the file names.
    file_dates = []
    for day in fcst_days:
        # Only forecasts produced are the 12Z and 18Z. Change to: range(0, 24, 6) to include 00Z and 06Z
        for hr in range(12, 19, 6):
            file_dates.append(f"{day}{hr:02}")

    df_cnrfc_list = []
    most_recent_file = None

    # Age, in hours, of the last forecast stored in the database.
    try:
        cnrfc_foreast_age = (datetime.now(pytz.utc) -
                             ForecastData.objects.latest("FORECAST_ISSUED").FORECAST_ISSUED).total_seconds()/3600

        # If the forecast is not more than 8 hours old, then exit.
        if cnrfc_foreast_age < 8:
            return None

    except Exception as err:
        logging.debug(f"Error: {err} \n Forecast Data table does not exist yet. Creating one...")
        print(f"Error: {err} \n Forecast Data table does not exist yet. Creating one...")

    for file in file_dates:
        try:
            df_file = pd.DataFrame().empty
            df_file = pd.read_csv(f"https://www.cnrfc.noaa.gov/csv/{file}_american_csv_export.zip")

            # Drop first row (the header is two rows and the 2nd row gets put into row 1 of the df; delete it)
            df_file = df_file.iloc[1:]

            # Put the forecast issued time in the dataframe so we can refer to it later.
            df_file["FORECAST_ISSUED"] = pd.to_datetime(datetime.strptime(file, "%Y%m%d%H"))

            # Add this dataframe to the list of dataframes.
            df_cnrfc_list.append(df_file)
            most_recent_file = file  # The date last file successfully pulled.
        except (HTTPError, URLError) as error:
            logging.warning(f'CNRFC HTTP Request failed {error} for {file}. Error code: {error}')
            continue

    # The last element in the list will be the most current forecast. Get that one.
    # df_cnrfc = df_cnrfc_list[-1].copy()

    # Case for failed download and empty dataframe
    if not df_cnrfc_list:
        df_cnrfc = pd.date_range(start=datetime.utcnow() - timedelta(hours=48),
                                 end=datetime.utcnow() + timedelta(hours=72), freq='H', normalize=True)
        df_cnrfc[["FORECAST_ISSUED", "R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst", "GMT"]] = np.nan

    # Download was successful, continue
    else:

        # Merge all the dataframes together into a single dataframe.
        df_cnrfc = pd.concat(df_cnrfc_list)
        # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT).dt.tz_localize('UTC').dt.tz_convert('US/Pacific')

        df_cnrfc.rename(columns={"MFAC1L": "R20_fcst", "RUFC1": "R30_fcst", "MFPC1": "R4_fcst", "MFAC1": "R11_fcst"},
                        inplace=True)
        df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst"]] = df_cnrfc[["R20_fcst", "R30_fcst", "R4_fcst",
                                                                              "R11_fcst"]].apply(pd.to_numeric) * 1000

        # Only keep the forecasts we care about
        df_cnrfc.drop(df_cnrfc.columns
                      .difference(["R20_fcst", "R30_fcst", "R4_fcst", "R11_fcst", "FORECAST_ISSUED", "GMT"]),
                      1, inplace=True)

    # all_columns = [f.name for f in ForecastData._meta.get_fields()]

    try:
        df_cnrfc = abay_forecast(df_cnrfc)
        # Convert the Timestamp to a pandas datetime object and convert to Pacific time.
        df_cnrfc.GMT = pd.to_datetime(df_cnrfc.GMT)
    except:
        logging.debug(f"The Oxbow Forecast Could Not be Created because {sys.exc_info()[0]}")
        print(f"The Oxbow Forecast Could Not be Created because {sys.exc_info()[0]}")

    DB_PATH = settings.DATABASES['default']['NAME']
    CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
    df_cnrfc.to_sql("forecast_data", CONN, if_exists='replace')
    CONN.close()
    return


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

    # If nan's exist, this will convert all values to nan and return an empty array.
    constrains = df.select_dtypes(include=[np.number]).dropna()\
        .apply(lambda x: np.abs(stats.zscore(x)) < z_thresh, result_type='reduce').all()

    # Drop (inplace) values set to be rejected. The dropna() is to account for any nan's that were removed
    # above in the constrains array.
    df.drop(df.dropna().index[~constrains], inplace=True)

    if df.shape[0] != orig_size:
        print(f"A total of {orig_size - df.shape[0]} data spikes detected in {meter.meter_name}. "
              f" The data have been removed")
    return df


def create_table():
    DB_PATH = settings.DATABASES['default']['NAME']
    CONN = sqlite3.connect(DB_PATH, check_same_thread=False)


if __name__ == "__main__":
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'AbayTracker.settings')
    log_stream = StringIO()
    logging.basicConfig(level=logging.INFO, handlers=[
        logging.FileHandler("pi_checker_err.log"),
        logging.StreamHandler()
    ])

    # Capture all errors and alert admin if fails.
    try:
        print(f"Running pi_checker version: {__version__}")
        main()
        get_cnrfc_data()
        t1.start(block=True)

    # An error occurred, alert admin and terminate program.
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        print(log_stream.getvalue())
        send_mail(f"7203750163@mms.att.net", "smotley@mac.com", log_stream.getvalue(), "Pi Checker Crashed")
        sys.exit()
