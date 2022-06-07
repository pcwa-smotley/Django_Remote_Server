from django.db import models
from django.contrib.auth.models import User
from django.dispatch import receiver
from django.db.models.signals import post_save
from datetime import datetime, timedelta
import calendar
import pytz
# Create your models here.

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    alert_ok_time_start = models.DateTimeField(null=True, blank=True)
    alert_ok_time_end = models.DateTimeField(null=True, blank=True)
    phone_number = models.CharField(blank=True, null=True, max_length=15)
    phone_carrier = models.CharField(blank=True, null=True, max_length=30)
    alarm_on = models.BooleanField(null=True)

    def __str__(self):
        return f'{self.user.first_name}'


class AlertPrefs(models.Model):
    user = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)
    afterbay_hi = models.FloatField(null=True, default=None)
    afterbay_lo = models.FloatField(null=True, default=None)
    oxbow_deviation = models.FloatField(null=True, default=None)
    rampup_oxbow = models.IntegerField(null=True, default=None)
    rampdown_oxbow = models.IntegerField(null=True, default=None)
    r4_hi = models.IntegerField(null=True, default=None)
    r4_lo = models.IntegerField(null=True, default=None)
    r30_hi = models.IntegerField(null=True, default=None)
    r30_lo = models.IntegerField(null=True, default=None)
    r11_hi = models.IntegerField(null=True, default=None)
    r11_lo = models.IntegerField(null=True, default=None)
    error_messages = {'Incorrect Format': ('Value outside of bounds')}

    def __str__(self):
        return f'{self.user.first_name}'


class Issued_Alarms(models.Model):
    user = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)
    alarm_trigger = models.TextField(null=True)
    alarm_setpoint = models.FloatField(null=True)
    trigger_value = models.FloatField(null=True)
    trigger_time = models.DateTimeField(null=True)
    alarm_sent = models.BooleanField(null=True)
    alarm_still_active = models.BooleanField(null=True)
    seen_on_website = models.BooleanField(null=True)


class Recreation_Data(models.Model):
    water_year_type = models.TextField(null=True,default="above_normal")
    today_recStart = models.DateTimeField(null=True, default=None)
    today_recEnd = models.DateTimeField(null=True, default=None)
    tomorrow_recStart = models.DateTimeField(null=True, default=None)
    tomorrow_recEnd = models.DateTimeField(null=True, default=None)
    tz = pytz.timezone('US/Pacific')

    # Rafting schedules change after Labor day. This will find the
    def getLaborDay(self, year):
        month = 9
        mycal = calendar.Calendar(0)
        cal = mycal.monthdatescalendar(year, month)
        if cal[0][0].month == month:
            ld = datetime.combine(cal[0][0], datetime.min.time())       # Date of Labor Day
            return ld.replace(tzinfo=pytz.timezone('US/Pacific'))       # Timezone aware Labor Day
        else:
            ld = datetime.combine(cal[1][0], datetime.min.time())       # Date of Labor Day
            return ld.replace(tzinfo=pytz.timezone('US/Pacific'))       # Timezone aware Labor Day

    def get_rec_release_times(self, date_query):
        year = date_query.year  # Used for Labor Day calc

        # Assume no rafting for given date until proven otherwise.
        recStart = None
        recEnd = None

        release_data = {
            "wet": {
                "weekdays_before_laborday": [0, 1, 2, 3, 4],  # Mon=0,Tue=1,Wed=2,Thu=3,Fri=4
                "weekdays_after_laborday": [1, 2, 3, 4],  # Tue=1, Wed=2, Thu=3, Fri=4
                "start_hour": 9,
                "end_hour": 12,
                "start_hour_weekend": 8,
                "end_hour_weekend": 12,
                "start_end_minute": 00,
                "start_end_minute_weekend": 00,
            },
            "above_normal": {
                "weekdays_before_laborday": [0, 1, 2, 3, 4, 5, 6],  # Mon=0,Tue=1,Wed=2,Thu=3,Fri=4,Sat=5,Sun=6
                "weekdays_after_laborday": [1, 2, 4, 5, 6],  # Tue=1,Wed=2,Fri=4,Sat=5,Sun=6
                "start_hour": 9,
                "end_hour": 12,
                "start_hour_weekend": 8,
                "end_hour_weekend": 12,
                "start_end_minute": 00,
                "start_end_minute_weekend": 00,
            },
            "below_normal": {
                "weekdays_before_laborday": [1, 2, 3, 4, 5, 6],  # Tue=1,Wed=2,Thu=3,Fri=4,Sat=5,Sun=6
                "weekdays_after_laborday": [1, 2, 4, 5, 6],  # Tue=1,Wed=2,Fri=4
                "start_hour": 9,
                "end_hour": 12,
                "start_hour_weekend": 8,
                "end_hour_weekend": 12,
                "start_end_minute": 00,
                "start_end_minute_weekend": 00,
            },
            "dry": {
                "weekdays_before_laborday": [1, 2, 4, 5, 6],  # Tue=1,Wed=2,Fri=4,Sat=5,Sun=6
                "weekdays_after_laborday": [2, 4, 5, 6],  # Wed=2,Fri=4,Sat=5,Sun=6
                "start_hour": 8,
                "end_hour": 11,
                "start_hour_weekend": 8,
                "end_hour_weekend": 11,
                "start_end_minute": 00,
                "start_end_minute_weekend": 30,
            },
            "critical": {
                "weekdays_before_laborday": [2, 4, 5, 6],  # Wed=2,Fri=4,Sat=5,Sun=6
                "weekdays_after_laborday": [5],  # Sat=5
                "start_hour": 8,
                "end_hour": 11,
                "start_hour_weekend": 8,
                "end_hour_weekend": 11,
                "start_end_minute": 00,
                "start_end_minute_weekend": 30,
            },
            "extreme_critical": {
                "weekdays_before_laborday": [2],  # Wed=2
                "weekdays_after_laborday": [],  # None
                "start_hour": 8,
                "end_hour": 11,
                "start_hour_weekend": 8,
                "end_hour_weekend": 11,
                "start_end_minute": 00,
                "start_end_minute_weekend": 30,
            }
        }

        # If today's date is between June 1st and Labor Day AND rec releases are made today
        if datetime(year, 6, 1, tzinfo=pytz.timezone('US/Pacific')) <= date_query <= self.getLaborDay(year) and \
                date_query.weekday() in release_data[self.water_year_type]["weekdays_before_laborday"]:
            recStartHr = release_data[self.water_year_type]["start_hour"]
            recEndHr = release_data[self.water_year_type]["end_hour"]
            recMinute = release_data[self.water_year_type]["start_end_minute"]
            # Check if today is a weekend day
            if date_query.weekday() >= 5:
                recStartHr = release_data[self.water_year_type]["start_hour_weekend"]
                recEndHr = release_data[self.water_year_type]["end_hour_weekend"]
                recMinute = release_data[self.water_year_type]["start_end_minute_weekend"]

            recStart = date_query.replace(hour=recStartHr, minute=recMinute, second=0, microsecond=0)
            #self.today_recStart = pytz.timezone('US/Pacific').localize(recStart)

            recEnd = date_query.replace(hour=recEndHr, minute=recMinute, second=0, microsecond=0)
            #self.today_recEnd = pytz.timezone('US/Pacific').localize(recEnd)

        # If today's date is between Labor Day and Sept 30th AND it's a rec release day
        if self.getLaborDay(year) <= date_query <= datetime(year, 9, 30, tzinfo=pytz.timezone('US/Pacific')) and \
                date_query.weekday() in release_data[self.water_year_type]["weekdays_after_laborday"]:
            recStartHr = release_data[self.water_year_type]["start_hour"]
            recEndHr = release_data[self.water_year_type]["end_hour"]
            recMinute = release_data[self.water_year_type]["start_end_minute"]
            # Check if today is a weekend day
            if date_query.weekday() >= 5:
                recStartHr = release_data[self.water_year_type]["start_hour_weekend"]
                recEndHr = release_data[self.water_year_type]["end_hour_weekend"]
                recMinute = release_data[self.water_year_type]["start_end_minute_weekend"]

            recStart = date_query.replace(hour=recStartHr, minute=recMinute, second=0, microsecond=0)
            #self.today_recStart = pytz.timezone('US/Pacific').localize(recStart)

            recEnd = date_query.replace(hour=recEndHr, minute=recMinute, second=0, microsecond=0)
            #self.today_recEnd = pytz.timezone('US/Pacific').localize(recEnddt)

        return recStart, recEnd

    @property
    def ramp_times(self):
        # Start by assuming there is no rafting today
        self.today_recStart = None
        self.today_recEnd = None
        self.tomorrow_recStart = None
        self.tomorrow_recEnd = None
        today = datetime.now(pytz.timezone('US/Pacific'))
        tomorrow = today + timedelta(days=1)


        self.today_recStart, self.today_recEnd = self.get_rec_release_times(today)
        self.tomorrow_recStart, self.tomorrow_recEnd = self.get_rec_release_times(tomorrow)

        # Save the results to the database
        self.save()
        return self.today_recStart, self.today_recEnd, self.tomorrow_recStart, self.tomorrow_recEnd


class PiData(models.Model):
    # This class is used to hold the PI Data being read from pi_checker.py
    # The idea here is that pi_checker will replace this table every time it pulls data, and our app will then
    # read from this table rather than ping pi_web every time it runs. This should be much faster.
    # If an item is not listed below in the table, that means:
    # 1) It can still be in the SQL table, but only if it's written by .to_sql(_) in pandas
    # 2) If it's not listed, it will not be found in a call to all objects, e.g. PiData.objects.all() will not contain
    #    a column for the item if it's not listed below.
    class Meta:
        db_table = 'pi_data'
        managed = False
    id = models.AutoField(primary_key=True)
    Timestamp = models.DateTimeField(null=True, default=None)
    R4_Flow = models.FloatField(null=True, default=None)
    R5_Flow = models.FloatField(null=True, default=None)
    R11_Flow = models.FloatField(null=True, default=None)
    R30_Flow = models.FloatField(null=True, default=None)
    UnitsAbbreviation = models.TextField(null=True, default=None)
    Afterbay_Elevation = models.FloatField(null=True, default=None)
    Afterbay_Elevation_Setpoint = models.FloatField(null=True, default=None)
    Oxbow_Gov_Setpoint = models.FloatField(null=True, default=None)
    Oxbow_Power = models.FloatField(null=True, default=None)
    Hell_Hole_Elevation = models.FloatField(null=True, default=None)
    GEN_MDFK_and_RA = models.FloatField(null=True, default=None)
    ADS_MDFK_and_RA = models.FloatField(null=True, default=None)
    ADS_Oxbow = models.FloatField(null=True, default=None)
    Pmin = models.FloatField(null=True, default=None)
    Pmax = models.FloatField(null=True, default=None)


class ForecastData(models.Model):
    class Meta:
        db_table = 'forecast_data'
        managed = False

    index = models.AutoField(primary_key=True)
    FORECAST_ISSUED = models.DateTimeField(null=True, default=None)
    GMT = models.DateTimeField(null=True, default=None)
    R4_fcst = models.FloatField(null=True, default=None)
    R30_fcst = models.FloatField(null=True, default=None)
    R11_fcst = models.FloatField(null=True, default=None)
    R20_fcst = models.FloatField(null=True, default=None)
    # MFRA_fcst = models.FloatField(null=True, default=None)
    # Pmin = models.FloatField(null=True, default=None)
    # Pmax = models.FloatField(null=True, default=None)
    # Abay_AF_Observed = models.FloatField(null=True, default=None)
    # Abay_AF_Change_Observed = models.FloatField(null=True, default=None)
    # RA_MW = models.FloatField(null=True, default=None)
    # MF_MW = models.FloatField(null=True, default=None)
    # Oxbow_fcst = models.FloatField(null=True, default=None)
    # Oxbow_Outflow = models.FloatField(null=True, default=None)
    # R5_Value = models.FloatField(null=True, default=None)
    # RA_Inflow = models.FloatField(null=True, default=None)
    # MF_Inflow = models.FloatField(null=True, default=None)
    # Ibay_Spill = models.FloatField(null=True, default=None)
    # R20_fcst_adjusted = models.FloatField(null=True, default=None)
    # Abay_Inflow = models.FloatField(null=True, default=None)
    # Abay_Outflow = models.FloatField(null=True, default=None)
    # Abay_AF_Change = models.FloatField(null=True, default=None)
    # Abay_AF_Change_Error = models.FloatField(null=True, default=None)
    # Abay_CFS_Error = models.FloatField(null=True, default=None)
    # Abay_AF_Fcst = models.FloatField(null=True, default=None)
    # Abay_Elev_Fcst = models.FloatField(null=True, default=None)


# Whenever there is a post_save in the User model, run the following code
@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        Profile.objects.create(user=instance)
        AlertPrefs.objects.create(user=instance)


@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()