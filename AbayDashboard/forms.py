from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User
from AbayDashboard.models import Profile, AlertPrefs
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator

class NewUserForm(UserCreationForm):
    # required = True means you can't fill out form without this.
    username = forms.CharField(required=False, max_length=30,
                             widget=forms.TextInput(attrs={'placeholder': 'Username'}))
    email = forms.EmailField(required=False,
                             widget=forms.TextInput(attrs={'placeholder':'Valid Email'}))
    first_name = forms.CharField(label="First Name", required=False,
                                 widget=forms.TextInput(attrs={'placeholder':'First Name'}))
    last_name = forms.CharField(label="Last Name", required=False,
                                widget=forms.TextInput(attrs={'placeholder': 'Last Name'}))

    def __str__(self):
        return f"{self.username}"

    def get_object(self, queryset=None):
        return self.request.user

    class Meta:
        model = User    # The basic django model we are using as our template here.
        fields = ("username", "first_name", "last_name", "email", "password1", "password2") # So, same as before, but now we're including email

    # When it gets saved, commit the data to the database
    def save(self, commit=True):
        user = super(NewUserForm, self).save(commit=False)  # Don't commit it yet until we modify the data.
        user.email = self.cleaned_data['email']
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        if commit:
            user.save()
        return user


class UserProfileForm(forms.ModelForm):
    error_messages =  {'Incorrect Format': ('Value outside of bounds.')}

    alert_ok_time_start = forms.TimeField(label="Start Time", required=False,
                                 widget=forms.TextInput(attrs={'placeholder': 'Alert Start Time',
                                                               'class':'form-control datetimepicker-input',
                                                               'data-target':'#alert_ok_time_start'}))
    alert_ok_time_end = forms.TimeField(label="End Time", required=False,
                                widget=forms.TextInput(attrs={'placeholder': 'Alert End Time',
                                                       'class':'form-control datetimepicker-input',
                                                              'data-target':'#alert_ok_time_end'}))
    phone_number = forms.CharField(required=False,
                                   validators=[
                                        RegexValidator(
                                            regex='^\d{9,15}',
                                            message= "Enter Area Code and Phone Number: (222) 555-5555"
                                            )
                                        ],
                                   widget=forms.TextInput(attrs={'placeholder': 'Phone Number',
                                                                 'data-target': '#phone_number'})
                                   )
    phone_carrier = forms.CharField(label="Carrier", required=False)

    def clean(self):
        cleaned_data = super().clean()
        carrier = cleaned_data.get("phone_carrier")
        phone = cleaned_data.get("phone_number")

        # When the form passes a blank string, treat it as a None type so that it is <null> in database
        if len(carrier) < 3:
            self.cleaned_data['phone_carrier'] = None
            carrier = None
        if len(phone) < 10:
            self.cleaned_data['phone_number'] = None
            phone = None
        start_end_times = [cleaned_data.get("alert_ok_time_start"), cleaned_data.get("alert_ok_time_end")]

        mms_info = [phone, carrier]

        # Check to ensure BOTH start time and end time are submitted, not just one or the other.
        if any(v is not None for v in start_end_times): # There is at least one value
            if any(se is None for se in start_end_times):   # There is at least one blank
                raise ValidationError(
                    "Both Start and End time must be submitted together."
                )

        # Check that BOTH a phone number and carrier were entered.
        if any(p is not None for p in mms_info): # There is at least one value
            if any(mm is None for mm in mms_info):   # There is at least one blank
                raise ValidationError(
                    "Please enter both phone AND provider information."
                )
        return self.cleaned_data


    class Meta:
        model = Profile
        fields = ("alarm_on", "alert_ok_time_start", "alert_ok_time_end", "phone_number", "phone_carrier") # So, same as before, but now we're including email
        field_order = ["alarm_on", "alert_ok_time_start", "alert_ok_time_end", "phone_number", "phone_carrier"]


class AlertForm(forms.ModelForm):
    error_messages = {'Incorrect Format': ('The phone number was entered incorrectly.')}
    class Meta:
        model = AlertPrefs
        fields = ("afterbay_hi", "afterbay_lo", "oxbow_deviation",
                  "r4_lo", "r4_hi", "r11_lo", "r11_hi", "r30_lo", "r30_hi", "rampup_oxbow", "rampdown_oxbow")

    # Order the field so that when it shows up in the template, it displays with the lower value on the left.
    field_order = ["afterbay_lo", "afterbay_hi", "oxbow_deviation",
                  "r4_lo", "r4_hi", "r11_lo", "r11_hi", "r30_lo", "r30_hi", "rampup_oxbow", "rampdown_oxbow"]

    def __init__(self, *args, **kwargs):
        super(AlertForm, self).__init__(*args, **kwargs)
        # Make all of the fields sow that they are not required
        for field in self.fields:
            self.fields[field].required = False