from django.shortcuts import render, redirect, HttpResponse
from .models import Profile, AlertPrefs, Issued_Alarms, Recreation_Data
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core import serializers
from django.template.defaulttags import register
from django.utils import timezone
import pytz
from django.template.loader import render_to_string
from django.template import RequestContext
from django.forms.models import model_to_dict
from .forms import UserProfileForm, AlertForm
from plotly.offline import plot
import plotly.graph_objects as go
import json
import psutil
import subprocess, os

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


def ajax_msg_builder(request):
    # When you do an ajax request, the template will not reload. Therefore, the {%include messages.html%} will
    # also not reload. Because of this, you need to pass any messages back to the ajax call through json data.
    django_messages = []
    for message in messages.get_messages(request):
        django_messages.append({
            "level": message.level,
            "message": message.message,
            "tags": message.tags,
        })
    data = {}
    data['msg'] = django_messages
    return json.dumps(data)


# Create your views here.
@login_required(login_url='/login')
def dashboard(request):
    if request.method == "POST":
        user_preferences = AlertForm(request.POST, instance=request.user.profile)
        user_profile = UserProfileForm(request.POST, instance=request.user.profile)
        if user_preferences.is_valid():
            # This just creates a profile the first time.
            obj, created = AlertPrefs.objects.get_or_create(user=request.user)
            AlertPrefs.objects.filter(user=request.user).update(
                                                            afterbay_lo=user_preferences.data['abay_lower'],
                                                            afterbay_hi=user_preferences.data['abay_upper'],
                                                            r4_hi=user_preferences.data['r4_hi'],
                                                            r4_lo=user_preferences.data['r4_lo'],
                                                            r30_hi=user_preferences.data['r30_hi'],
                                                            r30_lo=user_preferences.data['r30_lo'],
                                                            r11_hi=user_preferences.data['r11_hi'],
                                                            r11_lo=user_preferences.data['r11_lo'],
                                                            oxbow_deviation=user_preferences.data['oxbow_deviation'])
            Profile.objects.filter(user=request.user).update(
                alert_ok_time_start=user_profile.cleaned_data.get('alert_sTime'),
                alert_ok_time_end=user_profile.cleaned_data.get('alert_eTime'),
                alarm_on=user_profile.cleaned_data.get('alarm')
            )
            return redirect("AbayDashboard:dash_django")
        else:
            for msg in user_preferences.error_messages:
                messages.error(request, f"{msg}:{user_preferences.error_messages}")

    user_preferences = AlertForm(request.POST, instance=request.user.profile)
    user_profile = UserProfileForm(request.POST, instance=request.user.profile)
    return render(request=request,
                  template_name='AbayDashboard/dashboard.html',
                  context={'user_preferences':user_preferences,
                           'profile_form':user_profile})


@login_required(login_url='/login')
def dash_django(request):
    if request.method == "POST":
        alarm_preferences = AlertForm(request.POST, instance=request.user.profile)
        user_profile = UserProfileForm(request.POST, instance=request.user.profile)

        # We have three ajax form submittals on this page.
        #   1) For the user preferences (e.g. turing alarms on/off, phone numbers, etc)
        #   2) For the alert triggers (e.g. abay levels, etc.)
        #   3) Admin able to restart the pi_checker program

        if "restart_pi_checker" in request.POST:
            print("Not working yet")
            #restart_pi_checker()

        # If the POST contains data with "alert_ok_time_start", then we know it's from user_profile update request.
        if "alert_ok_time_start" in request.POST:
            if user_profile.is_valid():
                # This is only for creating the profile if one has not been created yet.
                obj, created = Profile.objects.get_or_create(user=request.user)
                Profile.objects.filter(user=request.user).update(
                    alarm_on=user_profile.cleaned_data['alarm_on'],
                    alert_ok_time_start=user_profile.cleaned_data['alert_ok_time_start'],
                    alert_ok_time_end=user_profile.cleaned_data['alert_ok_time_end'],
                    phone_number=user_profile.cleaned_data['phone_number'],
                    phone_carrier=user_profile.cleaned_data['phone_carrier'],
                )
                messages.success(request,f"Preferences Saved")  # NOTE: f-string: we are now passing the variable {username}
                if request.is_ajax():
                    message_data = ajax_msg_builder(request)
                    return HttpResponse(message_data, content_type="application/json")
            else:
                for msg in user_profile.errors:
                    messages.error(request, f"{user_profile.errors[msg].data[0].message}")
                    if request.is_ajax():
                        message_data = ajax_msg_builder(request)
                        return HttpResponse(message_data, content_type="application/json")

        # This is a POST request coming from the ajax for the alarm trigger form
        if 'afterbay_lo' in request.POST:
            if alarm_preferences.is_valid():
                # This just creates a profile the first time.
                obj, created = AlertPrefs.objects.get_or_create(user=request.user)
                AlertPrefs.objects.filter(user=request.user).update(
                        afterbay_lo=alarm_preferences.cleaned_data['afterbay_lo'],
                        afterbay_hi=alarm_preferences.cleaned_data['afterbay_hi'],
                        r4_hi=alarm_preferences.cleaned_data['r4_hi'],
                        r4_lo=alarm_preferences.cleaned_data['r4_lo'],
                        r30_hi=alarm_preferences.cleaned_data['r30_hi'],
                        r30_lo=alarm_preferences.cleaned_data['r30_lo'],
                        r11_hi=alarm_preferences.cleaned_data['r11_hi'],
                        r11_lo=alarm_preferences.cleaned_data['r11_lo'],
                        rampup_oxbow=alarm_preferences.cleaned_data['rampup_oxbow'],
                        rampdown_oxbow=alarm_preferences.cleaned_data['rampdown_oxbow'],
                        oxbow_deviation=alarm_preferences.cleaned_data['oxbow_deviation'])
                # Profile.objects.filter(user=request.user).update(
                #     alert_ok_time_start=user_profile.cleaned_data.get('alert_sTime'),
                #     alert_ok_time_end=user_profile.cleaned_data.get('alert_eTime'),
                #     alarm_on=user_profile.cleaned_data.get('alarm')
                # )
                messages.success(request,f"Value Updated")  # NOTE: f-string: we are now passing the variable {username} to the homepage

                # If the request is ajax, you don't want to reload the page. But since we're not reloading the page
                # we are also not going to reload {% messages.html %}. Therefore, you have to pass the message info
                # back through the ajax data
                if request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest':
                    message_data = ajax_msg_builder(request)
                    return HttpResponse(message_data, content_type="application/json")
                return redirect("AbayDashboard:dash_django")
            else:
                print(alarm_preferences.errors)
                for msg in alarm_preferences.errors:
                    messages.error(request, f"Failed: {msg}:{alarm_preferences.errors[msg].data[0].message}")
                    if request.is_ajax():
                        message_data = ajax_msg_builder(request)
                        return HttpResponse(message_data, content_type="application/json")

        # This is being send from the "Recently Triggered Alarms" modal. This works like email, with unread
        # messages and the ability to delete messages.
        if 'pk_delete' in request.POST:
            if request.is_ajax():
                message_data = ajax_msg_builder(request)
                pk_val = request.POST['pk_delete']
                Issued_Alarms.objects.filter(user=request.user, pk=pk_val).delete()
                return HttpResponse(message_data, content_type="application/json")
    # New alerts not seen on website yet
    new_alerts = list(Issued_Alarms.objects.values_list('id', flat=True).filter(user=request.user, seen_on_website=False))

    # The form data
    alarm_preferences = AlertForm(request.POST, instance=request.user.profile)
    user_profile = UserProfileForm(request.POST, instance=request.user.profile)

    # The values in the database for a given user
    user_alert_data = AlertPrefs.objects.filter(user=request.user)
    user_profile_data = Profile.objects.filter(user=request.user)
    user_issued_alarms = Issued_Alarms.objects.filter(user=request.user)
    run_rec_times = Recreation_Data.objects.all()[0].ramp_times
    rec_release_data = Recreation_Data.objects.all()

    user_alert_data_json = serializers.serialize("python", user_alert_data)
    user_profile_json = serializers.serialize("python", user_profile_data)
    user_issued_alarms_json = serializers.serialize("python", user_issued_alarms)
    rec_release_data_json = serializers.serialize("python", rec_release_data)

    for i, alarm in enumerate(user_issued_alarms_json):
        t = alarm['fields']['trigger_time']
        local_tz = pytz.timezone("US/Pacific")
        local_time = t.replace(tzinfo=pytz.UTC).astimezone(local_tz)
        user_issued_alarms_json[i]['fields']['trigger_time'] = local_time


    # Reset all values for seen on website to True. The next time the user logs on, these will reset. Note, we need
    # this to occur only on a GET, since any other POST request would reset this data.
    if request.method == "GET":
        Issued_Alarms.objects.filter(user=request.user).update(seen_on_website=True)
    # The entire job could be passed with only the user_alert_data; however, we also want to pass
    # the alarm_preferences form so that the <div> fields are in the order we want. That way, when we loop
    # in the template, it will display the LOW value first, which puts it on the left hand side and then
    # it will put the HIGH value on the right (see the "FIELD ORDER" parameter in the forms.py for the AlertForm)
    return render(request=request,
                  #template_name='AbayDashboard/includes/alarm_modal.html',
                  template_name='AbayDashboard/dash_django.html',
                  context={'alarm_preferences':alarm_preferences,
                           'profile_form':user_profile,
                           "user_alert_data":user_alert_data_json[0],
                           "user_profile_data":user_profile_json[0],
                           "user_issued_alarms":user_issued_alarms_json,
                           "rec_release_data": rec_release_data_json[0],
                           "new_alerts": len(new_alerts)})


def restart_pi_checker():
    pi_process = False
    for p in psutil.process_iter():
        if "python" in p.name():  # Any process that is python
            for arg in p.cmdline():  # Check all python processes
                if "pi_checker" in arg:  # If "pi_checker" is in any of those, program is running.
                    pi_process = True
    if not pi_process:
        thisPath = os.path.dirname(os.path.realpath(__file__))
        subprocess.run(['bash -c conda activate django_python', "python", os.path.join(thisPath, 'pi_checker.py')])
    return

