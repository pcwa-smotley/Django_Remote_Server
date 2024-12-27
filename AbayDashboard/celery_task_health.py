# AbayDashboard/tasks.py

from celery import Celery
from celery import shared_task
from django.core.mail import send_mail
from datetime import datetime, timedelta
from django.conf import settings
from .models import CeleryLog
from AbayDashboard.mailer import send_mail

app = Celery('wx')

@shared_task
def health_check():
    now = datetime.now()
    expected_update_interval = timedelta(minutes=30)  # Adjust as needed

    # Get the last update times of your tasks
    main_task_update = CeleryLog.objects.filter(task_name='main').order_by('-updated_at').first()
    cnrfc_task_update = CeleryLog.objects.filter(task_name='get_cnrfc_data').order_by('-updated_at').first()

    if not main_task_update or (now - main_task_update.updated_at) > expected_update_interval:
        send_mail("7203750163@mms.att.net", "smotley@mac.com", "error", "Pi Checker Crashed")

    if not cnrfc_task_update or (now - cnrfc_task_update.updated_at) > expected_update_interval:
        send_mail("7203750163@mms.att.net", "smotley@mac.com","error", "Pi Checker Crashed")

    return
# AbayDashboard/views.py
