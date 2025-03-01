from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings
import logging

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wx.settings')

app = Celery('wx',
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0',
             include=['AbayDashboard'])

app.log.setup_logging_subsystem()

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

@app.task(bind=True)
def debug_task(self):
    logging.info('Request: {0!r}'.format(self.request))