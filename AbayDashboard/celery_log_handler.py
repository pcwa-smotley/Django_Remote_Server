import logging
from django.apps import apps
from django.conf import settings

class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        # Only log messages with level WARNING and above
        if record.levelno >= logging.WARNING:
            CeleryLog = apps.get_model('AbayDashboard', 'CeleryLog')
            log_entry = CeleryLog(
                level=record.levelname,
                message=self.format(record)
            )
            log_entry.save()