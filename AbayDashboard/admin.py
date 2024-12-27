from django.contrib import admin
from .models import Profile, AlertPrefs, Issued_Alarms, Recreation_Data, PiData, CeleryLog
from django_celery_results.models import TaskResult

# Register your models here.
class UserAdmin(admin.ModelAdmin):
    fields = ["first_name",
              "last_name",
              "email",
              ""]

# Optionally customize the admin interface
class PeriodicTaskAdmin(admin.ModelAdmin):
    list_display = ('name', 'enabled', 'interval', 'start_time', 'last_run_at')
    search_fields = ('name',)
    list_filter = ('enabled', 'interval')
@admin.register(CeleryLog)
class CeleryLogAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'level', 'message')
    search_fields = ('level', 'message')
    list_filter = ('level', 'timestamp')


admin.site.register(Profile)
admin.site.register(AlertPrefs)
admin.site.register(Issued_Alarms)
admin.site.register(Recreation_Data)
admin.site.register(PiData)