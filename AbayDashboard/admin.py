from django.contrib import admin
from .models import Profile, AlertPrefs, Issued_Alarms, Recreation_Data

# Register your models here.
class UserAdmin(admin.ModelAdmin):
    fields = ["first_name",
              "last_name",
              "email",
              ""]


admin.site.register(Profile)
admin.site.register(AlertPrefs)
admin.site.register(Issued_Alarms)
admin.site.register(Recreation_Data)