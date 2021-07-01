# Generated by Django 3.2 on 2021-06-29 18:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('AbayDashboard', '0006_auto_20210625_1303'),
    ]

    operations = [
        migrations.RenameField(
            model_name='alertprefs',
            old_name='rafting_rampdown_time',
            new_name='rampdown_oxbow',
        ),
        migrations.RenameField(
            model_name='alertprefs',
            old_name='rafting_rampup_time',
            new_name='rampup_oxbow',
        ),
        migrations.AlterField(
            model_name='recreation_data',
            name='water_year_type',
            field=models.TextField(default='above_normal', null=True),
        ),
    ]
