# Generated by Django 3.2 on 2021-06-29 19:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('AbayDashboard', '0007_auto_20210629_1143'),
    ]

    operations = [
        migrations.AlterField(
            model_name='alertprefs',
            name='rampdown_oxbow',
            field=models.IntegerField(default=None, null=True),
        ),
        migrations.AlterField(
            model_name='alertprefs',
            name='rampup_oxbow',
            field=models.IntegerField(default=None, null=True),
        ),
    ]