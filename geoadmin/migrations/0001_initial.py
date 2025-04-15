# Generated by Django 5.2 on 2025-04-14 11:37

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='District',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('district_census_code', models.CharField(max_length=20)),
                ('district_name', models.CharField(max_length=100)),
                ('active_status', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='State',
            fields=[
                ('state_census_code', models.CharField(max_length=20, primary_key=True, serialize=False)),
                ('state_name', models.CharField(max_length=100)),
                ('active_status', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='Block',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('block_name', models.CharField(max_length=100)),
                ('block_census_code', models.CharField(max_length=20)),
                ('active_status', models.BooleanField(default=False)),
                ('district', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='geoadmin.district')),
            ],
        ),
        migrations.AddField(
            model_name='district',
            name='state',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='geoadmin.state'),
        ),
    ]
