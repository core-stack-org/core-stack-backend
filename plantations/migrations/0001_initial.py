# Generated by Django 5.1.5 on 2025-02-26 07:28

import plantations.models
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='KMLFile',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255)),
                ('file', models.FileField(upload_to=plantations.models.kml_file_path)),
                ('kml_hash', models.CharField(max_length=64, unique=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'verbose_name': 'KML File',
                'verbose_name_plural': 'KML Files',
                'ordering': ['-created_at'],
            },
        ),
    ]
