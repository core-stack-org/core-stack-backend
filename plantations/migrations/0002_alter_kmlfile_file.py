# Generated by Django 5.2 on 2025-05-21 11:09

import django.core.files.storage
import plantations.models
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('plantations', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='kmlfile',
            name='file',
            field=models.FileField(storage=django.core.files.storage.FileSystemStorage(allow_overwrite=True), upload_to=plantations.models.kml_file_path),
        ),
    ]
