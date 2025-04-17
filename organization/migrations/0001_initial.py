# Generated by Django 5.2 on 2025-04-17 09:34

import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255)),
                ('description', models.TextField(blank=True, null=True)),
                ('odk_project', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('created_by', models.CharField(blank=True, max_length=255, null=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('updated_by', models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
    ]
