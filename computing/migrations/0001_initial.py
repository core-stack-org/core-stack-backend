# Generated by Django 5.0.7 on 2025-04-01 05:50

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='LayerInfo',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('layer_name', models.CharField(max_length=255)),
                ('layer_type', models.CharField(choices=[('raster', 'Raster'), ('vector', 'Vector')], max_length=50)),
                ('workspace', models.CharField(blank=True, max_length=255, null=True)),
                ('layer_desc', models.TextField(blank=True, null=True)),
                ('excel_to_be_generated', models.BooleanField(default=False)),
                ('start_year', models.PositiveIntegerField(blank=True, null=True)),
                ('end_year', models.PositiveIntegerField(blank=True, null=True)),
                ('style_name', models.CharField(blank=True, max_length=255, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'ordering': ['-created_at'],
            },
        ),
    ]
