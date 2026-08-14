# Generated for LayerMapping STAC registry

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("computing", "0003_dataset_can_be_empty_dataset_is_active"),
    ]

    operations = [
        migrations.CreateModel(
            name="LayerMapping",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                (
                    "display_name",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                (
                    "layer_type",
                    models.CharField(
                        choices=[
                            ("vector", "Vector"),
                            ("raster", "Raster"),
                            ("point", "Point"),
                            ("custom", "Custom"),
                        ],
                        max_length=16,
                    ),
                ),
                ("layer_name", models.CharField(db_index=True, max_length=255)),
                (
                    "spatial_resolution_in_meters",
                    models.FloatField(blank=True, null=True),
                ),
                (
                    "ee_layer_name",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("db_dataset_name", models.CharField(db_index=True, max_length=255)),
                (
                    "geoserver_workspace_name",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                (
                    "geoserver_layer_name",
                    models.CharField(blank=True, default="", max_length=511),
                ),
                (
                    "start_year",
                    models.CharField(blank=True, default="", max_length=16),
                ),
                ("end_year", models.CharField(blank=True, default="", max_length=16)),
                (
                    "style_file_url",
                    models.CharField(blank=True, default="", max_length=1024),
                ),
                ("theme", models.CharField(blank=True, default="", max_length=255)),
                ("auto_stac", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Layer Mapping",
                "verbose_name_plural": "Layer Mappings",
                "unique_together": {("layer_name", "layer_type", "ee_layer_name")},
            },
        ),
    ]
