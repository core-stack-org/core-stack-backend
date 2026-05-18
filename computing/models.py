from django.db import models
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI


class LayerType(models.TextChoices):
    VECTOR = "vector", "Vector"
    RASTER = "raster", "Raster"
    POINT = "point", "Point"
    CUSTOM = "custom", "Custom"


class Dataset(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    layer_type = models.CharField(
        max_length=50, choices=LayerType.choices, null=True, blank=True
    )
    workspace = models.CharField(max_length=255, blank=True, null=True)
    style_name = models.CharField(max_length=255, blank=True, null=True)
    misc = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    updated_by = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = "Dataset"
        verbose_name_plural = "Datasets"

    def __str__(self):
        return str(self.name)


class Layer(models.Model):
    id = models.AutoField(primary_key=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    layer_name = models.CharField(max_length=511, blank=True, null=True)
    layer_version = models.CharField(max_length=255, blank=True, null=True)
    algorithm = models.CharField(max_length=511, blank=True, null=True)
    algorithm_version = models.CharField(max_length=255, blank=True, null=True)
    state = models.ForeignKey(StateSOI, on_delete=models.CASCADE)
    district = models.ForeignKey(DistrictSOI, on_delete=models.CASCADE)
    block = models.ForeignKey(TehsilSOI, on_delete=models.CASCADE)
    is_excel_generated = models.BooleanField(default=False, blank=True, null=True)
    gee_asset_path = models.CharField(
        max_length=511, blank=True, null=True, default="not available"
    )
    is_public_gee_asset = models.BooleanField(default=False)
    misc = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(max_length=255, blank=True, null=True)
    updated_by = models.CharField(max_length=255, blank=True, null=True)
    is_sync_to_geoserver = models.BooleanField(default=False)
    is_override = models.BooleanField(default=False)
    is_stac_specs_generated = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Layer"
        verbose_name_plural = "Layers"
        unique_together = (
            "dataset",
            "layer_name",
            "state",
            "district",
            "block",
            "layer_version",
        )

    def __str__(self):
        return str(self.layer_name)


class LayerMapping(models.Model):
    """Registry mirroring `data/STAC_specs/input/metadata/layer_mapping.csv`.

    Resolves a saved `Layer` (dataset + geoserver-style layer_name) to the
    canonical STAC `layer_name` and `layer_type` so STAC generation can be
    triggered automatically without hardcoding per-task strings.
    """

    id = models.AutoField(primary_key=True)
    display_name = models.CharField(max_length=255, blank=True, default="")
    layer_type = models.CharField(max_length=16, choices=LayerType.choices)
    layer_name = models.CharField(max_length=255, db_index=True)
    spatial_resolution_in_meters = models.FloatField(null=True, blank=True)
    ee_layer_name = models.CharField(max_length=255, blank=True, default="")
    db_dataset_name = models.CharField(max_length=255, db_index=True)
    geoserver_workspace_name = models.CharField(max_length=255, blank=True, default="")
    geoserver_layer_name = models.CharField(max_length=511, blank=True, default="")
    start_year = models.CharField(max_length=16, blank=True, default="")
    end_year = models.CharField(max_length=16, blank=True, default="")
    style_file_url = models.CharField(max_length=1024, blank=True, default="")
    theme = models.CharField(max_length=255, blank=True, default="")
    auto_stac = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Layer Mapping"
        verbose_name_plural = "Layer Mappings"
        unique_together = (("layer_name", "layer_type", "ee_layer_name"),)

    def __str__(self):
        return f"{self.layer_name} ({self.layer_type})"
