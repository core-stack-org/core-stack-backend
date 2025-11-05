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
