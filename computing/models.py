from django.db import models
from geoadmin.models import State, District, Block


class LayerType(models.TextChoices):
    VECTOR = "vector", "Vector"
    RASTER = "raster", "Raster"
    POINT = "point", "Point"
    CUSTOM = "custom", "Custom"

class Layer(models.Model):
    id = models.AutoField(primary_key=True)
    layer_name = models.CharField(max_length=511, blank=True, null=True)
    layer_version = models.CharField(max_length=255, blank=True, null=True)
    layer_type = models.CharField(max_length=50, choices=LayerType.choices)
    algorithm = models.CharField(max_length=511, blank=True, null=True)
    algorithm_version = models.CharField(max_length=255, blank=True, null=True)
    style_name = models.CharField(max_length=255, blank=True, null=True)
    is_excel_generated = models.BooleanField(default=False, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Layer"
        verbose_name_plural = "Layers"  
    
class Dataset(models.Model):
    id = models.AutoField(primary_key=True)
    layer = models.ForeignKey(Layer, on_delete=models.CASCADE)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    gee_asset_path = models.CharField(max_length=511, blank=True, null=True)
    is_public_gee_asset = models.BooleanField(default=False)
    workspace = models.CharField(max_length=255, blank=True, null=True)
    layer_nomenclature = models.CharField(max_length=511, blank=True, null=True)
    misc = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Dataset"
        verbose_name_plural = "Datasets"