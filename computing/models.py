from django.db import models
from geoadmin.models import State, District, Block


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
<<<<<<< HEAD
=======
    layer_version = models.CharField(max_length=255, blank=True, null=True)
    algorithm = models.CharField(max_length=511, blank=True, null=True)
    algorithm_version = models.CharField(max_length=255, blank=True, null=True)
    workspace = models.CharField(max_length=255, blank=True, null=True)
    style_name = models.CharField(max_length=255, blank=True, null=True)
>>>>>>> layer/save_db
    misc = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Dataset"
        verbose_name_plural = "Datasets"

<<<<<<< HEAD
=======
    def __str__(self):
        return str(self.name)

>>>>>>> layer/save_db

class Layer(models.Model):
    id = models.AutoField(primary_key=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    layer_name = models.CharField(max_length=511, blank=True, null=True)
<<<<<<< HEAD
    layer_version = models.CharField(max_length=255, blank=True, null=True)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    algorithm = models.CharField(max_length=511, blank=True, null=True)
    algorithm_version = models.CharField(max_length=255, blank=True, null=True)
    style_name = models.CharField(max_length=255, blank=True, null=True)
    is_excel_generated = models.BooleanField(default=False, blank=True, null=True)
    gee_asset_path = models.CharField(max_length=511, blank=True, null=True)
    is_public_gee_asset = models.BooleanField(default=False)
    workspace = models.CharField(max_length=255, blank=True, null=True)
=======
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    is_excel_generated = models.BooleanField(default=False, blank=True, null=True)
    gee_asset_path = models.CharField(max_length=511, blank=True, null=True, default="not available")
    is_public_gee_asset = models.BooleanField(default=False)
>>>>>>> layer/save_db
    misc = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Layer"
        verbose_name_plural = "Layers"
<<<<<<< HEAD
=======

    def __str__(self):
        return str(self.layer_name)
>>>>>>> layer/save_db
