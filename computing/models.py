from django.db import models
from geoadmin.models import State, District, Block


# Create your models here.

# create a dummy MWS model with mws id
class MWS:
    id = models.IntegerField(primary_key=True)

# Models for excel generation
class LayerInfo(models.Model):
    LAYER_TYPE_CHOICES = [
        ("raster", "Raster"),
        ("vector", "Vector"),
    ]

    id = models.AutoField(primary_key=True)
    layer_name = models.CharField(max_length=255)
    layer_type = models.CharField(max_length=50, choices=LAYER_TYPE_CHOICES)
    workspace = models.CharField(max_length=255, blank=True, null=True)
    layer_desc = models.TextField(blank=True, null=True)
    excel_to_be_generated = models.BooleanField(default=False)
    start_year = models.PositiveIntegerField(blank=True, null=True)
    end_year = models.PositiveIntegerField(blank=True, null=True)
    style_name = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.workspace + '/' + self.layer_name

    class Meta:
        ordering = ['-created_at']


