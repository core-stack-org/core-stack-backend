from django.db import models
from geoadmin.models import State, District, Block

# Create your models here.

class GeneratedLayerInfo(models.Model):
    LAYER_TYPE_CHOICES = [
        ("raster", "Raster"),
        ("vector", "Vector"),
    ]

    id = models.AutoField(primary_key=True)
    layer_name = models.CharField(max_length=255)
    layer_type = models.CharField(max_length=255, choices=LAYER_TYPE_CHOICES)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    gee_path = models.JSONField(blank=True, null=True)
    workspace = models.CharField(max_length=512, blank=True, null=True)
    algorithm = models.CharField(max_length=255)
    version = models.CharField(max_length=255)
    style_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='created_layer')
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='updated_layer')
    misc = models.JSONField(blank=True, null=True)

    def __str__(self):
        return self.layer_name

    class Meta:
        ordering = ['-created_at']

