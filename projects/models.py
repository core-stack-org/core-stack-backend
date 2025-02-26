import uuid
from django.db import models
from organization.models import Organization


class Project(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    organization = models.ForeignKey(Organization, on_delete=models.CASCADE, related_name='projects')
    description = models.TextField(blank=True, null=True)
    geojson_path = models.CharField(max_length=512, blank=True, null=True)  # Added for geoserver access
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='created_projects')
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='updated_projects')

    def __str__(self):
        return self.name
    
    class Meta:
        ordering = ['-created_at']


class AppType(models.TextChoices):
    PLANTATION = 'plantation', 'Plantations'
    WATERSHED = 'watershed', 'Watershed Planning' 
    # More types as apps are added in future


class ProjectApp(models.Model):
    id = models.AutoField(primary_key=True)
    project = models.ForeignKey(Project, on_delete=models.CASCADE, related_name='apps')
    app_type = models.CharField(max_length=50, choices=AppType.choices)
    enabled = models.BooleanField(default=True)
    
    class Meta:
        unique_together = ('project', 'app_type')
        
    def __str__(self):
        return f"{self.project.name} - {self.get_app_type_display()}"