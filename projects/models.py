from email.policy import default
import uuid
from django.db import models
from organization.models import Organization
from geoadmin.models import State, District, Block


class AppType(models.TextChoices):
    PLANTATION = "plantation", "Plantations"
    WATERSHED = "watershed", "Watershed Planning"
    WATERBODY_REJ = "waterbody", "Waterbody Rejuvenation"
    COMMUNITY_ENGAGEMENT = "community_engagement", "Community Engagement"
    # More types as apps are added in future


class Project(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    organization = models.ForeignKey(
        Organization, on_delete=models.CASCADE, related_name="projects"
    )
    description = models.TextField(blank=True, null=True)
    geojson_path = models.CharField(max_length=512, blank=True, null=True)
    state = models.ForeignKey(State, on_delete=models.CASCADE, null=True)
    district = models.ForeignKey(District, on_delete=models.CASCADE, null=True, blank=True)
    block = models.ForeignKey(Block, on_delete=models.CASCADE, null=True, blank=True)
    app_type = models.CharField(max_length=255, choices=AppType.choices)
    enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        "users.User", on_delete=models.CASCADE, related_name="created_projects"
    )
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        "users.User", on_delete=models.CASCADE, related_name="updated_projects"
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["-created_at"]
