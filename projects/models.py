import uuid
from django.db import models
from organization.models import Organization
from geoadmin.models import State


class AppType(models.TextChoices):
    PLANTATION = "plantation", "Plantations"
    WATERSHED = "watershed", "Watershed Planning"
    # More types as apps are added in future


class Project(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    organization = models.ForeignKey(
        Organization, on_delete=models.CASCADE, related_name="projects"
    )
    description = models.TextField(blank=True, null=True)
    geojson_path = models.CharField(max_length=512, blank=True, null=True)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
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


class PlantationProfile(models.Model):
    """
    Profile model specifically for projects with app_type='plantation'.
    """

    profile_id = models.AutoField(primary_key=True)
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        limit_choices_to={"app_type": AppType.PLANTATION},
        related_name="plantation_profiles",
    )
    config = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    modified_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Plantation Profile {self.profile_id} for Project {self.project.name}"

    def save(self, *args, **kwargs):
        """Override save to ensure this model is only used with plantation app types"""
        if self.project.app_type != AppType.PLANTATION:
            raise ValueError(
                "PlantationProfile can only be associated with plantation app types"
            )
        super().save(*args, **kwargs)
