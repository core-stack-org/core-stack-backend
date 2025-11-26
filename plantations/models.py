# plantations/models.py
import os
import hashlib
from django.db import models
from projects.models import Project, AppType
from users.models import User
from utilities.logger import setup_logger
from utilities.gee_utils import valid_gee_text
from django.conf import settings
from django.core.files.storage import FileSystemStorage

logger = setup_logger(__name__)

overwrite_storage = FileSystemStorage(
    allow_overwrite=True
)

def kml_file_path(instance, filename):
    """
    Generates the file path for a KML file.
    Format: saytrees/kml_files/project_{project_id}/{filename}
    
    This function creates the directory if it doesn't exist and uses the original filename.
    """

    logger.info(f"Generating KML file path for project: {instance.project.name}")
    
    project_id = instance.project.id
    org_name = instance.project.organization.name
    app_type = instance.project.app_type
    project_name = instance.project.name

    relative_directory = f"site_data/{org_name}/{app_type}/{project_id}_{valid_gee_text(project_name)}"

    full_path = os.path.join(settings.MEDIA_ROOT, relative_directory)

    logger.info(f"Ensuring directory exists: {full_path}")
    try:
        os.makedirs(full_path, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directory: {e}")

    logger.info(f"Returning path: {relative_directory}/{filename}")
    return f"{relative_directory}/{filename}"

class KMLFile(models.Model):
    id = models.AutoField(primary_key=True)
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="kml_files",
        limit_choices_to={"app_type": AppType.PLANTATION, "enabled": True},
        null=True,
    )
    name = models.CharField(max_length=255)
    file = models.FileField(upload_to=kml_file_path, storage=overwrite_storage, max_length=511)
    kml_hash = models.CharField(max_length=64, unique=True)  # md5 hash of file
    uploaded_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        """Override save to calculate file hash before saving"""
        if not self.kml_hash and self.file:
            # Calculate hash for new file
            self.file.seek(0)
            file_hash = hashlib.md5()
            for chunk in self.file.chunks():
                file_hash.update(chunk)
            self.kml_hash = file_hash.hexdigest()

        super().save(*args, **kwargs)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "KML File"
        verbose_name_plural = "KML Files"


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
    config_variables = models.JSONField(null=True, default=None)
    config_weight = models.JSONField(null=True, default=None)
    config_user_input = models.JSONField(
        null=True, default=None
    )  # comes from the frontend
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
