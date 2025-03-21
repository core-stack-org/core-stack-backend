# plantations/models.py
import os
import hashlib
from django.db import models
from projects.models import Project, AppType
from users.models import User
from utilities.constants import SITE_DATA_PATH


def kml_file_path(instance, filename):
    """
    Generates the file path for a KML file.
    Format: saytrees/kml_files/project_{project_id}/{filename}
    """
    project_id = instance.project.id
    org_name = instance.project.organization.name
    app_type = instance.project.app_type
    project_name = instance.project.name

    # Create directory if it doesn't exist
    directory = f"{org_name}/{app_type}/{project_id}_{project_name}"
    full_path = os.path.join(SITE_DATA_PATH, directory)
    os.makedirs(full_path, exist_ok=True)

    return f"{full_path}/{filename}"


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
    file = models.FileField(upload_to=kml_file_path)
    kml_hash = models.CharField(max_length=64, unique=True)  # SHA-256 hash of file
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
