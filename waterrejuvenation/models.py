from email.policy import default

from django.db import models

from gee_computing.models import GEEAccount
from geoadmin.models import State, District
import uuid
from projects.models import Project, AppType
import os

from users.models import User
from utilities.constants import SITE_DATA_PATH
from .tasks import Upload_Desilting_Points


# Create your models here.
def excel_file_path(instance, filename):
    """
    Generates the file path for a excel file.
    Format: saytrees/excel_files/project_{project_id}/{filename}
    """
    project_id = instance.project.id
    org_name = instance.project.organization.name
    app_type = instance.project.app_type
    project_name = instance.project.name

    # Create directory if it doesn't exist
    directory = f"site_data/{org_name}/{app_type}/{project_id}/{project_name}"
    file_path = f"{directory}/{filename}"

    return file_path


class WaterbodiesFileUploadLog(models.Model):
    id = models.AutoField(primary_key=True)
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="wate_rej_excel_files",
        limit_choices_to={"app_type": AppType.WATERBODY_REJ, "enabled": True},
        null=True,
    )
    name = models.CharField(max_length=255)
    file = models.FileField(upload_to=excel_file_path)
    excel_hash = models.CharField(max_length=64, unique=True)  # md5 hash of file
    uploaded_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    process = models.BooleanField(default=False)
    gee_account_id = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        """Override save to calculate file hash before saving"""
        if not self.excel_hash and self.file:
            # Calculate hash for new file
            self.file.seek(0)
            file_hash = hashlib.md5()
            for chunk in self.file.chunks():
                file_hash.update(chunk)
            self.excel_hash = file_hash.hexdigest()

        super().save(*args, **kwargs)
        Upload_Desilting_Points.delay(self.id, gee_project_id=self.gee_account_id)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Excel File"
        verbose_name_plural = "Excel Files"


class WaterbodiesDesiltingLog(models.Model):
    id = models.AutoField(primary_key=True)
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="wate_dslit",
        limit_choices_to={"app_type": AppType.WATERBODY_REJ, "enabled": True},
        null=True,
    )
    name_of_ngo = models.CharField(max_length=255, null=True)
    State = models.CharField(max_length=255, null=True)
    District = models.CharField(max_length=255, null=True)
    Taluka = models.CharField(max_length=255, null=True)
    Village = models.CharField(max_length=255, null=True)
    waterbody_name = models.CharField(max_length=255, null=True)
    lat = models.FloatField()
    lon = models.FloatField()
    slit_excavated = models.CharField(max_length=255, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    process = models.BooleanField(default=False)
    waterbody_id = models.CharField(max_length=255, null=True)
    closest_wb_lat = models.FloatField(null=True)
    closest_wb_long = models.FloatField(null=True)
    intersecting_mws = models.CharField(max_length=255, null=True)
    distance_closest_wb_pixel = models.IntegerField(null=True)
    excel_hash = models.CharField(max_length=64, null=True)
    intervention_year = models.CharField(max_length=255, null=True)

    def __str__(self):
        return self.waterbody_name
