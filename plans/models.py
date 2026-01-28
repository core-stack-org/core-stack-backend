from django.db import models

from geoadmin.models import Block, District, DistrictSOI, State, StateSOI, TehsilSOI
from organization.models import Organization
from projects.models import Project
from users.models import User


class ODKSyncLog(models.Model):
    class SyncCategory(models.TextChoices):
        RESOURCE = "resource", "Resource"
        WORK = "work", "Work"
        FEEDBACK = "feedback", "Feedback"

    class SyncStatus(models.TextChoices):
        PENDING = "pending", "Pending"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    category = models.CharField(max_length=20, choices=SyncCategory.choices)
    sync_type = models.CharField(max_length=50)
    xml_content = models.TextField()
    odk_url = models.URLField()
    status = models.CharField(
        max_length=20, choices=SyncStatus.choices, default=SyncStatus.PENDING
    )
    odk_response = models.JSONField(null=True, blank=True)
    error_details = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "ODK Sync Log"
        verbose_name_plural = "ODK Sync Logs"
        indexes = [
            models.Index(fields=["category", "sync_type"]),
            models.Index(fields=["status"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self):
        return f"{self.category}:{self.sync_type} - {self.status} ({self.created_at})"


class Plan(models.Model):
    plan_id = models.AutoField(primary_key=True)
    facilitator_name = models.CharField(max_length=300)
    plan = models.TextField(default="Default Plan")
    village_name = models.CharField(max_length=300)
    gram_panchayat = models.CharField(max_length=300)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)

    def __str__(self):
        return str(self.plan)


class PlanApp(models.Model):
    id = models.AutoField(primary_key=True)
    plan = models.CharField(max_length=255)
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        related_name="plans",
        limit_choices_to={"enabled": True},
        null=True,
    )
    organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
    facilitator_name = models.CharField(max_length=512, null=True, blank=True)
    state = models.ForeignKey(State, on_delete=models.CASCADE, null=True, blank=True)
    district = models.ForeignKey(
        District, on_delete=models.CASCADE, null=True, blank=True
    )
    block = models.ForeignKey(Block, on_delete=models.CASCADE, null=True, blank=True)
    state_soi = models.ForeignKey(
        StateSOI, on_delete=models.CASCADE, null=True, blank=True
    )
    district_soi = models.ForeignKey(
        DistrictSOI, on_delete=models.CASCADE, null=True, blank=True
    )
    tehsil_soi = models.ForeignKey(
        TehsilSOI, on_delete=models.CASCADE, null=True, blank=True
    )
    village_name = models.CharField(max_length=255)
    gram_panchayat = models.CharField(max_length=255)
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, related_name="created_plans"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, related_name="updated_plans"
    )
    enabled = models.BooleanField(default=True)
    is_completed = models.BooleanField(default=False)
    is_dpr_generated = models.BooleanField(default=False)
    is_dpr_reviewed = models.BooleanField(default=False)
    is_dpr_approved = models.BooleanField(default=False)
    latitude = models.DecimalField(
        max_digits=20, decimal_places=8, null=True, blank=True
    )
    longitude = models.DecimalField(
        max_digits=20, decimal_places=8, null=True, blank=True
    )

    def __str__(self):
        return str(self.plan)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Watershed Plan"
        verbose_name_plural = "Watershed Plans"
