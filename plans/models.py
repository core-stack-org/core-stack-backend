from django.db import models
from geoadmin.models import State, District, Block
from projects.models import Project, AppType
from organization.models import Organization
from users.models import User


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
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
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

    def __str__(self):
        return str(self.plan)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Watershed Plan"
        verbose_name_plural = "Watershed Plans"
