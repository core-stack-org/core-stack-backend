from django.db import models
from django.contrib.auth import get_user_model

import uuid

from nrm_app import settings

from django.contrib.auth.models import Group


class Organization(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    odk_project = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
            settings.AUTH_USER_MODEL,
            null=True,
            on_delete=models.CASCADE,
            related_name="organizations_created")

    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.name

    def getAllOrg(self):
        from users.models import User
        users = User.objects.filter(organization = self, groups__name='Administrator')
        return users