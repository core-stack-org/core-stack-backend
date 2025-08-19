from django.db import models
from django.utils import timezone
from rest_framework_api_key.models import AbstractAPIKey
from nrm_app.settings import AUTH_USER_MODEL


# Create your models here.
# models for state, district and blocks
class State(models.Model):
    state_census_code = models.CharField(max_length=20, primary_key=True)
    state_name = models.CharField(max_length=100)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.state_name


class District(models.Model):
    id = models.AutoField(primary_key=True)
    district_census_code = models.CharField(max_length=20)
    district_name = models.CharField(max_length=100)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.district_name


class Block(models.Model):
    id = models.AutoField(primary_key=True)
    block_name = models.CharField(max_length=100)
    block_census_code = models.CharField(max_length=20)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.block_name


class StateSOI(models.Model):
    id = models.AutoField(primary_key=True)
    state_name = models.CharField(max_length=255)
    active_status = models.BooleanField(default=False)

    def __str__(self):
        return self.state_name


class DistrictSOI(models.Model):
    id = models.AutoField(primary_key=True)
    state = models.ForeignKey(StateSOI, on_delete=models.CASCADE)
    district_name = models.CharField(max_length=255)
    active_status = models.BooleanField(default=False)

    def __str__(self):
        return self.district_name


class TehsilSOI(models.Model):
    id = models.AutoField(primary_key=True)
    district = models.ForeignKey(DistrictSOI, on_delete=models.CASCADE)
    tehsil_name = models.CharField(max_length=255)
    active_status = models.BooleanField(default=False)

    def __str__(self):
        return self.tehsil_name


class UserAPIKey(AbstractAPIKey):
    user = models.ForeignKey(AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="api_keys")
    name = models.CharField(max_length=255)
    api_key = models.CharField(max_length=255, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    @property
    def is_expired(self):
        """Check if key is expired"""
        if self.expires_at is None:
            return False
        return timezone.now() > self.expires_at

    def __str__(self) -> str:
        return f"{self.name} ({self.user.username})"
