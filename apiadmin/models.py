from django.db import models
from django.conf import settings


class ApiHitLog(models.Model):
    path = models.CharField(max_length=500)
    method = models.CharField(max_length=10)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL
    )

    ip_address = models.GenericIPAddressField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    query_params = models.TextField(null=True, blank=True)
    body = models.TextField(null=True, blank=True)

    # NEW: Store API key if available
    api_key = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"{self.method} {self.path} at {self.timestamp}"
