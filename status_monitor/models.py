from django.db import models


class Endpoint(models.Model):
    name = models.CharField(max_length=255)
    url = models.URLField(max_length=1024, unique=True)
    headers = models.JSONField(default=dict, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name


class StatusCheck(models.Model):
    endpoint = models.ForeignKey(
        Endpoint, on_delete=models.CASCADE, related_name="checks"
    )
    status_code = models.IntegerField(null=True, blank=True)
    response_time_ms = models.IntegerField(null=True, blank=True)
    is_up = models.BooleanField()
    error = models.TextField(blank=True, default="")
    checked_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-checked_at"]

    def __str__(self):
        status = "UP" if self.is_up else "DOWN"
        return f"{self.endpoint.name} - {status} @ {self.checked_at}"
