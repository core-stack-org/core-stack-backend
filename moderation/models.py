from datetime import datetime
from zoneinfo import ZoneInfo
from django.db import models
from django.utils import timezone

IST = ZoneInfo("Asia/Kolkata")
BASELINE_DATE = datetime(2025, 12, 14, tzinfo=IST)


class SyncMetadata(models.Model):
    sync_type = models.CharField(max_length=50, primary_key=True)
    last_synced_at = models.DateTimeField(null=True, blank=True)
    baseline_date = models.DateTimeField(default=BASELINE_DATE)

    class Meta:
        db_table = "sync_metadata"
        verbose_name = "Sync Metadata"
        verbose_name_plural = "Sync Metadata"

    def __str__(self):
        return f"{self.sync_type} - Last synced: {self.last_synced_at}"

    @classmethod
    def get_odk_sync_metadata(cls):
        obj, _ = cls.objects.get_or_create(
            sync_type="odk_sync",
            defaults={"baseline_date": BASELINE_DATE}
        )
        return obj

    def get_filter_date(self):
        return self.last_synced_at or self.baseline_date

    def update_last_synced(self):
        self.last_synced_at = timezone.now()
        self.save(update_fields=["last_synced_at"])
