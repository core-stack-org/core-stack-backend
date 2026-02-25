from django.db import models


class DroughtSeverity(models.TextChoices):
    D0 = "D0", "D0 - Abnormally Dry"
    D1 = "D1", "D1 - Moderate Drought"
    D2 = "D2", "D2 - Severe Drought"
    D3 = "D3", "D3 - Extreme Drought"
    D4 = "D4", "D4 - Exceptional Drought"


class AlertSource(models.TextChoices):
    SPEI = "spei", "SPEI Computation"
    IDM = "idm", "India Drought Monitor"


class DroughtAlert(models.Model):
    """
    Stores drought live alerts generated from SPEI computation
    or ingested from India Drought Monitor (IDM).
    """

    id = models.AutoField(primary_key=True)

    # Alert classification
    severity = models.CharField(
        max_length=2, choices=DroughtSeverity.choices, db_index=True
    )
    alert_type = models.CharField(
        max_length=4, choices=AlertSource.choices, db_index=True
    )

    # SPEI value (null for IDM-sourced alerts)
    spei_value = models.FloatField(null=True, blank=True)

    # Spatial information
    aoi_name = models.CharField(
        max_length=511, help_text="State/District/Block identifier", db_index=True
    )
    state = models.CharField(max_length=255, blank=True, null=True)
    district = models.CharField(max_length=255, blank=True, null=True)
    block = models.CharField(max_length=255, blank=True, null=True)
    area_sq_km = models.FloatField(
        null=True, blank=True, help_text="Affected area in sq km"
    )
    geometry = models.JSONField(
        null=True, blank=True, help_text="GeoJSON polygon of affected area"
    )

    # Temporal information
    alert_date = models.DateField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Status and metadata
    is_active = models.BooleanField(default=True, db_index=True)
    metadata = models.JSONField(
        null=True,
        blank=True,
        help_text="Source info, thresholds, processing details",
    )

    class Meta:
        verbose_name = "Drought Alert"
        verbose_name_plural = "Drought Alerts"
        ordering = ["-alert_date", "-created_at"]
        indexes = [
            models.Index(fields=["alert_date", "severity"]),
            models.Index(fields=["state", "district", "block"]),
        ]

    def __str__(self):
        return f"{self.get_severity_display()} | {self.aoi_name} | {self.alert_date}"

    @property
    def severity_label(self):
        return self.get_severity_display()
