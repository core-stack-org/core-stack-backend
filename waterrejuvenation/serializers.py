# plantations/serializers.py
from datetime import date, datetime

from rest_framework import serializers
from .models import WaterbodiesFileUploadLog
from projects.models import Project, AppType


def parse_upload_date(value):
    """Accept YYYY-MM-DD, ISO datetimes, and common day-first variants."""
    if value in (None, "", []):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in ("null", "none"):
        return None

    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise serializers.ValidationError(
        "Date has wrong format. Use one of these formats instead: YYYY-MM-DD."
    )


class FlexibleDateField(serializers.DateField):
    def to_internal_value(self, value):
        try:
            parsed = parse_upload_date(value)
        except serializers.ValidationError:
            self.fail("invalid", format="YYYY-MM-DD")
        if parsed is None:
            if getattr(self, "allow_null", False):
                return None
            self.fail("invalid", format="YYYY-MM-DD")
        return parsed


class ExcelFileSerializer(serializers.ModelSerializer):
    """Serializer for Waterbody Excel files"""

    uploaded_by_username = serializers.CharField(
        source="uploaded_by.username", read_only=True
    )

    is_lulc_required = serializers.BooleanField(required=False, default=True)
    is_processing_required = serializers.BooleanField(required=False, default=True)
    is_closest_wp = serializers.BooleanField(required=False, default=True)
    is_compute = serializers.BooleanField(required=False, default=False)
    start_date = FlexibleDateField(required=False, allow_null=True)
    end_date = FlexibleDateField(required=False, allow_null=True)

    class Meta:
        model = WaterbodiesFileUploadLog
        fields = [
            "id",
            "name",
            "file",
            "uploaded_by",
            "uploaded_by_username",
            "created_at",
            "gee_account_id",
            "is_processing_required",
            "is_lulc_required",
            "is_closest_wp",
            "is_compute",
            "start_date",
            "end_date",
        ]
        read_only_fields = ["id", "uploaded_by", "created_at"]
        extra_kwargs = {
            "start_date": {"required": False, "allow_null": True},
            "end_date": {"required": False, "allow_null": True},
        }
