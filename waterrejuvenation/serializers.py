# plantations/serializers.py
from rest_framework import serializers
from .models import WaterbodiesFileUploadLog
from projects.models import Project, AppType


class ExcelFileSerializer(serializers.ModelSerializer):
    """Serializer for Waterbody Excel files"""

    uploaded_by_username = serializers.CharField(
        source="uploaded_by.username", read_only=True
    )

    is_lulc_required = serializers.BooleanField(required=False, default=True)
    is_processing_required = serializers.BooleanField(required=False, default=True)
    is_closest_wp = serializers.BooleanField(required=False, default=True)
    is_compute = serializers.BooleanField(required=False, default=False)
    start_date = serializers.CharField(
        required=False, write_only=True, allow_blank=True, allow_null=True
    )
    end_date = serializers.CharField(
        required=False, write_only=True, allow_blank=True, allow_null=True
    )

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

    def create(self, validated_data):
        # start_date/end_date are request-only; they are not model fields.
        # Pass them through instance.save() so WaterbodiesFileUploadLog.save()
        # can queue Upload_Desilting_Points without objects.create() rejecting them.
        start_date = validated_data.pop("start_date", None) or None
        end_date = validated_data.pop("end_date", None) or None
        instance = WaterbodiesFileUploadLog(**validated_data)
        instance.save(start_date=start_date, end_date=end_date)
        return instance
