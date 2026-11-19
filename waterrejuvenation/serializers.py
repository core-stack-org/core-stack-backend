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
        ]
        read_only_fields = ["id", "uploaded_by", "created_at"]

    def create(self, validated_data):
        # These are not model fields. The upload view passes them through save()
        # so WaterbodiesFileUploadLog.save() can run or queue processing.
        start_date = validated_data.pop("start_date", None)
        end_date = validated_data.pop("end_date", None)
        instance = WaterbodiesFileUploadLog(**validated_data)
        instance.save(start_date=start_date, end_date=end_date)
        return instance
