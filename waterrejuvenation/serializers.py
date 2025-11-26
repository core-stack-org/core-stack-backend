# plantations/serializers.py
from rest_framework import serializers
from .models import WaterbodiesFileUploadLog
from projects.models import Project, AppType


class ExcelFileSerializer(serializers.ModelSerializer):
    """Serializer for KML files with basic information"""

    uploaded_by_username = serializers.CharField(
        source="uploaded_by.username", read_only=True
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
        ]
        read_only_fields = ["id", "uploaded_by", "created_at"]
