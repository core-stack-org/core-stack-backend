# plantations/serializers.py
from rest_framework import serializers
from .models import KMLFile
from projects.models import Project, AppType


class KMLFileSerializer(serializers.ModelSerializer):
    """Serializer for KML files with basic information"""

    uploaded_by_username = serializers.CharField(
        source="uploaded_by.username", read_only=True
    )

    class Meta:
        model = KMLFile
        fields = [
            "id",
            "name",
            "file",
            "uploaded_by",
            "uploaded_by_username",
            "created_at",
        ]
        read_only_fields = ["id", "uploaded_by", "created_at"]


class KMLFileDetailSerializer(serializers.ModelSerializer):
    """Serializer for detailed KML file information including GeoJSON data"""

    uploaded_by_username = serializers.CharField(
        source="uploaded_by.username", read_only=True
    )

    class Meta:
        model = KMLFile
        fields = [
            "id",
            "name",
            "file",
            "uploaded_by",
            "uploaded_by_username",
            "created_at",
        ]
        read_only_fields = ["id", "uploaded_by", "created_at"]
