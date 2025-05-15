# projects/serializers.py
from rest_framework import serializers
from .models import Project, AppType


class ProjectSerializer(serializers.ModelSerializer):
    organization_name = serializers.CharField(
        source="organization.name", read_only=True
    )
    app_type_display = serializers.CharField(
        source="get_app_type_display", read_only=True
    )

    class Meta:
        model = Project
        fields = [
            "id",
            "name",
            "organization",
            "organization_name",
            "description",
            "geojson_path",
            "state",
            "app_type",
            "app_type_display",
            "enabled",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class ProjectDetailSerializer(serializers.ModelSerializer):
    organization_name = serializers.CharField(
        source="organization.name", read_only=True
    )
    app_type_display = serializers.CharField(
        source="get_app_type_display", read_only=True
    )

    class Meta:
        model = Project
        fields = [
            "id",
            "name",
            "organization",
            "organization_name",
            "description",
            "geojson_path",
            "state",
            "app_type",
            "app_type_display",
            "enabled",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class AppTypeSerializer(serializers.Serializer):
    app_type = serializers.ChoiceField(choices=AppType.choices)
    enabled = serializers.BooleanField(default=True)
