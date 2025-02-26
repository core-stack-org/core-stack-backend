# projects/serializers.py
from rest_framework import serializers
from .models import Project, ProjectApp, AppType


class ProjectAppSerializer(serializers.ModelSerializer):
    app_type_display = serializers.CharField(source='get_app_type_display', read_only=True)
    
    class Meta:
        model = ProjectApp
        fields = ['id', 'app_type', 'app_type_display', 'enabled']
        read_only_fields = ['id']


class ProjectSerializer(serializers.ModelSerializer):
    organization_name = serializers.CharField(source='organization.name', read_only=True)
    
    class Meta:
        model = Project
        fields = ['id', 'name', 'organization', 'organization_name', 
                 'description', 'geojson_path', 'created_at', 'created_by', 'updated_at', 'updated_by']
        read_only_fields = ['id', 'created_at', 'updated_at']


class ProjectDetailSerializer(serializers.ModelSerializer):
    organization_name = serializers.CharField(source='organization.name', read_only=True)
    apps = ProjectAppSerializer(many=True, read_only=True)
    
    class Meta:
        model = Project
        fields = ['id', 'name', 'organization', 'organization_name', 
                 'description', 'geojson_path', 'created_at', 'created_by', 'updated_at', 'updated_by', 'apps']
        read_only_fields = ['id', 'created_at', 'updated_at']


class AppTypeSerializer(serializers.Serializer):
    app_type = serializers.ChoiceField(choices=AppType.choices)
    enabled = serializers.BooleanField(default=True)


class ProjectAppUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProjectApp
        fields = ['app_type', 'enabled']