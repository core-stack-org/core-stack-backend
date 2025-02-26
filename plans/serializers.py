from rest_framework import serializers

from .models import Plan
from geoadmin.models import State, District, Block
from projects.models import ProjectApp, AppType


class PlanSerializer(serializers.ModelSerializer):
    class Meta:
        model = Plan
        fields = "__all__"

    def validate(self, data):
        if not data["state"].active_status:
            raise serializers.ValidationError("The state is not active.")

        if not data["district"].active_status:
            raise serializers.ValidationError("The district is not active.")

        if not data["block"].active_status:
            raise serializers.ValidationError("The block is not active.")

        return data


class PlanAppSerializer(serializers.ModelSerializer):
    """
    Serializer for watershed plans with basic information
    """
    created_by_name = serializers.SerializerMethodField()
    
    class Meta:
        model = Plan
        fields = [
            'id', 'plan', 'project_app', 'organization', 
            'state', 'district', 'block', 'village', 'gram_panchayat',
            'created_by', 'created_by_name', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_by', 'created_at', 'updated_at', 'organization']
    
    def get_created_by_name(self, obj):
        if obj.created_by:
            return f"{obj.created_by.first_name} {obj.created_by.last_name}".strip() or obj.created_by.username
        return None


class PlanCreateSerializer(serializers.ModelSerializer):
    """
    Serializer for creating watershed plans
    """
    class Meta:
        model = Plan
        fields = [
            'name', 'state', 'district', 'block', 'village', 'gram_panchayat'
        ]
    
    def validate(self, data):
        """
        Additional validation to ensure required fields are present
        """
        required_fields = ['name', 'state', 'district', 'block', 'village', 'gram_panchayat']
        for field in required_fields:
            if field not in data or not data[field]:
                raise serializers.ValidationError(f"{field} is required")

        if not data["state"].active_status:
            raise serializers.ValidationError("The state is not active.")

        if not data["district"].active_status:
            raise serializers.ValidationError("The district is not active.")

        if not data["block"].active_status:
            raise serializers.ValidationError("The block is not active.")
            
        return data