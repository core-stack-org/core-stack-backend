from rest_framework import serializers

from .models import Plan, PlanApp
from geoadmin.models import State, District, Block
from projects.models import Project, AppType


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

class PlanAppListSerializer(serializers.ModelSerializer):
    """
    Serializer for listing watershed plans with PlanApp model
    """
    
    class Meta:
        model = PlanApp
        fields = "__all__"

class PlanAppSerializer(serializers.ModelSerializer):
    """
    Serializer for watershed plans with basic information
    """

    created_by_name = serializers.SerializerMethodField()

    class Meta:
        model = PlanApp
        fields = [
            "id",
            "plan",
            "project",
            "organization",
            "state",
            "district",
            "block",
            "village_name",
            "gram_panchayat",
            "created_by",
            "created_by_name",
            "created_at",
            "updated_at",
            "enabled",
            "is_completed",
            "is_dpr_generated",
            "is_dpr_reviewed",
            "is_dpr_approved",
        ]
        read_only_fields = [
            "id",
            "created_by",
            "created_at",
            "updated_at",
            "organization",
        ]

    def get_created_by_name(self, obj):
        if obj.created_by:
            return (
                f"{obj.created_by.first_name} {obj.created_by.last_name}".strip()
                or obj.created_by.username
            )
        return None


class PlanCreateSerializer(serializers.ModelSerializer):
    """
    Serializer for creating watershed plans
    """

    class Meta:
        model = PlanApp
        fields = ["plan", "state", "district", "block", "village_name", "gram_panchayat", "facilitator_name"]

    def validate(self, data):
        """
        Additional validation to ensure required fields are present
        """
        required_fields = [
            "plan",
            "state",
            "district",
            "block",
            "village_name",
            "gram_panchayat",
            "facilitator_name"
        ]
        for field in required_fields:
            if field not in data or not data[field]:
                raise serializers.ValidationError(f"{field} is required")

        request = self.context.get('request')
        if request and request.parser_context.get('kwargs'):
            project_id = request.parser_context['kwargs'].get('project_pk')
            if project_id and data.get('plan'):
                existing_plan = PlanApp.objects.filter(
                    project_id=project_id,
                    plan=data['plan']
                ).exists()
                
                if existing_plan:
                    raise serializers.ValidationError(
                        {"message": "A plan with this name already exists. Please provide a different name"}
                    )

        if not data["state"].active_status:
            raise serializers.ValidationError("The state is not active.")

        if not data["district"].active_status:
            raise serializers.ValidationError("The district is not active.")

        if not data["block"].active_status:
            raise serializers.ValidationError("The block is not active.")

        return data
