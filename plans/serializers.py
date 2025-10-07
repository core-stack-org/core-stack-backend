from rest_framework import serializers

from .models import Plan, PlanApp


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

    project_name = serializers.SerializerMethodField()
    organization_name = serializers.SerializerMethodField()
    created_by_name = serializers.SerializerMethodField()

    class Meta:
        model = PlanApp
        fields = [
            "id",
            "plan",
            "state",
            "district",
            "block",
            "village_name",
            "gram_panchayat",
            "facilitator_name",
            "organization",
            "organization_name",
            "project",
            "project_name",
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

    def get_project_name(self, obj):
        """
        Get the project name
        """
        if obj.project:
            return obj.project.name
        return None

    def get_organization_name(self, obj):
        """
        Get the organization name
        """
        if obj.organization:
            return obj.organization.name
        return None

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
        fields = [
            "plan",
            "state",
            "district",
            "block",
            "village_name",
            "gram_panchayat",
            "facilitator_name",
            # Optional fields
            "enabled",
            "is_completed",
            "is_dpr_generated",
            "is_dpr_reviewed",
            "is_dpr_approved",
        ]

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
            "facilitator_name",
        ]
        for field in required_fields:
            if field not in data or not data[field]:
                raise serializers.ValidationError(f"{field} is required")

        request = self.context.get("request")
        if request and request.parser_context.get("kwargs"):
            project_id = request.parser_context["kwargs"].get("project_pk")
            if project_id and data.get("plan"):
                existing_plan = PlanApp.objects.filter(
                    project_id=project_id, plan=data["plan"]
                ).exists()

                if existing_plan:
                    raise serializers.ValidationError(
                        {
                            "message": "A plan with this name already exists. Please provide a different name"
                        }
                    )

        if not data["state"].active_status:
            raise serializers.ValidationError("The state is not active.")

        if not data["district"].active_status:
            raise serializers.ValidationError("The district is not active.")

        if not data["block"].active_status:
            raise serializers.ValidationError("The block is not active.")

        return data


class PlanUpdateSerializer(serializers.ModelSerializer):
    """
    Serializer for updating watershed plans
    Allows updating all plan fields except auto-generated ones
    """

    class Meta:
        model = PlanApp
        fields = [
            "plan",
            "state",
            "district",
            "block",
            "village_name",
            "gram_panchayat",
            "facilitator_name",
            "enabled",
            "is_completed",
            "is_dpr_generated",
            "is_dpr_reviewed",
            "is_dpr_approved",
        ]

    def validate(self, data):
        if "plan" in data:
            instance = getattr(self, "instance", None)
            if instance:
                project = instance.project
                if project:
                    existing_plan = (
                        PlanApp.objects.filter(project=project, plan=data["plab"])
                        .exclude(id=instance.id)
                        .exists()
                    )

                    if existing_plan:
                        raise serializers.ValidationError(
                            {
                                "plan": "A plan with the same name already exists in this project"
                            }
                        )

        if "state" in data and not data["state"].active_status:
            raise serializers.ValidationError("The state is not active.")
        if "district" in data and not data["district"].active_status:
            raise serializers.ValidationError("The district is not active.")
        if "block" in data and not data["block"].active_status:
            raise serializers.ValidationError("The block is not active.")

        return data

    def update(self, instance, validated_data):
        """
        Update the plan instance and set updated_by
        """
        request = self.context.get("request")
        if request and hasattr(request, "user"):
            instance.updated_by = request.user

        return super().update(instance, validated_data)
