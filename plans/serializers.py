from rest_framework import serializers

from .models import Plan
from geoadmin.models import State, District, Block


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
