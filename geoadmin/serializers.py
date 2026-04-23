from rest_framework import serializers

from .models import DistrictSOI, StateSOI, TehsilSOI


class StateSerializer(serializers.ModelSerializer):
    class Meta:
        model = StateSOI
        fields = "__all__"


class DistrictSerializer(serializers.ModelSerializer):
    class Meta:
        model = DistrictSOI
        fields = "__all__"


class BlockSerializer(serializers.ModelSerializer):
    class Meta:
        model = TehsilSOI
        fields = "__all__"
