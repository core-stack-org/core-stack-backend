from rest_framework import serializers

from .models import GEEAccount


class GeeAccountSerializers(serializers.ModelSerializer):
    """Serializer for user details."""

    class Meta:
        model = GEEAccount
        fields = [
            "id",
            "name"

        ]
        read_only_fields = ["id", "name"]
