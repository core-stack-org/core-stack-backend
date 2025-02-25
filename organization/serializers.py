from rest_framework import serializers
from .models import Organization
from .schemas import OrganizationCreate, OrganizationRead

class PydanticModelSerializer(serializers.ModelSerializer):
    """
    A custom ModelSerializer that uses Pydantic for validation
    """
    def to_representation(self, instance):
        # Convert model instance to Pydantic model
        pydantic_model = OrganizationRead.from_orm(instance)
        return pydantic_model.model_dump()

    def to_internal_value(self, data):
        # Validate input data using Pydantic
        validated_data = OrganizationCreate(**data)
        return validated_data.model_dump()

class OrganizationSerializer(PydanticModelSerializer):
    class Meta:
        model = Organization
        fields = '__all__'