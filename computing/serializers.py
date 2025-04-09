from rest_framework import serializers
from .models import GeneratedLayerInfo
from geoadmin.models import State, District, Block
from users.models import User


class StateSerializer(serializers.ModelSerializer):
    """Serializer for the State model"""
    class Meta:
        model = State
        fields = ['id', 'name']


class DistrictSerializer(serializers.ModelSerializer):
    """Serializer for the District model"""
    class Meta:
        model = District
        fields = ['id', 'name', 'state']


class BlockSerializer(serializers.ModelSerializer):
    """Serializer for the Block model"""
    class Meta:
        model = Block
        fields = ['id', 'name', 'district']


class UserLightSerializer(serializers.ModelSerializer):
    """Lightweight serializer for User model"""
    class Meta:
        model = User
        fields = ['id', 'username', 'email']


class GeneratedLayerInfoSerializer(serializers.ModelSerializer):
    """
    Serializer for the GeneratedLayerInfo model.
    """
    created_by = UserLightSerializer(read_only=True)
    updated_by = UserLightSerializer(read_only=True)
    
    class Meta:
        model = GeneratedLayerInfo
        fields = [
            'id', 'layer_name', 'layer_type', 'state', 'district', 'block',
            'gee_path', 'workspace', 'algorithm', 'version', 'style_name',
            'created_at', 'created_by', 'updated_at', 'updated_by', 'misc'
        ]


class GeneratedLayerInfoCreateSerializer(serializers.ModelSerializer):
    """
    Serializer for creating a new GeneratedLayerInfo instance.
    """
    class Meta:
        model = GeneratedLayerInfo
        fields = [
            'layer_name', 'layer_type', 'state', 'district', 'block',
            'gee_path', 'workspace', 'algorithm', 'version', 'style_name', 'misc'
        ]

    def validate(self, data):
        """
        Validate that the district belongs to the state and block belongs to the district.
        """
        state = data.get('state')
        district = data.get('district')
        block = data.get('block')

        if district and state and district.state != state:
            raise serializers.ValidationError(
                {"district": "District does not belong to the selected state."}
            )

        if block and district and block.district != district:
            raise serializers.ValidationError(
                {"block": "Block does not belong to the selected district."}
            )

        return data


class GeneratedLayerInfoDetailSerializer(serializers.ModelSerializer):
    """
    Detailed serializer for the GeneratedLayerInfo model with nested related objects.
    """
    state = StateSerializer(read_only=True)
    district = DistrictSerializer(read_only=True)
    block = BlockSerializer(read_only=True)
    created_by = UserLightSerializer(read_only=True)
    updated_by = UserLightSerializer(read_only=True)
    
    class Meta:
        model = GeneratedLayerInfo
        fields = [
            'id', 'layer_name', 'layer_type', 'state', 'district', 'block',
            'gee_path', 'workspace', 'algorithm', 'version', 'style_name',
            'created_at', 'created_by', 'updated_at', 'updated_by', 'misc'
        ]


class GeneratedLayerInfoUpdateSerializer(serializers.ModelSerializer):
    """
    Serializer for updating an existing GeneratedLayerInfo instance.
    """
    class Meta:
        model = GeneratedLayerInfo
        fields = [
            'layer_name', 'layer_type', 'state', 'district', 'block',
            'gee_path', 'workspace', 'algorithm', 'version', 'style_name', 'misc'
        ]
        
    def validate(self, data):
        """
        Validate that the district belongs to the state and block belongs to the district.
        """
        state = data.get('state')
        district = data.get('district')
        block = data.get('block')

        # If not updating all fields, get the current values
        if state is None:
            state = self.instance.state
        if district is None:
            district = self.instance.district
        if block is None:
            block = self.instance.block

        if district and state and district.state != state:
            raise serializers.ValidationError(
                {"district": "District does not belong to the selected state."}
            )

        if block and district and block.district != district:
            raise serializers.ValidationError(
                {"block": "Block does not belong to the selected district."}
            )

        return data


class GeneratedLayerInfoListSerializer(serializers.ModelSerializer):
    """
    Simplified serializer for listing GeneratedLayerInfo instances.
    """
    state_name = serializers.CharField(source='state.name', read_only=True)
    district_name = serializers.CharField(source='district.name', read_only=True)
    block_name = serializers.CharField(source='block.name', read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)
    
    class Meta:
        model = GeneratedLayerInfo
        fields = [
            'id', 'layer_name', 'layer_type', 'state_name', 'district_name', 'block_name',
            'algorithm', 'version', 'created_at', 'created_by_username'
        ]