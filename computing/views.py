from django.shortcuts import render
from rest_framework import viewsets, permissions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import GeneratedLayerInfo
from geoadmin.models import State, District, Block
from users.models import User
from .serializers import (
    GeneratedLayerInfoSerializer,
    GeneratedLayerInfoCreateSerializer,
    GeneratedLayerInfoDetailSerializer,
    GeneratedLayerInfoUpdateSerializer,
    GeneratedLayerInfoListSerializer
)

class IsLayerCreatorOrAdmin(permissions.BasePermission):
    """
    Permission class that allows only the creator of the layer or admins to access the endpoint.
    """
    def has_object_permission(self, request, view, obj):
        return (request.user.is_authenticated and (request.user.is_superuser or request.user == obj.created_by))


class GeneratedLayerInfoViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows generated layers to be viewed or edited.
    """
    queryset = GeneratedLayerInfo.objects.all()
    serializer_class = GeneratedLayerInfoSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_serializer_class(self):
        """
        Return appropriate serializer class based on the action.
        """
        if self.action == 'create':
            return GeneratedLayerInfoCreateSerializer
        elif self.action == 'retrieve':
            return GeneratedLayerInfoDetailSerializer
        elif self.action in ['update', 'partial_update']:
            return GeneratedLayerInfoUpdateSerializer
        elif self.action == 'list':
            return GeneratedLayerInfoListSerializer
        return GeneratedLayerInfoSerializer
        
    def perform_create(self, serializer):
        """
        Associate the current user with the layer being created.
        """
        serializer.save(created_by=self.request.user, updated_by=self.request.user)
    
    def perform_update(self, serializer):
        """
        Associate the current user as the updater of the layer.
        """
        serializer.save(updated_by=self.request.user)

    @action(detail=False, methods=['get'])
    def by_state(self, request):
        """
        Filter layers by state.
        """
        state_id = request.query_params.get('state_id', None)
        if state_id is not None:
            layers = self.get_queryset().filter(state_id=state_id)
            serializer = self.get_serializer(layers, many=True)
            return Response(serializer.data)
        return Response({"error": "State ID is required"}, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'])
    def by_district(self, request):
        """
        Filter layers by district.
        """
        district_id = request.query_params.get('district_id', None)
        if district_id is not None:
            layers = self.get_queryset().filter(district_id=district_id)
            serializer = self.get_serializer(layers, many=True)
            return Response(serializer.data)
        return Response({"error": "District ID is required"}, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'])
    def by_block(self, request):
        """
        Filter layers by block.
        """
        block_id = request.query_params.get('block_id', None)
        if block_id is not None:
            layers = self.get_queryset().filter(block_id=block_id)
            serializer = self.get_serializer(layers, many=True)
            return Response(serializer.data)
        return Response({"error": "Block ID is required"}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['get'])
    def by_algorithm(self, request):
        """
        Filter layers by algorithm.
        """
        algorithm = request.query_params.get('algorithm', None)
        if algorithm is not None:
            layers = self.get_queryset().filter(algorithm=algorithm)
            serializer = self.get_serializer(layers, many=True)
            return Response(serializer.data)
        return Response({"error": "Algorithm is required"}, status=status.HTTP_400_BAD_REQUEST)


def create_generated_layer(data, user_id):
    """
    Function to create a new generated layer.
    Assumes `state`, `district`, and `block` are already model instances.
    """
    user = User.objects.get(id=user_id)
    try:
        # 🔹 Create the GeneratedLayerInfo entry
        new_layer = GeneratedLayerInfo.objects.create(
            layer_name=data.get("layer_name"),
            layer_type=data.get("layer_type"),
            state=data.get("state"),
            district=data.get("district"),
            block=data.get("block"),
            gee_path=data.get("gee_path", None),
            workspace=data.get("workspace", ""),
            algorithm=data.get("algorithm"),
            version=data.get("version"),
            style_name=data.get("style_name"),
            created_by=user,
            updated_by=user,
            misc=data.get("misc", {}),
        )

        return new_layer  # Return the created instance

    except Exception as e:
        raise ValueError(f"Error creating generated layer: {e}")


def update_generated_layer(layer_id, data, user):
    """
    Function to update an existing generated layer.
    """
    try:
        layer = GeneratedLayerInfo.objects.get(id=layer_id)
        for field, value in data.items():
            setattr(layer, field, value)
        layer.updated_by = user
        layer.save()
        return layer
    except GeneratedLayerInfo.DoesNotExist:
        raise ValueError("Generated Layer with the given ID does not exist")

