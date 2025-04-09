from django.shortcuts import render
from django.core.exceptions import ObjectDoesNotExist
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


def create_dataset_for_generated_layer(state_name, district_name, block_name, layer_name, user, gee_path=None, layer_type=None, workspace=None, algorithm=None, version=None, style_name=None, misc=None):
    """
    Function to create a dataset for new generated layer.
    """
    try:
        # Assuming `user` is already a `User` instance. If it's an ID, fetch the User
        if isinstance(user, int):
            user = User.objects.get(id=user)  # Fetch user if ID is passed

        # Lookup State, District, and Block
        state = State.objects.get(state_name=state_name)
        
        if district_name:
            district = District.objects.get(district_name=district_name, state_id=state.state_census_code)
        else:
            raise ValueError("District name is required if state is provided.")
        
        if block_name:
            block = Block.objects.get(block_name=block_name, district_id=district.id)
        else:
            raise ValueError("Block name is required if district is provided.")

        # Create the GeneratedLayerInfo entry
        new_layer = GeneratedLayerInfo.objects.create(
            layer_name=layer_name,
            layer_type=layer_type,
            state=state,
            district=district,
            block=block,
            gee_path=gee_path,
            workspace=workspace,
            algorithm=algorithm,
            version=version,
            style_name=style_name,
            created_by=user.username,
            updated_by=user.username,
            misc=misc if misc is not None else {},
        )

        return new_layer

    except ObjectDoesNotExist as e:
        raise ValueError(f"Error creating generated layer: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error: {str(e)}")


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

