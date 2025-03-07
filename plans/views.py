# plans/views.py
from django.shortcuts import get_object_or_404
from rest_framework import viewsets, permissions, status
from rest_framework.response import Response
from projects.models import Project, ProjectApp, AppType
from users.permissions import IsOrganizationMember, HasProjectPermission
from .models import Plan
from .serializers import PlanSerializer, PlanCreateSerializer


class PlanViewSet(viewsets.ModelViewSet):
    """
    ViewSet for watershed planning operations
    """
    serializer_class = PlanSerializer
    permission_classes = [permissions.IsAuthenticated, HasProjectPermission]
    # For the HasProjectPermission to work correctly
    app_type = AppType.WATERSHED
    
    def get_queryset(self):
        """
        Filter plans by project
        """
        project_id = self.kwargs.get('project_pk')
        if project_id:
            # Get the watershed app for this project
            try:
                project_app = ProjectApp.objects.get(
                    project_id=project_id,
                    app_type=AppType.WATERSHED,
                    enabled=True
                )
                return Plan.objects.filter(project_app=project_app)
            except ProjectApp.DoesNotExist:
                return Plan.objects.none()
        return Plan.objects.none()
    
    def get_serializer_class(self):
        """
        Use different serializers based on the action
        """
        if self.action in ['create']:
            return PlanCreateSerializer
        return PlanSerializer
    
    def create(self, request, *args, **kwargs):
        """
        Create a new watershed plan
        """
        project_id = self.kwargs.get('project_pk')
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get project and check if watershed app is enabled
        project = get_object_or_404(Project, id=project_id)
        try:
            project_app = ProjectApp.objects.get(
                project=project,
                app_type=AppType.WATERSHED,
                enabled=True
            )
        except ProjectApp.DoesNotExist:
            return Response(
                {"detail": "Watershed planning app is not enabled for this project."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        # Add project_app, organization, and created_by
        plan = serializer.save(
            project_app=project_app,
            organization=project.organization,
            created_by=request.user
        )
        
        # Use the full serializer for response
        return Response(
            PlanSerializer(plan).data,
            status=status.HTTP_201_CREATED
        )
    
    def update(self, request, *args, **kwargs):
        """
        Update a watershed plan
        """
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        
        # Use PlanCreateSerializer for validation
        create_serializer = PlanCreateSerializer(
            instance, 
            data=request.data, 
            partial=partial
        )
        create_serializer.is_valid(raise_exception=True)
        
        # Save the instance with validated data
        updated_instance = create_serializer.save()
        
        # Use PlanSerializer for response
        return Response(PlanSerializer(updated_instance).data)
    
    def perform_destroy(self, instance):
        """
        Delete a watershed plan
        """
        instance.delete()