import os
import hashlib
from django.conf import settings
from django.shortcuts import get_object_or_404
from django.db import IntegrityError
from rest_framework import viewsets, permissions, status
from rest_framework.response import Response
from rest_framework.decorators import action
from projects.models import Project, ProjectApp, AppType
from users.permissions import IsOrganizationMember, HasProjectPermission
from .models import KMLFile
from .serializers import KMLFileSerializer, KMLFileDetailSerializer
from .utils.kml_converter import convert_kml_to_geojson, merge_geojson_files


class KMLFileViewSet(viewsets.ModelViewSet):
    """ViewSet for KML file operations"""
    serializer_class = KMLFileSerializer
    permission_classes = [permissions.IsAuthenticated, HasProjectPermission]
    # For the HasProjectPermission to work correctly
    app_type = AppType.PLANTATION
    
    def get_queryset(self):
        """Filter KML files by project"""
        project_id = self.kwargs.get('project_pk')
        if project_id:
            # Get the plantation app for this project
            try:
                project_app = ProjectApp.objects.get(
                    project_id=project_id,
                    app_type=AppType.PLANTATION,
                    enabled=True
                )
                return KMLFile.objects.filter(project_app=project_app)
            except ProjectApp.DoesNotExist:
                return KMLFile.objects.none()
        return KMLFile.objects.none()
    
    def get_serializer_class(self):
        if self.action == 'retrieve':
            return KMLFileDetailSerializer
        return KMLFileSerializer
    
    def create(self, request, *args, **kwargs):
        """Create a new KML file"""
        project_id = self.kwargs.get('project_pk')
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get project and check if plantation app is enabled
        project = get_object_or_404(Project, id=project_id)
        try:
            project_app = ProjectApp.objects.get(
                project=project,
                app_type=AppType.PLANTATION,
                enabled=True
            )
        except ProjectApp.DoesNotExist:
            return Response(
                {"detail": "Plantation app is not enabled for this project."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get uploaded file
        uploaded_file = request.FILES.get('file')
        if not uploaded_file:
            return Response(
                {"detail": "No file was uploaded."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Validate file extension
        if not uploaded_file.name.lower().endswith('.kml'):
            return Response(
                {"detail": "Only KML files are allowed."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Calculate file hash to check for duplicates
        uploaded_file.seek(0)
        file_hash = hashlib.sha256()
        for chunk in uploaded_file.chunks():
            file_hash.update(chunk)
        kml_hash = file_hash.hexdigest()
        
        # Check if file with same hash already exists
        if KMLFile.objects.filter(kml_hash=kml_hash).exists():
            return Response(
                {"detail": "This KML file has already been uploaded."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Prepare data for serializer
        data = {
            'name': request.data.get('name', uploaded_file.name),
            'file': uploaded_file,
            'project_app': project_app.id
        }
        
        serializer = self.get_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        
        try:
            # Save KML file
            kml_file = serializer.save(
                project_app=project_app,
                uploaded_by=request.user,
                kml_hash=kml_hash
            )
            
            # Convert KML to GeoJSON
            file_path = kml_file.file.path
            geojson_data = convert_kml_to_geojson(file_path)
            
            if geojson_data:
                # Update GeoJSON data in the model
                kml_file.geojson_data = geojson_data
                kml_file.save(update_fields=['geojson_data'])
                
                # Update the merged GeoJSON file for the project
                self.update_project_geojson(project)
            
            return Response(
                serializer.data,
                status=status.HTTP_201_CREATED
            )
            
        except IntegrityError:
            return Response(
                {"detail": "Error saving KML file."},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    def perform_destroy(self, instance):
        """Override destroy to update project GeoJSON after deletion"""
        project = instance.project_app.project
        super().perform_destroy(instance)
        self.update_project_geojson(project)
    
    def update_project_geojson(self, project):
        """Update the merged GeoJSON file for a project"""
        try:
            # Get all KML files for this project's plantation app
            project_app = ProjectApp.objects.get(
                project=project,
                app_type=AppType.PLANTATION,
                enabled=True
            )
            
            kml_files = KMLFile.objects.filter(project_app=project_app)
            
            # Collect GeoJSON data
            geojson_list = [kml.geojson_data for kml in kml_files if kml.geojson_data]
            
            if geojson_list:
                # Create directory if it doesn't exist
                directory = 'saytrees/geojson'
                full_path = os.path.join(settings.MEDIA_ROOT, directory)
                os.makedirs(full_path, exist_ok=True)
                
                # Create merged GeoJSON
                output_path = f"{full_path}/project_{project.id}.geojson"
                if merge_geojson_files(geojson_list, output_path):
                    # Update project geojson_path
                    relative_path = f"{directory}/project_{project.id}.geojson"
                    project.geojson_path = relative_path
                    project.save(update_fields=['geojson_path'])
                    return True
            else:
                # No KML files, clear the geojson_path
                project.geojson_path = None
                project.save(update_fields=['geojson_path'])
            
            return False
        
        except Exception as e:
            print(f"Error updating project GeoJSON: {str(e)}")
            return False