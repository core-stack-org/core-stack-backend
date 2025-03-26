import os
import hashlib
from django.conf import settings
from django.shortcuts import get_object_or_404
from django.db import IntegrityError
from rest_framework import viewsets, permissions, status
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser, FileUploadParser
from projects.models import Project, AppType
from users.permissions import IsOrganizationMember, HasProjectPermission
from utilities.gee_utils import valid_gee_text
from .models import KMLFile
from .serializers import KMLFileSerializer, KMLFileDetailSerializer
from .utils.kml_converter import convert_kml_to_geojson, merge_geojson_files


class KMLFileViewSet(viewsets.ModelViewSet):
    """ViewSet for KML file operations"""

    serializer_class = KMLFileSerializer
    permission_classes = [permissions.IsAuthenticated, HasProjectPermission]
    parser_classes = [MultiPartParser, FormParser, FileUploadParser]
    # For the HasProjectPermission to work correctly
    app_type = AppType.PLANTATION

    def get_queryset(self):
        """Filter KML files by project"""
        project_id = self.kwargs.get("project_pk")
        if project_id:
            # Get the plantation project
            try:
                project = Project.objects.get(
                    id=project_id, app_type=AppType.PLANTATION, enabled=True
                )
                return KMLFile.objects.filter(project=project)
            except Project.DoesNotExist:
                return KMLFile.objects.none()
        return KMLFile.objects.none()

    def get_serializer_class(self):
        if self.action == "retrieve":
            return KMLFileDetailSerializer
        return KMLFileSerializer

    def create(self, request, *args, **kwargs):
        """Create new KML files - supports both single and multiple file uploads"""
        project_id = self.kwargs.get("project_pk")
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get project and check if it's a plantation project and enabled
        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.PLANTATION, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"detail": "Plantation project not found or not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if we have files in the request
        files = []

        # Handle single file upload case
        if "file" in request.FILES:
            files.append(request.FILES["file"])

        # Handle multiple files upload case
        if "files[]" in request.FILES:
            print("Found multiple files with 'files[]'")
            file_list = request.FILES.getlist("files[]")
            print(f"Number of files in 'files[]': {len(file_list)}")
            for f in file_list:
                print(f"  - {f.name}")
            files.extend(file_list)

        if not files:
            return Response(
                {"detail": "No files were uploaded."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Process each file
        created_files = []
        errors = []

        for uploaded_file in files:
            # Validate file extension
            if not uploaded_file.name.lower().endswith(".kml"):
                errors.append(
                    f"File '{uploaded_file.name}' is not a KML file. Only KML files are allowed."
                )
                continue

            # Calculate file hash to check for duplicates
            uploaded_file.seek(0)
            file_hash = hashlib.md5()
            for chunk in uploaded_file.chunks():
                file_hash.update(chunk)
            kml_hash = file_hash.hexdigest()

            # Check if file with same hash already exists
            if KMLFile.objects.filter(kml_hash=kml_hash).exists():
                errors.append(f"File '{uploaded_file.name}' has already been uploaded.")
                continue

            # Prepare data for serializer
            data = {
                "name": request.data.get("name", valid_gee_text(uploaded_file.name)),
                "file": uploaded_file,
                "project": project.id,
            }

            serializer = self.get_serializer(data=data)

            try:
                if serializer.is_valid():
                    # Save KML file
                    kml_file = serializer.save(
                        project=project,
                        uploaded_by=request.user,
                        kml_hash=kml_hash,
                    )

                    # # Convert KML to GeoJSON
                    # file_path = kml_file.file.path
                    # geojson_data = convert_kml_to_geojson(file_path)
                    #
                    # if geojson_data:
                    #     # Update GeoJSON data in the model
                    #     kml_file.geojson_data = geojson_data
                    #     kml_file.save(update_fields=["geojson_data"])

                    created_files.append(serializer.data)
                else:
                    errors.append(
                        f"Error validating file '{uploaded_file.name}': {serializer.errors}"
                    )
            except IntegrityError as e:
                errors.append(f"Error saving file '{uploaded_file.name}': {str(e)}")
                continue

        # # Update the merged GeoJSON file for the project if any files were created
        # if created_files:
        #     self.update_project_geojson(project)

        # Prepare response
        response_data = {"files_created": len(created_files), "files": created_files}

        if errors:
            response_data["errors"] = errors

        # Return 201 if at least one file was created, otherwise 400
        status_code = (
            status.HTTP_201_CREATED if created_files else status.HTTP_400_BAD_REQUEST
        )

        return Response(response_data, status=status_code)

    def perform_destroy(self, instance):
        """Override destroy to update project GeoJSON after deletion"""
        project = instance.project
        super().perform_destroy(instance)
        self.update_project_geojson(project)

    def update_project_geojson(self, project):
        """Update the merged GeoJSON file for a project"""
        try:
            # Get all KML files for this project
            kml_files = KMLFile.objects.filter(project=project)

            # Collect GeoJSON data
            geojson_list = [
                kml.geojson_data
                for kml in kml_files
                if hasattr(kml, "geojson_data") and kml.geojson_data
            ]

            if geojson_list:
                # Create directory if it doesn't exist
                directory = "saytrees/geojson"
                full_path = os.path.join(settings.MEDIA_ROOT, directory)
                os.makedirs(full_path, exist_ok=True)

                # Create merged GeoJSON
                output_path = f"{full_path}/project_{project.id}.geojson"
                if merge_geojson_files(geojson_list, output_path):
                    # Update project geojson_path
                    relative_path = f"{directory}/project_{project.id}.geojson"
                    project.geojson_path = relative_path
                    project.save(update_fields=["geojson_path"])
                    return True
            else:
                # No KML files, clear the geojson_path
                project.geojson_path = None
                project.save(update_fields=["geojson_path"])
        except Exception as e:
            print(f"Error updating project GeoJSON: {str(e)}")
            return False
