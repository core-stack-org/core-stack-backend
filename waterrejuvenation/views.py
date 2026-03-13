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
from .models import WaterbodiesFileUploadLog
from .serializers import ExcelFileSerializer
from rest_framework.views import APIView
from .utils import get_merged_waterbodies_with_zoi
import pandas as pd
from .utils import validate_excel_headers, EXPECTED_EXCEL_HEADERS


class WaterRejExcelFileViewSet(viewsets.ModelViewSet):
    """ViewSet for KML file operations"""

    serializer_class = ExcelFileSerializer
    # permission_classes = [permissions.IsAuthenticated, HasProjectPermission]
    parser_classes = [MultiPartParser, FormParser, FileUploadParser]
    # For the HasProjectPermission to work correctly
    app_type = AppType.WATERBODY_REJ
    schema = None

    def get_queryset(self):
        """Filter Excel files by project"""
        project_id = self.kwargs.get("project_pk")
        if project_id:
            # Get the plantation project
            try:
                project = Project.objects.get(
                    id=project_id, app_type=AppType.WATERBODY_REJ, enabled=True
                )
                return WaterbodiesFileUploadLog.objects.filter(project=project)
            except Project.DoesNotExist:
                return WaterbodiesFileUploadLog.objects.none()
        return WaterbodiesFileUploadLog.objects.none()

    def create(self, request, *args, **kwargs):
        print("inside create api")
        print(request.data)
        """Create new excel files - supports both single and multiple file uploads"""
        project_id = self.kwargs.get("project_pk")
        print("Project: " + str(project_id))
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get project and check if it's a plantation project and enabled
        try:
            project = Project.objects.get(id=project_id, app_type=AppType.WATERBODY_REJ)
        except Project.DoesNotExist:
            return Response(
                {"detail": "Plantation project not found or not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if we have files in the request
        files = []
        print(request.FILES)
        # Handle single file upload case
        if "file" in request.FILES:
            files.append(request.FILES["file"])
        # Handle multiple files upload case
        if "files" in request.FILES:
            print("Found multiple files with 'files[]'")
            file_list = request.FILES.getlist("files")
            print(f"Number of files in 'files[]': {len(file_list)}")
            for f in file_list:
                print(f"  - {f.name}")
            files.extend(file_list)
        print(files)
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
            if not uploaded_file.name.lower().endswith(".xlsx"):
                errors.append(
                    f"File '{uploaded_file.name}' is not a KML file. Only KML files are allowed."
                )
                continue

            # Calculate file hash to check for duplicates
            uploaded_file.seek(0)
            file_hash = hashlib.md5()
            for chunk in uploaded_file.chunks():
                file_hash.update(chunk)
            excel_hash = file_hash.hexdigest()
            print(excel_hash)
            # Check if file with same hash already exists
            if WaterbodiesFileUploadLog.objects.filter(
                project=project, excel_hash=excel_hash
            ).exists():
                errors.append(f"File '{uploaded_file.name}' has already been uploaded.")
                continue

            # Prepare data for serializer
            data = {
                "name": request.data.get("name", valid_gee_text(uploaded_file.name)),
                "file": uploaded_file,
                "project": project.id,
                "gee_account_id": request.data.get("gee_account_id"),
                "is_lulc_required": request.data.get("is_lulc_required", True),
                "is_processing_required": request.data.get(
                    "is_processing_required", True
                ),
                "is_closest_wp": request.data.get("is_closest_wp", True),
                "is_compute": request.data.get("is_compute", False),
            }
            print(data)
            serializer = self.get_serializer(data=data)

            try:
                uploaded_file.seek(0)

                is_valid, error_msg = validate_excel_headers(
                    uploaded_file, EXPECTED_EXCEL_HEADERS
                )

                if not is_valid:
                    errors.append(
                        f"File '{uploaded_file.name}' format error: {error_msg}"
                    )
                    continue

                # VERY IMPORTANT
                uploaded_file.seek(0)
                if serializer.is_valid():
                    print("inside serailizer sv")
                    # Save excel file
                    excel_file = serializer.save(
                        project=project,
                        uploaded_by=request.user,
                        excel_hash=excel_hash,
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
                    print(created_files)
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


# Import your merging function (adjust path as needed)
