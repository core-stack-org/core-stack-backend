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

        # Get project
        try:
            project = Project.objects.get(id=project_id, app_type=AppType.WATERBODY_REJ)
        except Project.DoesNotExist:
            return Response(
                {"detail": "Project not found or not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Determine if re-upload is allowed
        allow_reupload = request.data.get("allow_reupload", True)
        # Default behavior:
        # - force_regenerate=True (default): delete all points and recreate
        # - force_regenerate=False: keep existing points and add only new ones
        def parse_bool(val, default=True):
            if val is None:
                return default
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                return bool(val)
            if isinstance(val, str):
                return val.strip().lower() in ("true", "1", "yes", "y")
            return default

        # Get uploaded files
        files = []
        if "file" in request.FILES:
            files.append(request.FILES["file"])
        if "files" in request.FILES:
            files.extend(request.FILES.getlist("files"))

        if not files:
            return Response(
                {"detail": "No files were uploaded."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        created_files = []
        errors = []
        force_regenerate = parse_bool(
            request.data.get("force_regenerate", request.data.get("force_regeneration", True)),
            default=True,
        )

        for uploaded_file in files:
            # Validate file extension
            if not uploaded_file.name.lower().endswith(".xlsx"):
                errors.append(
                    f"File '{uploaded_file.name}' is not an Excel file. Only .xlsx allowed."
                )
                continue

            # Calculate file hash
            uploaded_file.seek(0)
            file_hash = hashlib.md5()
            for chunk in uploaded_file.chunks():
                file_hash.update(chunk)
            excel_hash = file_hash.hexdigest()
            print(f"Hash for {uploaded_file.name}: {excel_hash}")

            # Check for duplicates
            existing_file = WaterbodiesFileUploadLog.objects.filter(
                project=project, excel_hash=excel_hash
            ).first()
            if existing_file:
                if allow_reupload:
                    # Delete old file to allow re-upload
                    existing_file.delete()
                    print(f"Deleted previous upload for {uploaded_file.name}")
                else:
                    errors.append(
                        f"File '{uploaded_file.name}' has already been uploaded."
                    )
                    continue

            # Prepare serializer data
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

                uploaded_file.seek(0)
                if serializer.is_valid():
                    excel_file = serializer.save(
                        project=project,
                        uploaded_by=request.user,
                        excel_hash=excel_hash,
                    )
                    created_files.append(serializer.data)

                    # Celery trigger is handled here so we can pass `force_regenerate`
                    # without requiring a DB column.
                    if excel_file.is_compute:
                        from .tasks import Upload_Desilting_Points

                        Upload_Desilting_Points.apply_async(
                            kwargs={
                                "file_obj_id": excel_file.id,
                                "gee_account_id": excel_file.gee_account_id,
                                "is_lulc_required": excel_file.is_lulc_required,
                                "is_processing_required": excel_file.is_processing_required,
                                "is_closest_wp": excel_file.is_closest_wp,
                                "is_force_regeneration": force_regenerate,
                            },
                            queue="waterbody1",
                        )
                else:
                    errors.append(
                        f"Error validating file '{uploaded_file.name}': {serializer.errors}"
                    )
            except IntegrityError as e:
                errors.append(f"Error saving file '{uploaded_file.name}': {str(e)}")
                continue

        # Prepare response
        response_data = {"files_created": len(created_files), "files": created_files}
        if errors:
            response_data["errors"] = errors

        status_code = (
            status.HTTP_201_CREATED if created_files else status.HTTP_400_BAD_REQUEST
        )
        return Response(response_data, status=status_code)


# Import your merging function (adjust path as needed)
