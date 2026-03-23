import io

import pandas as pd
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from organization.models import Organization
from projects.models import AppType, Project
from rest_framework.test import APITestCase

from users.models import User
from waterrejuvenation.models import WaterbodiesFileUploadLog

class WaterRejExcelUploadCreateApiTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="test_user",
            email="test_user@example.com",
            password="test_password",
        )
        self.org = Organization.objects.create(name="TestOrg")
        self.user.organization = self.org
        self.user.save(update_fields=["organization"])

        self.project = Project.objects.create(
            name="TestProject",
            organization=self.org,
            app_type=AppType.WATERBODY_REJ,
            enabled=True,
            created_by=self.user,
            updated_by=self.user,
        )

        self.client.force_authenticate(user=self.user)

    def _make_valid_excel_bytes(self):
        # Column names must match what's used in:
        # - waterrejuvenation/utils.py -> EXPECTED_EXCEL_HEADERS (case-insensitive)
        # - waterrejuvenation/tasks.py -> row.get("Name of NGO"), etc (exact casing)
        df = pd.DataFrame(
            [
                {
                    "Sr No.": 1,
                    "Name of NGO": "NGO A",
                    "State": "State A",
                    "District": "District A",
                    "Taluka": "Taluka A",
                    "Village": "Village A",
                    "Name of the waterbody ": "Waterbody A",
                    "Latitude": 19.123,
                    "Longitude": 73.567,
                    "Silt Excavated as per App": "10",
                    "Intervention_year": "2017",
                }
            ]
        )

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        buf.seek(0)
        return buf.read()

    def test_create_upload_single_file_force_regenerate_false(self):
        excel_bytes = self._make_valid_excel_bytes()
        uploaded = SimpleUploadedFile(
            "waterrejuvenation_test.xlsx",
            excel_bytes,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        url = f"/api/v1/projects/{self.project.id}/waterrejuvenation/excel/"
        payload = {
            "file": uploaded,
            "gee_account_id": 1,
            "is_lulc_required": True,
            "is_processing_required": True,
            "is_closest_wp": True,
            "is_compute": False,  # do not trigger Celery in unit test
            "force_regenerate": False,
        }

        resp = self.client.post(url, data=payload, format="multipart")
        self.assertEqual(resp.status_code, 201, resp.content)

        self.assertEqual(resp.data["files_created"], 1)
        self.assertTrue(
            WaterbodiesFileUploadLog.objects.filter(project=self.project).exists()
        )
