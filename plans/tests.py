# plans/tests.py
import csv
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase, APIClient
from rest_framework import status

from dpr.models import ODK_settlement
from .models import Plan
from .utils import fetch_db_data
from projects.models import Project, AppType
from organization.models import Organization
from users.models import User, UserProjectGroup
from django.contrib.auth.models import Group, Permission


class PlanModelTest(TestCase):
    def setUp(self):
        # Create organization
        self.organization = Organization.objects.create(name="Test Organization")

        # Create project with app_type
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
            app_type=AppType.WATERSHED,
            enabled=True,
        )

        # Create user
        self.user = User.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="password123",
            organization=self.organization,
        )

    def test_plan_creation(self):
        plan = Plan.objects.create(
            name="Test Watershed Plan",
            project=self.project,
            organization=self.organization,
            state="Test State",
            district="Test District",
            block="Test Block",
            village="Test Village",
            gram_panchayat="Test GP",
            created_by=self.user,
        )

        # Check model attributes
        self.assertEqual(plan.name, "Test Watershed Plan")
        self.assertEqual(plan.project, self.project)
        self.assertEqual(plan.organization, self.organization)
        self.assertEqual(plan.state, "Test State")
        self.assertEqual(plan.district, "Test District")
        self.assertEqual(plan.created_by, self.user)


class PlanAPITest(APITestCase):
    def setUp(self):
        self.client = APIClient()

        # Create organization
        self.organization = Organization.objects.create(name="Test Organization")

        # Create project with app_type
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
            app_type=AppType.WATERSHED,
            enabled=True,
        )

        # Create admin user
        self.admin_user = User.objects.create_user(
            username="admin",
            email="admin@example.com",
            password="password123",
            organization=self.organization,
            is_superadmin=True,
        )

        # Create edit user
        self.edit_user = User.objects.create_user(
            username="editor",
            email="editor@example.com",
            password="password123",
            organization=self.organization,
        )

        # Create view user
        self.view_user = User.objects.create_user(
            username="viewer",
            email="viewer@example.com",
            password="password123",
            organization=self.organization,
        )

        # Create groups and permissions
        self.admin_group = Group.objects.create(name="Project Admin")
        self.editor_group = Group.objects.create(name="Project Editor")
        self.viewer_group = Group.objects.create(name="Project Viewer")

        # Create permissions
        Permission.objects.get_or_create(
            codename="view_watershed",
            name="Can view watershed planning data",
            content_type_id=1,  # This would typically be correct content type ID
        )

        Permission.objects.get_or_create(
            codename="add_watershed",
            name="Can add watershed planning data",
            content_type_id=1,
        )

        Permission.objects.get_or_create(
            codename="change_watershed",
            name="Can change watershed planning data",
            content_type_id=1,
        )

        Permission.objects.get_or_create(
            codename="delete_watershed",
            name="Can delete watershed planning data",
            content_type_id=1,
        )

        # Assign permissions to groups
        view_perm = Permission.objects.get(codename="view_watershed")
        add_perm = Permission.objects.get(codename="add_watershed")
        change_perm = Permission.objects.get(codename="change_watershed")
        delete_perm = Permission.objects.get(codename="delete_watershed")

        self.admin_group.permissions.add(view_perm, add_perm, change_perm, delete_perm)
        self.editor_group.permissions.add(view_perm, add_perm, change_perm)
        self.viewer_group.permissions.add(view_perm)

        # Assign users to project roles
        UserProjectGroup.objects.create(
            user=self.edit_user, project=self.project, group=self.editor_group
        )

        UserProjectGroup.objects.create(
            user=self.view_user, project=self.project, group=self.viewer_group
        )

        # Create a test plan
        self.plan = Plan.objects.create(
            name="Test Watershed Plan",
            project=self.project,
            organization=self.organization,
            state="Test State",
            district="Test District",
            block="Test Block",
            village="Test Village",
            gram_panchayat="Test GP",
            created_by=self.admin_user,
        )

        # URLs
        self.plans_list_url = reverse(
            "project-plan-list", kwargs={"project_pk": self.project.pk}
        )
        self.plan_detail_url = reverse(
            "project-plan-detail",
            kwargs={"project_pk": self.project.pk, "pk": self.plan.pk},
        )

    def test_list_plans_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        response = self.client.get(self.plans_list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_list_plans_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        response = self.client.get(self.plans_list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_list_plans_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        response = self.client.get(self.plans_list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_plan_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        data = {
            "name": "New Watershed Plan",
            "state": "New State",
            "district": "New District",
            "block": "New Block",
            "village": "New Village",
            "gram_panchayat": "New GP",
        }

        response = self.client.post(self.plans_list_url, data)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["name"], "New Watershed Plan")
        self.assertEqual(response.data["state"], "New State")

        # Check plan was created in database
        self.assertEqual(Plan.objects.count(), 2)

    def test_create_plan_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        data = {
            "name": "Editor Plan",
            "state": "Editor State",
            "district": "Editor District",
            "block": "Editor Block",
            "village": "Editor Village",
            "gram_panchayat": "Editor GP",
        }

        response = self.client.post(self.plans_list_url, data)

        # Editor should be able to create plans
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["name"], "Editor Plan")

    def test_create_plan_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        data = {
            "name": "Viewer Plan",
            "state": "Viewer State",
            "district": "Viewer District",
            "block": "Viewer Block",
            "village": "Viewer Village",
            "gram_panchayat": "Viewer GP",
        }

        response = self.client.post(self.plans_list_url, data)

        # Viewer should not be able to create plans
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_update_plan_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        data = {
            "name": "Updated Plan",
            "state": "Updated State",
            "district": "Updated District",
            "block": "Updated Block",
            "village": "Updated Village",
            "gram_panchayat": "Updated GP",
        }

        response = self.client.put(self.plan_detail_url, data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["name"], "Updated Plan")

        # Verify database was updated
        updated_plan = Plan.objects.get(pk=self.plan.pk)
        self.assertEqual(updated_plan.name, "Updated Plan")

    def test_update_plan_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        data = {
            "name": "Editor Updated",
            "state": self.plan.state,
            "district": self.plan.district,
            "block": self.plan.block,
            "village": self.plan.village,
            "gram_panchayat": self.plan.gram_panchayat,
        }

        response = self.client.patch(self.plan_detail_url, {"name": "Editor Updated"})

        # Editor should be able to update plans
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["name"], "Editor Updated")

    def test_update_plan_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        data = {"name": "Viewer Updated"}

        response = self.client.patch(self.plan_detail_url, data)

        # Viewer should not be able to update plans
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_plan_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        response = self.client.delete(self.plan_detail_url)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        # Verify plan was deleted
        self.assertEqual(Plan.objects.count(), 0)

    def test_delete_plan_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        response = self.client.delete(self.plan_detail_url)

        # Editor should not be able to delete plans
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        # Verify plan was not deleted
        self.assertEqual(Plan.objects.count(), 1)

    def test_delete_plan_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        response = self.client.delete(self.plan_detail_url)

        # Viewer should not be able to delete plans
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        # Verify plan was not deleted
        self.assertEqual(Plan.objects.count(), 1)


# Minimal valid ODK settlement JSON (same structure as ODK submissions stored in data_settlement)
def _make_settlement_json(plan_id, block_name, settlement_id="SETT001", review_state="hasIssues"):
    return {
        "__id": f"uuid:{settlement_id}",
        "__system": {"reviewState": review_state, "submissionDate": "2024-01-01T00:00:00Z"},
        "block_name": block_name,
        "plan_id": str(plan_id),
        "GPS_point": {
            "point_mapsappearance": {
                "coordinates": [78.5, 20.5]
            }
        },
        "Settlements_id": settlement_id,
        "Settlements_name": "Test Settlement",
        "MNREGA_INFORMATION": {
            "NREGA_aware": 10,
            "NREGA_applied": 5,
            "NREGA_job_card": 3,
            "total_household": 2,
            "NREGA_work_days": 100,
            "q1": "yes",
            "select_one_Y_N": "yes",
            "select_one_demands": "wages",
            "select_multiple_issues": "delayed_payment",
            "select_one_contributions": "labour",
        },
    }


def _create_settlement(plan_id, block_name, settlement_id="SETT001",
                       is_deleted=False, is_moderated=False, review_state="hasIssues",
                       data_override=None):
    data = data_override or _make_settlement_json(plan_id, block_name, settlement_id, review_state)
    return ODK_settlement.objects.create(
        settlement_id=settlement_id,
        settlement_name="Test Settlement",
        submission_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        submitted_by="test_user",
        status_re=review_state,
        latitude=20.5,
        longitude=78.5,
        block_name=block_name,
        number_of_households=10,
        largest_caste="General",
        smallest_caste="SC",
        settlement_status="active",
        plan_id=str(plan_id),
        plan_name="Test Plan",
        uuid=f"uuid:{settlement_id}",
        farmer_family={},
        livestock_census={},
        nrega_job_aware=10,
        nrega_job_applied=5,
        nrega_past_work="yes",
        nrega_raise_demand="yes",
        nrega_demand="wages",
        nrega_issues="delayed_payment",
        nrega_community="labour",
        data_settlement=data,
        is_deleted=is_deleted,
        is_moderated=is_moderated,
    )


class FetchDbDataTest(TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def _csv_path(self, name="test.csv"):
        return os.path.join(self.tmp_dir, name)

    def test_returns_true_and_writes_csv_for_valid_settlement(self):
        _create_settlement(plan_id="42", block_name="test block")
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertTrue(result)
        self.assertTrue(os.path.exists(csv_path))
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        self.assertEqual(len(rows), 1)
        self.assertIn("latitude", rows[0])
        self.assertIn("longitude", rows[0])
        self.assertEqual(rows[0]["sett_id"], "SETT001")
        self.assertEqual(rows[0]["sett_name"], "Test Settlement")

    def test_moderated_record_uses_moderated_json(self):
        moderated_data = _make_settlement_json("42", "test block")
        moderated_data["Settlements_name"] = "Moderated Settlement Name"
        _create_settlement(
            plan_id="42",
            block_name="test block",
            settlement_id="SETT002",
            is_moderated=True,
            data_override=moderated_data,
        )
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertTrue(result)
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sett_name"], "Moderated Settlement Name")

    def test_deleted_records_excluded(self):
        _create_settlement(plan_id="42", block_name="test block", is_deleted=True)
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertFalse(result)
        self.assertFalse(os.path.exists(csv_path))

    def test_rejected_submissions_excluded_by_transform(self):
        _create_settlement(
            plan_id="42", block_name="test block",
            settlement_id="SETT003", review_state="rejected"
        )
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertFalse(result)

    def test_returns_false_for_no_matching_records(self):
        csv_path = self._csv_path()
        result = fetch_db_data(csv_path, "settlement", "nonexistent_block", "99")
        self.assertFalse(result)

    def test_returns_false_for_unknown_resource_type(self):
        csv_path = self._csv_path()
        result = fetch_db_data(csv_path, "unknown_type", "test_block", "42")
        self.assertFalse(result)

    def test_block_name_with_spaces_matches_underscore_block_param(self):
        _create_settlement(plan_id="42", block_name="test block")
        csv_path = self._csv_path()

        # block param uses underscore; DB has spaces — should still match
        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertTrue(result)

    def test_block_name_with_parentheses_matches_normalized_block_param(self):
        _create_settlement(plan_id="42", block_name="Keonjhar (Kendujhar)")
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "keonjhar_kendujhar", "42")

        self.assertTrue(result)

    def test_block_name_with_extra_spaces_matches_normalized_block_param(self):
        _create_settlement(plan_id="42", block_name="Keonjhar  (Kendujhar)")
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "keonjhar_kendujhar", "42")

        self.assertTrue(result)

    def test_block_name_with_multiple_parenthesized_segments_matches_normalized_block_param(self):
        _create_settlement(plan_id="42", block_name="(Keonjhar) (Kendujhar)")
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "keonjhar_kendujhar", "42")

        self.assertTrue(result)

    def test_only_matching_plan_id_returned(self):
        _create_settlement(plan_id="42", block_name="test block", settlement_id="S1")
        _create_settlement(plan_id="99", block_name="test block", settlement_id="S2")
        csv_path = self._csv_path()

        result = fetch_db_data(csv_path, "settlement", "test_block", "42")

        self.assertTrue(result)
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sett_id"], "S1")


class AddResourcesAPITest(APITestCase):

    def setUp(self):
        self.client = APIClient()
        self.url = reverse("add_resources")

    @patch("plans.api.build_layer", return_value=True)
    @patch("plans.api.sync_form_type", return_value=True)
    def test_returns_201_when_db_data_exists(self, mock_sync, mock_build):
        _create_settlement(plan_id="42", block_name="test block")

        response = self.client.post(self.url, {
            "layer_name": "test_layer",
            "resource_type": "settlement",
            "plan_id": "42",
            "plan_name": "test plan",
            "district_name": "test district",
            "block_name": "test block",
        })

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        mock_sync.assert_called_once_with("settlement")
        mock_build.assert_called_once()

    @patch("plans.api.sync_form_type", return_value=True)
    def test_returns_404_when_no_db_data(self, mock_sync):
        response = self.client.post(self.url, {
            "layer_name": "test_layer",
            "resource_type": "settlement",
            "plan_id": "99",
            "plan_name": "test plan",
            "district_name": "test district",
            "block_name": "no block",
        })

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch("plans.api.build_layer", return_value=True)
    @patch("plans.api.sync_form_type", return_value=False)
    def test_proceeds_with_db_data_even_when_sync_fails(self, mock_sync, mock_build):
        _create_settlement(plan_id="42", block_name="test block")

        response = self.client.post(self.url, {
            "layer_name": "test_layer",
            "resource_type": "settlement",
            "plan_id": "42",
            "plan_name": "test plan",
            "district_name": "test district",
            "block_name": "test block",
        })

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        mock_build.assert_called_once()

    @patch("plans.api.build_layer", return_value=False)
    @patch("plans.api.sync_form_type", return_value=True)
    def test_returns_500_when_build_layer_fails(self, mock_sync, mock_build):
        _create_settlement(plan_id="42", block_name="test block")

        response = self.client.post(self.url, {
            "layer_name": "test_layer",
            "resource_type": "settlement",
            "plan_id": "42",
            "plan_name": "test plan",
            "district_name": "test district",
            "block_name": "test block",
        })

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
