# plans/tests.py
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from .models import Plan
from projects.models import Project, ProjectApp, AppType
from organization.models import Organization
from users.models import User, UserProjectGroup
from django.contrib.auth.models import Group, Permission


class PlanModelTest(TestCase):
    def setUp(self):
        # Create organization
        self.organization = Organization.objects.create(
            name="Test Organization"
        )
        
        # Create project
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization
        )
        
        # Create project app
        self.project_app = ProjectApp.objects.create(
            project=self.project,
            app_type=AppType.WATERSHED,
            enabled=True
        )
        
        # Create user
        self.user = User.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="password123",
            organization=self.organization
        )
        
    def test_plan_creation(self):
        plan = Plan.objects.create(
            name="Test Watershed Plan",
            project_app=self.project_app,
            organization=self.organization,
            state="Test State",
            district="Test District",
            block="Test Block",
            village="Test Village",
            gram_panchayat="Test GP",
            created_by=self.user
        )
        
        # Check model attributes
        self.assertEqual(plan.name, "Test Watershed Plan")
        self.assertEqual(plan.project_app, self.project_app)
        self.assertEqual(plan.organization, self.organization)
        self.assertEqual(plan.state, "Test State")
        self.assertEqual(plan.district, "Test District")
        self.assertEqual(plan.created_by, self.user)


class PlanAPITest(APITestCase):
    def setUp(self):
        self.client = APIClient()
        
        # Create organization
        self.organization = Organization.objects.create(
            name="Test Organization"
        )
        
        # Create project
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization
        )
        
        # Create project app
        self.project_app = ProjectApp.objects.create(
            project=self.project,
            app_type=AppType.WATERSHED,
            enabled=True
        )
        
        # Create admin user
        self.admin_user = User.objects.create_user(
            username="admin",
            email="admin@example.com",
            password="password123",
            organization=self.organization,
            is_superadmin=True
        )
        
        # Create edit user
        self.edit_user = User.objects.create_user(
            username="editor",
            email="editor@example.com",
            password="password123",
            organization=self.organization
        )
        
        # Create view user
        self.view_user = User.objects.create_user(
            username="viewer",
            email="viewer@example.com",
            password="password123",
            organization=self.organization
        )
        
        # Create groups and permissions
        self.admin_group = Group.objects.create(name="Project Admin")
        self.editor_group = Group.objects.create(name="Project Editor")
        self.viewer_group = Group.objects.create(name="Project Viewer")
        
        # Create permissions
        Permission.objects.get_or_create(
            codename="view_watershed",
            name="Can view watershed planning data",
            content_type_id=1  # This would typically be correct content type ID
        )
        
        Permission.objects.get_or_create(
            codename="add_watershed",
            name="Can add watershed planning data",
            content_type_id=1
        )
        
        Permission.objects.get_or_create(
            codename="change_watershed",
            name="Can change watershed planning data",
            content_type_id=1
        )
        
        Permission.objects.get_or_create(
            codename="delete_watershed",
            name="Can delete watershed planning data",
            content_type_id=1
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
            user=self.edit_user,
            project=self.project,
            group=self.editor_group
        )
        
        UserProjectGroup.objects.create(
            user=self.view_user,
            project=self.project,
            group=self.viewer_group
        )
        
        # Create a test plan
        self.plan = Plan.objects.create(
            name="Test Watershed Plan",
            project_app=self.project_app,
            organization=self.organization,
            state="Test State",
            district="Test District",
            block="Test Block",
            village="Test Village",
            gram_panchayat="Test GP",
            created_by=self.admin_user
        )
        
        # URLs
        self.plans_list_url = reverse(
            'project-plan-list',
            kwargs={'project_pk': self.project.pk}
        )
        self.plan_detail_url = reverse(
            'project-plan-detail',
            kwargs={'project_pk': self.project.pk, 'pk': self.plan.pk}
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
            'name': 'New Watershed Plan',
            'state': 'New State',
            'district': 'New District',
            'block': 'New Block',
            'village': 'New Village',
            'gram_panchayat': 'New GP'
        }
        
        response = self.client.post(self.plans_list_url, data)
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['name'], 'New Watershed Plan')
        self.assertEqual(response.data['state'], 'New State')
        
        # Check plan was created in database
        self.assertEqual(Plan.objects.count(), 2)
    
    def test_create_plan_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        data = {
            'name': 'Editor Plan',
            'state': 'Editor State',
            'district': 'Editor District',
            'block': 'Editor Block',
            'village': 'Editor Village',
            'gram_panchayat': 'Editor GP'
        }
        
        response = self.client.post(self.plans_list_url, data)
        
        # Editor should be able to create plans
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['name'], 'Editor Plan')
    
    def test_create_plan_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        data = {
            'name': 'Viewer Plan',
            'state': 'Viewer State',
            'district': 'Viewer District',
            'block': 'Viewer Block',
            'village': 'Viewer Village',
            'gram_panchayat': 'Viewer GP'
        }
        
        response = self.client.post(self.plans_list_url, data)
        
        # Viewer should not be able to create plans
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
    
    def test_update_plan_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        data = {
            'name': 'Updated Plan',
            'state': 'Updated State',
            'district': 'Updated District',
            'block': 'Updated Block',
            'village': 'Updated Village',
            'gram_panchayat': 'Updated GP'
        }
        
        response = self.client.put(self.plan_detail_url, data)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['name'], 'Updated Plan')
        
        # Verify database was updated
        updated_plan = Plan.objects.get(pk=self.plan.pk)
        self.assertEqual(updated_plan.name, 'Updated Plan')
    
    def test_update_plan_as_editor(self):
        self.client.force_authenticate(user=self.edit_user)
        data = {
            'name': 'Editor Updated',
            'state': self.plan.state,
            'district': self.plan.district,
            'block': self.plan.block,
            'village': self.plan.village,
            'gram_panchayat': self.plan.gram_panchayat
        }
        
        response = self.client.patch(self.plan_detail_url, {'name': 'Editor Updated'})
        
        # Editor should be able to update plans
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['name'], 'Editor Updated')
    
    def test_update_plan_as_viewer(self):
        self.client.force_authenticate(user=self.view_user)
        data = {
            'name': 'Viewer Updated'
        }
        
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