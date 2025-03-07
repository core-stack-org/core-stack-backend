# projects/tests.py
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from .models import Project, ProjectApp, AppType
from organization.models import Organization
from users.models import User

class ProjectModelTest(TestCase):
    def setUp(self):
        self.organization = Organization.objects.create(
            name="Test Organization",
            description="Test Description"
        )
        
    def test_project_creation(self):
        project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
            description="Test Project Description",
            geojson_path="/path/to/geojson"
        )
        self.assertEqual(project.name, "Test Project")
        self.assertEqual(project.organization, self.organization)
        self.assertEqual(project.description, "Test Project Description")
        self.assertEqual(project.geojson_path, "/path/to/geojson")
        
    def test_project_app_creation(self):
        project = Project.objects.create(
            name="Test Project",
            organization=self.organization
        )
        
        project_app = ProjectApp.objects.create(
            project=project,
            app_type=AppType.PLANTATION,
            enabled=True
        )
        
        self.assertEqual(project_app.project, project)
        self.assertEqual(project_app.app_type, AppType.PLANTATION)
        self.assertTrue(project_app.enabled)

class ProjectAPITest(APITestCase):
    def setUp(self):
        self.client = APIClient()
        
        # Create organization
        self.organization = Organization.objects.create(
            name="Test Organization"
        )
        
        # Create admin user
        self.admin_user = User.objects.create_user(
            username="admin",
            email="admin@example.com",
            password="password123",
            organization=self.organization,
            is_superadmin=True
        )
        
        # Create regular user
        self.regular_user = User.objects.create_user(
            username="user",
            email="user@example.com",
            password="password123",
            organization=self.organization
        )
        
        # Create a test project
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
            description="Test description"
        )
        
        # Create project app
        self.project_app = ProjectApp.objects.create(
            project=self.project,
            app_type=AppType.WATERSHED,
            enabled=True
        )
        
        # URLs
        self.projects_url = reverse('project-list')
        self.project_detail_url = reverse('project-detail', kwargs={'pk': self.project.pk})
        self.project_apps_url = reverse('project-apps', kwargs={'pk': self.project.pk})
        
    def test_list_projects_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        response = self.client.get(self.projects_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        
    def test_list_projects_as_regular_user(self):
        self.client.force_authenticate(user=self.regular_user)
        response = self.client.get(self.projects_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        
    def test_create_project_as_admin(self):
        self.client.force_authenticate(user=self.admin_user)
        data = {
            'name': 'New Test Project',
            'description': 'New test description',
            'geojson_path': '/path/to/geojson'
        }
        
        response = self.client.post(self.projects_url, data)
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['name'], 'New Test Project')
        self.assertEqual(response.data['geojson_path'], '/path/to/geojson')
        
    def test_retrieve_project_with_apps(self):
        self.client.force_authenticate(user=self.admin_user)
        response = self.client.get(self.project_detail_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['name'], 'Test Project')
        self.assertEqual(len(response.data['apps']), 1)
        self.assertEqual(response.data['apps'][0]['app_type'], 'watershed')
        
    def test_enable_app_for_project(self):
        self.client.force_authenticate(user=self.admin_user)
        url = reverse('project-enable-app', kwargs={'pk': self.project.pk})
        data = {
            'app_type': 'plantation',
            'enabled': True
        }
        
        response = self.client.post(url, data)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['app_type'], 'plantation')
        self.assertTrue(response.data['enabled'])
        
        # Check that we now have 2 apps for the project
        self.assertEqual(ProjectApp.objects.filter(project=self.project).count(), 2)