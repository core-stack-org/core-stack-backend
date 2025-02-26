# plantations/tests.py
import os
import hashlib
import tempfile
from django.test import TestCase
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from .models import KMLFile
from projects.models import Project, ProjectApp, AppType
from organization.models import Organization
from users.models import User


class KMLFileModelTest(TestCase):
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
            app_type=AppType.PLANTATION,
            enabled=True
        )
        
        # Create user
        self.user = User.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="password123",
            organization=self.organization
        )
        
    def test_kml_file_creation(self):
        # Create a temporary KML file
        content = b'<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"></kml>'
        temp_file = SimpleUploadedFile("test.kml", content)
        
        # Calculate expected hash
        file_hash = hashlib.sha256()
        file_hash.update(content)
        expected_hash = file_hash.hexdigest()
        
        # Create KML file model
        kml_file = KMLFile.objects.create(
            project_app=self.project_app,
            name="Test KML",
            file=temp_file,
            uploaded_by=self.user
        )
        
        # Check model attributes
        self.assertEqual(kml_file.name, "Test KML")
        self.assertEqual(kml_file.project_app, self.project_app)
        self.assertEqual(kml_file.uploaded_by, self.user)
        self.assertEqual(kml_file.kml_hash, expected_hash)


class KMLFileAPITest(APITestCase):
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
            app_type=AppType.PLANTATION,
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
        
        # URL for uploading KML
        self.kml_list_url = reverse(
            'project-kml-list',
            kwargs={'project_pk': self.project.pk}
        )
    
    def test_upload_kml_file(self):
        self.client.force_authenticate(user=self.admin_user)
        
        # Create a temporary KML file
        content = b'<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"></kml>'
        
        # Upload KML file
        response = self.client.post(
            self.kml_list_url,
            {
                'name': 'Test KML',
                'file': SimpleUploadedFile("test.kml", content)
            },
            format='multipart'
        )
        
        # Check response
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['name'], 'Test KML')
        
        # Check that KML file was created
        self.assertEqual(KMLFile.objects.count(), 1)
        
        # Check that file was saved
        kml_file = KMLFile.objects.first()
        self.assertTrue(os.path.exists(kml_file.file.path))
        
        # Clean up
        kml_file.file.delete()
    
    def test_duplicate_kml_detection(self):
        self.client.force_authenticate(user=self.admin_user)
        
        # Create a temporary KML file
        content = b'<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"></kml>'
        
        # Upload KML file first time
        response1 = self.client.post(
            self.kml_list_url,
            {
                'name': 'Test KML 1',
                'file': SimpleUploadedFile("test1.kml", content)
            },
            format='multipart'
        )
        
        # Check first upload succeeded
        self.assertEqual(response1.status_code, status.HTTP_201_CREATED)
        
        # Upload same KML file again with different name
        response2 = self.client.post(
            self.kml_list_url,
            {
                'name': 'Test KML 2',
                'file': SimpleUploadedFile("test2.kml", content)
            },
            format='multipart'
        )
        
        # Check second upload was rejected
        self.assertEqual(response2.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('already been uploaded', response2.data['detail'])
        
        # Clean up
        KMLFile.objects.first().file.delete()