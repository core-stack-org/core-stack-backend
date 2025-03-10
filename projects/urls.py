# projects/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_nested import routers
from .views import ProjectViewSet, ProjectAppViewSet

# Main router for projects
router = DefaultRouter()
router.register(r"projects", ProjectViewSet, basename="project")

# Nested router for project apps
projects_router = routers.NestedSimpleRouter(router, r"projects", lookup="project")
projects_router.register(r"apps", ProjectAppViewSet, basename="project-app")

urlpatterns = [
    path("", include(router.urls)),
    path("", include(projects_router.urls)),
]
