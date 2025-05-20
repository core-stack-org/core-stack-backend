from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import OrganizationViewSet

# Create a router for organization endpoints
router = DefaultRouter()
router.register(r"organizations", OrganizationViewSet)

urlpatterns = [
    path("", include(router.urls)),
]
