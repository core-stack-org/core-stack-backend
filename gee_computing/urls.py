# projects/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import GEEAccountView

# Main router for projects
router = DefaultRouter()
router.register(r"geeaccounts", GEEAccountView, basename="geeaccounts")

urlpatterns = [
    path("", include(router.urls)),
]
