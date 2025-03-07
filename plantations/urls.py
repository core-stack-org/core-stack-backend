# plantations/urls.py
from django.urls import path, include
from rest_framework_nested import routers
from .views import KMLFileViewSet
from projects.urls import router as projects_router


plantation_router = routers.NestedSimpleRouter(
    projects_router, 
    r'projects', 
    lookup='project'
)
plantation_router.register(
    r'plantation/kml',
    KMLFileViewSet,
    basename='project-kml'
)

urlpatterns = [
    path('', include(plantation_router.urls)),
]