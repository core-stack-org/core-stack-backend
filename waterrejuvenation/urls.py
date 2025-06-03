# plantations/urls.py
from django.urls import path, include
from rest_framework_nested import routers
from .views import WaterRejExcelFileViewSet
from projects.urls import router as projects_router


water_rej_router = routers.NestedSimpleRouter(
    projects_router, r"projects", lookup="project"
)
water_rej_router.register(r"waterrejuvenation/excel", WaterRejExcelFileViewSet, basename="project-water-rej-excel")



urlpatterns = [
    path("", include(water_rej_router.urls)),
]
