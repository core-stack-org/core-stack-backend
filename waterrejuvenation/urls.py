# plantations/urls.py
from django.urls import path, include
from rest_framework_nested import routers

from .views import WaterRejExcelFileViewSet
from projects.urls import router as projects_router

# import the two endpoints from your api module
# - get_merged_waterbodies_with_zoi : function that generates/returns merged JSON (can be called to force regeneration)
# - get_waterbodies_by_admin_and_uid: decorated DRF function that serves the merged JSON via HTTP (swagger + API key decorator)
from .api import (
    get_merged_waterbodies_with_zoi,
    get_waterbodies_by_admin_and_uid,
    get_waterbodies_by_uid,
)

# Nested router registration (unchanged)
water_rej_router = routers.NestedSimpleRouter(
    projects_router, r"projects", lookup="project"
)
water_rej_router.register(
    r"waterrejuvenation/excel",
    WaterRejExcelFileViewSet,
    basename="project-water-rej-excel",
)

urlpatterns = [
    # include nested router urls
    path("", include(water_rej_router.urls)),
    # Endpoint to trigger generation (GET or POST as your function supports)
    # Example: GET /plantations/generate_waterbodies_data/?state=RAJASTHAN&district=alwar&tehsil=alwar
    path(
        "get_waterbodies_data_by_admin/",
        get_waterbodies_by_admin_and_uid,
        name="generate_waterbodies_data",
    ),
    path(
        "get_waterbody_data/",
        get_waterbodies_by_uid,
        name="generate_waterbodies_data",
    ),
]
