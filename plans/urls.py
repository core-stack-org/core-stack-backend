from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework_nested import routers

from organization.urls import router as organizations_router
from projects.urls import router as projects_router

from . import api
from .views import GlobalPlanViewSet, OrganizationPlanViewSet, PlanViewSet

watershed_router = routers.NestedSimpleRouter(
    projects_router, r"projects", lookup="project"
)
watershed_router.register(r"watershed/plans", PlanViewSet, basename="project-plan")

org_watershed_router = routers.NestedSimpleRouter(
    organizations_router, r"organizations", lookup="organization"
)
org_watershed_router.register(
    r"watershed/plans", OrganizationPlanViewSet, basename="organization-plan"
)
global_router = DefaultRouter()
global_router.register(r"watershed/plans", GlobalPlanViewSet, basename="global-plan")

urlpatterns = [
    path("get_plans/", api.get_plans, name="get_plans"),
    path("add_plan/", api.add_plan, name="add_plan"),
    path("add_resources/", api.add_resources, name="add_resources"),
    path("add_works/", api.add_works, name="add_works"),
    path(
        "sync_offline_data/resource/<str:resource_type>/",
        api.sync_offline_data,
        name="sync_offline_data",
    ),
    path(
        "sync_offline_data/work/<str:work_type>/",
        api.sync_offline_data,
        name="sync_offline_data",
    ),
    path(
        "sync_offline_data/feedback/<str:feedback_type>/",
        api.sync_offline_data,
        name="sync_offline_data",
    ),
    path("", include(watershed_router.urls)),
    path("", include(org_watershed_router.urls)),
    path("", include(global_router.urls)),
]
