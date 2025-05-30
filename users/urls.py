from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_nested import routers
from .views import (
    UserViewSet,
    GroupViewSet,
    RegisterView,
    LoginView,
    LogoutView,
    UserProjectGroupViewSet,
    TokenRefreshView,
)
from projects.urls import (
    router as projects_router,
)  # Import the router from projects.urls

# Main router
router = DefaultRouter()
router.register(r"users", UserViewSet)
router.register(r"groups", GroupViewSet)

# Nested router for project users
projects_user_router = routers.NestedSimpleRouter(
    projects_router, r"projects", lookup="project"
)
projects_user_router.register(
    r"users", UserProjectGroupViewSet, basename="project-user"
)

urlpatterns = [
    path("auth/register/", RegisterView.as_view({"post": "create"}), name="register"),
    path(
        "auth/register/available_organizations/",
        RegisterView.as_view({"get": "available_organizations"}),
        name="available-organizations",
    ),
    path("auth/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("auth/login/", LoginView.as_view(), name="login"),
    path("auth/logout/", LogoutView.as_view(), name="logout"),
    path("", include(router.urls)),
    path("", include(projects_user_router.urls)),
]
