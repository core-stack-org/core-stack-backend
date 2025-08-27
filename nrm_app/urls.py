"""
URL configuration for nrm_app project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import include, path
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from rest_framework import permissions

schema_view = get_schema_view(
    openapi.Info(
        title="CoRE Stack APIs",
        default_version="v1",
        description="CoRE Stack API",
        terms_of_service="",
        contact=openapi.Contact(email="support@core-stack.org"),
        license=openapi.License(name=""),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/v1/", include("geoadmin.urls")),
    path("api/v1/", include("computing.urls")),
    path("api/v1/", include("plans.urls")),
    path("api/v1/", include("dpr.urls")),
    path("api/v1/", include("stats_generator.urls")),
    path("api/v1/", include("organization.urls")),
    path("api/v1/", include("users.urls")),
    path("api/v1/", include("projects.urls")),
    path("api/v1/", include("plantations.urls")),
    path("api/v1/", include("public_api.urls")),
    # Swagger Doc
    path(
        "swagger<format>/", schema_view.without_ui(cache_timeout=0), name="schema-json"
    ),
    path(
        "swagger/",
        schema_view.with_ui("swagger", cache_timeout=0),
        name="schema-swagger-ui",
    ),
    path("redoc/", schema_view.with_ui("redoc", cache_timeout=0), name="schema-redoc"),
    path("", schema_view.with_ui("redoc", cache_timeout=0), name="schema-redoc"),
]
