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
from django.urls import path, include
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from bot_interface.api import whatsapp_webhook
from public_api.schema import PublicAPISchemaGenerator


_PUBLIC_API_REDOC_DESCRIPTION = """
The Core Stack API is organized around REST. It uses predictable URLs, query parameters or JSON bodies, JSON responses, and standard HTTP status codes.

# Authentication

Authenticate Dataset and Waterbody requests with an API key:

```
X-API-Key: <your-api-key>
```

1. `POST /api/v1/auth/login/` with `username` and `password` — returns a JWT `access` token.
2. `POST /api/v1/generate_api_key/` with `Authorization: Bearer <access>` — returns `data.api_key`.
3. Send that key as `X-API-Key` on every subsequent request.

You can also create a key at [dashboard.core-stack.org](https://dashboard.core-stack.org/).

# Base URL

`https://geoserver.core-stack.org`

| Version | Prefix | Response | Use |
| --- | --- | --- | --- |
| **v1** | `/api/v1/` | Raw JSON. Errors: `{"error": "..."}`. | Existing integrations |
| **v2** | `/api/v2/` | `{status, error_message, data}` | New integrations |

v2 tehsil sheets accept `data=drought,stream_order`. v2 active locations accept optional `state`, `district`, and `tehsil`. Geometry `data` is a GeoJSON FeatureCollection you can open in QGIS.

# First request

1. **List active locations** — tehsils that already have data.
2. **Retrieve admin details** or **Retrieve a micro-watershed ID** — if you start from a coordinate.
3. **List tehsil datasets** or **List MWS geometries** — use the exact place names from step 1 or 2.

To request a new tehsil, use the [Geospatial Data Request Form](https://docs.google.com/forms/d/e/1FAIpQLSesYshZg_HmNc0FgF-JSBye-AeN6mdyrhF2cjGmqLYeD7WgZA/viewform).
"""

schema_view = get_schema_view(
    openapi.Info(
        title="CoRE Stack APIs",
        default_version="public",
        description=_PUBLIC_API_REDOC_DESCRIPTION,
        terms_of_service="",
        contact=openapi.Contact(email="support@core-stack.org"),
        license=openapi.License(name="CC BY 4.0"),
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
    generator_class=PublicAPISchemaGenerator,
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
    path("api/v2/", include("public_api.urls_v2")),
    path("api/v1/", include("gee_computing.urls")),
    path("api/v1/", include("community_engagement.urls")),
    path("api/v1/", include("bot_interface.urls"), name="whatsapp_webhook"),
    path("api/v1/", include("waterrejuvenation.urls")),
    path("api/v2/", include("waterrejuvenation.urls_v2")),
    path("api/v1/", include("moderation.urls")),
    # Status page
    path("status/", include("status_monitor.urls")),
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
