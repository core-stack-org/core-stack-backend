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


_API_V2_REDOC_DESCRIPTION = """
## CoRE Stack Public APIs

Authenticate every request with header: **`X-API-Key: <your-api-key>`**

Public dataset and waterbody APIs are available under **`/api/v1/`** and **`/api/v2/`** (see **Dataset APIs v2** and **Waterbodies API v2** in the sidebar).

---

### Get MWS Time Series Data (v2 fortnight format)

**`GET /api/v2/get_mws_data/`** — same query params as v1 (`state`, `district`, `tehsil`, `mws_id`). Optional `regenerate=true` bypasses MongoDB cache.

```json
{
  "status": "success",
  "error_message": null,
  "data": {
    "metadata": { "mws_id": "12_208104" },
    "fortnight": {
      "time": ["2024-01-01", "2024-01-15"],
      "et": [2.5, 3.1],
      "runoff": [1.3, 0.8],
      "precipitation": [10.2, 5.4]
    },
    "fortnight_units": {
      "time": "iso8601",
      "time_step": "15_days",
      "et": "mm",
      "runoff": "mm",
      "precipitation": "mm"
    }
  }
}
```

**`GET /api/v1/get_mws_data/`** returns legacy `data.time_series` rows (not the fortnight arrays above).
"""

schema_view = get_schema_view(
    openapi.Info(
        title="CoRE Stack APIs",
        default_version="v2",
        description=_API_V2_REDOC_DESCRIPTION,
        terms_of_service="",
        contact=openapi.Contact(email="support@core-stack.org"),
        license=openapi.License(name="CC BY 4.0"),
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
