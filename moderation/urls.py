from django.urls import path
from .api import (
    get_paginated_submissions,
    get_form_names,
    update_submission,
    delete_submission,
    sync_updated_submissions,
    trigger_odk_sync,
    site_paln,
    site_validate,
)

urlpatterns = [
    path(
        "submissions/<str:form>/<int:plan_id>/",
        get_paginated_submissions,
        name="get_paginated_submissions",
    ),
    path("forms/", get_form_names),
    path("submissions/<str:form_name>/<path:uuid>/modify/", update_submission),
    path("submissions/<str:form_name>/<path:uuid>/delete/", delete_submission),
    path("ODKSubmissionEvent", sync_updated_submissions),
    path("sync/", trigger_odk_sync, name="trigger_odk_sync"),
    path("plan_site/", site_paln, name="plan_site"),
    path("validate_site/", site_validate, name="validate_site"),
]
