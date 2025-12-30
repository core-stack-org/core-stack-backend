from django.urls import path
from .api import (
    get_paginated_submissions,
    get_form_names,
    update_submission,
    delete_submission,
    sync_updated_submissions,
    fetch_and_parse_odk_form,
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
    path("form-name-label/", fetch_and_parse_odk_form),
]
