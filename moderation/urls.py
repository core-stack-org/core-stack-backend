from django.urls import path
from .api import get_paginated_submissions, get_form_names, delete_odk_submission

urlpatterns = [
    path(
        "submissions/<str:form>/<int:plan_id>/",
        get_paginated_submissions,
        name="get_paginated_submissions",
    ),
    path("forms/", get_form_names),
    # urls.py
    path(
        "delete/<int:project_id>/<str:form_id>/<str:submission_uuid>/",
        delete_odk_submission,
    ),
]
