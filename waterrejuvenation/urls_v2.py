from django.urls import path

from .api import (
    get_waterbodies_by_admin_and_uid_v2,
    get_waterbodies_by_uid_v2,
)

urlpatterns = [
    path(
        "get_waterbodies_data_by_admin/",
        get_waterbodies_by_admin_and_uid_v2,
        name="generate_waterbodies_data_v2",
    ),
    path(
        "get_waterbody_data/",
        get_waterbodies_by_uid_v2,
        name="generate_waterbody_data_v2",
    ),
]
