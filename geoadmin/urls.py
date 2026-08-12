from django.urls import path

from . import api

urlpatterns = [
    path("get_states/", api.get_states, name="get_states"),
    path("get_districts/<int:state_id>/", api.get_districts, name="get_districts"),
    path("get_blocks/<int:district_id>/", api.get_blocks, name="get_blocks"),
    path("activate_location/", api.activate_location, name="activate_location"),
    path("proposed_blocks/", api.proposed_blocks, name="proposed_blocks"),
    path("generate_api_key/", api.generate_api_key, name="generate-api-key"),
    path("get_user_api_keys/", api.get_user_api_keys, name="get-user-api-keys"),
    path("gp_tehsil_wise/", api.fetch_gp_tehsilwise, name="gp_tehsil_wise"),
    path("get_gp/", api.get_GP, name="get_gp"),
    path(
        "get_gp_mapped_with_plan/",
        api.get_gp_mapped_with_plan,
        name="get_gp_mapped_with_plan",
    ),
    path(
        "get_district_org/",
        api.get_district_org,
        name="get_district_org",
    ),
    path("get_tehsil_org/", api.get_tehsil_org, name="get_tehsil_org"),
]
