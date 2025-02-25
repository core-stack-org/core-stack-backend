from django.urls import path

from . import api

urlpatterns = [
    path("get_plans/", api.get_plans, name="get_plans"),
    path("add_plan/", api.add_plan, name="add_plan"),
    path("add_resources/", api.add_resources, name="add_resources"),
    path("add_works/", api.add_works, name="add_works"),
    path("sync_offline_data/<str:resource_type>/", api.sync_offline_data, name="sync_offline_data"),
]