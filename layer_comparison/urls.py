from django.urls import path
from . import views

app_name = "layer_comparison"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/states/", views.states_api, name="states"),
    path("api/districts/", views.districts_api, name="districts"),
    path("api/blocks/", views.blocks_api, name="blocks"),
    path("api/layers/", views.layers_api, name="layers"),
    path("api/compare/", views.compare_api, name="compare"),
]
