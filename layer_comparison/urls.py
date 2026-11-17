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
    path("api/excel-sheets/", views.excel_sheets_api, name="excel-sheets"),
    path("api/excel-analysis/", views.excel_analysis_api, name="excel-analysis"),
    path("api/excel-scatter/", views.excel_scatter_api, name="excel-scatter"),
    path("api/excel-multi/", views.excel_multi_api, name="excel-multi"),
]
