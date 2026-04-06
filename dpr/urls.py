from django.urls import path

from . import api

urlpatterns = [
    path("generate_dpr/", api.generate_dpr, name="generate_dpr"),
    path("generate_mws_report/", api.generate_mws_report, name="generate_mws_report"),
    path("generate_resource_report/", api.generate_resource_report, name="generate_resource_report"),
    path("download_mws_report/", api.download_mws_report, name="download_mws_report"),
    path("generate_tehsil_report/", api.generate_tehsil_report, name="generate_tehsil_report"),

    # DPR Data API
    path("dpr_data/<int:plan_id>/summary/", api.dpr_summary, name="dpr_summary"),
    path("dpr_data/<int:plan_id>/team-details/", api.dpr_team_details, name="dpr_team_details"),
    path("dpr_data/<int:plan_id>/village-brief/", api.dpr_village_brief, name="dpr_village_brief"),
    path("dpr_data/<int:plan_id>/settlements/", api.dpr_settlements, name="dpr_settlements"),
    path("dpr_data/<int:plan_id>/crops/", api.dpr_crops, name="dpr_crops"),
    path("dpr_data/<int:plan_id>/livestock/", api.dpr_livestock, name="dpr_livestock"),
    path("dpr_data/<int:plan_id>/wells/", api.dpr_wells, name="dpr_wells"),
    path("dpr_data/<int:plan_id>/waterbodies/", api.dpr_waterbodies, name="dpr_waterbodies"),
    path("dpr_data/<int:plan_id>/maintenance/", api.dpr_maintenance, name="dpr_maintenance"),
    path("dpr_data/<int:plan_id>/nrm-works/", api.dpr_nrm_works, name="dpr_nrm_works"),
    path("dpr_data/<int:plan_id>/livelihood/", api.dpr_livelihood, name="dpr_livelihood"),
    path("dpr_data/<int:plan_id>/status-tracking/", api.dpr_status_tracking, name="dpr_status_tracking"),
    path("dpr_data/<int:plan_id>/demand-status/", api.dpr_update_demand_status, name="dpr_update_demand_status"),
]
