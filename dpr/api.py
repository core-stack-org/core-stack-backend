import json
import uuid
from datetime import date, datetime

from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.templatetags.i18n import language
from django.urls import reverse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.decorators import api_view, schema
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from utilities.auth_check_decorator import api_security_check
from utilities.auth_utils import auth_free
from utilities.logger import setup_logger
from nrm_app.settings import BASE_API_URL

from .gen_dpr import (
    get_plan_details,
)
from .serializers import (
    CropSerializer,
    DPRSummarySerializer,
    LivelihoodSerializer,
    LivestockSerializer,
    MaintenanceSerializer,
    NRMWorkSerializer,
    SettlementSerializer,
    TeamDetailsSerializer,
    VillageBriefSerializer,
    WaterbodySerializer,
    WellSerializer,
)
from .services import (
    get_crops_data,
    get_dpr_report_status,
    get_dpr_report_status_summary,
    get_dpr_summary,
    get_dpr_status_tracking,
    get_global_status_tracking,
    get_livestock_data,
    get_livelihood_data,
    get_maintenance_data,
    get_nrm_works_data,
    get_settlements_data,
    get_team_details,
    get_village_brief,
    get_waterbodies_data,
    get_wells_data,
    patch_dpr_report_status,
    update_demand_status,
)
from .gen_mws_report import (
    get_change_detection_data,
    get_land_conflict_industrial_data,
    get_cropping_intensity,
    get_cropping_year_range,
    get_drought_data,
    get_osm_data,
    get_soge_data,
    get_hydro_tabular_data,
    get_cropping_water_hydro_data,
    get_terrain_and_lulc_data,
    get_surface_Water_bodies_data,
    get_terrain_data,
    get_village_data,
    get_water_balance_data,
    get_fortnightly_water_balance_data,
    get_ndvi_timeseries_data,
    get_ndvi_timeseries_tree_data,
    get_factory_data,
    get_mining_data,
    get_green_credit_data,
)
from .gen_tehsil_report import (
    get_tehsil_data,
    get_pattern_intensity,
    get_agri_water_stress_data,
    get_agri_water_drought_data,
    get_agri_water_irrigation_data,
    get_agri_low_yield_data,
    get_forest_degrad_data,
    get_mining_presence_data,
    get_socio_economic_caste_data,
    get_socio_economic_nrega_data,
    get_fishery_water_potential_data,
    get_agroforestry_transition_data,
)
from .gen_village_report import (
    load_block_sheets,
    get_village_polygon_and_info,
    get_development_data,
    get_block_development_data,
    calculate_demographics,
    get_basic_infrastructure,
    get_health_and_wash,
    get_education_institutions,
    get_financial_inclusion,
    get_welfare_inclusion,
    get_community_institutes,
    get_livelihood_diversification,
    get_livestock_management,
    get_livestock_count,
    get_land_cultivation,
    get_all_villages_land_cultivation,
    get_irrigation_Infra,
    get_agri_support_service,
    get_ecological_climate_resiliance,
    get_all_villages_basic_infrastructure,
    get_all_villages_health_and_wash,
    get_all_villages_education_institutions,
    get_all_villages_financial_inclusion,
    get_all_villages_welfare_inclusion,
    get_all_villages_community_institutes,
    get_all_villages_livestock_management,
    get_all_villages_irrigation_infra,
    get_all_villages_agri_support_service,
    get_all_villages_ecological_climate_resiliance,
    get_mwses_ids,
)
from .gen_report_download import render_pdf_with_firefox
from .utils import validate_email, transform_name
from .tasks import generate_dpr_task
import tempfile
import os
from .generate_yuktdhara_format import csv_to_kml, fetch_data
import zipfile
from geoadmin.models import GramPanchayat
from plans.models import PlanApp

state_param = openapi.Parameter(
    "state",
    openapi.IN_QUERY,
    description="Name of the state (e.g. 'Uttar Pradesh')",
    type=openapi.TYPE_STRING,
    required=True,
)
district_param = openapi.Parameter(
    "district",
    openapi.IN_QUERY,
    description="Name of the district (e.g. 'Jaunpur')",
    type=openapi.TYPE_STRING,
    required=True,
)
tehsil_param = openapi.Parameter(
    "tehsil",
    openapi.IN_QUERY,
    description="Name of the tehsil (e.g. 'Badlapur')",
    type=openapi.TYPE_STRING,
    required=True,
)
mws_id_param = openapi.Parameter(
    "uid",
    openapi.IN_QUERY,
    description="Unique MWS identifier (e.g. '12_234647')",
    type=openapi.TYPE_STRING,
    required=True,
)
authorization_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API Key in format: <your-api-key>",
    type=openapi.TYPE_STRING,
    required=True,
)

logger = setup_logger(__name__)


# MARK: Generate DPR
@api_view(["POST"])
@schema(None)
@auth_free
def generate_dpr(request):
    try:
        plan_id = request.data.get("plan_id")
        email_id = request.data.get("email_id")
        # language = request.data.get("language")
        regenerate = request.data.get("regenerate", False)

        logger.info(
            "Generating DPR for plan ID: %s and email ID: %s (regenerate=%s)",
            plan_id,
            email_id,
            regenerate,
        )

        valid_email = validate_email(email_id)

        if not valid_email:
            return Response(
                {"error": "Invalid email address"}, status=status.HTTP_400_BAD_REQUEST
            )

        plan = get_plan_details(plan_id)
        logger.info("Plan found: %s", plan)
        if plan is None:
            return Response(
                {"error": "Plan not found"}, status=status.HTTP_404_NOT_FOUND
            )

        generate_dpr_task.apply_async(args=[plan_id, email_id, regenerate], queue="dpr")

        return Response(
            {
                "message": f"DPR generation task initiated and will be sent to the email ID: {email_id}"
            },
            status=status.HTTP_202_ACCEPTED,
        )

    except Exception as e:
        logger.exception("Exception in generate_dpr api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@swagger_auto_schema(
    method="get",
    manual_parameters=[
        state_param,
        district_param,
        tehsil_param,
        mws_id_param,
        authorization_param,
    ],
    responses={
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "Data": "Use the url on web to render the mws report",
                }
            },
        ),
        400: openapi.Response(description="Bad Request - Invalid parameters"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(description="Internal Server Error"),
    },
)
# MARK: MWS Report
@api_security_check(auth_type="Auth_free")
@schema(None)
def generate_mws_report(request):
    try:
        # ? Extract and transform parameters
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        # Transform district, block, and state
        district = transform_name(result["district"])
        block = transform_name(result["block"])
        state = transform_name(result["state"])
        uid = result["uid"]

        # print("Api Processing End 1", datetime.now())

        # ? OSM description generation
        parameter_block, parameter_mws = get_osm_data(state, district, block, uid)

        # ? Terrain Description generation
        (
            terrain_mws,
            mws_areas,
            block_areas,
            terrain_comp,
            terrain_land_use,
            lulc_mws_slope,
            lulc_block_slope,
            lulc_mws_plain,
            lulc_block_plain,
            terrain_category_pct,
            terrain_plain_pct,
            terrain_slope_pct,
        ) = get_terrain_data(state, district, block, uid)

        # ? Degradation Description generation
        (
            land_degrad,
            tree_degrad,
            urbanization,
            restore_desc,
            crop_intensity_sankey,
            tree_reduction_sankey,
            urbanization_sankey,
        ) = get_change_detection_data(state, district, block, uid)

        # ? Cropping Year Range
        year_range_text = get_cropping_year_range(state, district, block, uid)

        # ? Surface Waterbody Description
        (
            swb_intro_desc,
            swb_desc,
            trend_desc,
            final_desc,
            swb_season_avg_desc,
            kharif_data,
            rabi_data,
            zaid_data,
            water_years,
        ) = get_surface_Water_bodies_data(state, district, block, uid)

        # ? Water Balance Description
        (
            water_balance_trend,
            wb_desc,
            good_rainfall,
            bad_rainfall,
            mean_water_balance,
            precip_data,
            runoff_data,
            et_data,
            dg_data,
            wb_years,
        ) = get_water_balance_data(state, district, block, uid)

        # ? Fortnightly Water Balance Description
        (
            fortnight_labels,
            fortnight_precip_data,
            fortnight_et_data,
            fortnight_runoff_data,
        ) = get_fortnightly_water_balance_data(state, district, block, uid)

        # ? SOGE Description
        soge_desc = get_soge_data(state, district, block, uid)

        # ? Hydro Tabular Data
        (
            min_elev,
            max_elev,
            mean_elev,
            dem_relief,
            aquifer_class,
            soge_class,
            soge_dev_percent,
            drainage_density,
            total_length,
            area,
            perimeter,
            compactness,
        ) = get_hydro_tabular_data(state, district, block, uid)

        # ? Cropping, Water, and Hydrology Data
        cwh_data = get_cropping_water_hydro_data(state, district, block, uid)

        # ? Tree Map Terrain LULC Data
        terrain_lulc_data = get_terrain_and_lulc_data(state, district, block, uid)

        # ? Drought Description
        drought_desc, drought_weeks, mod_drought, sev_drought, drysp_all, dg_years = (
            get_drought_data(state, district, block, uid)
        )

        # ? Village Profile
        (
            villages_name,
            villages_sc,
            villages_st,
            villages_pop,
            swc_works,
            lr_works,
            plantation_work,
            iof_works,
            ofl_works,
            ca_works,
            ofw_works,
            villages_intersect_pct,
            villages_id,
        ) = get_village_data(state, district, block, uid)

        # ? Cropping Intensity Description
        inten_desc1, inten_desc2, single, double, triple, uncrop, crop_years = (
            get_cropping_intensity(state, district, block, uid)
        )

        # ? NDVI Timeseries (Crops)
        ndvi_labels, ndvi_data = get_ndvi_timeseries_data(state, district, block, uid)

        # ? NDVI Timeseries (Trees)
        ndvi_tree_labels, ndvi_tree_data = get_ndvi_timeseries_tree_data(state, district, block, uid)

        # ? LCW and Industrial Data Description
        lcw_desc = get_land_conflict_industrial_data(state, district, block, uid)
        factory_desc = get_factory_data(state, district, block, uid)
        mining_desc = get_mining_data(state, district, block, uid)

        green_credits = get_green_credit_data(state, district, block, uid)

        context = {
            "state": state,
            "district": district,
            "block": block,
            "mws_id": uid,
            "base_url": BASE_API_URL,
            "block_osm": parameter_block,
            "mws_osm": parameter_mws,
            "terrain_mws": terrain_mws,
            "terrain_comp": terrain_comp,
            "terrain_land_use": terrain_land_use,
            "terrain_category_pct": terrain_category_pct,
            "terrain_plain_pct": terrain_plain_pct,
            "terrain_slope_pct": terrain_slope_pct,
            "land_degrad": land_degrad,
            "tree_degrad": tree_degrad,
            "urbanization": urbanization,
            "restore_desc": restore_desc,
            "crop_intensity_sankey": json.dumps(crop_intensity_sankey),
            "tree_reduction_sankey": json.dumps(tree_reduction_sankey),
            "urbanization_sankey": json.dumps(urbanization_sankey),
            "year_range_text": year_range_text,
            "swb_intro_desc": swb_intro_desc,
            "swb_desc": swb_desc,
            "trend_desc": trend_desc,
            "swb_season_desc": final_desc,
            "swb_season_avg_desc": swb_season_avg_desc,
            "water_balance_trend": water_balance_trend,
            "mean_water_balance": mean_water_balance,
            "wb_desc": wb_desc,
            "good_rainfall": good_rainfall,
            "bad_rainfall": bad_rainfall,
            "drought_desc": drought_desc,
            "inten_desc1": inten_desc1,
            "inten_desc2": inten_desc2,
            "soge_desc": soge_desc,
            "min_elev": min_elev,
            "max_elev": max_elev,
            "mean_elev": mean_elev,
            "dem_relief": dem_relief,
            "aquifer_class": aquifer_class,
            "soge_class": soge_class,
            "soge_dev_percent": soge_dev_percent,
            "drainage_density": drainage_density,
            "drainage_length": total_length,
            "area": area,
            "perimeter": perimeter,
            "compactness": compactness,
            **cwh_data,
            **terrain_lulc_data,
            "mws_areas": json.dumps(mws_areas),
            "block_areas": json.dumps(block_areas),
            "lulc_mws_slope": json.dumps(lulc_mws_slope),
            "lulc_block_slope": json.dumps(lulc_block_slope),
            "lulc_mws_plain": json.dumps(lulc_mws_plain),
            "lulc_block_plain": json.dumps(lulc_block_plain),
            "kharif_data": json.dumps(kharif_data),
            "rabi_data": json.dumps(rabi_data),
            "zaid_data": json.dumps(zaid_data),
            "precip_data": json.dumps(precip_data),
            "runoff_data": json.dumps(runoff_data),
            "et_data": json.dumps(et_data),
            "dg_data": json.dumps(dg_data),
            "fortnight_labels": json.dumps(fortnight_labels),
            "fortnight_precip_data": json.dumps(fortnight_precip_data),
            "fortnight_et_data": json.dumps(fortnight_et_data),
            "fortnight_runoff_data": json.dumps(fortnight_runoff_data),
            "swc_works": json.dumps(swc_works),
            "lr_works": json.dumps(lr_works),
            "plantation_work": json.dumps(plantation_work),
            "iof_works": json.dumps(iof_works),
            "ofl_works": json.dumps(ofl_works),
            "ca_works": json.dumps(ca_works),
            "ofw_works": json.dumps(ofw_works),
            "drought_weeks": json.dumps(drought_weeks),
            "mod_drought": json.dumps(mod_drought.astype(int).tolist()),
            "sev_drought": json.dumps(sev_drought.astype(int).tolist()),
            "villages_name": json.dumps(villages_name),
            "villages_intersect_pct": json.dumps(villages_intersect_pct),
            "villages_id": json.dumps(villages_id),
            "villages_sc": json.dumps(villages_sc),
            "villages_st": json.dumps(villages_st),
            "villages_pop": json.dumps(villages_pop),
            "single": json.dumps(single),
            "double": json.dumps(double),
            "triple": json.dumps(triple),
            "uncrop": json.dumps(uncrop),
            "crop_years": json.dumps(crop_years),
            "ndvi_labels": json.dumps(ndvi_labels),
            "ndvi_data": json.dumps(ndvi_data),
            "ndvi_tree_labels": json.dumps(ndvi_tree_labels),
            "ndvi_tree_data": json.dumps(ndvi_tree_data),
            "water_years": json.dumps(water_years),
            "wb_years": json.dumps(wb_years),
            "drysp_all": json.dumps(drysp_all),
            "dg_years": json.dumps(dg_years),
            "lcw_desc": lcw_desc,
            "factory_desc": factory_desc,
            "mining_desc": mining_desc,
            "green_credit_desc": green_credits,
        }

        # print("Api Processing End 1", datetime.now())

        return render(request, "mws-report.html", context)

    except Exception as e:
        logger.exception("Exception in generate_mws_report api :: ", e)
        return render(request, "error-page.html", {})


@api_view(["GET"])
@schema(None)
@auth_free
def generate_resource_report(request):
    try:
        # ? district, block, plan_id
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        context = {
            "district": transform_name(result["district"]),
            "block": transform_name(result["block"]),
            "plan_id": result["plan_id"],
            "plan_name": result["plan_name"],
        }

        return render(request, "resource-report.html", context)
    except Exception as e:
        logger.exception("Exception in generate_resource_report api :: ", e)
        return render(request, "error-page.html", {})


@api_view(["GET"])
@schema(None)
@auth_free
def download_report(request):
    report_type = request.GET.get("report_type")
    mode = request.GET.get("mode", "download")

    if not report_type:
        return HttpResponseBadRequest("Missing 'report_type' parameter")

    # Define required params based on report type
    if report_type == "mws":
        required = ("state", "district", "block", "uid", "report_type")
    elif report_type == "resource":
        required = ("district", "block", "plan_id", "plan_name", "report_type")
    else:
        return HttpResponseBadRequest(f"Unknown report_type: {report_type}")

    missing = [k for k in required if k not in request.GET]
    if missing:
        return HttpResponseBadRequest(f"Missing query params: {', '.join(missing)}")

    if report_type == "mws":
        report_html_url = (
            f"https://geoserver.core-stack.org/api/v1/generate_mws_report/"
            f"?state={request.GET.get('state')}&district={request.GET.get('district')}&block={request.GET.get('block')}&uid={request.GET.get('uid')}"
        )
        filename = f"mws_report_{request.GET.get('uid')}.pdf"
    elif report_type == "resource":
        report_html_url = (
            f"https://geoserver.core-stack.org/api/v1/generate_resource_report/"
            f"?district={request.GET.get('district')}&block={request.GET.get('block')}&plan_id={request.GET.get('plan_id')}&plan_name={request.GET.get('plan_name')}"
        )
        filename = f"resource_report_{request.GET.get('plan_name')}.pdf"

    pdf_bytes = render_pdf_with_firefox(report_html_url)

    resp = HttpResponse(pdf_bytes, content_type="application/pdf")
    if mode == "view":
        resp["Content-Disposition"] = f'inline; filename="{filename}"'
    else:
        resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


@api_view(["GET"])
@schema(None)
@auth_free
def generate_tehsil_report(request):
    try:
        # ? district, block, mwsId
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        # ? OSM description generation
        parameter_block = get_tehsil_data(
            result["state"], result["district"], result["block"]
        )

        # ? Pattern intensity
        mws_pattern_intensity_with_active_pattern = get_pattern_intensity(
            result["state"], result["district"], result["block"]
        )

        mws_pattern_intensity = mws_pattern_intensity_with_active_pattern.get(
            "intensity", None
        )

        mws_active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "mws_active_patterns", []
        )

        pattern_display_mapping = mws_pattern_intensity_with_active_pattern.get(
            "pattern_display_mapping", []
        )

        # ? Agriculture data
        groundwater_stress = get_agri_water_stress_data(
            result["state"], result["district"], result["block"]
        )
        high_drought_incidence, weighted_drought_timeline = get_agri_water_drought_data(
            result["state"], result["district"], result["block"]
        )
        high_irrigation_risk, irrigation_timeline = get_agri_water_irrigation_data(
            result["state"], result["district"], result["block"]
        )
        low_yield, yield_sankey = get_agri_low_yield_data(
            result["state"], result["district"], result["block"]
        )
        forest_degradation, forest_sankey = get_forest_degrad_data(
            result["state"], result["district"], result["block"]
        )
        mining_presence, mining_pie = get_mining_presence_data(
            result["state"], result["district"], result["block"]
        )
        socio_caste, caste_pie = get_socio_economic_caste_data(
            result["state"], result["district"], result["block"]
        )
        socio_nrega, nrega_pie = get_socio_economic_nrega_data(
            result["state"], result["district"], result["block"]
        )
        fishery_potential, fishery_timeline = get_fishery_water_potential_data(
            result["state"], result["district"], result["block"]
        )
        agroforestry_transition, agroforestry_sankey = get_agroforestry_transition_data(
            result["state"], result["district"], result["block"]
        )

        # print("Active Patterns", active_pattern)
        active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "active_patterns", []
        )

        village_active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "village_active_patterns", []
        )

        # =====================================================

        context = {
            "state": result["state"],
            "district": result["district"],
            "block": result["block"],
            "block_osm": parameter_block,
            "mws_pattern_intensity_json": json.dumps(mws_pattern_intensity),
            "active_pattern": active_pattern,
            "village_active_pattern": village_active_pattern,
            "pattern_display_mapping_json": pattern_display_mapping,
            "mws_active_patterns_json": json.dumps(mws_active_pattern),
            "groundwater_stress_json": json.dumps(groundwater_stress),
            "high_drought_incidence_json": json.dumps(high_drought_incidence),
            "drought_timeline_json": json.dumps(weighted_drought_timeline),
            "high_irrigation_risk_json": json.dumps(high_irrigation_risk),
            "irrigation_timeline_json": json.dumps(irrigation_timeline),
            "low_yield_json": json.dumps(low_yield),
            "yield_sankey_json": json.dumps(yield_sankey),
            "forest_degradation_json": json.dumps(forest_degradation),
            "forest_sankey_json": json.dumps(forest_sankey),
            "mining_presence_json": json.dumps(mining_presence),
            "mining_pie_json": json.dumps(mining_pie),
            "socio_caste_json": json.dumps(socio_caste),
            "caste_pie_json": json.dumps(caste_pie),
            "socio_nrega_json": json.dumps(socio_nrega),
            "nrega_pie_json": json.dumps(nrega_pie),
            "fishery_potential_json": json.dumps(fishery_potential),
            "fishery_timeline_json": json.dumps(fishery_timeline),
            "agroforestry_transition_json": json.dumps(agroforestry_transition),
            "agroforestry_sankey_json": json.dumps(agroforestry_sankey),
        }

        return render(request, "block-report.html", context)

    except Exception as e:
        logger.exception("Exception in generate_tehsil_report api :: ", e)
        return render(request, "error-page.html", {})


@api_view(["GET"])
@auth_free
@schema(None)
@api_security_check(auth_type="Auth_free")
def generate_tehsil_patterns_data(request):
    try:
        # ? district, block, mwsId
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        # ? OSM description generation
        parameter_block = get_tehsil_data(
            result["state"], result["district"], result["block"]
        )

        # ? Pattern intensity
        mws_pattern_intensity_with_active_pattern = get_pattern_intensity(
            result["state"], result["district"], result["block"]
        )

        mws_pattern_intensity = mws_pattern_intensity_with_active_pattern.get(
            "intensity", None
        )

        mws_active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "mws_active_patterns", []
        )

        pattern_display_mapping = mws_pattern_intensity_with_active_pattern.get(
            "pattern_display_mapping", []
        )

        # ? Agriculture data
        groundwater_stress = get_agri_water_stress_data(
            result["state"], result["district"], result["block"]
        )
        high_drought_incidence, weighted_drought_timeline = get_agri_water_drought_data(
            result["state"], result["district"], result["block"]
        )
        high_irrigation_risk, irrigation_timeline = get_agri_water_irrigation_data(
            result["state"], result["district"], result["block"]
        )
        low_yield, yield_sankey = get_agri_low_yield_data(
            result["state"], result["district"], result["block"]
        )
        forest_degradation, forest_sankey = get_forest_degrad_data(
            result["state"], result["district"], result["block"]
        )
        mining_presence, mining_pie = get_mining_presence_data(
            result["state"], result["district"], result["block"]
        )
        socio_caste, caste_pie = get_socio_economic_caste_data(
            result["state"], result["district"], result["block"]
        )
        socio_nrega, nrega_pie = get_socio_economic_nrega_data(
            result["state"], result["district"], result["block"]
        )
        fishery_potential, fishery_timeline = get_fishery_water_potential_data(
            result["state"], result["district"], result["block"]
        )
        agroforestry_transition, agroforestry_sankey = get_agroforestry_transition_data(
            result["state"], result["district"], result["block"]
        )

        # print("Active Patterns", active_pattern)
        active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "active_patterns", []
        )

        village_active_pattern = mws_pattern_intensity_with_active_pattern.get(
            "village_active_patterns", []
        )

        # =====================================================

        context = {
            "state": result["state"],
            "district": result["district"],
            "block": result["block"],
            "block_osm": parameter_block,
            "mws_pattern_intensity_json": json.dumps(mws_pattern_intensity),
            "active_pattern": active_pattern,
            "village_active_pattern": village_active_pattern,
            "pattern_display_mapping_json": pattern_display_mapping,
            "mws_active_patterns_json": json.dumps(mws_active_pattern),
            "groundwater_stress_json": json.dumps(groundwater_stress),
            "high_drought_incidence_json": json.dumps(high_drought_incidence),
            "drought_timeline_json": json.dumps(weighted_drought_timeline),
            "high_irrigation_risk_json": json.dumps(high_irrigation_risk),
            "irrigation_timeline_json": json.dumps(irrigation_timeline),
            "low_yield_json": json.dumps(low_yield),
            "yield_sankey_json": json.dumps(yield_sankey),
            "forest_degradation_json": json.dumps(forest_degradation),
            "forest_sankey_json": json.dumps(forest_sankey),
            "mining_presence_json": json.dumps(mining_presence),
            "mining_pie_json": json.dumps(mining_pie),
            "socio_caste_json": json.dumps(socio_caste),
            "caste_pie_json": json.dumps(caste_pie),
            "socio_nrega_json": json.dumps(socio_nrega),
            "nrega_pie_json": json.dumps(nrega_pie),
            "fishery_potential_json": json.dumps(fishery_potential),
            "fishery_timeline_json": json.dumps(fishery_timeline),
            "agroforestry_transition_json": json.dumps(agroforestry_transition),
            "agroforestry_sankey_json": json.dumps(agroforestry_sankey),
        }

        return Response(context, status=status.HTTP_200_OK)

    except Exception as e:
        logger.exception("Exception in generate_tehsil_patterns_data api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@auth_free
@schema(None)
def generate_village_report(request):
    """
    Generate comprehensive village report with all sections.
    """
    state = request.GET.get("state")
    district = request.GET.get("district")
    block = request.GET.get("block")
    village_id = request.GET.get("villageId")

    if village_id == "0":
        return render(
            request,
            "village-report-unavailable.html",
            {
                "state": state,
                "district": district,
                "block": block,
            },
        )

    # Load all Excel sheets once — shared by every data function below
    df, df_facilities, df_nrega, df_livestock = load_block_sheets(
        state, district, block
    )

    # antyodaya sheet is mandatory — if it's missing the report cannot be built
    if df is None:
        return render(
            request,
            "village-report-unavailable.html",
            {
                "state": state,
                "district": district,
                "block": block,
                "village_id": village_id,
            },
        )

    # Check data availability for this specific village
    village_row = df[df["village_id"] == str(village_id).strip()]
    if village_row.empty or str(village_row.iloc[0].get("data_availability_status", "")).strip().lower() != "matched":
        return render(
            request,
            "village-report-unavailable.html",
            {
                "state": state,
                "district": district,
                "block": block,
                "village_id": village_id,
            },
        )

    # Get village polygon and info from GeoServer
    village_data = get_village_polygon_and_info(state, district, block, village_id)

    # Get Development Scores
    development_score = get_development_data(
        state,
        district,
        block,
        village_id,
        df=df,
        df_facilities=df_facilities,
        df_nrega=df_nrega,
    )
    block_development_score = get_block_development_data(
        state, district, block, df=df, df_facilities=df_facilities, df_nrega=df_nrega
    )

    # Calculate demographic data with percentages
    demographic_data = calculate_demographics(village_data["properties"])

    # Calculate Basic Infra
    _basic_infra_result = get_basic_infrastructure(
        state, district, block, village_id, df=df
    )
    basic_infra_data = _basic_infra_result.get("scores", [])
    basic_infra_raw_params = _basic_infra_result.get("raw_params", {})
    basic_infra_performance = _basic_infra_result.get("performance", [])

    # Calculate Health and Wash
    _health_wash_result   = get_health_and_wash(state, district, block, village_id, df=df, df_facilities=df_facilities)
    health_wash_data      = _health_wash_result.get("data", [])
    health_wash_raw_params = _health_wash_result.get("raw_params", {})
    health_wash_performance = _health_wash_result.get("performance", [
        "High" if (health_wash_data[0] if health_wash_data else 0) > 0.66 else "Low",
        "Low" if (health_wash_data[1] if len(health_wash_data) > 1 else 0) <= 0.33 else (
            "High" if (health_wash_data[1] if len(health_wash_data) > 1 else 0) > 0.66 else "Medium"
        ),
    ])
    health_wash_colors = _health_wash_result.get("colors", [])

    # Calculate Education Institutions
    education_data = get_education_institutions(
        state, district, block, village_id, df_facilities=df_facilities
    )

    # Calculate Financial Inclusion
    _finance_result      = get_financial_inclusion(state, district, block, village_id, df=df, df_facilities=df_facilities)
    finance_data         = _finance_result.get("data", [])
    finance_raw_params   = _finance_result.get("raw_params", {})
    finance_performance  = _finance_result.get("performance", [])
    finance_colors       = _finance_result.get("colors", [])

    # Calculate Welfare
    _welfare_result      = get_welfare_inclusion(state, district, block, village_id, df=df, df_facilities=df_facilities)
    welfare_data         = _welfare_result.get("data", [])
    welfare_raw_params   = _welfare_result.get("raw_params", {})
    welfare_performance  = _welfare_result.get("performance", [])
    welfare_colors       = _welfare_result.get("colors", [])

    # Calculate Community Institutions
    _community_result    = get_community_institutes(state, district, block, village_id, df=df)
    community_data       = _community_result.get("data", [])
    community_raw_params = _community_result.get("raw_params", {})
    community_performance = _community_result.get("performance", [])
    community_colors     = _community_result.get("colors", [])

    # Livelihood Diversification
    _livelihood_result      = get_livelihood_diversification(state, district, block, village_id, df=df)
    livelihood_data         = _livelihood_result.get("data", [])
    livelihood_raw_params   = _livelihood_result.get("raw_params", {})
    livelihood_performance  = _livelihood_result.get("performance", [])
    livelihood_colors       = _livelihood_result.get("colors", [])

    # Livestock Management
    _livestock_mgmt_result   = get_livestock_management(state, district, block, village_id, df=df, df_facilities=df_facilities)
    livestock_data           = _livestock_mgmt_result.get("data", [])
    livestock_raw_params     = _livestock_mgmt_result.get("raw_params", {})
    livestock_performance    = _livestock_mgmt_result.get("performance", [])
    livestock_colors         = _livestock_mgmt_result.get("colors", [])

    livestock_count_data = get_livestock_count(
        state, district, block, village_id, df_livestock=df_livestock
    )

    # Land Cultivation
    _land_result             = get_land_cultivation(state, district, block, village_id, df=df)
    land_cultivation_data    = _land_result.get("data", [])
    land_raw_params          = _land_result.get("raw_params", {})
    land_performance         = _land_result.get("performance", [])
    land_colors              = _land_result.get("colors", [])

    # Irrigation data
    _irrigation_result       = get_irrigation_Infra(state, district, block, village_id, df=df)
    irrigation_data          = _irrigation_result.get("data", [])
    irrigation_raw_params    = _irrigation_result.get("raw_params", {})

    # Agriculture Support
    _agri_result             = get_agri_support_service(state, district, block, village_id, df=df, df_facilities=df_facilities)
    agri_support_data        = _agri_result.get("data", [])
    agri_support_raw_params  = _agri_result.get("raw_params", {})
    agri_support_performance = _agri_result.get("performance", [])
    agri_support_colors      = _agri_result.get("colors", [])

    # Climate Resiliance
    _climate_result          = get_ecological_climate_resiliance(state, district, block, village_id, df=df, df_nrega=df_nrega)
    climate_resiliance_data  = _climate_result.get("data", [])
    climate_raw_params       = _climate_result.get("raw_params", {})

    # Map Data
    basic_infra_map = get_all_villages_basic_infrastructure(
        state, district, block, df=df, df_nrega=df_nrega
    )
    health_wash_map = get_all_villages_health_and_wash(state, district, block, df=df)
    education_map = get_all_villages_education_institutions(
        state, district, block, df_facilities=df_facilities, df_nrega=df_nrega
    )
    financial_map = get_all_villages_financial_inclusion(
        state, district, block, df=df, df_facilities=df_facilities, df_nrega=df_nrega
    )
    welfare_map = get_all_villages_welfare_inclusion(
        state, district, block, df=df, df_facilities=df_facilities, df_nrega=df_nrega
    )
    community_map = get_all_villages_community_institutes(state, district, block, df=df)
    livestock_map = get_all_villages_livestock_management(
        state, district, block, df=df, df_facilities=df_facilities
    )
    land_cultivation_map = get_all_villages_land_cultivation(
        state, district, block, df=df
    )
    irrigation_infra_map = get_all_villages_irrigation_infra(
        state, district, block, df=df
    )
    agri_support_map = get_all_villages_agri_support_service(
        state, district, block, df=df, df_facilities=df_facilities
    )
    climate_resiliance_map = get_all_villages_ecological_climate_resiliance(
        state, district, block, df=df, df_nrega=df_nrega
    )

    mws_ids = get_mwses_ids(state, district, block, village_id)
    mws_pattern_intensity = get_pattern_intensity(state, district, block)

    # Build context for template
    context = {
        # Location info
        "state": state,
        "district": district,
        "block": block,
        "village_id": village_id,
        "base_url": BASE_API_URL,
        "village_name": village_data["village_name"],
        "gram_panchayat_name": village_data["gram_panchayat_name"],
        "area_hectares": village_data["area_hectares"],
        "village_polygon": json.dumps(village_data["village_polygon"]),
        # RADAR CHART DATA
        "village_scores": json.dumps(development_score),
        "block_average_scores": json.dumps(block_development_score),
        # DEMOGRAPHIC DATA
        "demographic_data": demographic_data,
        # BASIC INFRASTRUCTURE DATA
        "basic_infra_data": json.dumps(basic_infra_data),
        "basic_infra_raw_params": basic_infra_raw_params,
        "basic_infra_performance": basic_infra_performance,
        # HEALTH AND WASH DATA
        "health_wash_data": json.dumps(health_wash_data),
        "health_wash_performance": health_wash_performance,
        "health_wash_colors": health_wash_colors,
        "health_wash_raw_params": health_wash_raw_params,
        # Education Data
        "education_data": json.dumps(education_data),
        # Finance Data
        "finance_data": json.dumps(finance_data),
        "finance_performance": finance_performance,
        "finance_colors": finance_colors,
        "finance_raw_params": finance_raw_params,
        # Welfare Inclusion
        "welfare_data": json.dumps(welfare_data),
        "welfare_performance": welfare_performance,
        "welfare_colors": welfare_colors,
        "welfare_raw_params": welfare_raw_params,
        # Community Institutions
        "community_data": json.dumps(community_data),
        "community_performance": community_performance,
        "community_colors": community_colors,
        "community_raw_params": community_raw_params,
        # Livelihood Diversification
        "livelihood_data": json.dumps(livelihood_data),
        "livelihood_performance": livelihood_performance,
        "livelihood_colors": livelihood_colors,
        "livelihood_raw_params": livelihood_raw_params,
        # Livestock Management
        "livestock_data": json.dumps(livestock_data),
        "livestock_performance": livestock_performance,
        "livestock_colors": livestock_colors,
        "livestock_raw_params": livestock_raw_params,
        "livestock_count_data": json.dumps(livestock_count_data),
        # Land Cultivation
        "land_cultivation_data": json.dumps(land_cultivation_data),
        "land_performance": land_performance,
        "land_colors": land_colors,
        "land_raw_params": land_raw_params,
        # Irrigation Data
        "irrigation_data": json.dumps(irrigation_data),
        "irrigation_raw_params": irrigation_raw_params,
        # Agri Support Data
        "agri_support_data": json.dumps(agri_support_data),
        "agri_support_performance": agri_support_performance,
        "agri_support_colors": agri_support_colors,
        "agri_support_raw_params": agri_support_raw_params,
        # Climate Data
        "climate_resiliance_data": json.dumps(climate_resiliance_data),
        "climate_raw_params": climate_raw_params,
        "village_polygon": json.dumps(village_data["village_polygon"]),
        "basic_infra_map": json.dumps(basic_infra_map),
        "health_wash_map": json.dumps(health_wash_map),
        "education_map": json.dumps(education_map),
        "financial_map": json.dumps(financial_map),
        "welfare_map": json.dumps(welfare_map),
        "community_map": json.dumps(community_map),
        "livestock_map": json.dumps(livestock_map),
        "land_cultivation_map": json.dumps(land_cultivation_map),
        "irrigation_infra_map": json.dumps(irrigation_infra_map),
        "agri_support_map": json.dumps(agri_support_map),
        "climate_resiliance_map": json.dumps(climate_resiliance_map),
        "mws_ids": json.dumps(mws_ids),
        "pattern_intensity": json.dumps(mws_pattern_intensity["intensity"]),
        "mws_active_patterns": json.dumps(mws_pattern_intensity["mws_active_patterns"]),
        "pattern_display_mapping": json.dumps(
            mws_pattern_intensity["pattern_display_mapping"]
        ),
    }

    return render(request, "village-report.html", context)


# ---------------------------------------------------------------------------
# DPR Data API
# ---------------------------------------------------------------------------

VALID_MAINTENANCE_TYPES = {"gw", "agri", "swb", "swb_rs"}


class DPRPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 200


def _get_plan_or_404(plan_id):
    plan = get_plan_details(plan_id)
    if plan is None:
        return None, Response(
            {"error": "Plan not found"}, status=status.HTTP_404_NOT_FOUND
        )
    return plan, None


def _paginated_response(request, data, serializer_class):
    paginator = DPRPagination()
    page = paginator.paginate_queryset(data, request)
    serializer = serializer_class(page, many=True)
    return paginator.get_paginated_response(serializer.data)


# MARK: DPR Summary
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_summary(request, plan_id):
    plan, err = _get_plan_or_404(plan_id)
    if err:
        return err
    data = get_dpr_summary(plan_id)
    data["plan_name"] = plan.plan
    data["village_name"] = plan.village_name
    return Response(DPRSummarySerializer(data).data)


# MARK: Section A
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_team_details(request, plan_id):
    plan, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return Response(TeamDetailsSerializer(get_team_details(plan)).data)


# MARK: Section B
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_village_brief(request, plan_id):
    plan, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return Response(VillageBriefSerializer(get_village_brief(plan)).data)


# MARK: Section C
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_settlements(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(
        request, get_settlements_data(plan_id), SettlementSerializer
    )


@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_crops(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(request, get_crops_data(plan_id), CropSerializer)


@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_livestock(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(
        request, get_livestock_data(plan_id), LivestockSerializer
    )


# MARK: Section D
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_wells(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(request, get_wells_data(plan_id), WellSerializer)


@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_waterbodies(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(
        request, get_waterbodies_data(plan_id), WaterbodySerializer
    )


# MARK: Section E
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_maintenance(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    maintenance_type = request.query_params.get("type", "gw")
    if maintenance_type not in VALID_MAINTENANCE_TYPES:
        return Response(
            {
                "error": f"Invalid type. Choose from: {', '.join(sorted(VALID_MAINTENANCE_TYPES))}"
            },
            status=status.HTTP_400_BAD_REQUEST,
        )
    return _paginated_response(
        request, get_maintenance_data(plan_id, maintenance_type), MaintenanceSerializer
    )


# MARK: Section F
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_nrm_works(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(request, get_nrm_works_data(plan_id), NRMWorkSerializer)


# MARK: Section G
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_livelihood(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return _paginated_response(
        request, get_livelihood_data(plan_id), LivelihoodSerializer
    )


# MARK: DPR Report Status Summary
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_report_status_summary(request):
    filters = {}
    for key in ("state_id", "district_id", "block_id"):
        val = request.query_params.get(key)
        if val:
            try:
                filters[key] = int(val)
            except ValueError:
                return Response(
                    {"error": f"'{key}' must be an integer"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
    org_id = request.query_params.get("organization_id")
    if org_id:
        try:
            filters["organization_id"] = str(uuid.UUID(org_id))
        except ValueError:
            return Response(
                {"error": "'organization_id' must be a valid UUID"},
                status=status.HTTP_400_BAD_REQUEST,
            )
    return Response(get_dpr_report_status_summary(filters))


# MARK: Global Status Tracking
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_global_status_tracking(request):
    filters = {}
    for key in ("state_id", "district_id", "block_id"):
        val = request.query_params.get(key)
        if val:
            try:
                filters[key] = int(val)
            except ValueError:
                return Response(
                    {"error": f"'{key}' must be an integer"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
    org_id = request.query_params.get("organization_id")
    if org_id:
        try:
            filters["organization_id"] = str(uuid.UUID(org_id))
        except ValueError:
            return Response(
                {"error": "'organization_id' must be a valid UUID"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    status_filter = request.query_params.get("status")
    if status_filter:
        from .services import VALID_DEMAND_STATUSES

        if status_filter not in VALID_DEMAND_STATUSES:
            return Response(
                {
                    "error": f"Invalid status. Choose from: {', '.join(sorted(VALID_DEMAND_STATUSES))}"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        filters["status"] = status_filter

    return Response(get_global_status_tracking(filters))


# MARK: DPR Status Tracking
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def dpr_status_tracking(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err
    return Response(get_dpr_status_tracking(plan_id))


# MARK: DPR Report Workflow Status
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET", "PATCH"])
@schema(None)
def dpr_report_status(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err

    if request.method == "GET":
        data = get_dpr_report_status(plan_id)
        if data is None:
            return Response(
                {"error": "DPR report not found for this plan"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(data)

    result, error = patch_dpr_report_status(plan_id, request.data, request.user)
    if error:
        return Response({"error": error}, status=status.HTTP_400_BAD_REQUEST)
    return Response(result)


# MARK: Update Demand Status
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["PATCH"])
@schema(None)
def dpr_update_demand_status(request, plan_id):
    _, err = _get_plan_or_404(plan_id)
    if err:
        return err

    resource_type = request.data.get("resource_type")
    resource_id = request.data.get("resource_id")
    new_status = request.data.get("status")

    if not all([resource_type, resource_id, new_status]):
        return Response(
            {"error": "resource_type, resource_id, and status are required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    result, error = update_demand_status(
        plan_id, resource_type, resource_id, new_status
    )
    if error:
        return Response({"error": error}, status=status.HTTP_400_BAD_REQUEST)
    return Response(result)


# api to download csv and kml file of demand data
@api_security_check(auth_type="JWT_or_API_key", allowed_methods=["GET"])
@schema(None)
def export_yuktdhara(request):
    gp_id = request.query_params.get("gp_id")

    if not gp_id:
        return Response(
            {
                "success": False,
                "message": "gp_id is required",
            },
            status=400,
        )

    try:
        gp = GramPanchayat.objects.get(gram_panchayat_code=gp_id)

    except GramPanchayat.DoesNotExist:
        return Response(
            {
                "success": False,
                "message": "Gram Panchayat not found",
            },
            status=404,
        )

    plans_exists = PlanApp.objects.filter(
        gp_id=gp_id,
        enabled=True,
    ).exists()

    if not plans_exists:
        return Response(
            {
                "success": False,
                "message": "No plans mapped with this Gram Panchayat",
            },
            status=404,
        )

    gp_name = gp.gram_panchayat_name

    with tempfile.TemporaryDirectory() as temp_dir:

        csv_path = os.path.join(
            temp_dir,
            f"Yuktdhara_{gp_name}.csv",
        )

        kml_path = os.path.join(
            temp_dir,
            f"Yuktdhara_{gp_name}.kml",
        )

        zip_path = os.path.join(
            temp_dir,
            f"Yuktdhara_{gp_name}.zip",
        )

        fetch_data(gp_id, csv_path)

        csv_to_kml(csv_path, kml_path)

        with zipfile.ZipFile(zip_path, "w") as zipf:

            zipf.write(
                csv_path,
                arcname=os.path.basename(csv_path),
            )

            zipf.write(
                kml_path,
                arcname=os.path.basename(kml_path),
            )

        with open(zip_path, "rb") as f:

            response = HttpResponse(
                f.read(),
                content_type="application/zip",
            )

            response["Content-Disposition"] = (
                f'attachment; filename="Yuktdhara_{gp_name}.zip"'
            )

            return response
