import json
from datetime import date, datetime

from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.urls import reverse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response

from utilities.auth_check_decorator import api_security_check
from utilities.auth_utils import auth_free
from utilities.logger import setup_logger

from .gen_dpr import (
    get_plan_details,
)
from .gen_multi_mws_report import (
    get_cropping_mws_data,
    get_degrad_mws_data,
    get_drought_mws_data,
    get_lulc_mws_data,
    get_mws_data,
    get_reduction_mws_data,
    get_surface_wb_mws_data,
    get_terrain_mws_data,
    get_urban_mws_data,
    get_water_balance_mws_data,
)
from .gen_mws_report import (
    get_change_detection_data,
    get_land_conflict_industrial_data,
    get_cropping_intensity,
    get_double_cropping_area,
    get_drought_data,
    get_osm_data,
    get_soge_data,
    get_surface_Water_bodies_data,
    get_terrain_data,
    get_village_data,
    get_water_balance_data,
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
from .gen_report_download import render_pdf_with_firefox
from .utils import validate_email, transform_name
from .tasks import generate_dpr_task

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
@auth_free
@schema(None)
def generate_dpr(request):
    try:
        plan_id = request.data.get("plan_id")
        email_id = request.data.get("email_id")
        regenerate = request.data.get("regenerate", False)

        logger.info(
            "Generating DPR for plan ID: %s and email ID: %s (regenerate=%s)",
            plan_id, email_id, regenerate
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
        # ? district, block, mwsId
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        # print("Api Processing End 1", datetime.now())

        # ? OSM description generation
        parameter_block, parameter_mws = get_osm_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

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
        ) = get_terrain_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Degradation Description generation
        land_degrad, tree_degrad, urbanization, restore_desc = (
            get_change_detection_data(
                result["state"], result["district"], result["block"], result["uid"]
            )
        )

        # ? Double Cropping Description Generation
        double_crop_des, year_range_text = get_double_cropping_area(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Surface Waterbody Description
        (
            swb_desc,
            trend_desc,
            final_desc,
            kharif_data,
            rabi_data,
            zaid_data,
            water_years,
        ) = get_surface_Water_bodies_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Water Balance Description
        (
            wb_desc,
            good_rainfall,
            bad_rainfall,
            precip_data,
            runoff_data,
            et_data,
            dg_data,
            wb_years,
        ) = get_water_balance_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? SOGE Description
        soge_desc = get_soge_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Drought Description
        drought_desc, drought_weeks, mod_drought, sev_drought, drysp_all, dg_years = (
            get_drought_data(
                result["state"], result["district"], result["block"], result["uid"]
            )
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
        ) = get_village_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Cropping Intensity Description
        inten_desc1, inten_desc2, single, double, triple, uncrop, crop_years = (
            get_cropping_intensity(
                result["state"], result["district"], result["block"], result["uid"]
            )
        )

        # ? LCW and Industrial Data Description
        lcw_desc = get_land_conflict_industrial_data(
            result["state"], result["district"], result["block"], result["uid"]
        )
        factory_desc = get_factory_data(
            result["state"], result["district"], result["block"], result["uid"]
        )
        mining_desc = get_mining_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        green_credits = get_green_credit_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        context = {
            "district": result["district"],
            "block": result["block"],
            "mws_id": result["uid"],
            "block_osm": parameter_block,
            "mws_osm": parameter_mws,
            "terrain_mws": terrain_mws,
            "terrain_comp": terrain_comp,
            "terrain_land_use": terrain_land_use,
            "land_degrad": land_degrad,
            "tree_degrad": tree_degrad,
            "urbanization": urbanization,
            "restore_desc": restore_desc,
            "double_crop_des": double_crop_des,
            "year_range_text": year_range_text,
            "swb_desc": swb_desc,
            "trend_desc": trend_desc,
            "swb_season_desc": final_desc,
            "wb_desc": wb_desc,
            "good_rainfall": good_rainfall,
            "bad_rainfall": bad_rainfall,
            "drought_desc": drought_desc,
            "inten_desc1": inten_desc1,
            "inten_desc2": inten_desc2,
            "soge_desc": soge_desc,
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
            "villages_sc": json.dumps(villages_sc),
            "villages_st": json.dumps(villages_st),
            "villages_pop": json.dumps(villages_pop),
            "single": json.dumps(single),
            "double": json.dumps(double),
            "triple": json.dumps(triple),
            "uncrop": json.dumps(uncrop),
            "crop_years": json.dumps(crop_years),
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
@auth_free
@schema(None)
def generate_resource_report(request):
    try:
        # ? district, block, plan_id
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        context = {
            "district": result["district"],
            "block": result["block"],
            "plan_id": result["plan_id"],
            "plan_name": result["plan_name"],
        }

        return render(request, "resource-report.html", context)
    except Exception as e:
        logger.exception("Exception in generate_resource_report api :: ", e)
        return render(request, "error-page.html", {})


@api_view(["GET"])
@auth_free
@schema(None)
def download_mws_report(request):
    # Require the usual params, but render from your external domain
    required = ("state", "district", "block", "uid")
    missing = [k for k in required if k not in request.GET]
    if missing:
        return HttpResponseBadRequest(f"Missing query params: {', '.join(missing)}")

    qs = request.GET.urlencode()
    report_html_url = (
        f"https://geoserver.core-stack.org/api/v1/generate_mws_report/?{qs}"
    )
    pdf_bytes = render_pdf_with_firefox(report_html_url)

    filename = f"mws_report_{request.GET.get('uid')}.pdf"
    resp = HttpResponse(pdf_bytes, content_type="application/pdf")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


@api_view(["GET"])
@auth_free
@schema(None)
@api_security_check(auth_type="Auth_free")
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
        mws_pattern_intensity = get_pattern_intensity(
            result["state"], result["district"], result["block"]
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

        context = {
            "district": result["district"],
            "block": result["block"],
            "block_osm": parameter_block,
            "mws_pattern_intensity_json": json.dumps(mws_pattern_intensity),
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
