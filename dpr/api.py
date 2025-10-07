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
    create_dpr_document,
    get_mws_ids_for_report,
    get_plan_details,
    send_dpr_email,
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
    get_cropping_intensity,
    get_double_cropping_area,
    get_drought_data,
    get_osm_data,
    get_soge_data,
    get_surface_Water_bodies_data,
    get_terrain_data,
    get_village_data,
    get_water_balance_data,
)
from .gen_report_download import render_pdf_with_firefox
from .utils import validate_email

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


@api_view(["POST"])
@auth_free
@schema(None)
def generate_dpr(request):
    try:
        plan_id = request.data.get("plan_id")
        email_id = request.data.get("email_id")

        logger.info(
            "Generating DPR for plan ID: %s and email ID: %s", plan_id, email_id
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

        doc = create_dpr_document(plan)

        mws_Ids = get_mws_ids_for_report(plan)

        mws_reports = []
        successful_mws_ids = []

        state = str(plan.state.state_name).lower().replace(" ", "_")
        district = str(plan.district.district_name).lower().replace(" ", "_")
        block = str(plan.block.block_name).lower().replace(" ", "_")

        for ids in mws_Ids:
            try:
                report_html_url = (
                    f"https://geoserver.core-stack.org/api/v1/generate_mws_report/"
                    f"?state={state}&district={district}&block={block}&uid={ids}"
                )
                mws_report = render_pdf_with_firefox(report_html_url)
                mws_reports.append(mws_report)
                successful_mws_ids.append(ids)
            except Exception as e:
                logger.error(f"Failed to generate MWS report for ID {ids}: {e}")

        resource_report = None
        resource_html_url = (
            f"https://geoserver.core-stack.org/api/v1/generate_resource_report/"
            f"?district={district}&block={block}&plan_id={plan_id}"
        )

        try:
            resource_report = render_pdf_with_firefox(resource_html_url)
        except Exception as e:
            logger.error(f"Failed to generate resource report: {e}")

        send_dpr_email(
            doc=doc,
            email_id=email_id,
            plan_name=plan.plan,
            mws_reports=mws_reports,
            mws_Ids=successful_mws_ids,
            resource_report=resource_report,
            resource_report_url=resource_html_url,
        )

        return Response(
            {
                "message": f"DPR generated successfully and sent to the email ID: {email_id}"
            },
            status=status.HTTP_201_CREATED,
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
        double_crop_des = get_double_cropping_area(
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
        }

        # print("Api Processing End 1", datetime.now())

        return render(request, "mws-report.html", context)

    except Exception as e:
        logger.exception("Exception in generate_mws_report api :: ", e)
        return render(request, "error-page.html", {})


@api_view(["POST"])
@auth_free
@schema(None)
def generate_multi_report(request):
    try:
        # ? district, block
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        data = json.loads(request.body)

        # Extract the two lists
        filters = data.get("filters", [])
        mwsList = data.get("mwsList", [])

        # ? Block Overview of selected MWS and filter
        mws_desc = get_mws_data(
            result["state"], result["district"], result["block"], mwsList, filters
        )

        # ? Terrain Overview
        terrain_desc = get_terrain_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? LULC Overview
        lulc_desc = get_lulc_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Land Degradtion Overview
        land_degrad_desc = get_degrad_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Tree Cover Reduction Overview
        tree_reduce_desc = get_reduction_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Urbanization Overview
        urban_desc = get_urban_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Cropping Intensity
        inten_desc1, inten_desc2, inten_desc3, single, double, triple, uncrop = (
            get_cropping_mws_data(
                result["state"], result["district"], result["block"], mwsList
            )
        )

        # ? Surface Water bodies Overview
        swb_desc, rabi_desc, kh_desc_1, kh_desc_2, kh_desc_3 = get_surface_wb_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Water balance Overview
        deltag_desc, good_rainfall_desc, bad_rainfall_desc = get_water_balance_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        # ? Drought Overview
        get_drought_mws_data(
            result["state"], result["district"], result["block"], mwsList
        )

        context = {
            "district": result["district"],
            "block": result["block"],
            "mwsList": json.dumps(mwsList),
            "block_osm": mws_desc,
            "terrain_desc": terrain_desc,
            "lulc_desc": lulc_desc,
            "land_degrad_desc": land_degrad_desc,
            "tree_reduce_desc": tree_reduce_desc,
            "urban_desc": urban_desc,
            "inten_desc1": inten_desc1,
            "inten_desc2": inten_desc2,
            "inten_desc3": inten_desc3,
            "single": json.dumps(single),
            "double": json.dumps(double),
            "triple": json.dumps(triple),
            "uncrop": json.dumps(uncrop),
            "swb_desc": swb_desc,
            "rabi_desc": rabi_desc,
            "kh_desc_1": kh_desc_1,
            "kh_desc_2": kh_desc_2,
            "kh_desc_3": kh_desc_3,
            "deltag_desc": deltag_desc,
            "good_rainfall_desc": good_rainfall_desc,
            "bad_rainfall_desc": bad_rainfall_desc,
        }

        return render(request, "multi-mws-report.html", context)

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
