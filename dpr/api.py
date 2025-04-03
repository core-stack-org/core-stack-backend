from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.shortcuts import render

import json
from .gen_dpr import create_dpr_document, get_plan, send_dpr_email
from .gen_mws_report import (
    get_osm_data,
    get_terrain_data,
    get_change_detection_data,
    get_double_cropping_area,
    get_surface_Water_bodies_data,
    get_water_balance_data,
    get_cropping_intensity,
    get_drought_data,
    get_village_data,
)
from .utils import validate_email
from utilities.logger import setup_logger


logger = setup_logger(__name__)


@api_view(["POST"])
def generate_dpr(request):
    try:
        plan_id = request.data.get("plan_id")
        email_id = request.data.get("email_id")

        logger.info(
            "Generating DPR for plan ID: %s and email ID: %s", plan_id, email_id
        )

        check_email = validate_email(email_id)

        if not check_email:
            return Response(
                {"error": "Invalid email address"}, status=status.HTTP_400_BAD_REQUEST
            )

        plan = get_plan(plan_id)
        logger.info("Plan found: %s", plan)
        if plan is None:
            return Response(
                {"error": "Plan not found"}, status=status.HTTP_404_NOT_FOUND
            )

        doc = create_dpr_document(plan)
        send_dpr_email(doc, email_id, plan.plan)

        return Response(
            {
                "message": f"DPR generated successfully and sent to the email ID: {email_id}"
            },
            status=status.HTTP_201_CREATED,
        )

    except Exception as e:
        logger.exception("Exception in generate_dpr api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
def generate_mws_report(request):
    try:
        # ? district, block, mwsId
        params = request.GET
        result = {}

        for key, value in params.items():
            result[key] = value

        # ? OSM description generation
        parameter_block, parameter_mws = get_osm_data(
            result["district"], result["block"], result["uid"]
        )

        # ? Terrain Description generation
        (
            terrain_mws,
            mws_areas,
            block_areas,
            terrain_comp,
            terrain_land_use,
            terrain_mws_slope,
            terrain_block_slope,
            terrain_mws_plain,
            terrain_block_plain,
        ) = get_terrain_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Degradation Description generation
        land_degrad, tree_degrad, urbanization = get_change_detection_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Double Cropping Description Generation
        double_crop_des = get_double_cropping_area(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Surface Waterbody Description
        swb_desc, trend_desc, final_desc, kharif_data, rabi_data, zaid_data = (
            get_surface_Water_bodies_data(
                result["state"], result["district"], result["block"], result["uid"]
            )
        )

        # ? Water Balance Description
        wb_desc, precip_data, runoff_data, et_data, dg_data = get_water_balance_data(
            result["state"], result["district"], result["block"], result["uid"]
        )

        # ? Drought Description
        drought_desc, drought_weeks = get_drought_data(
            result["state"], result["district"], result["block"], result["uid"]
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

        get_cropping_intensity(
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
            "double_crop_des": double_crop_des,
            "swb_desc": swb_desc,
            "swb_season_desc": final_desc,
            "drought_desc": drought_desc,
            "mws_areas": json.dumps(mws_areas),
            "block_areas": json.dumps(block_areas),
            "terrain_mws_slope": json.dumps(terrain_mws_slope),
            "terrain_block_slope": json.dumps(terrain_block_slope),
            "terrain_mws_plain": json.dumps(terrain_mws_plain),
            "terrain_block_plain": json.dumps(terrain_block_plain),
            "trend_desc": json.dumps(trend_desc),
            "kharif_data": json.dumps(kharif_data),
            "rabi_data": json.dumps(rabi_data),
            "zaid_data": json.dumps(zaid_data),
            "wb_desc": json.dumps(wb_desc),
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
            "villages_name": json.dumps(villages_name),
            "villages_sc": json.dumps(villages_sc),
            "villages_st": json.dumps(villages_st),
            "villages_pop": json.dumps(villages_pop),
        }

        return render(request, "mws-report.html", context)

    except Exception as e:
        logger.exception("Exception in generate_mws_report api :: ", e)
        return render(request, "error-page.html", {})
