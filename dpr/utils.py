import json
import re
import warnings

import pytz
import requests
from django.db.models import Max
from django.utils import timezone

from nrm_app.settings import ODK_PASSWORD, ODK_USERNAME
from utilities.constants import (
    ODK_URL_agri,
    ODK_URL_crop,
    ODK_URL_gw,
    ODK_URL_livelihood,
    ODK_URL_settlement,
    ODK_URL_waterbody,
    ODK_URL_well,
    ODK_URL_AGRI_MAINTENANCE,
    ODK_URL_GW_MAINTENANCE,
    ODK_URL_RS_WATERBODY_MAINTENANCE,
    ODK_URL_WATERBODY_MAINTENANCE,
)

from .models import (
    ODK_agri,
    ODK_crop,
    ODK_groundwater,
    ODK_livelihood,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    Agri_maintenance,
    GW_maintenance,
    SWB_maintenance,
    SWB_RS_maintenance,
)

warnings.filterwarnings("ignore")


def get_url(geoserver_url, workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{geoserver_url}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    return geojson_url


def get_url_wms(layerName, dynamicBbox):
    geojson_url_wms = f"https://geoserver.core-stack.org:8443/geoserver/wms?service=WMS&version=1.1.0&request=GetMap&layers=${layerName}&bbox=${dynamicBbox}&width=768&height=330&srs=EPSG%3A4326&styles=&format=application/openlayers"
    return geojson_url_wms


def get_vector_layer_geoserver(geoserver_url, workspace, layer_name):
    """Fetch vector layer data from GeoServer and return as GeoJSON."""
    url = get_url(geoserver_url, workspace, layer_name)
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Check if the response content is not empty and is valid JSON
        if response.content:
            return response.json()
        else:
            print(f"Empty response for layer '{layer_name}'.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the vector layer '{layer_name}' from GeoServer: {e}")
        print(f"Request URL: {url}")
        if response is not None:
            print(f"Response status code: {response.status_code}")
            # print(f"Response content: {response.text}")
        return None


def sync_db_odk():
    print("sync settlement")
    sync_settlement()

    print("sync well")
    sync_well()

    print("sync waterbody")
    sync_waterbody()

    print("sync groundwater")
    sync_groundwater()

    print("sync agri")
    sync_agri()

    print("sync livelihood")
    sync_livelihood()

    print("sync cropping patterns")
    sync_cropping_pattern()

    print("sync maintenance data")
    sync_agri_maintenance()
    sync_gw_maintenance()
    sync_swb_maintenance()
    sync_swb_rs_maintenance()


def fetch_odk_data_sync(ODK_URL):
    """Fetch ODK data from the given ODK URL."""
    try:
        response = requests.get(ODK_URL, auth=(ODK_USERNAME, ODK_PASSWORD))
        response.raise_for_status()
        response_dict = json.loads(response.content)
        response_list = response_dict["value"]
        return response_list
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch ODK data from the given URL: {e}")
        return None


def sync_settlement():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_settlement)
    # print("ODK data settlement", odk_resp_list[:1])
    settlement = ODK_settlement()  # settlement obj for the db model

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_settlement.objects.aggregate(
            Max("submission_time")
        )["submission_time__max"]

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return

        settlement.settlement_id = record.get("Settlements_id", "")
        settlement.settlement_name = record.get("Settlements_name", "")
        settlement.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        settlement.submitted_by = record.get("__system", {}).get("submitterName", "")
        settlement.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        # settlement.latitude = (
        #     record.get("GPS_point", {})
        #     .get("point_mapsappearance", {})
        #     .get("coordinates", [])[1]
        # )
        # settlement.longitude = (
        #     record.get("GPS_point", {})
        #     .get("point_mapsappearance", {})
        #     .get("coordinates", [])[0]
        # )
        settlement.latitude, settlement.longitude = extract_coordinates(record)
        settlement.block_name = record.get("block_name", "")
        settlement.number_of_households = record.get("number_households", "") or 0
        settlement.largest_caste = record.get("select_one_type", "") or "None"
        settlement.smallest_caste = record.get("caste_group_single", "") or "None"
        settlement.settlement_status = record.get("caste_group_mixed", "") or "None"
        settlement.plan_id = record.get("plan_id", "") or "0"
        settlement.plan_name = record.get("plan_name", "") or "0"
        settlement.uuid = record.get("__id", "") or "0"
        settlement.system = record.get("__system", {})
        settlement.gps_point = record.get("GPS_point", {})

        settlement.farmer_family = record.get("farmer_family", {})
        settlement.livestock_census = record.get("Livestock_Census", {})

        mgnrega_info = record.get("MNREGA_INFORMATION", {})
        settlement.nrega_job_aware = mgnrega_info.get("NREGA_aware", "") or 0
        settlement.nrega_job_applied = mgnrega_info.get("NREGA_applied", "") or 0
        settlement.nrega_job_card = mgnrega_info.get("NREGA_have_job_card", "") or 0
        settlement.nrega_without_job_card = mgnrega_info.get("total_household", "") or 0
        settlement.nrega_work_days = mgnrega_info.get("NREGA_work_days", "") or 0
        settlement.nrega_past_work = mgnrega_info.get("work_demands", "") or "0"
        settlement.nrega_raise_demand = mgnrega_info.get("select_one_Y_N", "") or "0"
        settlement.nrega_demand = mgnrega_info.get("select_one_demands", "") or "0"
        settlement.nrega_issues = mgnrega_info.get("select_multiple_issues", "") or "0"
        settlement.nrega_community = (
            mgnrega_info.get("select_one_contributions", "") or "0"
        )
        settlement.data_settlement = record
        settlement.save()


def sync_well():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_well)
    # print("ODK data well", odk_resp_list[:1])
    well = ODK_well()  # well object

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_settlement.objects.aggregate(
            Max("submission_time")
        )["submission_time__max"]

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return

        well.well_id = record.get("well_id", "")
        well.uuid = record.get("__id", "") or "0"
        well.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        well.beneficiary_settlement = record.get("beneficiary_settlement", "") or "0"
        well.block_name = record.get("block_name", "") or "0"
        well.owner = record.get("select_one_owns", "") or "0"
        well.households_benefitted = record.get("households_benefited", "") or 0
        well.caste_uses = record.get("select_multiple_caste_use", "") or "0"

        well_usage = record.get("Well_usage", {})
        well_condition = record.get("Well_condition", {})
        well.is_functional = (
            well_usage.get("select_one_Functional_Non_functional", "")
            or "No Data Provided"
        )
        well.need_maintenance = (
            well_condition.get("select_one_maintenance", "") or "No Data Provided"
        )
        well.plan_id = record.get("plan_id", "") or "0"
        well.plan_name = record.get("plan_name", "") or "0"
        well.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            well.latitude = round(coordinates[1], 2)
            well.longitude = round(coordinates[0], 2)
        else:
            well.latitude = 0.0
            well.longitude = 0.0

        well.system = record.get("__system", {})
        well.gps_point = record.get("GPS_point", {})
        well.data_well = record
        well.save()


def sync_waterbody():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_waterbody)
    # print("ODK data waterbody", odk_resp_list[:1])
    waterbody = ODK_waterbody()

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_settlement.objects.aggregate(
            Max("submission_time")
        )["submission_time__max"]

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return

        waterbody.waterbody_id = record.get("waterbodies_id", "")
        waterbody.uuid = record.get("__id", "") or "0"
        waterbody.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        waterbody.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "0"
        )
        waterbody.block_name = record.get("block_name", "") or ""
        waterbody.who_manages = record.get("select_one_manages", "") or "0"
        waterbody.specify_other_manager = record.get("text_one_manages", "") or "0"
        waterbody.owner = record.get("select_one_owns", "") or "0"
        waterbody.caste_who_uses = record.get("select_multiple_caste_use", "") or "0"
        waterbody.household_benefitted = record.get("households_benefited", "") or 0
        waterbody.water_structure_type = (
            record.get("select_one_water_structure", "") or "0"
        )
        waterbody.water_structure_other = (
            record.get("select_one_water_structure_other", "") or "0"
        )

        waterbody.identified_by = (
            record.get("select_one_identified", "") or "No Data Provided"
        )
        waterbody.need_maintenance = (
            record.get("select_one_maintenance") or "No Data Provided"
        )
        waterbody.plan_id = record.get("plan_id", "") or "0"
        waterbody.plan_name = record.get("plan_name", "") or "0"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            waterbody.latitude = round(coordinates[1], 2)
            waterbody.longitude = round(coordinates[0], 2)
        else:
            waterbody.latitude = 0.0
            waterbody.longitude = 0.0

        waterbody.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        waterbody.system = record.get("__system", {})
        waterbody.gps_point = record.get("GPS_point", {})
        waterbody.data_waterbody = record
        waterbody.save()


def sync_groundwater():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_gw)
    recharge_st = ODK_groundwater()

    for record in odk_resp_list:
        recharge_st.recharge_structure_id = record.get("work_id", "")
        recharge_st.uuid = record.get("__id", "") or "0"
        recharge_st.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        recharge_st.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "0"
        )
        recharge_st.block_name = record.get("block_name", "") or ""
        recharge_st.work_type = record.get("TYPE_OF_WORK_ID", "") or ""
        recharge_st.plan_id = record.get("plan_id", "") or "0"
        recharge_st.plan_name = record.get("plan_name", "") or "0"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            recharge_st.latitude = coordinates[1]
            recharge_st.longitude = coordinates[0]
        else:
            recharge_st.latitude = "0"
            recharge_st.longitude = "0"
        recharge_st.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        recharge_st.system = record.get("__system", {})
        recharge_st.gps_point = record.get("GPS_point", {})
        work_types = ["Check_dam", "Loose_Boulder_Structure", "Trench_cum_bunds"]
        for work_type in work_types:
            if work_type in record:
                recharge_st.update_work_dimensions(
                    work_type=work_type, work_details=record[work_type]
                )
        recharge_st.data_groundwater = record
        if recharge_st.status_re.lower() != "rejected":
            print("SAVING RECHARGE STRUCTURE")
            recharge_st.save()


def sync_agri():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_agri)

    irrigation = ODK_agri()

    for record in odk_resp_list:
        irrigation.irrigation_work_id = record.get("work_id", "")
        irrigation.uuid = record.get("__id", "") or "0"
        irrigation.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        irrigation.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "0"
        )
        irrigation.block_name = record.get("block_name", "") or ""
        irrigation.work_type = record.get("TYPE_OF_WORK_ID", "") or ""
        irrigation.plan_id = record.get("plan_id", "") or "0"
        irrigation.plan_name = record.get("plan_name", "") or "0"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            irrigation.latitude = coordinates[1]
            irrigation.longitude = coordinates[0]
        else:
            irrigation.latitude = "0"
            irrigation.longitude = "0"
        irrigation.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        irrigation.system = record.get("__system", {})
        irrigation.gps_point = record.get("GPS_point", {})
        work_types = ["new_well", "Land_leveling", "Farm_pond"]
        for work_type in work_types:
            if work_type in record:
                irrigation.update_work_dimensions(
                    work_type=work_type, work_details=record[work_type]
                )
        irrigation.data_agri = record
        irrigation.save()


def sync_livelihood():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_livelihood)

    ODK_livelihood.objects.all().delete()

    for record in odk_resp_list:
        livelihood = ODK_livelihood()
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_livelihood.objects.aggregate(
            Max("submission_time")
        )["submission_time__max"]

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return
        livelihood.uuid = record.get("__id", "") or "0"
        livelihood.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        livelihood.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "0"
        )
        livelihood.block_name = record.get("block_name", "") or "0"
        livelihood.beneficiary_contact = (
            record.get("Beneficiary_Contact_Number", "") or "0"
        )
        livelihood.livestock_development = (
            record.get("select_one_promoting_livestock", "") or "0"
        )
        livelihood.fisheries = record.get("select_one_promoting_fisheries", "") or "0"
        livelihood.common_asset = record.get("select_one_common_asset", "") or "0"
        livelihood.plan_id = record.get("plan_id", "") or "0"
        livelihood.plan_name = record.get("plan_name", "") or "0"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            livelihood.latitude = coordinates[1]
            livelihood.longitude = coordinates[0]
        else:
            livelihood.latitude = "0"
            livelihood.longitude = "0"

        livelihood.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        livelihood.system = record.get("__system", {})
        livelihood.gps_point = record.get("GPS_point", {})
        livelihood.data_livelihood = record
        livelihood.save()


def sync_cropping_pattern():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_crop)
    print("ODK data cropping pattern", odk_resp_list[:1])
    cropping_pattern = ODK_crop()

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_crop.objects.aggregate(Max("submission_time"))[
            "submission_time__max"
        ]

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return
        cropping_pattern.crop_grid_id = record.get("crop_Grid_id", "")
        cropping_pattern.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        cropping_pattern.uuid = record.get("__id", "") or "None"
        cropping_pattern.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "None"
        )
        cropping_pattern.irrigation_source = (
            record.get("select_multiple_widgets", "") or "None"
        )
        cropping_pattern.land_classification = (
            record.get("select_one_classified", "") or "None"
        )

        # For Kharif season, check if 'other' is selected and use the _other field if it is
        kharif_crops = record.get("select_multiple_cropping_kharif", "")
        if kharif_crops and "other" in kharif_crops.lower():
            kharif_other = record.get("select_multiple_cropping_kharif_other", "")
            if kharif_other:
                cropping_pattern.cropping_patterns_kharif = (
                    kharif_crops + ": " + kharif_other
                )
            else:
                cropping_pattern.cropping_patterns_kharif = kharif_crops
        else:
            cropping_pattern.cropping_patterns_kharif = kharif_crops or "None"

        # For Rabi season, check if 'other' is selected and use the _other field if it is
        rabi_crops = record.get("select_multiple_cropping_Rabi", "")
        if rabi_crops and "other" in rabi_crops.lower():
            rabi_other = record.get("select_multiple_cropping_Rabi_other", "")
            if rabi_other:
                cropping_pattern.cropping_patterns_rabi = rabi_crops + ": " + rabi_other
            else:
                cropping_pattern.cropping_patterns_rabi = rabi_crops
        else:
            cropping_pattern.cropping_patterns_rabi = rabi_crops or "None"

        # For Zaid season, check if 'other' is selected and use the _other field if it is
        zaid_crops = record.get("select_multiple_cropping_Zaid", "")
        if zaid_crops and "other" in zaid_crops.lower():
            zaid_other = record.get("select_multiple_cropping_Zaid_other", "")
            if zaid_other:
                cropping_pattern.cropping_patterns_zaid = zaid_crops + ": " + zaid_other
            else:
                cropping_pattern.cropping_patterns_zaid = zaid_crops
        else:
            cropping_pattern.cropping_patterns_zaid = zaid_crops or "None"

        cropping_pattern.agri_productivity = (
            record.get("select_one_productivity", "") or "None"
        )
        cropping_pattern.plan_id = record.get("plan_id", "") or "0"
        cropping_pattern.plan_name = record.get("plan_name", "") or "0"
        cropping_pattern.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        cropping_pattern.system = record.get("__system", {})
        cropping_pattern.data_crop = record
        cropping_pattern.save()


def sync_agri_maintenance():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_AGRI_MAINTENANCE)
    print(f"ODK data agri maintenance: {len(odk_resp_list)} records found")

    Agri_maintenance.objects.all().delete()

    for record in odk_resp_list:
        agri_maintenance = Agri_maintenance()
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )

        agri_maintenance.uuid = record.get("__id", "") or "0"
        agri_maintenance.work_id = record.get("work_id", "") or "0"
        agri_maintenance.corresponding_work_id = (
            record.get("corresponding_work_id", "") or "0"
        )
        agri_maintenance.plan_id = record.get("plan_id", "") or "0"
        agri_maintenance.plan_name = record.get("plan_name", "") or "0"
        agri_maintenance.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            agri_maintenance.latitude = round(coordinates[1], 2)
            agri_maintenance.longitude = round(coordinates[0], 2)
        else:
            agri_maintenance.latitude = 0.0
            agri_maintenance.longitude = 0.0
        agri_maintenance.data_agri_maintenance = record
        agri_maintenance.save()


def sync_gw_maintenance():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_GW_MAINTENANCE)
    print(f"ODK data gw maintenance: {len(odk_resp_list)} records found")

    GW_maintenance.objects.all().delete()

    for record in odk_resp_list:
        gw_maintenance = GW_maintenance()
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )

        gw_maintenance.uuid = record.get("__id", "") or "0"
        gw_maintenance.work_id = record.get("work_id", "") or "0"
        gw_maintenance.corresponding_work_id = (
            record.get("corresponding_work_id", "") or "0"
        )
        gw_maintenance.plan_id = record.get("plan_id", "") or "0"
        gw_maintenance.plan_name = record.get("plan_name", "") or "0"
        gw_maintenance.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            gw_maintenance.latitude = round(coordinates[1], 2)
            gw_maintenance.longitude = round(coordinates[0], 2)
        else:
            gw_maintenance.latitude = 0.0
            gw_maintenance.longitude = 0.0
        gw_maintenance.data_gw_maintenance = record
        gw_maintenance.save()
    print(f"Synced {GW_maintenance.objects.count()} GW_maintenance records")


def sync_swb_maintenance():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_WATERBODY_MAINTENANCE)
    print(f"ODK data swb maintenance: {len(odk_resp_list)} records found")

    SWB_maintenance.objects.all().delete()

    for record in odk_resp_list:
        swb_maintenance = SWB_maintenance()
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )

        swb_maintenance.uuid = record.get("__id", "") or "0"
        swb_maintenance.work_id = record.get("work_id", "") or "0"
        swb_maintenance.corresponding_work_id = (
            record.get("corresponding_work_id", "") or "0"
        )
        swb_maintenance.plan_id = record.get("plan_id", "") or "0"
        swb_maintenance.plan_name = record.get("plan_name", "") or "0"
        swb_maintenance.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            swb_maintenance.latitude = round(coordinates[1], 2)
            swb_maintenance.longitude = round(coordinates[0], 2)
        else:
            swb_maintenance.latitude = 0.0
            swb_maintenance.longitude = 0.0
        swb_maintenance.data_swb_maintenance = record
        swb_maintenance.save()
    print(f"Synced {SWB_maintenance.objects.count()} SWB_maintenance records")


def sync_swb_rs_maintenance():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_RS_WATERBODY_MAINTENANCE)
    print(f"ODK data swb rs maintenance: {len(odk_resp_list)} records found")

    SWB_RS_maintenance.objects.all().delete()

    for record in odk_resp_list:
        swb_rs_maintenance = SWB_RS_maintenance()
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )

        swb_rs_maintenance.uuid = record.get("__id", "") or "0"
        swb_rs_maintenance.work_id = record.get("work_id", "") or "0"
        swb_rs_maintenance.corresponding_work_id = (
            record.get("corresponding_work_id", "") or "0"
        )
        swb_rs_maintenance.plan_id = record.get("plan_id", "") or "0"
        swb_rs_maintenance.plan_name = record.get("plan_name", "") or "0"
        swb_rs_maintenance.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            swb_rs_maintenance.latitude = round(coordinates[1], 2)
            swb_rs_maintenance.longitude = round(coordinates[0], 2)
        else:
            swb_rs_maintenance.latitude = 0.0
            swb_rs_maintenance.longitude = 0.0
        swb_rs_maintenance.data_swb_rs_maintenance = record
        swb_rs_maintenance.save()


def validate_email(emailid):
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if not re.match(email_regex, emailid):
        return False
    else:
        return True


def check_submission_time(record, model):
    submission_date = timezone.datetime.strptime(
        record.get("__system", {}).get("submissionDate", ""), "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    submission_date = timezone.make_aware(submission_date, pytz.UTC)
    latest_submission_time = model.objects.aggregate(Max("submission_time"))[
        "submission_time__max"
    ]

    if latest_submission_time and submission_date <= latest_submission_time:
        return False, True
    return True, False


def extract_coordinates(record):
    try:
        gps_point = record.get("GPS_point", {})
        if not gps_point:
            return None, None

        # Check for both possible key names
        maps_appearance = gps_point.get("point_mapsappearance") or gps_point.get(
            "point_mapappearance"
        )
        if not maps_appearance:
            return None, None

        coordinates = maps_appearance.get("coordinates", [])
        if len(coordinates) < 2:
            return None, None

        return coordinates[1], coordinates[0]  # latitude, longitude
    except (AttributeError, IndexError, TypeError):
        return None, None


def format_text(text):
    """
    Converts text with underscores to properly formatted text.
    Example: 'Delayed_payments_for_works' -> 'Delayed Payments For Works'
    """
    if not text:
        return ""

    formatted_text = text.replace("_", " ")
    return formatted_text + "\n"
