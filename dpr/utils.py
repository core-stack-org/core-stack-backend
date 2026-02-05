import json
import re
import ssl
import socket
from io import BytesIO
import warnings

import pytz
import requests
from django.db.models import Max
from django.utils import timezone
from django.core.mail import EmailMessage
from django.core.mail.backends.smtp import EmailBackend

from nrm_app.settings import EMAIL_HOST, EMAIL_HOST_PASSWORD, EMAIL_HOST_USER, EMAIL_PORT, EMAIL_TIMEOUT, EMAIL_USE_SSL, ODK_PASSWORD, ODK_USERNAME
from utilities.constants import (
    ODK_URL_AGRI_MAINTENANCE,
    ODK_URL_GW_MAINTENANCE,
    ODK_URL_RS_WATERBODY_MAINTENANCE,
    ODK_URL_WATERBODY_MAINTENANCE,
    ODK_URL_agri,
    ODK_URL_crop,
    ODK_URL_gw,
    ODK_URL_livelihood,
    ODK_URL_settlement,
    ODK_URL_waterbody,
    ODK_URL_well,
)
from utilities.logger import setup_logger

from .models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_agri,
    ODK_crop,
    ODK_groundwater,
    ODK_livelihood,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    SWB_maintenance,
    SWB_RS_maintenance,
)

warnings.filterwarnings("ignore")

logger = setup_logger(__name__)


def get_url(geoserver_url, workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{geoserver_url}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    return geojson_url


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


# TODO: Align with the sync in moderation/utils/update_csdb.py
def sync_db_odk():
    sync_settlement()
    sync_well()
    sync_waterbody()
    sync_groundwater()
    sync_agri()
    sync_livelihood()
    sync_cropping_pattern()
    sync_agri_maintenance()
    sync_gw_maintenance()
    sync_swb_maintenance()
    sync_swb_rs_maintenance()
    logger.info("ODK data synced successfully")


def determine_caste_fields(record):
    """
    Determine caste group whether it's a Single Caste Group or Mixed Caste Group
    """
    count_sc = record.get("count_sc")
    count_st = record.get("count_st")
    count_obc = record.get("count_obc")
    count_general = record.get("count_general")

    caste_counts_mapping = {
        "SC": count_sc,
        "ST": count_st,
        "OBC": count_obc,
        "GENERAL": count_general,
    }

    valid_castes = []
    for caste, count in caste_counts_mapping.items():
        if count is not None and count != "":
            try:
                count_value = float(count) if isinstance(count, str) else count
                if count_value > 0:
                    valid_castes.append(caste)
            except (ValueError, TypeError):
                if count:
                    valid_castes.append(caste)

    if not valid_castes:
        return None, None, None, True

    if len(valid_castes) == 1:
        largest_caste = "Single Caste Group"
        smallest_caste = valid_castes[0]
        settlement_status = "NA"
        return largest_caste, smallest_caste, settlement_status, False

    else:
        largest_caste = "Mixed Caste Group"
        smallest_caste = "NA"
        settlement_status = ", ".join(sorted(valid_castes))
        return largest_caste, smallest_caste, settlement_status, False


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
    # print("ODK data settlement", odk_resp_list[:3])

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

        settlement_id = record.get("Settlements_id", "")

        # to preserve existing data
        settlement, created = ODK_settlement.objects.get_or_create(
            settlement_id=settlement_id,
            defaults={
                "settlement_name": "",
                "submission_time": submission_date,
                "submitted_by": "",
                "status_re": "in progress",
                "block_name": "",
                "number_of_households": 0,
                "largest_caste": "None",
                "smallest_caste": "None",
                "settlement_status": "None",
                "plan_id": "NA",
                "plan_name": "NA",
                "uuid": "NA",
                "system": {},
                "gps_point": {},
                "farmer_family": {},
                "livestock_census": {},
                "nrega_job_aware": 0,
                "nrega_job_applied": 0,
                "nrega_job_card": 0,
                "nrega_without_job_card": 0,
                "nrega_work_days": 0,
                "nrega_past_work": "NA",
                "nrega_raise_demand": "NA",
                "nrega_demand": "NA",
                "nrega_issues": "NA",
                "nrega_community": "NA",
                "data_settlement": {},
            },
        )

        def safe_update_field(
            field_name, odk_key, default_value=None, nested_path=None
        ):
            if nested_path:
                # for nested data like mgnrega_info.get("NREGA_aware")
                nested_data = record
                for key in nested_path:
                    nested_data = nested_data.get(key, {})
                    if not nested_data:
                        break
                if nested_data and odk_key in nested_data:
                    setattr(
                        settlement,
                        field_name,
                        nested_data.get(odk_key) or default_value,
                    )
            else:
                if odk_key in record:
                    value = record.get(odk_key)
                    if value is not None and value != "":
                        setattr(settlement, field_name, value)
                    elif default_value is not None:
                        setattr(settlement, field_name, default_value)

        settlement.settlement_name = record.get("Settlements_name", "")
        settlement.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        settlement.submitted_by = record.get("__system", {}).get("submitterName", "")
        settlement.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        settlement.latitude, settlement.longitude = extract_coordinates(record)
        settlement.block_name = record.get("block_name", "")
        settlement.number_of_households = record.get("number_households", "") or 0
        settlement.plan_id = record.get("plan_id", "") or "NA"
        settlement.plan_name = record.get("plan_name", "") or "NA"
        settlement.uuid = record.get("__id", "") or "NA"
        settlement.system = record.get("__system", {})
        settlement.gps_point = record.get("GPS_point", {})
        settlement.farmer_family = record.get("farmer_family", {})
        settlement.livestock_census = record.get("Livestock_Census", {})
        settlement.data_settlement = record

        largest_caste, smallest_caste, settlement_status, use_existing_logic = (
            determine_caste_fields(record)
        )
        if use_existing_logic:
            # update fields conditionally (preserve data if ODK field is unavailable)
            safe_update_field(
                "largest_caste", "select_one_type", "NA"
            )  # single or mixed
            safe_update_field(
                "smallest_caste", "caste_group_single", "NA"
            )  # single val
            safe_update_field(
                "settlement_status", "caste_group_mixed", "NA"
            )  # mixed val
        else:
            settlement.largest_caste = largest_caste
            settlement.smallest_caste = smallest_caste
            settlement.settlement_status = settlement_status

        mgnrega_info = record.get("MNREGA_INFORMATION", {})
        settlement.nrega_job_aware = mgnrega_info.get("NREGA_aware", "") or 0
        settlement.nrega_job_applied = mgnrega_info.get("NREGA_applied", "") or 0
        settlement.nrega_job_card = mgnrega_info.get("NREGA_have_job_card", "") or 0
        settlement.nrega_without_job_card = mgnrega_info.get("total_household", "") or 0
        settlement.nrega_work_days = mgnrega_info.get("NREGA_work_days", "") or 0
        settlement.nrega_past_work = mgnrega_info.get("work_demands", "") or "NA"
        settlement.nrega_raise_demand = mgnrega_info.get("select_one_Y_N", "") or "NA"
        settlement.nrega_demand = mgnrega_info.get("select_one_demands", "") or "NA"
        # Get NREGA issues and handle "other" option
        nrega_issues = mgnrega_info.get("select_multiple_issues", "") or "NA"
        if isinstance(nrega_issues, str) and "other" in nrega_issues.lower():
            other_text = mgnrega_info.get("select_multiple_issues_other", "")
            if other_text:
                if nrega_issues.lower() == "other":
                    nrega_issues = other_text
                else:
                    nrega_issues = f"{nrega_issues}"
        settlement.nrega_issues = nrega_issues
        settlement.nrega_community = (
            mgnrega_info.get("select_one_contributions", "") or "NA"
        )
        settlement.save()


def sync_well():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_well)
    # print("ODK data well", odk_resp_list[:1])
    well = ODK_well()

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)

        well.well_id = record.get("well_id", "")
        well.uuid = record.get("__id", "") or "NA"
        well.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )

        well.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        well.beneficiary_settlement = record.get("beneficiary_settlement", "") or "NA"
        well.block_name = record.get("block_name", "") or "NA"
        well.owner = record.get("select_one_owns", "") or "NA"
        well.households_benefitted = record.get("households_benefited", "") or 0
        well.caste_uses = record.get("select_multiple_caste_use", "") or "NA"

        well_usage = record.get("Well_usage", {})
        well_condition = record.get("Well_condition", {})
        well.is_functional = (
            well_usage.get("select_one_Functional_Non_functional", "") or "NA"
        )
        well.need_maintenance = well_usage.get("is_maintenance_required", "") or "NA"
        if well.need_maintenance == "NA":
            well.need_maintenance = well_condition.get("select_one_maintenance") or "NA"
        well.plan_id = record.get("plan_id", "") or "NA"
        well.plan_name = record.get("plan_name", "") or "NA"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            well.latitude = round(coordinates[1], 8)
            well.longitude = round(coordinates[0], 8)
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

        waterbody.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )

        waterbody.waterbody_id = record.get("waterbodies_id", "")
        waterbody.uuid = record.get("__id", "") or "NA"
        waterbody.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        waterbody.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "NA"
        )
        waterbody.block_name = record.get("block_name", "") or "NA"
        waterbody.who_manages = record.get("select_one_manages", "") or "NA"
        waterbody.specify_other_manager = record.get("text_one_manages", "") or "NA"
        waterbody.owner = record.get("select_one_owns", "") or "NA"
        waterbody.caste_who_uses = record.get("select_multiple_caste_use", "") or "NA"
        waterbody.household_benefitted = record.get("households_benefited", "") or 0
        waterbody.water_structure_type = (
            record.get("select_one_water_structure", "") or "NA"
        )
        waterbody.water_structure_other = (
            record.get("select_one_water_structure_other", "") or "NA"
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
            waterbody.latitude = round(coordinates[1], 8)
            waterbody.longitude = round(coordinates[0], 8)
        else:
            waterbody.latitude = 0.0
            waterbody.longitude = 0.0

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

        recharge_st.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )

        recharge_st.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "NA"
        )
        recharge_st.block_name = record.get("block_name", "") or "NA"
        recharge_st.work_type = record.get("TYPE_OF_WORK_ID", "") or "NA"
        recharge_st.plan_id = record.get("plan_id", "") or "NA"
        recharge_st.plan_name = record.get("plan_name", "") or "NA"
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
            recharge_st.latitude = 0.0
            recharge_st.longitude = 0.0
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
            recharge_st.save()


def sync_agri():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_agri)

    irrigation = ODK_agri()

    for record in odk_resp_list:
        irrigation.irrigation_work_id = record.get("work_id", "")
        irrigation.uuid = record.get("__id", "") or "0"

        irrigation.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )

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
            irrigation.latitude = 0.0
            irrigation.longitude = 0.0

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
            livelihood.latitude = 0.0
            livelihood.longitude = 0.0

        livelihood.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )
        livelihood.system = record.get("__system", {})
        livelihood.gps_point = record.get("GPS_point", {})
        livelihood.data_livelihood = record
        livelihood.save()


def sync_cropping_pattern():
    odk_resp_list = fetch_odk_data_sync(ODK_URL_crop)

    for record in odk_resp_list:
        submission_date = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        submission_date = timezone.make_aware(submission_date, pytz.UTC)
        latest_submission_time = ODK_crop.objects.aggregate(Max("submission_time"))[
            "submission_time__max"
        ]

        cropping_pattern = ODK_crop()
        cropping_pattern.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )

        # if latest_submission_time and submission_date <= latest_submission_time:
        #     print("The DB is already synced with the latest submissions")
        #     return
        uuid_val = record.get("__id", "") or "NA"
        crop_grid_id = record.get("crop_Grid_id", "")
        cropping_pattern.crop_grid_id = crop_grid_id if crop_grid_id and crop_grid_id != "undefined" else uuid_val
        cropping_pattern.submission_time = timezone.datetime.strptime(
            record.get("__system", {}).get("submissionDate", ""),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        )
        cropping_pattern.uuid = uuid_val
        cropping_pattern.beneficiary_settlement = (
            record.get("beneficiary_settlement", "") or "NA"
        )
        cropping_pattern.irrigation_source = (
            record.get("select_multiple_widgets", "") or "NA"
        )
        cropping_pattern.land_classification = (
            record.get("select_one_classified", "") or "NA"
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
            cropping_pattern.cropping_patterns_kharif = kharif_crops or "NA"

        # For Rabi season, check if 'other' is selected and use the _other field if it is
        rabi_crops = record.get("select_multiple_cropping_Rabi", "")
        if rabi_crops and "other" in rabi_crops.lower():
            rabi_other = record.get("select_multiple_cropping_Rabi_other", "")
            if rabi_other:
                cropping_pattern.cropping_patterns_rabi = rabi_crops + ": " + rabi_other
            else:
                cropping_pattern.cropping_patterns_rabi = rabi_crops
        else:
            cropping_pattern.cropping_patterns_rabi = rabi_crops or "NA"

        # For Zaid season, check if 'other' is selected and use the _other field if it is
        zaid_crops = record.get("select_multiple_cropping_Zaid", "")
        if zaid_crops and "other" in zaid_crops.lower():
            zaid_other = record.get("select_multiple_cropping_Zaid_other", "")
            if zaid_other:
                cropping_pattern.cropping_patterns_zaid = zaid_crops + ": " + zaid_other
            else:
                cropping_pattern.cropping_patterns_zaid = zaid_crops
        else:
            cropping_pattern.cropping_patterns_zaid = zaid_crops or "NA"

        cropping_pattern.agri_productivity = (
            record.get("select_one_productivity", "") or "NA"
        )
        cropping_pattern.plan_id = record.get("plan_id", "") or "NA"
        cropping_pattern.plan_name = record.get("plan_name", "") or "NA"
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

        agri_maintenance.status_re = (
            record.get("__system", {}).get("reviewState", "") or "in progress"
        )

        agri_maintenance.uuid = record.get("__id", "") or "0"
        agri_maintenance.work_id = record.get("work_id", "") or "0"
        agri_maintenance.corresponding_work_id = (
            record.get("corresponding_work_id", "") or "0"
        )
        agri_maintenance.plan_id = record.get("plan_id", "") or "0"
        agri_maintenance.plan_name = record.get("plan_name", "") or "0"
        try:
            coordinates = (
                record.get("GPS_point", {})
                .get("point_mapsappearance", {})
                .get("coordinates", [])
            )
        except AttributeError:
            coordinates = []
        if len(coordinates) >= 2:
            agri_maintenance.latitude = round(coordinates[1], 8)
            agri_maintenance.longitude = round(coordinates[0], 8)
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
            gw_maintenance.latitude = round(coordinates[1], 8)
            gw_maintenance.longitude = round(coordinates[0], 8)
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
            swb_maintenance.latitude = round(coordinates[1], 8)
            swb_maintenance.longitude = round(coordinates[0], 8)
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
            swb_rs_maintenance.latitude = round(coordinates[1], 8)
            swb_rs_maintenance.longitude = round(coordinates[0], 8)
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


def format_text_demands(text):
    """
    Helps in converting demands in proper text
    """
    if not text:
        return ""

    items = text.split()
    formatted_items = []

    for item in items:
        item_with_spaces = item.replace("_", " ")
        formatted_item = " ".join(
            word.capitalize() for word in item_with_spaces.split()
        )
        formatted_items.append(formatted_item)

    formatted_text = "\n".join(formatted_items)
    return formatted_text


def format_text(text):
    """
    Converts text with underscores to properly formatted text.
    Example: 'Delayed_payments_for_works' -> 'Delayed Payments For Works'
    """
    if not text:
        return ""

    formatted_text = text.replace("_", " ")
    return formatted_text.capitalize() + "\n\n"


def get_waterbody_repair_activities(data_waterbody, water_structure_type):
    """
    Extract repair activities based on water structure type from data_waterbody.
    Handles 'other' cases where the specific repair activity is in a separate field.

    Args:
        data_waterbody (dict): The nested waterbody data dictionary
        water_structure_type (str): The type of water structure

    Returns:
        str: The repair activities or "NA" if none found
    """
    if not data_waterbody or not water_structure_type:
        return "NA"

    structure_type_mapping = {
        "canal": "Repair_of_canal",
        "bunding": "Repair_of_bunding",
        "check dam": "Repair_of_check_dam",
        "farm bund": "Repair_of_farm_bund",
        "farm pond": "Repair_of_farm_ponds",
        "soakage pits": "Repair_of_soakage_pits",
        "recharge pits": "Repair_of_recharge_pits",
        "rock fill dam": "Repair_of_rock_fill_dam",
        "stone bunding": "Repair_of_stone_bunding",
        "community pond": "Repair_of_community_pond",
        "diversion drains": "Repair_of_diversion_drains",
        "large water body": "Repair_of_large_water_body",
        "model5 structure": "Repair_of_model5_structure",
        "percolation tank": "Repair_of_percolation_tank",
        "earthen gully plug": "Repair_of_earthen_gully_plug",
        "30-40 model structure": "Repair_of_30_40_model_structure",
        "loose boulder structure": "Repair_of_loose_boulder_structure",
        "trench cum bund network": "Repair_of_trench_cum_bund_network",
        "water absorption trenches": "Repair_of_Water_absorption_trenches",
        "drainage soakage channels": "Repair_of_drainage_soakage_channels",
        "staggered contour trenches": "Repair_of_Staggered_contour_trenches",
        "continuous contour trenches": "Repair_of_Continuous_contour_trenches",
    }

    structure_type_lower = water_structure_type.lower().strip()
    if structure_type_lower.startswith("other:"):
        repair_fields = [
            key for key in data_waterbody.keys() if key.startswith("Repair_of_")
        ]
        for field in repair_fields:
            if data_waterbody.get(field):
                repair_value = data_waterbody.get(field)
                other_field = field + "_other"
                if (
                    repair_value
                    and repair_value.lower() == "other"
                    and data_waterbody.get(other_field)
                ):
                    return f"Other: {data_waterbody.get(other_field)}"
                elif repair_value:
                    return repair_value.replace("_", " ").title()
        return "NA"

    repair_field = structure_type_mapping.get(structure_type_lower)

    if not repair_field:
        return "NA"

    repair_activity = data_waterbody.get(repair_field)

    if not repair_activity:
        return "NA"

    if repair_activity.lower() == "other":
        other_field = repair_field + "_other"
        other_value = data_waterbody.get(other_field)
        if other_value:
            return f"Other: {other_value}"
        else:
            return "Other"

    return repair_activity.replace("_", " ").title()


def sort_key(settlement):
    return (settlement == "NA", settlement.lower() if settlement != "NA" else "")


def transform_name(name):
    if not name:
        return name

    name = re.sub(r"[()]", "", name)
    name = re.sub(r"[-\s]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = re.sub(r"^_|_$", "", name)
    return name.lower()

def to_utf8(value):
    """Ensure value is a properly encoded UTF-8 string for Word document.
    
    Handles cases where UTF-8 text was incorrectly decoded as Latin-1,
    resulting in garbled characters like 'à²ªà²¾à²...' for Kannada/Hindi text.
    """
    if value is None:
        return "NA"
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.decode('latin-1')
    if not isinstance(value, str):
        value = str(value)
    try:
        return value.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return value


def send_dpr_email(
    doc,
    email_id,
    plan_name,
    mws_reports,
    mws_Ids,
    resource_report,
    resource_report_url,
    state_name="",
    district_name="",
    tehsil_name="",
):
    try:
        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        doc_bytes = buffer.getvalue()
        buffer.close()

        mws_table_html = ""
        if mws_reports and mws_Ids:
            mws_rows = "".join(
                f'<tr><td style="padding: 10px 16px; border-bottom: 1px solid #eee;">{mws_id}</td>'
                f'<td style="padding: 10px 16px; border-bottom: 1px solid #eee; text-align: center;">'
                f'<a href="{report_url}" style="color: #2563eb; text-decoration: none;">View Report</a></td></tr>'
                for mws_id, report_url in zip(mws_Ids, mws_reports)
            )
            mws_table_html = f"""
            <div style="margin: 24px 0;">
                <p style="font-weight: 600; color: #374151; margin-bottom: 12px;">MWS Reports</p>
                <table style="width: 100%; border-collapse: collapse; background: #f9fafb; border-radius: 8px; overflow: hidden;">
                    <thead>
                        <tr style="background: #f3f4f6;">
                            <th style="padding: 12px 16px; text-align: left; font-weight: 600; color: #374151;">MWS ID</th>
                            <th style="padding: 12px 16px; text-align: center; font-weight: 600; color: #374151;">Report</th>
                        </tr>
                    </thead>
                    <tbody>{mws_rows}</tbody>
                </table>
            </div>
            """

        email_body = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="margin: 0; padding: 0; background-color: #f3f4f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto; padding: 40px 20px;">
                <div style="background: #ffffff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden;">
                    <div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%); padding: 32px; text-align: center;">
                        <h1 style="color: #ffffff; margin: 0; font-size: 24px; font-weight: 600;">Detailed Project Report</h1>
                        <p style="color: #bfdbfe; margin: 12px 0 0 0; font-size: 16px; font-weight: 500;">{to_utf8(plan_name)}</p>
                        <p style="color: #93c5fd; margin: 8px 0 0 0; font-size: 13px;">{to_utf8(tehsil_name)} · {to_utf8(district_name)} · {to_utf8(state_name)}</p>
                    </div>
                    <div style="padding: 32px;">
                        <p style="color: #374151; font-size: 15px; line-height: 1.6; margin: 0 0 20px 0;">
                            Hi,<br><br>
                            Please find attached the Detailed Project Report for <strong>{to_utf8(plan_name)}</strong>.
                        </p>
                        {mws_table_html}
                        <div style="margin: 24px 0; padding: 16px; background: #f0fdf4; border-radius: 8px; border-left: 4px solid #22c55e;">
                            <p style="margin: 0; color: #166534; font-weight: 600;">Resource Report</p>
                            <a href="{resource_report_url}" style="color: #15803d; text-decoration: none; font-size: 14px;">View Report →</a>
                        </div>
                    </div>
                    <div style="background: #f9fafb; padding: 24px 32px; border-top: 1px solid #e5e7eb;">
                        <p style="margin: 0; color: #6b7280; font-size: 14px;">
                            Thanks and Regards,<br>
                            <strong style="color: #374151;">CoRE Stack Team</strong>
                        </p>
                    </div>
                </div>
                <p style="text-align: center; color: #9ca3af; font-size: 12px; margin-top: 24px;">
                    This is an automated email from CoRE Stack.
                </p>
            </div>
        </body>
        </html>
        """

        backend = EmailBackend(
            host=EMAIL_HOST,
            port=EMAIL_PORT,
            username=EMAIL_HOST_USER,
            password=EMAIL_HOST_PASSWORD,
            use_ssl=EMAIL_USE_SSL,
            timeout=EMAIL_TIMEOUT,
            ssl_context=ssl.create_default_context(),
        )

        email = EmailMessage(
            subject=f"DPR of plan: {plan_name}",
            body=email_body,
            from_email=EMAIL_HOST_USER,
            to=[email_id],
            connection=backend,
        )

        # Set content type to HTML
        email.content_subtype = "html"

        email.attach(
            f"DPR_{plan_name}.docx",
            doc_bytes,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        if resource_report is not None:
            email.attach(
                f"Resource Report_{plan_name}.pdf", resource_report, "application/pdf"
            )

        logger.info("Sending DPR email to %s", email_id)
        email.send(fail_silently=False)
        logger.info("DPR email sent.")
        backend.close()

    except socket.error as e:
        logger.error(f"Socket error: {e}")
    except ssl.SSLError as e:
        logger.error(f"SSL error: {e}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")