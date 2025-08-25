import csv
import json
import logging
import re

import requests

from datetime import datetime, timezone
import dateutil.parser

from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
from utilities.constants import (
    ODK_URL_agri,
    ODK_URL_gw,
    ODK_URL_livelihood,
    ODK_URL_settlement,
    ODK_URL_swb,
    ODK_URL_waterbody,
    ODK_URL_well,
    ODK_URL_SESSION,
)

logger = logging.getLogger(__name__)

_token_cache = {
    "token": None,
    "expires_at": None,
}


# MARK: Fetch ODK Data
def fetch_odk_data(csv_path, resource_type, block, plan_id):
    print("CSV path: ", csv_path)
    if resource_type == "settlement":
        print("inside settlement")
        odk_data(
            ODK_URL_settlement, csv_path, block, plan_id, resource_type="settlement"
        )

    elif resource_type == "well":
        print("inside well")
        odk_data(ODK_URL_well, csv_path, block, plan_id, resource_type="well")

    elif resource_type == "waterbody":
        print("inside waterbody")
        odk_data(ODK_URL_waterbody, csv_path, block, plan_id, resource_type="waterbody")

    elif resource_type == "plan_gw":
        print("inside ground water plan")
        odk_data(ODK_URL_gw, csv_path, block, plan_id, resource_type="plan_gw")

    elif resource_type == "main_swb":
        print("inside surface water bodies plan")
        odk_data(ODK_URL_swb, csv_path, block, plan_id, resource_type="main_swb")

    elif resource_type == "plan_agri":
        print("inside agri plan")
        odk_data(ODK_URL_agri, csv_path, block, plan_id, resource_type="plan_agri")

    elif resource_type == "livelihood":
        print("inside livelihood")
        odk_data(
            ODK_URL_livelihood, csv_path, block, plan_id, resource_type="livelihood"
        )

    return True


def odk_data(ODK_url, csv_path, block, plan_id, resource_type):
    request_obj_odk = requests.get(ODK_url, auth=(ODK_USERNAME, ODK_PASSWORD))
    response_dict = json.loads(request_obj_odk.content)
    response_list = response_dict["value"]
    logger.info(f"Fetched data from the ODK: {ODK_url}")
    all_keys = set()

    if resource_type == "settlement":
        modified_response_list = modify_response_list_settlement(
            response_list, block, plan_id
        )
    elif resource_type == "well":
        modified_response_list = modify_response_list_well(
            response_list, block, plan_id
        )
    elif resource_type == "waterbody":
        modified_response_list = modify_response_list_waterbody(
            response_list, block, plan_id
        )

    elif resource_type == "plan_gw":
        modified_response_list = modify_response_list_plan(
            response_list, block, plan_id
        )
        for item in modified_response_list:
            all_keys.update(extract_keys(item))
        fieldnames = list(all_keys)

    elif resource_type == "main_swb":
        modified_response_list = modify_response_list_plan(
            response_list, block, plan_id
        )
        for item in modified_response_list:
            all_keys.update(extract_keys(item))
        fieldnames = list(all_keys)

    elif resource_type == "plan_agri":
        modified_response_list = modify_response_list_plan(
            response_list, block, plan_id
        )
        for item in modified_response_list:
            all_keys.update(extract_keys(item))
        fieldnames = list(all_keys)

    elif resource_type == "livelihood":
        modified_response_list = modify_response_list_livelihood(
            response_list, block, plan_id
        )
        for item in modified_response_list:
            all_keys.update(extract_keys(item))
        fieldnames = list(all_keys)

    if not modified_response_list:
        print(f"No ODK data found for the given Plan ID: {plan_id}")
        return False

    if resource_type in ["settlement", "well", "waterbody"]:
        header_keys = modified_response_list[0].keys()
        print("FIELD NAMES", header_keys)
        with open(csv_path, "w", encoding="utf-8") as output_file:
            dict_writer = csv.DictWriter(
                output_file, fieldnames=header_keys, extrasaction="ignore"
            )
            dict_writer.writeheader()
            dict_writer.writerows(modified_response_list)
            logger.info(f"CSV generated for resource : {resource_type}")
    elif resource_type in ["plan_gw", "main_swb", "plan_agri", "livelihood"]:
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            dict_writer.writeheader()
            for item in modified_response_list:
                flattened_item = flatten_dict(item)
                dict_writer.writerow(flattened_item)
            logger.info(f"CSV generated for the work : {resource_type}")


# MARK: Modify ODK Settlement Data
def modify_response_list_settlement(res, block, plan_id):
    res_list = []
    print(f"block name: {block} and plan id: {plan_id}")
    for result in res:
        if result is None:
            continue

        if result.get("__system", {}).get("reviewState") == "rejected":
            continue

        try:
            if result.get("block_name").lower() != block.lower():
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude = None
        longitude = None

        if (
            isinstance(result, dict)
            and result.get("GPS_point") is not None
            and result["GPS_point"].get("point_mapsappearance") is not None
            and "coordinates" in result["GPS_point"]["point_mapsappearance"]
        ):
            try:
                latitude = result["GPS_point"]["point_mapsappearance"]["coordinates"][1]
                longitude = result["GPS_point"]["point_mapsappearance"]["coordinates"][
                    0
                ]
            except Exception as e:
                print(f"Could not get the coordinates for settlement: {e}")

        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["sett_id"] = result["Settlements_id"]
        result["sett_name"] = result["Settlements_name"]
        try:
            mgnrega_info = result.get("MNREGA_INFORMATION", {})
        except Exception as e:
            print(e)
            continue
        if mgnrega_info:
            result["job_aware"] = mgnrega_info.get("NREGA_aware", "") or 0
            result["job_applied"] = mgnrega_info.get("NREGA_applied", "") or 0
            result["job_card"] = mgnrega_info.get("NREGA_job_card", "") or 0
            result["without_jc"] = mgnrega_info.get("total_household", "") or 0
            result["work_days"] = mgnrega_info.get("NREGA_work_days", "") or 0
            result["past_work"] = mgnrega_info.get("q1", "") or "0"
            result["raise_demand"] = mgnrega_info.get("select_one_Y_N", "") or "0"
            result["demand"] = mgnrega_info.get("select_one_demands", "") or "0"
            result["issues"] = mgnrega_info.get("select_multiple_issues", "") or "0"
            result["community"] = (
                mgnrega_info.get("select_one_contributions", "") or "0"
            )
        res_list.append(result)
    return res_list


# MARK: Modify ODK Well Data
def modify_response_list_well(res, block, plan_id):
    res_list = []
    for result in res:
        if result is None:
            continue

        if result.get("__system", {}).get("reviewState") == "rejected":
            continue

        try:
            if result.get("block_name").lower() != block.lower():
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude = None
        longitude = None

        if (
            isinstance(result, dict)
            and result.get("GPS_point") is not None
            and result["GPS_point"].get("point_mapappearance") is not None
            and "coordinates" in result["GPS_point"]["point_mapappearance"]
        ):
            try:
                latitude = result["GPS_point"]["point_mapappearance"]["coordinates"][1]
                longitude = result["GPS_point"]["point_mapappearance"]["coordinates"][0]
            except Exception as e:
                print(f"Could not get the coordinates for settlement: {e}")

        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["well_id"] = result["well_id"]
        try:
            result["ben_settlement"] = result.get("beneficiary_settlement", "") or "0"
            who_owns = result["select_one_owns"]
            if who_owns:
                who_owns = str(who_owns).lower()
                if who_owns == "other" or who_owns == "any other":
                    result["owner"] = result.get("text_one_owns", "") or ""
                else:
                    result["owner"] = result.get("select_one_owns", "") or ""
            else:
                result["owner"] = "0"
            result["hh_benefitted"] = result.get("households_benefited", "") or "0"
            result["caste"] = result.get("select_multiple_caste_use", "") or "0"
            result["functional"] = (
                result.get("select_one_Functional_Non_functional", "") or 0
            )
            result["need_maintenance"] = result.get("select_one_maintenance", "") or "0"
            repair_value = result.get("select_one_repairs_well")
            if repair_value:
                repair_value = str(repair_value).lower()
                if repair_value == "other":
                    result["repair"] = (
                        result.get("select_one_repairs_well_other", "") or "0"
                    )
                else:
                    result["repair"] = repair_value
            else:
                result["repair"] = "0"
        except Exception as e:
            print("Exception occured in adding data from ODK to well layer: ", e)
            continue
        res_list.append(result)

    return res_list


# MARK: Modify ODK Waterbody Data
def modify_response_list_waterbody(res, block, plan_id):
    res_list = []
    for result in res:
        if result is None:
            continue
        if result.get("__system", {}).get("reviewState") == "rejected":
            continue
        try:
            if result.get("block_name").lower() != block.lower():
                continue
        except AttributeError:
            continue
        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude = None
        longitude = None
        if (
            isinstance(result, dict)
            and result.get("GPS_point") is not None
            and result["GPS_point"].get("point_mapappearance") is not None
            and "coordinates" in result["GPS_point"]["point_mapappearance"]
        ):
            try:
                latitude = result["GPS_point"]["point_mapappearance"]["coordinates"][1]
                longitude = result["GPS_point"]["point_mapappearance"]["coordinates"][0]
            except Exception as e:
                print(f"Could not get the coordinates for settlement: {e}")
        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["wb_id"] = result["waterbodies_id"]

        # type_of_water_st = result["select_one_water_structure"]
        # if type_of_water_st:
        #         type_of_water_st = str(type_of_water_st).lower()
        #         if type_of_water_st == "other":
        #             result["wbs_type"] = result.get("select_one_water_structure_other", "") or ""
        #         else:
        #             result["wbs_type"] = result.get("select_one_water_structure", "") or ""
        # else:
        #     result["wbs_type"] = "0"

        result["wbs_type"] = result.get("select_one_water_structure", "") or "0"

        try:
            manager = result["select_one_manages"]
            if manager:
                manager = str(manager).lower()
                if manager == "other":
                    result["who_manages"] = result.get("text_one_manages", "") or ""
                else:
                    result["who_manages"] = result.get("select_one_manages", "") or ""
            else:
                result["who_manages"] = "0"

            who_owns = result["select_one_owns"]
            if who_owns:
                who_owns = str(who_owns).lower()
                if who_owns == "other" or who_owns == "any other":
                    result["owner"] = result.get("text_one_owns", "") or ""
                else:
                    result["owner"] = result.get("select_one_owns", "") or ""
            else:
                result["owner"] = "0"
            result["caste"] = result.get("select_multiple_caste_use", "") or "0"
            result["hh_benefitted"] = result.get("households_benefited", "") or 0
            result["identified"] = result.get("select_one_identified", "") or "0"
            result["need_maintenance"] = result.get("select_one_maintenance") or "0"

            # Handle the dynamic water structure dimensions
            # water_structure_type = result.get("select_one_water_structure", "").lower().replace("_", " ")
            # water_structure_dimension = {}
            # for key, value in result.items():
            #     if isinstance(value, dict):
            #         structure_type = key.lower().replace("_", " ")
            #         if structure_type == water_structure_type:
            #             water_structure_dimension = {
            #                 "length": next((v for k, v in value.items() if k.startswith("Length")), None),
            #                 "breadth": next((v for k, v in value.items() if k.startswith("Breadth")), None),
            #                 "width": next((v for k, v in value.items() if k.startswith("Width")), None),
            #                 "depth": next((v for k, v in value.items() if k.startswith("Depth")), None),
            #                 "height": next((v for k, v in value.items() if k.startswith("Height")), None),
            #             }
            #             break

            # Add the dimensions to the result dictionary
            # result.update(water_structure_dimension)
        except Exception as e:
            print("Exception in adding a water structure record: ", e)
            continue
        res_list.append(result)
    return res_list


# MARK: Modify ODK Plan Data
def modify_response_list_plan(res, block, plan_id):
    res_list = []
    for result in res:
        if result is None:
            continue

        if result.get("__system", {}).get("reviewState") == "rejected":
            continue

        try:
            if result.get("block_name").lower() != block.lower():
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude = None
        longitude = None

        if (
            isinstance(result, dict)
            and result.get("GPS_point") is not None
            and result["GPS_point"].get("point_mapsappearance") is not None
            and "coordinates" in result["GPS_point"]["point_mapsappearance"]
        ):
            try:
                latitude = result["GPS_point"]["point_mapsappearance"]["coordinates"][1]
                longitude = result["GPS_point"]["point_mapsappearance"]["coordinates"][
                    0
                ]
            except Exception as e:
                print(f"Could not get the coordinates for settlement: {e}")

        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["work_id"] = result["work_id"]

        work_type = None
        selected_work = None

        if "TYPE_OF_WORK" in result:
            work_type = result["TYPE_OF_WORK"]
        elif "TYPE_OF_WORK_ID" in result:
            work_type = result["TYPE_OF_WORK_ID"]

        if work_type:
            result["work_type"] = work_type

            work_type_key = re.sub(r"[^a-zA-Z0-9]+", "_", work_type)

            if work_type_key in result:
                selected_work = result[work_type_key]
                if selected_work:
                    result["selected_work"] = selected_work
                else:
                    result["selected_work"] = work_type_key
            elif work_type in result:
                selected_work = result[work_type]
                if selected_work:
                    result["selected_work"] = selected_work
                else:
                    result["selected_work"] = work_type
            else:
                result["selected_work"] = work_type

        result["ben_settlement"] = result["beneficiary_settlement"]
        result["ben_name"] = result["Beneficiary_Name"]
        result["ben_contact"] = result["Beneficiary_Contact_Number"]
        res_list.append(result)

    return res_list


# MARK: Modify ODK Livelihood Data
def modify_response_list_livelihood(res, block, plan_id):
    res_list = []
    for result in res:
        # if result["__system"]["reviewState"] != "rejected":
        if result is None:
            continue

        if result.get("__system", {}).get("reviewState") == "rejected":
            continue

        try:
            if result.get("block_name").lower() != block.lower():
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude = None
        longitude = None

        if (
            isinstance(result, dict)
            and result.get("GPS_point") is not None
            and result["GPS_point"].get("point_mapappearance") is not None
            and "coordinates" in result["GPS_point"]["point_mapappearance"]
        ):
            try:
                latitude = result["GPS_point"]["point_mapappearance"]["coordinates"][1]
                longitude = result["GPS_point"]["point_mapappearance"]["coordinates"][0]
            except Exception as e:
                print(f"Could not get the coordinates for settlement: {e}")

        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        res_list.append(result)
    return res_list


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def extract_keys(d, parent_key="", sep="_"):
    keys = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        keys.append(new_key)
        if isinstance(v, dict):
            keys.extend(extract_keys(v, new_key, sep=sep))
    return keys


# MARK: Bearer Token
def fetch_bearer_token(email: str, password: str) -> str:
    try:
        if _token_cache["token"] and _token_cache["expires_at"]:
            now = datetime.now(timezone.utc)
            if now < _token_cache["expires_at"]:
                return _token_cache["token"]

        response = requests.post(
            ODK_URL_SESSION, json={"email": email, "password": password}
        )
        print("Response: ", response)
        if response.status_code == 200:
            response_data = response.json()
            _token_cache["token"] = response_data.get("token")
            _token_cache["expires_at"] = dateutil.parser.parse(
                response_data.get("expiresAt")
            )
            return _token_cache["token"]
        else:
            raise Exception(
                f"Failed to fetch bearer token. Status code: {response.status_code}"
            )
    except Exception as e:
        print(f"An error occurred while fetching the bearer token: {str(e)}")
        raise
