import csv
import logging
import re
from datetime import datetime, timezone

import dateutil.parser
import requests

from dpr.models import (
    ODK_settlement, ODK_well, ODK_waterbody,
    ODK_groundwater, ODK_agri, ODK_livelihood, ODK_crop,
    SWB_maintenance, SWB_RS_maintenance, GW_maintenance, Agri_maintenance,
    ODK_agrohorticulture,
)
from moderation.utils.utils import (
    MODEL_FIELD_EXTRACTORS as _MODERATION_EXTRACTORS,
    extract_lat_lon_from_gps,
)
from utilities.constants import ODK_URL_SESSION

logger = logging.getLogger(__name__)

_token_cache = {
    "token": None,
    "expires_at": None,
}

# MARK: Helper
def normalize_name(name):
    """
    Normalize names for comparison by:
    - Converting to lowercase
    - Removing punctuation such as parentheses
    - Collapsing separators to underscores
    - Canonicalizing known spelling variants
    """
    if not name:
        return ""
    normalized = re.sub(r"[()]", " ", str(name).lower())
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    return normalized

_RESOURCE_TYPES_FLAT_HEADER = frozenset({
    "settlement", "well", "waterbody", "cropping",
})
_RESOURCE_TYPES_UNION_HEADER = frozenset({
    "plan_gw", "plan_agri", "main_swb", "main_gw", "main_swb_rs", "main_agri",
    "livelihood", "agrohorticulture",
})


def _write_csv(resource_type, modified_response_list, all_keys, csv_path):
    if resource_type in _RESOURCE_TYPES_FLAT_HEADER:
        header_keys = modified_response_list[0].keys()
        with open(csv_path, "w", encoding="utf-8") as output_file:
            dict_writer = csv.DictWriter(
                output_file, fieldnames=header_keys, extrasaction="ignore"
            )
            dict_writer.writeheader()
            dict_writer.writerows(modified_response_list)
    elif resource_type in _RESOURCE_TYPES_UNION_HEADER:
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            dict_writer = csv.DictWriter(csvfile, fieldnames=list(all_keys))
            dict_writer.writeheader()
            for item in modified_response_list:
                dict_writer.writerow(flatten_dict(item))
    logger.info(f"CSV generated for '{resource_type}' at {csv_path}")


# MARK: Modify ODK Settlement Data
def modify_response_list_settlement(res, block, plan_id):
    res_list = []
    logger.info(
        "modify_response_list_settlement: block=%s plan_id=%s input_records=%d",
        block, plan_id, len(res) if res else 0,
    )
    for result in res:
        if result is None:
            continue
        
        if result.get("__system", {}).get("reviewState") == "rejected":
            continue
        
        try:
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["sett_id"] = result["Settlements_id"]
        result["sett_name"] = result["Settlements_name"]
        try:
            mgnrega_info = result.get("MNREGA_INFORMATION", {})
        except Exception:
            logger.exception("modify_response_list_settlement: failed reading MNREGA_INFORMATION")
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
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["well_id"] = result["well_id"]

        well_usage_section = result.get("Well_usage", {})
        try:
            result["ben_settlement"] = result.get("beneficiary_settlement", "") or "NA"
            result["owner"] = result.get("select_one_owns", "") or "NA"
            result["hh_benefitted"] = result.get("households_benefited", "") or "NA"
            result["caste"] = result.get("select_multiple_caste_use", "") or "NA"
            result["functional"] = (
                well_usage_section.get("select_one_Functional_Non_functional", "")
                or "NA"
            )
            result["need_maintenance"] = (
                well_usage_section.get("select_one_maintenance", "") or "NA"
            )
            repair_value = well_usage_section.get("select_one_repairs_well")
            if repair_value:
                repair_value = str(repair_value).lower()
                if repair_value == "other":
                    result["repair"] = (
                        well_usage_section.get("select_one_repairs_well_other", "")
                        or "NA"
                    )
                else:
                    result["repair"] = repair_value
            else:
                result["repair"] = "NA"
        except Exception:
            logger.exception("modify_response_list_well: failed enriching record")
            continue
        res_list.append(result)

    return res_list


# MARK: Modify ODK Waterbody Data
def modify_response_list_waterbody(res, block, plan_id):
    res_list = []
    logger.info(
        "modify_response_list_waterbody: block=%s plan_id=%s input_records=%d",
        block, plan_id, len(res) if res else 0,
    )
    for result in res:
        if result is None:
            continue
        if result.get("__system", {}).get("reviewState") == "rejected":
            continue
        try:
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue
        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
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
        except Exception:
            logger.exception("modify_response_list_waterbody: failed enriching record")
            continue
        res_list.append(result)
    return res_list


# MARK: Modify ODK Cropping Data
def modify_reponse_list_cropping(res, block, plan_id):
    res_list = []
    logger.info(
        "modify_reponse_list_cropping: block=%s plan_id=%s input_records=%d",
        block, plan_id, len(res) if res else 0,
    )
    
    for result in res:
        if result is None:
            continue
        
        if result.get("__system", {}).get("reviewState") == "rejected":
            continue
        
        try:
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue
        

        if str(result.get("plan_id")) != str(plan_id):
            continue
        

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        result["crop_id"] = result["__id"]
        
        # Settlement and land basic information (5 keys)
        result["sett_name"] = result.get("beneficiary_settlement", "") or ""
        result["uncropp_br"] = result.get("Uncropped_barren_land", "") or ""
        result["irrigatn"] = result.get("select_multiple_widgets", "") or ""
        result["land_cls"] = result.get("select_one_classified", "") or ""
        result["crop_seas"] = result.get("select_one_practice", "") or ""
        
        # Kharif season information (3 keys)
        result["crop_khrf"] = result.get("select_multiple_cropping_kharif", "") or ""
        result["crop_kh_o"] = result.get("select_multiple_cropping_kharif_other", "") or ""
        result["area_khrf"] = result.get("total_area_cultivation_kharif", "") or ""
        
        # Rabi season information (3 keys)
        result["crops_rabi"] = result.get("select_multiple_cropping_Rabi", "") or ""
        result["crop_rb_o"] = result.get("select_multiple_cropping_Rabi_other", "") or ""
        result["area_rabi"] = result.get("total_area_cultivation_Rabi", "") or ""
        
        # Zaid season information (3 keys)
        result["crops_zaid"] = result.get("select_multiple_cropping_Zaid", "") or ""
        result["crop_zd_o"] = result.get("select_multiple_cropping_Zaid_other", "") or ""
        result["area_zaid"] = result.get("total_area_cultivation_Zaid", "") or ""
        
        # Soil and productivity information (4 keys)
        result["productiv"] = result.get("select_one_productivity", "") or ""
        result["soil_deg"] = result.get("soil_degraded", "") or ""
        result["deg_reas"] = result.get("select_one_reason_degradation", "") or ""
        result["deg_reas2"] = result.get("select_one_reason_degradation_1", "") or ""
        
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
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
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
            if normalize_name(result.get("block_name").lower()) != normalize_name(block):
                continue
        except AttributeError:
            continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        latitude, longitude = extract_lat_lon_from_gps(result.get("GPS_point"))
        if latitude is not None and longitude is not None:
            result["latitude"] = latitude
            result["longitude"] = longitude

        result["status_re"] = result["__system"]["reviewState"]
        res_list.append(result)
    return res_list


# MARK: Modify ODK Maintenance / Agrohorticulture (Generic)
def modify_response_list_work(res, block, plan_id):
    """
    Robust transform for maintenance and agrohorticulture submissions.
    Unlike `modify_response_list_plan`, this avoids bracket-access on keys
    that maintenance/agrohorticulture blobs may not carry (e.g. work_id,
    Beneficiary_Name). Block filter is best-effort: applied only when the
    blob actually has block_name (these models have no block_name DB column).
    """
    res_list = []
    for result in res:
        if result is None:
            continue
        if not isinstance(result, dict):
            continue
        if result.get("__system", {}).get("reviewState") == "rejected":
            continue

        blob_block = result.get("block_name")
        if blob_block:
            try:
                if normalize_name(str(blob_block).lower()) != normalize_name(block):
                    continue
            except AttributeError:
                continue

        if str(result.get("plan_id")) != str(plan_id):
            continue

        lat, lon = extract_lat_lon_from_gps(result.get("GPS_point"))
        if lat is not None and lon is not None:
            result["latitude"] = lat
            result["longitude"] = lon

        sys_info = result.get("__system") or {}
        if isinstance(sys_info, dict):
            review_state = sys_info.get("reviewState")
            if review_state:
                result["status_re"] = review_state

        res_list.append(result)
    return res_list


# Layer-build source-of-truth registry. Key = resource_type / work_type the
# `/add_resources` and `/add_works` endpoints accept. Tuple = (Model, JSON
# blob field, model has block_name column for DB-level pre-filtering).
#
# Resources (workspace=resources): settlement, well, waterbody, cropping
# Works (workspace=works):
#   plan_gw           — new recharge structures (groundwater)
#   main_gw           — maintenance of recharge structures
#   plan_agri         — new irrigation structures
#   main_agri         — maintenance of irrigation structures
#   main_swb          — surface water body maintenance (water-structure form)
#   main_swb_rs       — remote-sensed surface water body maintenance
#   livelihood        — livelihood
#   agrohorticulture  — agrohorticulture
_DB_CONFIG = {
    "settlement":       (ODK_settlement,        "data_settlement",         True),
    "well":             (ODK_well,              "data_well",               True),
    "waterbody":        (ODK_waterbody,         "data_waterbody",          True),
    "cropping":         (ODK_crop,              "data_crop",               False),
    "plan_gw":          (ODK_groundwater,       "data_groundwater",        True),
    "plan_agri":        (ODK_agri,              "data_agri",               True),
    "livelihood":       (ODK_livelihood,        "data_livelihood",         True),
    "main_swb":         (SWB_maintenance,       "data_swb_maintenance",    False),
    "main_gw":          (GW_maintenance,        "data_gw_maintenance",     False),
    "main_swb_rs":      (SWB_RS_maintenance,    "data_swb_rs_maintenance", False),
    "main_agri":        (Agri_maintenance,      "data_agri_maintenance",   False),
    # `data_agohorticulture` is the actual model field name — there is a
    # spelling typo in the schema. Respecting it here to avoid a migration.
    "agrohorticulture": (ODK_agrohorticulture,  "data_agohorticulture",    False),
}

# Fields never useful to project alongside the JSON blob: the blob itself,
# soft-delete metadata, and relational fields (FKs serialise poorly via values()).
_PROJECTION_EXCLUDED_FIELDS = frozenset({
    "data_before_moderation",
    "is_deleted",
    "deleted_at",
    "deleted_by",
    "moderated_by",
})


def _scalar_projection_fields(model, json_blob_field: str) -> list:
    """
    Concrete, non-relational fields on `model` safe to project via `.values()`
    alongside the raw ODK JSON blob.  Captures everything moderation can edit
    (settlement_name, block_name, nrega_*, lat/lon, status_re, ...) so the
    generated layer reflects the latest moderated values, not just the
    original ODK submission stored in `data_<resource>`.
    """
    skip = {json_blob_field, *_PROJECTION_EXCLUDED_FIELDS}
    fields = []
    for f in model._meta.get_fields():
        if not getattr(f, "concrete", False):
            continue
        if f.many_to_one or f.one_to_one or f.many_to_many or f.one_to_many:
            continue
        if f.name in skip:
            continue
        fields.append(f.name)
    return fields


def _merge_moderated(blob: dict, friendly: dict, friendly_canonical: bool) -> dict:
    """
    Merge friendly DB column values into the raw ODK blob so the layer carries
    both the original ODK keys (Settlements_name, GPS_point, ...) and the
    moderated friendly columns (settlement_name, block_name, ...).

    `friendly_canonical=True` (model has a moderation extractor): friendly
    columns are kept in sync with every moderation edit, so they win on
    collision.  `False` (no extractor, e.g. SWB_maintenance): moderation only
    touches the blob, so the blob wins on collision.

    GeoPackage/SQLite (the downstream layer format) is case-insensitive on
    column names, so a blob key like "GPS_point" and a friendly column
    "gps_point" would collide on write.  We dedupe case-insensitively in
    favour of the canonical side; non-colliding keys (e.g. "Settlements_name"
    vs "settlement_name") are both preserved.
    """
    if friendly_canonical:
        winner, loser = friendly, blob
    else:
        winner, loser = blob, friendly

    winner_lower = {k.lower() for k in winner}
    loser_filtered = {
        k: v for k, v in loser.items()
        if k.lower() not in winner_lower
    }
    return {**loser_filtered, **winner}


# MARK: Fetch DB Data
def fetch_db_data(csv_path, resource_type, block, plan_id) -> int:
    """
    Build the CSV of records for the given (resource_type, plan_id, block)
    by reading from our DB (post-moderation source of truth).

    Returns the number of rows actually written to the CSV; 0 means no
    usable data was found and the caller should treat it as a soft 404.
    """
    logger.info(
        f"fetch_db_data: starting — resource_type={resource_type}, "
        f"plan_id={plan_id}, block={block}, csv_path={csv_path}"
    )

    entry = _DB_CONFIG.get(resource_type)
    if not entry:
        logger.warning(f"fetch_db_data: unknown resource_type '{resource_type}'")
        return 0

    model, data_field, has_block_col = entry
    projection_fields = _scalar_projection_fields(model, data_field)
    friendly_canonical = model in _MODERATION_EXTRACTORS
    logger.info(
        f"fetch_db_data: querying {model.__name__}.{data_field} "
        f"with plan_id={plan_id}, is_deleted=False"
        + (
            ", block_name filtered in Python via normalize_name(...)"
            if has_block_col
            else " (no block_name column, skipping DB block filter)"
        )
    )
    logger.info(
        f"fetch_db_data: projecting blob '{data_field}' + "
        f"{len(projection_fields)} moderated column(s) "
        f"(friendly_canonical={friendly_canonical}): {projection_fields}"
    )

    qs = model.objects.filter(plan_id=str(plan_id), is_deleted=False)

    raw_rows = list(qs.values(data_field, *projection_fields))
    logger.info(
        f"fetch_db_data: DB returned {len(raw_rows)} record(s) for "
        f"resource_type={resource_type}, plan_id={plan_id}"
    )

    response_list = []
    empty_blob_count = 0
    for row in raw_rows:
        blob = row.get(data_field) or {}
        if not blob:
            empty_blob_count += 1
            continue
        friendly = {k: v for k, v in row.items() if k != data_field}
        response_list.append(_merge_moderated(blob, friendly, friendly_canonical))

    if empty_blob_count:
        logger.warning(
            f"fetch_db_data: skipped {empty_blob_count} record(s) with empty "
            f"{data_field}"
        )

    if not response_list:
        logger.warning(
            f"fetch_db_data: no usable records for resource_type={resource_type}, "
            f"plan_id={plan_id}, block={block}"
        )
        return 0

    logger.info(
        f"fetch_db_data: running transform for resource_type={resource_type} "
        f"on {len(response_list)} record(s) (each enriched with "
        f"{len(projection_fields)} moderated column(s))"
    )

    all_keys = set()
    if resource_type == "settlement":
        rows = modify_response_list_settlement(response_list, block, plan_id)
    elif resource_type == "well":
        rows = modify_response_list_well(response_list, block, plan_id)
    elif resource_type == "waterbody":
        rows = modify_response_list_waterbody(response_list, block, plan_id)
    elif resource_type == "cropping":
        rows = modify_reponse_list_cropping(response_list, block, plan_id)
    elif resource_type in ["plan_gw", "main_swb", "plan_agri"]:
        rows = modify_response_list_plan(response_list, block, plan_id)
        for item in rows:
            all_keys.update(extract_keys(item))
    elif resource_type == "livelihood":
        rows = modify_response_list_livelihood(response_list, block, plan_id)
        for item in rows:
            all_keys.update(extract_keys(item))
    elif resource_type in [
        "main_gw", "main_swb_rs", "main_agri", "agrohorticulture",
    ]:
        rows = modify_response_list_work(response_list, block, plan_id)
        for item in rows:
            all_keys.update(extract_keys(item))
    else:
        logger.warning(
            f"fetch_db_data: no transform defined for resource_type='{resource_type}'"
        )
        return 0

    logger.info(
        f"fetch_db_data: transform produced {len(rows)} row(s) "
        f"(filtered from {len(response_list)}) for resource_type={resource_type}"
    )

    if not rows:
        logger.warning(
            f"fetch_db_data: transform returned empty list for "
            f"resource_type={resource_type}, plan_id={plan_id}, block={block}"
        )
        return 0

    logger.info(f"fetch_db_data: writing CSV to {csv_path}")
    _write_csv(resource_type, rows, all_keys, csv_path)
    logger.info(
        f"fetch_db_data: done — {len(rows)} row(s) written to {csv_path} "
        f"(columns include {len(projection_fields)} moderated friendly field(s))"
    )
    return len(rows)


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
