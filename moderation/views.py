from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
import requests
from moderation.utils.update_csdb import *
from moderation.utils.get_submissions import ODKSubmissionsChecker
from moderation.utils.form_mapping import feedback_form
from moderation.models import SyncMetadata
import ee
import math
import json
from moderation.utils.demand_validator_lulc import (
    compute_lulc_auto,
    normalize_structure_name,
)
from utilities.constants import (
    DRAINAGE_LINES_ASSET,
    GLOBAL_DRAINAGE_EPS_M,
    GEOSERVER_BASE,
    WORKS_WORKSPACE,
    RESOURCES_WORKSPACE,
)
from utilities.gee_utils import ee_initialize

FETCH_FIELD_MAP = {
    ODK_settlement: "data_settlement",
    ODK_well: "data_well",
    ODK_waterbody: "data_waterbody",
    ODK_groundwater: "data_groundwater",
    ODK_agri: "data_agri",
    ODK_livelihood: "data_livelihood",
    ODK_crop: "data_crop",
    Agri_maintenance: "data_agri_maintenance",
    GW_maintenance: "data_gw_maintenance",
    SWB_maintenance: "data_swb_maintenance",
    SWB_RS_maintenance: "data_swb_rs_maintenance",
    ODK_agrohorticulture: "data_agohorticulture",
}


def paginate_queryset(queryset, page=1, per_page=10):
    paginator = Paginator(queryset, per_page)

    try:
        obj_page = paginator.page(page)
    except PageNotAnInteger:
        obj_page = paginator.page(1)
    except EmptyPage:
        obj_page = paginator.page(paginator.num_pages)

    data = list(obj_page.object_list)

    return {
        "page": obj_page.number,
        "total_pages": paginator.num_pages,
        "total_objects": paginator.count,
        "data": data,
    }


class SubmissionsOfPlan:

    @staticmethod
    def _fetch(model, plan_id, page):
        field_name = FETCH_FIELD_MAP.get(model)
        if not field_name:
            raise ValueError(f"No fetch field configured for {model.__name__}")
        qs = (
            model.objects.filter(plan_id=plan_id)
            .exclude(is_deleted=True)
            .values_list(field_name, "is_moderated")
        )
        if page is None:
            data = list(qs)
            return {
                "page": 1,
                "total_pages": 1,
                "total_objects": len(data),
                "data": data,
            }
        return paginate_queryset(qs, page)

    @staticmethod
    def get_settlement(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_settlement, plan_id, page)

    @staticmethod
    def get_well(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_well, plan_id, page)

    @staticmethod
    def get_waterbody(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_waterbody, plan_id, page)

    @staticmethod
    def get_groundwater(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_groundwater, plan_id, page)

    @staticmethod
    def get_agri(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_agri, plan_id, page)

    @staticmethod
    def get_livelihood(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_livelihood, plan_id, page)

    @staticmethod
    def get_crop(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_crop, plan_id, page)

    @staticmethod
    def get_agri_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(Agri_maintenance, plan_id, page)

    @staticmethod
    def get_gw_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(GW_maintenance, plan_id, page)

    @staticmethod
    def get_swb_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_maintenance, plan_id, page)

    @staticmethod
    def get_swb_rs_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_RS_maintenance, plan_id, page)

    @staticmethod
    def get_agrohorticulture(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_agrohorticulture, plan_id, page)


def sync_odk_to_csdb():
    (
        settlement_submissions,
        well_submissions,
        waterbody_submissions,
        groundwater_submissions,
        agri_submissions,
        livelihood_submissions,
        cropping_submissions,
        agri_maintenance_submissions,
        gw_maintenance_submissions,
        swb_maintenance_submissions,
        swb_rs_maintenance_submissions,
        agrohorticulture_submissions,
    ) = sync_odk_data(get_edited_updated_all_submissions)
    checker = ODKSubmissionsChecker()
    res = checker.process("updated")
    for form_name, status in res.items():
        if form_name in feedback_form:
            print("passed feedback form")
            continue
        if status.get("is_updated"):
            if form_name == "Settlement Form":
                resync_settlement(settlement_submissions)
            elif form_name == "Well Form":
                resync_well(well_submissions)
            elif form_name == "water body form":
                resync_waterbody(waterbody_submissions)
            elif form_name == "new recharge structure form":
                resync_gw(groundwater_submissions)
            elif form_name == "new irrigation form":
                resync_agri(agri_submissions)
            elif form_name == "livelihood form":
                resync_livelihood(livelihood_submissions)
            elif form_name == "cropping pattern form":
                resync_cropping(cropping_submissions)
            elif form_name == "propose maintenance on existing irrigation form":
                resync_agri_maintenance(agri_maintenance_submissions)
            elif form_name == "propose maintenance on water structure form":
                resync_gw_maintenance(gw_maintenance_submissions)
            elif form_name == "propose maintenance on existing water recharge form":
                resync_swb_maintenance(swb_maintenance_submissions)
            elif (
                form_name
                == "propose maintenance of remotely sensed water structure form"
            ):
                resync_swb_rs_maintenance(swb_rs_maintenance_submissions)
            elif form_name == "Agrohorticulture":
                resync_agrohorticulture(agrohorticulture_submissions)
            else:
                print("passed wrong form name")

    metadata = SyncMetadata.get_odk_sync_metadata()
    metadata.update_last_synced()
    return JsonResponse({"status": "Sync complete", "result": res})


# DEMAND VALIDATOR LOGICS
EE_AVAILABLE = ee_initialize(account_id=17)


def compute_slope_mean_30m(lat: float, lon: float, buffer_m: int = 30) -> float:
    """Mean slope (%) within buffer_m around point using SRTM."""
    if not EE_AVAILABLE:
        return 0.0

    dem = ee.Image("USGS/SRTMGL1_003")
    slope_deg = ee.Terrain.slope(dem)

    slope_pct = (
        slope_deg.multiply(math.pi / 180.0)
        .tan()
        .multiply(100.0)
        .rename("slope_pct")
        .unmask(0)
    )

    pt = ee.Geometry.Point([lon, lat])
    buf = pt.buffer(buffer_m)
    scale = slope_pct.projection().nominalScale()

    stats = slope_pct.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=buf, scale=scale, bestEffort=True
    )

    v = stats.get("slope_pct").getInfo()
    return round(float(v or 0.0), 2)


def compute_catchment_minmax_30m(lat: float, lon: float, buffer_m: int = 30) -> dict:
    """Min/Max catchment area (ha) in buffer_m around point."""
    if not EE_AVAILABLE:
        return {"min": 0.0, "max": 0.0}

    ca = (
        ee.Image("projects/ext-datasets/assets/datasets/catchment_area_multiflow")
        .select(0)
        .rename("ha")
        .unmask(0)
    )

    pt = ee.Geometry.Point([lon, lat])
    buf = pt.buffer(buffer_m)
    scale = ca.projection().nominalScale()

    stats = ca.reduceRegion(
        reducer=ee.Reducer.min().combine(ee.Reducer.max(), sharedInputs=True),
        geometry=buf,
        scale=scale,
        bestEffort=True,
        maxPixels=1e9,
    )

    ca_min = stats.get("ha_min").getInfo()
    ca_max = stats.get("ha_max").getInfo()

    return {
        "min": round(float(ca_min or 0.0), 2),
        "max": round(float(ca_max or 0.0), 2),
    }


def compute_stream_order(lat: float, lon: float) -> int:
    """Stream order at point (integer)."""
    if not EE_AVAILABLE:
        return 0

    so = (
        ee.Image(
            "projects/corestack-datasets/assets/datasets/Stream_Order_Raster_India"
        )
        .select(0)
        .rename("so")
        .unmask(0)
    )

    pt = ee.Geometry.Point([lon, lat])
    scale = so.projection().nominalScale()

    stats = so.reduceRegion(
        reducer=ee.Reducer.first(), geometry=pt, scale=scale, bestEffort=True
    )

    v = stats.get("so").getInfo()
    return int(round(float(v or 0)))


def compute_drainage_distance_m(lat: float, lon: float, scale: int = 30) -> float:
    """
    Vector -> distance image -> reduceRegion(min) at point.
    Returns distance in meters.
    """
    if not EE_AVAILABLE:
        return 0.0

    pt = ee.Geometry.Point([lon, lat])
    drainage = ee.FeatureCollection(DRAINAGE_LINES_ASSET)

    dist_img = drainage.distance(searchRadius=10000, maxError=1)

    stats = dist_img.reduceRegion(
        reducer=ee.Reducer.min(), geometry=pt, scale=scale, maxPixels=1e9
    )

    d = stats.get("distance").getInfo()  # band name is "distance"
    return round(float(d or 0.0), 2)


with open("moderation/utils/rules.json", "r", encoding="utf-8") as f:
    RULES = json.load(f)


def in_range(v: float, r: dict) -> bool:
    if not isinstance(r, dict):
        return False
    if "min" in r and v < float(r["min"]):
        return False
    if "max" in r and v > float(r["max"]):
        return False
    return True


def classify_numeric(value, rule: dict, label: str):
    """
    Handles:
      - legacy: {"max": 15}
      - standard: {"accepted": {...}, "partially_accepted": {...}}
      - standard: {"accepted": {...}} only
    """
    if value is None or rule is None:
        return ("not_evaluated", f"{label} not evaluated (missing value/rule).")

    v = float(value)

    # legacy slope: {"max": n}
    if "max" in rule and "accepted" not in rule:
        mx = float(rule["max"])
        if v <= mx:
            return ("accepted", f"{label} {v:.2f} ≤ {mx} → accepted.")
        return ("not_accepted", f"{label} {v:.2f} > {mx} → not accepted.")

    acc = rule.get("accepted")
    part = rule.get("partially_accepted")

    if isinstance(acc, dict) and in_range(v, acc):
        return ("accepted", f"{label} {v:.2f} within accepted {acc} → accepted.")
    if isinstance(part, dict) and in_range(v, part):
        return (
            "partially_accepted",
            f"{label} {v:.2f} within partial {part} → partially accepted.",
        )

    # If accepted exists but didn't match, it is not accepted
    if isinstance(acc, dict):
        return (
            "not_accepted",
            f"{label} {v:.2f} outside accepted/partial ranges → not accepted.",
        )

    return ("not_evaluated", f"{label} rule format not recognized.")


def classify_stream_order(value, rule: dict):
    if value is None or rule is None:
        return ("not_evaluated", "Stream order not evaluated (missing value/rule).")

    v = int(value)

    accepted = rule.get("accepted")
    if accepted is None:
        accepted = rule.get("valid", [])  # supports legacy 'valid'
    partial = rule.get("partially_accepted", [])

    if v in (accepted or []):
        return ("accepted", f"Stream order {v} in {accepted} → accepted.")
    if v in (partial or []):
        return (
            "partially_accepted",
            f"Stream order {v} in {partial} → partially accepted.",
        )
    return (
        "not_accepted",
        f"Stream order {v} not in accepted/partial sets → not accepted.",
    )


def classify_lulc(value, rule: dict):
    if not value or rule is None:
        return ("not_evaluated", "LULC not evaluated (missing value/rule).")

    v_raw = str(value).strip()
    v = v_raw.lower()

    acc = [str(x).strip().lower() for x in (rule.get("accepted") or [])]
    part = [str(x).strip().lower() for x in (rule.get("partially_accepted") or [])]
    notacc = [str(x).strip().lower() for x in (rule.get("not_accepted") or [])]

    if v in acc:
        return ("accepted", f"LULC '{v_raw}' is accepted.")
    if v in part:
        return ("partially_accepted", f"LULC '{v_raw}' is partially accepted.")
    if v in notacc:
        return ("not_accepted", f"LULC '{v_raw}' is not accepted.")
    return ("not_evaluated", f"LULC '{v_raw}' not found in rule lists.")


def evaluate_site_from_rules(site: dict) -> dict:
    key = normalize_structure_name(site.get("structure_type", ""))
    rules = RULES.get(key)

    if not rules:
        return {
            "suitable": False,
            "parameters": {},
            "overall_comment": f"No rules found for structure '{key}'.",
        }

    params = {}
    statuses = []
    failures = []

    # slope
    if "slope" in rules:
        cat, expl = classify_numeric(site.get("slope"), rules["slope"], "Slope (%)")
        params["slope"] = {
            "category": cat,
            "value": site.get("slope"),
            "explanation": expl,
            "rule": rules["slope"],
        }
        statuses.append(cat)
        if cat == "not_accepted":
            failures.append(expl)

    # catchment
    if "catchment_area" in rules:
        cat, expl = classify_numeric(
            site.get("catchment_area"), rules["catchment_area"], "Catchment (ha)"
        )
        params["catchment_area"] = {
            "category": cat,
            "value": site.get("catchment_area"),
            "explanation": expl,
            "rule": rules["catchment_area"],
        }
        statuses.append(cat)
        if cat == "not_accepted":
            failures.append(expl)

    # stream order
    if "stream_order" in rules:
        cat, expl = classify_stream_order(
            site.get("stream_order"), rules["stream_order"]
        )
        params["stream_order"] = {
            "category": cat,
            "value": site.get("stream_order"),
            "explanation": expl,
            "rule": rules["stream_order"],
        }
        statuses.append(cat)
        if cat == "not_accepted":
            failures.append(expl)

    # drainage distance (with global epsilon override)
    if "drainage_distance" in rules:
        dd = site.get("drainage_distance")
        if dd is None:
            cat, expl = ("not_evaluated", "Drainage distance missing.")
        else:
            dd = float(dd)
            if dd <= GLOBAL_DRAINAGE_EPS_M:
                cat = "not_accepted"
                expl = f"Rejected globally: drainage distance {dd:.1f} m ≤ {GLOBAL_DRAINAGE_EPS_M:.1f} m (on/too close to drainage line)."
            else:
                cat, expl = classify_numeric(
                    dd, rules["drainage_distance"], "Drainage distance (m)"
                )

        params["drainage_distance"] = {
            "category": cat,
            "value": site.get("drainage_distance"),
            "explanation": expl,
            "rule": rules["drainage_distance"],
        }
        statuses.append(cat)
        if cat == "not_accepted":
            failures.append(expl)

    # lulc
    if "lulc" in rules:
        cat, expl = classify_lulc(site.get("lulc_class"), rules["lulc"])
        params["lulc"] = {
            "category": cat,
            "value": site.get("lulc_class"),
            "explanation": expl,
            "rule": rules["lulc"],
        }
        statuses.append(cat)
        if cat == "not_accepted":
            failures.append(expl)

    suitable = "not_accepted" not in statuses

    overall_comment = "Rule-based evaluation completed."
    if not suitable and failures:
        overall_comment += " Failures: " + " | ".join(failures[:3])

    return {
        "suitable": suitable,
        "parameters": params,
        "overall_comment": overall_comment,
    }


LAYER_CONFIG = {
    "plan_agri": WORKS_WORKSPACE,
    "plan_gw": WORKS_WORKSPACE,
    "waterbody": RESOURCES_WORKSPACE,
}


def build_layer_name(prefix: str, plan_number: str, district: str, block: str) -> str:
    return f"{prefix}_{plan_number}_{district}_{block}"


def extract_lon_lat_from_geom(geom: dict):
    """
    Extract lon/lat from GeoJSON geometry.
    Supports Point and Polygon-like nested coordinate arrays.
    """
    coords = geom.get("coordinates")
    if coords is None:
        return None, None

    def find_pair(c):
        if (
            isinstance(c, (list, tuple))
            and len(c) >= 2
            and isinstance(c[0], (int, float))
            and isinstance(c[1], (int, float))
        ):
            return c[0], c[1]

        if isinstance(c, (list, tuple)):
            for item in c:
                lon, lat = find_pair(item)
                if lon is not None and lat is not None:
                    return lon, lat

        return None, None

    return find_pair(coords)


STRUCTURE_FIELDS = ["TYPE_OF_WO", "work_type", "selected_w", "select_o_4"]


def get_structure_type(props: dict) -> str:
    """Try extracting structure type from known GeoServer fields."""
    for field in STRUCTURE_FIELDS:
        val = props.get(field)
        if val:
            return str(val).strip()
    return ""


def fetch_sites_from_layer(prefix: str, plan_number: str, district: str, block: str):
    """
    Calls GeoServer WFS for the given plan layer and extracts:
    - id
    - lat, lon
    - structure_type
    """
    district = district.lower()
    block = block.lower()

    workspace = LAYER_CONFIG[prefix]
    layer_name = build_layer_name(prefix, plan_number, district, block)
    wfs_url = f"{GEOSERVER_BASE}{workspace}/ows"

    # IMPORTANT: keep this EXACT like your old working code:
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": f"{workspace}:{layer_name}",
        "outputFormat": "application/json",
    }

    resp = requests.get(wfs_url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    sites = []
    features = data.get("features", [])

    for idx, feat in enumerate(features):
        geom = feat.get("geometry") or {}
        lon, lat = extract_lon_lat_from_geom(geom)

        props = feat.get("properties") or {}

        # fallback to lat/lon fields if geometry extraction fails
        if lon is None or lat is None:
            lon = props.get("longitude")
            lat = props.get("latitude")

        if lon is None or lat is None:
            continue

        structure_type = get_structure_type(props)
        site_id = f"{layer_name}_{idx}"

        sites.append(
            {
                "id": site_id,
                "lat": float(lat),
                "lon": float(lon),
                "structure_type": structure_type,
            }
        )

    return layer_name, sites


class DemandValidator:
    """
    validate demands on the basis of pre-decided rules
    """

    @staticmethod
    def validate_site(lat, lon, structure_type, lulc_class=None):
        if lat is None or lon is None or not structure_type:
            return "lat, lon, structure_type are required"
        try:
            lat = float(lat)
            lon = float(lon)
        except ValueError:
            return "lat and lon must be numeric"

        if not EE_AVAILABLE:
            return "Earth Engine not available"

        if not lulc_class:
            lulc_class = compute_lulc_auto(lat, lon, structure_type)

        # Compute values from GEE
        slope = compute_slope_mean_30m(lat, lon, buffer_m=30)
        ca_range = compute_catchment_minmax_30m(lat, lon, buffer_m=30)
        stream_order = compute_stream_order(lat, lon)
        drainage_distance = compute_drainage_distance_m(lat, lon, scale=30)

        # Representative catchment: max in 30m buffer
        catchment_rep = ca_range["max"]

        site = {
            "structure_type": structure_type,
            "slope": slope,
            "catchment_area": catchment_rep,
            "stream_order": stream_order,
            "drainage_distance": drainage_distance,
            "lulc_class": lulc_class,
        }

        evaluation = evaluate_site_from_rules(site)

        return {
            "lat": lat,
            "lon": lon,
            "structure_type": structure_type,
            "raw_values": {
                "slope_mean_30m": slope,
                "catchment_min_30m": ca_range["min"],
                "catchment_max_30m": ca_range["max"],
                "stream_order": stream_order,
                "drainage_distance": drainage_distance,
                "lulc_class": lulc_class,
            },
            "evaluation": evaluation,
        }

    @staticmethod
    def plan_sites(plan_number, district, block, layer_type):
        """
        Input:
        {
          "plan_number": "116",
          "district": "bhilwara",
          "block": "mandalgarh",
          "layer_type": "plan_agri"
        }
        """

        if not plan_number or not district or not block or not layer_type:
            return "plan_number, district, block, layer_type are required"

        if layer_type not in LAYER_CONFIG:
            return "layer_type must be one of {list(LAYER_CONFIG.keys())}"

        try:
            layer_name, sites = fetch_sites_from_layer(
                layer_type, plan_number, district, block
            )
        except Exception as e:
            return f"GeoServer fetch failed: {e}"

        return {"layer_name": layer_name, "site_count": len(sites), "sites": sites}
