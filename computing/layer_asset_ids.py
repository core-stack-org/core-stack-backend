"""
Expected GEE asset IDs for layer-generation API responses.
Mirrors naming used in Celery task implementations.
"""

import datetime

from utilities.constants import GEE_PATHS
from utilities.gee_utils import get_gee_dir_path, valid_gee_text


def resolve_asset_id_field(asset_id=None, asset_ids=None):
    """
    API response value for asset_id:
    - one asset -> string
    - multiple assets -> list of strings
    """
    if asset_ids is not None:
        if len(asset_ids) == 0:
            return None
        if len(asset_ids) == 1:
            return asset_ids[0]
        return asset_ids
    if isinstance(asset_id, list):
        if len(asset_id) == 0:
            return None
        if len(asset_id) == 1:
            return asset_id[0]
        return asset_id
    return asset_id


def hydrological_year_range(start_year, end_year):
    return range(int(start_year), int(end_year) + 1)


def hydrological_years_from_date_window(start_date=None, end_date=None):
    """Match computing.zoi_layers.zoi._resolve_zoi_time_window year extraction."""
    if not start_date or not end_date:
        return 2017, 2024
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    start_year = start_dt.year if start_dt.month >= 7 else start_dt.year - 1
    end_year = end_dt.year if end_dt.month >= 7 else end_dt.year - 1
    return start_year, end_year


def tehsil_suffix(district, block):
    return valid_gee_text(str(district).lower()) + "_" + valid_gee_text(str(block).lower())


def mws_asset_id(state, district, block, description):
    base = get_gee_dir_path(
        [state, district, block], asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
    )
    return base + description


def admin_boundary_asset_id(state, district, block):
    return mws_asset_id(
        state, district, block, "admin_boundary_" + tehsil_suffix(district, block)
    )


def nrega_asset_id(state, district, block):
    return mws_asset_id(state, district, block, "nrega_" + tehsil_suffix(district, block))


def drainage_lines_asset_id(state, district, block):
    return mws_asset_id(
        state, district, block, "drainage_lines_" + tehsil_suffix(district, block)
    )


def mws_filtered_asset_id(state, district, block):
    return mws_asset_id(
        state, district, block, "filtered_mws_" + tehsil_suffix(district, block) + "_uid"
    )


def lulc_v3_asset_id(state, district, block, end_year):
    description = (
        f"{tehsil_suffix(district, block)}_{end_year}-07-01_{end_year + 1}-06-30_LULCmap_10m"
    )
    return mws_asset_id(state, district, block, description)


def lulc_map_asset_ids(state, district, block, start_year, end_year, version=None):
    """One LULC raster asset per hydrological year (tehsil v2/v3 clip)."""
    ids = []
    for year in hydrological_year_range(start_year, end_year):
        description = (
            f"{tehsil_suffix(district, block)}_{year}-07-01_{year + 1}-06-30_LULCmap_10m"
        )
        if version == "v2":
            description += "_v2"
        ids.append(mws_asset_id(state, district, block, description))
    return ids


def lulc_tehsil_asset_id(state, district, block, end_year, version="v3"):
    description = (
        f"{tehsil_suffix(district, block)}_{end_year}-07-01_{end_year + 1}-06-30_LULCmap_10m"
    )
    if version == "v2":
        description += "_v2"
    return mws_asset_id(state, district, block, description)


def lulc_tehsil_asset_ids(state, district, block, start_year, end_year, version="v3"):
    return lulc_map_asset_ids(state, district, block, start_year, end_year, version=version)


def lulc_v3_clip_asset_ids(state, district, block, start_year, end_year):
    return lulc_map_asset_ids(state, district, block, start_year, end_year)


def lulc_v4_asset_id(state, district, block):
    return mws_asset_id(
        state, district, block, "lulc_v4_" + tehsil_suffix(district, block)
    )


def lulc_vector_asset_id(state, district, block):
    return mws_asset_id(
        state, district, block, "lulc_vector_" + tehsil_suffix(district, block)
    )


def hydrology_asset_ids(state, district, block, is_annual=False):
    suffix = tehsil_suffix(district, block)
    if is_annual:
        descriptions = [
            f"Prec_annual_{suffix}",
            f"Runoff_annual_{suffix}",
            f"ET_annual_{suffix}",
            f"filtered_delta_g_annual_{suffix}_uid",
            f"well_depth_net_value_{suffix}",
        ]
    else:
        descriptions = [
            f"Prec_fortnight_{suffix}",
            f"Runoff_fortnight_{suffix}",
            f"ET_fortnight_{suffix}",
            f"filtered_delta_g_fortnight_{suffix}_uid",
            f"well_depth_net_value_{suffix}",
        ]
    return [mws_asset_id(state, district, block, d) for d in descriptions]


def change_detection_asset_ids(state, district, block, start_year, end_year):
    base = f"change_{tehsil_suffix(district, block)}"
    params = (
        "Urbanization",
        "Degradation",
        "Deforestation",
        "Afforestation",
        "CropIntensity",
    )
    return [
        mws_asset_id(state, district, block, f"{base}_{param}_{start_year}_{end_year}")
        for param in params
    ]


def change_detection_vector_asset_ids(state, district, block, start_year, end_year):
    suffix = tehsil_suffix(district, block)
    params = ("ccd", "ch", "overall")
    return [
        mws_asset_id(
            state,
            district,
            block,
            f"change_vector_{suffix}_{param}_{start_year}_{end_year}",
        )
        for param in params
    ]


def tree_health_raster_asset_ids(state, district, block, start_year, end_year):
    suffix = tehsil_suffix(district, block)
    asset_ids = []
    for year in range(int(start_year), int(end_year) + 1):
        asset_ids.append(
            mws_asset_id(state, district, block, f"ccd_raster_{suffix}_{year}")
        )
        asset_ids.append(
            mws_asset_id(state, district, block, f"ch_raster_{suffix}_{year}")
        )
    asset_ids.append(
        mws_asset_id(state, district, block, f"overall_change_raster_{suffix}")
    )
    return asset_ids


def tree_health_vector_asset_ids(state, district, block, start_year, end_year):
    suffix = tehsil_suffix(district, block)
    return [
        mws_asset_id(
            state, district, block, f"change_vector_{suffix}_ch_{start_year}_{end_year}"
        ),
        mws_asset_id(
            state,
            district,
            block,
            f"change_vector_{suffix}_ccd_{start_year}_{end_year}",
        ),
        mws_asset_id(state, district, block, f"overall_change_vector_{suffix}"),
    ]


def drought_causality_asset_id(state, district, block, end_year):
    return mws_asset_id(
        state, district, block, "drought_" + tehsil_suffix(district, block)
    )


def merge_swb_ponds_asset_id(state, district, block):
    return mws_asset_id(state, district, block, "_merged_swb_ponds")


def natural_depression_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        "natural_depression_"
        + valid_gee_text(str(district))
        + "_"
        + valid_gee_text(str(block))
        + "_raster",
    )


def distance_to_drainage_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        "distance_to_drainage_line_"
        + valid_gee_text(str(district))
        + "_"
        + valid_gee_text(str(block))
        + "_raster",
    )


def slope_percentage_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        valid_gee_text(str(district))
        + "_"
        + valid_gee_text(str(block))
        + "_slope_percentage_raster",
    )


def catchment_area_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        "catchment_area_" + tehsil_suffix(district, block) + "_raster",
    )


def fes_clart_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        valid_gee_text(str(district))
        + "_"
        + valid_gee_text(str(block))
        + "_clart_fes",
    )


def mws_connectivity_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        tehsil_suffix(district, block) + "_mws_connectivity",
    )


def mws_centroid_asset_id(state, district, block):
    return mws_asset_id(
        state,
        district,
        block,
        tehsil_suffix(district, block) + "_mws_centroid",
    )


def cropping_intensity_asset_ids(state, district, block, start_year=None, end_year=None):
    suffix = tehsil_suffix(district, block)
    base = mws_asset_id(state, district, block, f"cropping_intensity_{suffix}")
    ids = [base]
    if start_year is not None and end_year is not None:
        for year in hydrological_year_range(start_year, end_year):
            ids.append(f"{base}_{year}_{end_year}")
    return list(dict.fromkeys(ids))


def ndvi_timeseries_asset_ids(state, district, block, start_year, end_year):
    suffix = tehsil_suffix(district, block)
    base = mws_asset_id(state, district, block, f"ndvi_timeseries_{suffix}")
    ids = []
    f_start_date = datetime.datetime.strptime(f"{int(start_year)}-07-01", "%Y-%m-%d")
    end_date = datetime.datetime.strptime(f"{int(end_year) + 1}-06-30", "%Y-%m-%d")
    while f_start_date <= end_date:
        f_end_date = f_start_date + datetime.timedelta(days=364)
        if f_end_date > end_date:
            break
        ids.append(
            f"{base}_{f_start_date.date()}_{f_end_date.date()}"
        )
        f_start_date = f_end_date
    ids.extend([f"{base}_crop", f"{base}_tree", f"{base}_shrub"])
    return list(dict.fromkeys(ids))


def drought_layer_asset_ids(state, district, block, start_year, end_year):
    suffix = tehsil_suffix(district, block)
    ids = [mws_asset_id(state, district, block, f"drought_{suffix}")]
    for year in range(int(start_year), int(end_year) + 1):
        ids.append(
            mws_asset_id(state, district, block, f"drought_{suffix}_{year}_v2")
        )
    return list(dict.fromkeys(ids))


def zoi_pipeline_asset_ids(
    state, district, block, start_year=2017, end_year=2024, app_type="MWS"
):
    suffix = tehsil_suffix(district, block)
    base = get_gee_dir_path(
        [state, district, block], asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    ndvi_base = base + suffix
    ids = [
        base + f"swb3_{suffix}",
        base + f"zoi_{suffix}",
        base + f"zoi_cropping_intensity_{suffix}",
        base + f"cropping_intensity_zoi_{suffix}",
        ndvi_base,
    ]
    for year in hydrological_year_range(start_year, end_year):
        ids.append(f"{ndvi_base}_ndvi_{year}")
    return list(dict.fromkeys(ids))


def swb_pipeline_asset_ids(state, district, block):
    suffix = tehsil_suffix(district, block)
    return [
        mws_asset_id(state, district, block, f"swb1_{suffix}"),
        mws_asset_id(state, district, block, f"swb2_{suffix}"),
        mws_asset_id(state, district, block, f"swb3_{suffix}"),
        mws_asset_id(state, district, block, f"swb4_{suffix}"),
    ]
