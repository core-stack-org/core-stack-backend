import ee

from gee_computing.models import GEEAccount
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    is_gee_asset_exists,
    ee_initialize,
    get_gee_dir_path,
    export_vector_asset_to_gee,
    build_gee_helper_paths,
)
from functools import reduce


def merge_drought_layers_chunks(
    roi,
    asset_suffix,
    asset_folder_list,
    app_type,
    current_year,
    chunk_size,
    gee_account_id,
):
    print("app type {app_type}")
    ee_initialize(gee_account_id)
    gee_obj = GEEAccount.objects.get(pk=gee_account_id)

    helper_layer_path = build_gee_helper_paths(app_type, gee_obj.helper_account.name)
    dst_filename = f"drought_{asset_suffix}_{current_year}_v2"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )

    size = roi.size().getInfo()
    parts = size // chunk_size
    assets = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = f"{asset_suffix}_drought_{start}-{end}_{current_year}_v2"
        src_asset_id = (
            get_gee_dir_path(asset_folder_list, asset_path=helper_layer_path)
            + block_name_for_parts
        )
        if is_gee_asset_exists(src_asset_id):
            assets.append(ee.FeatureCollection(src_asset_id))

    asset = ee.FeatureCollection(assets).flatten()
    task_id = export_vector_asset_to_gee(asset, dst_filename, asset_id)
    return task_id


def merge_yearly_layers(
    asset_suffix, asset_folder_list, app_type, start_year, end_year, gee_account_id
):
    print(asset_suffix)
    print(asset_folder_list)
    print(f"merge yearly layers {app_type}")

    ee_initialize(gee_account_id)
    gee_obj = GEEAccount.objects.get(pk=gee_account_id)

    description = f"drought_{asset_suffix}"

    gee_asset = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    asset_id = f"{gee_asset}{description}"

    print(description)
    print(asset_id)

    if is_gee_asset_exists(asset_id):
        ee.data.deleteAsset(asset_id)

    def get_collection_path(year: int) -> str:
        asset_path = GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        return f"{get_gee_dir_path(asset_folder_list, asset_path=asset_path)}drought_{asset_suffix}_{year}_v2"

    # Base collection
    first_year_fc = ee.FeatureCollection(get_collection_path(start_year))

    geometries_with_ids = first_year_fc.map(
        lambda f: ee.Feature(
            f.geometry(),
            {
                "uid": f.get("uid"),
                "area_in_ha": f.get("area_in_ha"),
                "avg_dryspell": f.get("dryspell_length_" + str(start_year)),
            },
        )
    )

    def merge_year_data(feature):
        uid = feature.get("uid")

        def process_year(prev_feature, year):
            feat = ee.Feature(prev_feature)

            year_fc = ee.FeatureCollection(get_collection_path(year))

            filtered = year_fc.filter(ee.Filter.equals("uid", uid))

            # ✅ SAFE feature
            year_feature = ee.Feature(
                ee.Algorithms.If(
                    filtered.size().gt(0), filtered.first(), ee.Feature(None, {})
                )
            )

            # -------------------------
            # Property lists
            # -------------------------
            base_props = [
                "drought_labels_" + str(year),
                "dryspell_length_" + str(year),
                "freq_of_drought_" + str(year) + "_at_threshold_0",
                "freq_of_drought_" + str(year) + "_at_threshold_1",
                "freq_of_drought_" + str(year) + "_at_threshold_2",
                "freq_of_drought_" + str(year) + "_at_threshold_3",
                "intensity_of_drought_" + str(year) + "_at_threshold_0",
                "intensity_of_drought_" + str(year) + "_at_threshold_1",
                "intensity_of_drought_" + str(year) + "_at_threshold_2",
                "intensity_of_drought_" + str(year) + "_at_threshold_3",
                "number_of_weeks_in_no_drought_" + str(year),
                "number_of_weeks_in_mild_drought_" + str(year),
                "number_of_weeks_in_moderate_drought_" + str(year),
                "number_of_weeks_in_severe_drought_" + str(year),
                "kharif_cropped_sqkm_" + str(year),
                "monsoon_onset_" + str(year),
                "percent_of_area_cropped_kharif_" + str(year),
                "total_weeks_" + str(year),
            ]

            renamed_props = [
                "drlb_" + str(year),
                "drysp_" + str(year),
                "frth0_" + str(year),
                "frth1_" + str(year),
                "frth2_" + str(year),
                "frth3_" + str(year),
                "inth0_" + str(year),
                "inth1_" + str(year),
                "inth2_" + str(year),
                "inth3_" + str(year),
                "w_no_" + str(year),
                "w_mld_" + str(year),
                "w_mod_" + str(year),
                "w_sev_" + str(year),
                "kh_cr_" + str(year),
                "m_ons_" + str(year),
                "pcr_k_" + str(year),
                "t_wks_" + str(year),
            ]

            # -------------------------
            # SAFE getter
            # -------------------------
            def safe_get(prop):
                return ee.Algorithms.If(
                    year_feature.get(prop), year_feature.get(prop), None
                )

            prop_values = ee.List(base_props).map(lambda x: safe_get(x))

            base_dict = ee.Dictionary.fromLists(ee.List(renamed_props), prop_values)

            # -------------------------
            # Rainfall properties
            # -------------------------
            all_properties = year_feature.propertyNames()

            orig_rainfall_names = all_properties.filter(
                ee.Filter.stringStartsWith("item", "monthly_rainfall_deviation_")
            )

            def rename_property(prop):
                return ee.String(prop).replace("monthly_rainfall_deviation_20", "rd")

            renamed_rainfall_names = orig_rainfall_names.map(rename_property)

            rainfall_values = orig_rainfall_names.map(lambda x: safe_get(x))

            rainfall_dict = ee.Dictionary.fromLists(
                renamed_rainfall_names, rainfall_values
            )

            # -------------------------
            # SAFE dryspell
            # -------------------------
            dryspell_val = ee.Number(
                ee.Algorithms.If(
                    year_feature.get("dryspell_length_" + str(year)),
                    year_feature.get("dryspell_length_" + str(year)),
                    0,
                )
            )

            total_dryspell = ee.Number(feat.get("avg_dryspell")).add(dryspell_val)

            return (
                feat.set(base_dict)
                .set(rainfall_dict)
                .set("avg_dryspell", total_dryspell)
            )

        feature = feature.set("avg_dryspell", 0)

        years = range(start_year, end_year + 1)
        processed_feature = reduce(process_year, years, feature)

        num_years = ee.Number(end_year).subtract(start_year).add(1)

        avg_dryspell = ee.Number(processed_feature.get("avg_dryspell")).divide(
            num_years
        )

        return processed_feature.set("avg_dryspell", avg_dryspell)

    merged_fc = geometries_with_ids.map(merge_year_data)

    task_id = export_vector_asset_to_gee(merged_fc, description, asset_id)

    return task_id
