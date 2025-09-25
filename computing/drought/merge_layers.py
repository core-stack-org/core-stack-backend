import ee

from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    is_gee_asset_exists,
    ee_initialize,
    get_gee_dir_path,
    export_vector_asset_to_gee,
)
from functools import reduce


def merge_drought_layers_chunks(
    roi, asset_suffix, asset_folder_list, app_type, current_year, chunk_size
):
    dst_filename = "drought_" + asset_suffix + "_" + str(current_year)
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_HELPER_PATH"]
        )
        + dst_filename
    )

    size = roi.size().getInfo()
    parts = size // chunk_size
    assets = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = (
            asset_suffix
            + "_drought_"
            + str(start)
            + "-"
            + str(end)
            + "_"
            + str(current_year)
        )
        src_asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_HELPER_PATH"]
            )
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
    # Create required GEE asset path components
    ee_initialize(gee_account_id)

    # Create export asset path (must be constant for export)
    description = f"drought_{asset_suffix}_{start_year}_{end_year}"
    asset_id = f"{get_gee_dir_path(asset_folder_list)}{description}"

    # Check if asset already exists
    if is_gee_asset_exists(asset_id):
        return None

    def get_collection_path(year: int) -> str:
        """Get the full path for a year's collection."""
        return f"{get_gee_dir_path(asset_folder_list, asset_path=GEE_PATHS[app_type]['GEE_HELPER_PATH'])}drought_{asset_suffix}_{year}"

    # Get base feature collection
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
        """Server-side function to merge data from all years."""
        uid = feature.get("uid")

        def process_year(prev_feature, year):
            """Process a single year's data."""
            feat = ee.Feature(prev_feature)

            # Get year's collection
            year_fc = ee.FeatureCollection(get_collection_path(year))
            year_feature = ee.Feature(
                year_fc.filter(ee.Filter.equals("uid", uid)).first()
            )

            # Base property names
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

            # Base property names
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

            prop_names = ee.List(base_props)

            renamed_prop_names = ee.List(renamed_props)

            # Get property values
            prop_values = prop_names.map(lambda x: year_feature.get(x))

            # Create base properties dictionary
            base_dict = ee.Dictionary.fromLists(renamed_prop_names, prop_values)

            all_properties = ee.Feature(year_feature).propertyNames()

            # Filter properties that start with the prefix
            orig_rainfall_names = all_properties.filter(
                ee.Filter.stringStartsWith("item", "monthly_rainfall_deviation_")
            )

            def rename_property(prop):
                return ee.String(prop).replace("monthly_rainfall_deviation_20", "rd")

            renamed_rainfall_names = orig_rainfall_names.map(rename_property)

            # Get rainfall values
            rainfall_values = orig_rainfall_names.map(lambda x: year_feature.get(x))

            # Create rainfall properties dictionary
            rainfall_dict = ee.Dictionary.fromLists(
                renamed_rainfall_names, rainfall_values
            )

            # Adding dry_spell length values from previous years with current year
            total_dryspell = ee.Number(feat.get("avg_dryspell")).add(
                ee.Number(year_feature.get("dryspell_length_" + str(year)))
            )

            return (
                feat.set(base_dict)
                .set(rainfall_dict)
                .set("avg_dryspell", total_dryspell)
            )

        feature = feature.set("avg_dryspell", 0)

        # Process all years
        years = range(start_year, end_year + 1)
        processed_feature = reduce(process_year, years, feature)

        num_years = ee.Number(end_year).subtract(start_year).add(1)
        avg_dryspell = ee.Number(processed_feature.get("avg_dryspell")).divide(
            num_years
        )

        # Set average dryspell and remove total
        return processed_feature.set("avg_dryspell", avg_dryspell)

    # Process all features
    merged_fc = geometries_with_ids.map(merge_year_data)

    # Export feature collection to GEE
    task_id = export_vector_asset_to_gee(merged_fc, description, asset_id)
    return task_id
