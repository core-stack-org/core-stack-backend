import ee
from computing.utils import sync_layer_to_geoserver, save_layer_info_to_db
from utilities.auth_utils import auth_free
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
)
from nrm_app.celery import app


def get_column_name(base_name, year):
    abbreviations = {
        "Low_Density": "lo_de",
        "High_Density": "hi_de",
        "Missing_Data": "mi_da",
    }
    base = abbreviations.get(base_name, base_name[:5])
    return f"{base}_{str(year)}"


def get_mws_features(state, district, block):
    asset_id = (
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    return ee.FeatureCollection(asset_id)


@app.task(bind=True)
def tree_health_ccd_vector(self, state, district, block, start_year, end_year):
    ee_initialize()

    mws_features = get_mws_features(state, district, block)
    roi = mws_features

    uid_data = {}

    for year in range(start_year, end_year + 1):
        print(f"Processing year {year}")

        task_list = [overall_vector(roi, state, district, block, year)]
        task_id_list = check_task_status(task_list)
        print(
            f"Change vector task completed for year {year} - task_id_list: {task_id_list}"
        )

        year_asset_id = (
            get_gee_asset_path(state, district, block)
            + f"tree_health_ccd_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_{year}"
        )
        try:
            year_fc = ee.FeatureCollection(year_asset_id).getInfo()
            if not year_fc or "features" not in year_fc:
                print(f"Warning: No features found for year {year}")
                continue

            for feature in year_fc["features"]:
                uid = feature["properties"].get("uid")
                area_in_ha = feature["properties"].get("area_in_ha")
                if not uid:
                    print(f"Warning: Feature without UID found in year {year}")
                    continue

                if uid not in uid_data:
                    uid_data[uid] = {
                        "uid": uid,
                        "area_in_ha": area_in_ha,
                        "properties": {},
                    }

                # Add data for each classification type with proper column names
                for class_type in ["lo_de", "hi_de", "mi_da"]:
                    old_column = f"{class_type}_{year}"
                    new_column = get_column_name(class_type, year)
                    value = feature["properties"].get(old_column, 0)
                    uid_data[uid]["properties"][new_column] = value

        except Exception as e:
            print(f"Error processing year {year}: {str(e)}")
            continue

    final_features = []
    mws_info = mws_features.getInfo()

    for uid, data in uid_data.items():
        matching_feature = next(
            (f for f in mws_info["features"] if f["properties"]["uid"] == uid), None
        )
        # export_vector_asset_to_gee
        if matching_feature:
            total_density = sum(
                data["properties"].get(get_column_name("High_Density", year), 0)
                for year in range(start_year, end_year + 1)
            )
            avg_density = total_density / (end_year - start_year + 1)

            data["properties"]["avg_den"] = avg_density
            data["properties"]["area_in_ha"] = data["area_in_ha"]
            data["properties"]["uid"] = uid

            final_feature = {
                "type": "Feature",
                "geometry": matching_feature["geometry"],
                "properties": data["properties"],
            }
            final_features.append(final_feature)
        else:
            print(f"Warning: No matching geometry found for UID {uid}")

    if not final_features:
        raise ValueError(
            "No features to export! Check if data was properly processed for all years."
        )

    final_fc = ee.FeatureCollection(final_features)

    geo_filename = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_tree_health_ccd_vector_{start_year}_{end_year}"
    description = (
        "tree_health_ccd_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + str(start_year)
        + "_"
        + str(end_year)
    )
    asset_id = get_gee_asset_path(state, district, block) + description
    task = export_vector_asset_to_gee(final_fc, description, asset_id)
    task_id_list = check_task_status([task])
    print(
        f"ccd vector task completed for year {start_year}_{end_year} - task_id_list: {task_id_list}"
    )
    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=geo_filename,
            asset_id=asset_id,
            dataset_name="Ccd Vector",
        )
    make_asset_public(asset_id)
    final_fc = {"type": "FeatureCollection", "features": final_features}
    try:
        sync_res = sync_layer_to_geoserver(state, final_fc, geo_filename, "ccd")
        if sync_res["status_code"] == 201:
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=geo_filename,
                asset_id=asset_id,
                dataset_name="Ccd Vector",
                sync_to_geoserver=True,
            )
    except Exception as e:
        print(f"Error syncing combined data to GeoServer: {e}")
        raise

    return {
        "status": "Completed",
        "features_processed": len(final_features),
        "year_range": f"{start_year}-{end_year}",
        "filename": geo_filename,
    }


def overall_vector(roi, state, district, block, year):
    """Generate vector data for different density classes."""
    args = [
        {"value": 0.0, "label": "Low_Density"},
        {"value": 1.0, "label": "High_Density"},
        {"value": 2.0, "label": "Missing_Data"},
    ]
    return generate_vector(roi, args, state, district, block, year)


def generate_vector(roi, args, state, district, block, year):
    """Generate vector data for a specific year based on raster data."""
    raster = ee.Image(
        get_gee_asset_path(state, district, block)
        + "tree_health_ccd_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + str(year)
    )

    fc = roi
    for arg in args:
        raster = raster.select(["cc"])
        mask = raster.eq(ee.Number(arg["value"]))
        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc, reducer=ee.Reducer.sum(), scale=25, crs=raster.projection()
        )

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            column_name = get_column_name(arg["label"], year)
            return feature.set(column_name, value)

        fc = fc.map(process_feature)

    description = (
        "tree_health_ccd_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + str(year)
    )
    task = export_vector_asset_to_gee(
        fc, description, get_gee_asset_path(state, district, block) + description
    )
    return task
