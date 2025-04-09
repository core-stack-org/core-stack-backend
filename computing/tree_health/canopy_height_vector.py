import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)
from computing.views import create_dataset_for_generated_layer


def get_column_name(base_name, year):
    """
    Generate column name with 10 character limit including year.
    Examples:
    Short_Trees -> sh_tr_2023
    Medium_Height_Trees -> md_tr_2023
    Tall_Trees -> tl_tr_2023
    Missing_Data -> mi_da_2023
    """
    abbreviations = {
        'Short_Trees': 'sh_tr',
        'Medium_Height_Trees': 'md_tr',
        'Tall_Trees': 'tl_tr',
        'Missing_Data': 'mi_da'
    }
    base = abbreviations.get(base_name, base_name[:5])
    return f"{base}_{str(year)}"  # Uses last 2 digits of year


@app.task(bind=True)
def tree_health_ch_vector(self, state, district, block, start_year, end_year, user):
    """Process canopy height data for multiple years and combine into a single GeoServer layer."""
    ee_initialize()
    # Get the reference MWS features
    mws_features = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    # Dictionary to store aggregated data for each UID
    uid_data = {}

    # Process data for each year
    for year in range(start_year, end_year):
        print(f"Processing year {year}")
        
        task_list = [overall_vector(mws_features, state, district, block, year)]
        task_id_list = check_task_status(task_list)
        print(f"Change vector task completed for year {year} - task_id_list: {task_id_list}")

        # Get the feature collection for the current year
        year_asset_id = (
            get_gee_asset_path(state, district, block)
            + f"tree_health_ch_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_{year}"
        )
        
        try:
            year_fc = ee.FeatureCollection(year_asset_id).getInfo()
            if not year_fc or 'features' not in year_fc:
                print(f"Warning: No features found for year {year}")
                continue
                
            # Process features for the current year
            for feature in year_fc['features']:
                uid = feature['properties'].get('uid')
                area_in_ha = feature['properties'].get('area_in_ha')
                if not uid:
                    print(f"Warning: Feature without UID found in year {year}")
                    continue
                    
                if uid not in uid_data:
                    uid_data[uid] = {
                        'uid': uid,
                        'area_in_ha': area_in_ha,
                        'properties': {},
                        'geometry': feature['geometry']
                    }
                
                # Add data for each classification type with proper column names
                for class_type in ['sh_tr', 'md_tr', 'tl_tr', 'mi_da']:
                    old_column = f"{class_type}_{year}"
                    new_column = get_column_name(class_type, year)
                    value = feature['properties'].get(old_column, 0)
                    uid_data[uid]['properties'][new_column] = value

        except Exception as e:
            print(f"Error processing year {year}: {str(e)}")
            continue

    # Create the final combined feature collection
    final_features = []
    
    for uid, data in uid_data.items():
        # Calculate statistics for each height class
        total_short = sum(
            data['properties'].get(get_column_name('Short_Trees', year), 0)
            for year in range(start_year, end_year + 1)
        )
        total_medium = sum(
            data['properties'].get(get_column_name('Medium_Height_Trees', year), 0)
            for year in range(start_year, end_year + 1)
        )
        total_tall = sum(
            data['properties'].get(get_column_name('Tall_Trees', year), 0)
            for year in range(start_year, end_year + 1)
        )
        
        # Add computed statistics with proper column names
        data['properties']['avg_sh_tr'] = total_short / (end_year - start_year + 1)
        data['properties']['avg_md_tr'] = total_medium / (end_year - start_year + 1)
        data['properties']['avg_tl_tr'] = total_tall / (end_year - start_year + 1)
        data['properties']['area_in_ha'] = area_in_ha
        data['properties']['uid'] = uid
        
        final_feature = {
            'type': 'Feature',
            'geometry': data['geometry'],
            'properties': data['properties']
        }
        final_features.append(final_feature)

    if not final_features:
        raise ValueError("No features to export! Check if data was properly processed for all years.")

    # Create the final feature collection
    final_fc = {
        'type': 'FeatureCollection',
        'features': final_features
    }

    # Sync to GeoServer with a name that indicates the year range
    layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_tree_health_ch_vector_{start_year}_{end_year}"
    
    try:
        sync_res = sync_layer_to_geoserver(state, final_fc, layer_name, "canopy_height")
    except Exception as e:
        print(f"Error syncing combined data to GeoServer: {e}")
        raise
    
    # Generated Dataset data to db 
    try:
        create_dataset_for_generated_layer(state, district, block, layer_name, user, gee_path=None, layer_type='vector', workspace='canopy_height', algorithm=None, version=None, style_name=None, misc=None)
        print("Dataset entry created for canopy_height vector")
    except Exception as e:
        print(f"Exception while creating entry for canopy_height vector in dataset table: {str(e)}")

    return {
        "status": "Completed",
        "features_processed": len(final_features),
        "year_range": f"{start_year}-{end_year}",
        "filename": layer_name
    }


def overall_vector(roi, state, district, block, year):
    """Generate vector data for different height classes."""
    args = [
        {"value": 0, "label": "Short_Trees"},
        {"value": 1, "label": "Medium_Height_Trees"},
        {"value": 2, "label": "Tall_Trees"},
        {"value": 3, "label": "Missing_Data"},
    ]
    return generate_vector(roi, args, state, district, block, year)


def generate_vector(roi, args, state, district, block, year):
    """Generate vector data for a specific year based on raster data."""
    raster = ee.Image(
        get_gee_asset_path(state, district, block)
        + "tree_health_ch_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_"
        + str(year)
    )

    fc = roi
    for arg in args:
        raster = raster.select(["ch_class"])
        mask = raster.eq(ee.Number(arg["value"]))
        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.sum(),
            scale=25,
            crs=raster.projection()
        )

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            # Use the new column naming function
            column_name = get_column_name(arg["label"], year)
            return feature.set(column_name, value)

        fc = fc.map(process_feature)

    description = (
        "tree_health_ch_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
        + "_"
        + str(year)
    )

    task = ee.batch.Export.table.toAsset(
        collection=fc,
        description=description,
        assetId=get_gee_asset_path(state, district, block) + description,
    )
    task.start()
    return task.status()["id"]