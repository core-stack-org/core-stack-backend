import os
import geopandas as gpd
import pandas as pd

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    PROJECT_ROOT,
    build_output_vector_path,
    load_precomputed_watersheds,
    read_validated_vector_file,
    write_vector_output,
    validate_geometry,
)
from computing.STAC_specs import generate_STAC_layerwise

from computing.config_loader import (
    PAN_INDIA_SOGE_PATH,
    LOCAL_SOGE_OUTPUT,
)

GEOSERVER_WORKSPACE = "soge"


def _compute_soge_for_watersheds(watersheds_gdf, soge_gdf):
    """
    Finds the largest intersecting SOGE feature for each watershed and transfers its properties.
    Mimics the Earth Engine implementation using GeoPandas overlay and area calculations.
    """
    # Calculate watershed area in ha if missing
    if "area_in_ha" not in watersheds_gdf.columns:
        mws_metric = watersheds_gdf.to_crs("EPSG:6933")
        watersheds_gdf["area_in_ha"] = mws_metric.geometry.area / 10000.0

    if soge_gdf.empty:
        # Create an empty dataframe to merge, forcing the No Data logic below
        largest_intersections = pd.DataFrame(columns=["uid"])
    else:
        # Reproject to metric CRS for accurate area intersection
        metric_crs = "EPSG:6933"
        mws_metric = watersheds_gdf.to_crs(metric_crs)
        soge_metric = soge_gdf.to_crs(metric_crs)
        
        # Calculate intersections
        intersection_gdf = gpd.overlay(mws_metric, soge_metric, how='intersection')
        intersection_gdf["intersection_area_ha"] = intersection_gdf.geometry.area / 10000.0
        
        # Sort and keep the largest intersection per watershed uid
        intersection_gdf = intersection_gdf.sort_values(by="intersection_area_ha", ascending=False)
        largest_intersections = intersection_gdf.drop_duplicates(subset=["uid"], keep="first").copy()
        
        # Calculate percentage area
        largest_intersections["pct_area_soge"] = (
            largest_intersections["intersection_area_ha"] / largest_intersections["area_in_ha"]
        ) * 100.0
        
        # Rename columns to match desired output
        col_mapping = {
            "intersection_area_ha": "max_intersection_area_ha",
            "block": "soge_block",
            "district": "soge_district",
            "objectid": "soge_objectid",
            "state": "soge_state",
            "tehsil": "soge_tehsil",
        }
        largest_intersections = largest_intersections.rename(columns=col_mapping)

    # Columns to transfer from SOGE to MWS
    target_cols = [
        "uid", "max_intersection_area_ha", "pct_area_soge", "class",
        "agwd_dom_i", "agwd_irr", "agwd_tot", "ar_gwr_tot", "code",
        "gwr_2011_2", "na_gwa", "nat_discha", "sgw_dev_pe",
        "soge_block", "soge_district", "soge_objectid", "soge_state", "soge_tehsil"
    ]
    
    # Keep only target columns that actually exist in the intersections
    cols_to_keep = [c for c in target_cols if c in largest_intersections.columns]
    
    # Left join to retain all original watersheds
    result_gdf = watersheds_gdf.merge(largest_intersections[cols_to_keep], on="uid", how="left")
    
    # Fill defaults for No Data / non-intersecting
    result_gdf["max_intersection_area_ha"] = result_gdf.get("max_intersection_area_ha", pd.Series(dtype=float)).fillna(0)
    result_gdf["pct_area_soge"] = result_gdf.get("pct_area_soge", pd.Series(dtype=float)).fillna(0)
    result_gdf["class"] = result_gdf.get("class", pd.Series(dtype=str)).fillna("No Data")
    
    numeric_cols = [
        "agwd_dom_i", "agwd_irr", "agwd_tot", "ar_gwr_tot", "code", 
        "gwr_2011_2", "na_gwa", "nat_discha", "sgw_dev_pe", "soge_objectid"
    ]
    for c in numeric_cols:
        if c in result_gdf.columns:
            result_gdf[c] = result_gdf[c].fillna(-9999)
        else:
            result_gdf[c] = -9999
            
    string_cols = ["soge_block", "soge_district", "soge_state", "soge_tehsil"]
    for c in string_cols:
        if c in result_gdf.columns:
            result_gdf[c] = result_gdf[c].fillna("")
        else:
            result_gdf[c] = ""
            
    return result_gdf


@app.task(bind=True)
def generate_soge_vector_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"soge_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"soge_vector_{asset_suffix}".lower()
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    if not os.path.exists(PAN_INDIA_SOGE_PATH):
        print(f"Warning: PAN INDIA SOGE file not found at {PAN_INDIA_SOGE_PATH}. Proceeding with No Data.")
        # Create empty dummy GDF
        soge_gdf = gpd.GeoDataFrame(geometry=[])
    else:
        print("Loading SOGE data overlapping ROI...")
        soge_gdf = gpd.read_file(PAN_INDIA_SOGE_PATH, mask=watersheds_gdf)
        soge_gdf = validate_geometry(soge_gdf)
        if soge_gdf.empty:
            raise ValueError("PAN INDIA SOGE file has no valid geometries overlapping ROI")
        print(f"Loaded {len(soge_gdf)} SOGE features")

    result_gdf = _compute_soge_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        soge_gdf=soge_gdf,
    )
    print(f"Final valid SOGE mapped features: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_SOGE_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local SOGE vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")
        if geoserver_response and geoserver_response.get("status_code") in (200, 201):
            layer_at_geoserver = True

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="SOGE",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for SOGE vector")
            
            try:
                layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                    state=state,
                    district=district,
                    block=block,
                    layer_name="stage_of_groundwater_extraction_vector",
                )
                update_layer_sync_status(
                    layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
                )
                print("STAC metadata updated for SOGE vector")
            except Exception as e:
                print(f"Error generating STAC: {e}")

    return layer_at_geoserver



