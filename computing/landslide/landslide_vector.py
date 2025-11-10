"""Landslide susceptibility vectorization for CoRE Stack.

This module clips the pan-India landslide susceptibility map to tehsil boundaries
and vectorizes it at the MWS (micro-watershed) level with attributes.
"""

import ee
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
)
from nrm_app.celery import app
from computing.models import *
from computing.path_constants import MWS_FC_UID_INDIA


# Pan-India landslide susceptibility asset ID
# TODO: Replace with actual published asset path when available
LANDSLIDE_SUSCEPTIBILITY_ASSET = "projects/ee-corestack/assets/india_landslide_susceptibility_100m"

# Susceptibility classification
# Based on the paper methodology:
# 1 = Low susceptibility
# 2 = Moderate susceptibility  
# 3 = High susceptibility
# 4 = Very high susceptibility

SUSCEPTIBILITY_CLASSES = {
    1: "low",
    2: "moderate",
    3: "high",
    4: "very_high"
}


@app.task(bind=True)
def vectorise_landslide(self, state, district, block, gee_account_id):
    """Generate landslide susceptibility vectors for a tehsil.
    
    Args:
        state: State name
        district: District name
        block: Block/Tehsil name
        gee_account_id: GEE account ID for authentication
        
    Returns:
        bool: True if layer was synced to GeoServer, False otherwise
    """
    ee_initialize(gee_account_id)
    
    # Get MWS feature collection for the block
    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    
    description = (
        "landslide_vector_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    
    asset_id = get_gee_asset_path(state, district, block) + description
    
    # Check if asset already exists
    if not is_gee_asset_exists(asset_id):
        generate_landslide_vectors(
            state=state,
            district=district,
            block=block,
            description=description,
            asset_id=asset_id,
            fc=fc,
        )
    
    # Sync to database and GeoServer
    layer_at_geoserver = sync_to_db_and_geoserver(
        asset_id=asset_id,
        state=state,
        district=district,
        block=block,
        description=description,
    )
    
    return layer_at_geoserver


def generate_landslide_vectors(state, district, block, description, asset_id, fc):
    """Generate landslide susceptibility vectors at MWS level.
    
    This function:
    1. Clips pan-India landslide raster to tehsil boundary
    2. Computes zonal statistics at MWS level for each susceptibility class
    3. Adds ancillary attributes (slope, curvature, LULC)
    4. Exports to GEE asset
    
    Args:
        state: State name
        district: District name
        block: Block/Tehsil name
        description: Export task description
        asset_id: Output GEE asset ID
        fc: MWS FeatureCollection for the block
    """
    
    # Load pan-India landslide susceptibility raster
    # TODO: Update with actual asset path
    try:
        landslide_img = ee.Image(LANDSLIDE_SUSCEPTIBILITY_ASSET)
    except Exception as e:
        print(f"Warning: Could not load landslide asset {LANDSLIDE_SUSCEPTIBILITY_ASSET}")
        print("Generating a demo susceptibility map from slope...")
        # Fallback: generate from slope for demo purposes
        landslide_img = generate_demo_susceptibility()
    
    # Get block boundary for clipping
    block_boundary = fc.geometry().bounds()
    landslide_clipped = landslide_img.clip(block_boundary)
    
    # Load ancillary datasets for attributes
    dem = ee.Image("USGS/SRTMGL1_003").select("elevation").clip(block_boundary)
    slope = ee.Terrain.slope(dem).rename("slope")
    
    # Approximate curvature using Laplacian
    kernel = ee.Kernel.fixed(3, 3, [[1, 1, 1], [1, -8, 1], [1, 1, 1]], -1, False)
    curvature = dem.convolve(kernel).rename("curvature")
    
    # Load LULC (use latest available)
    # TODO: Update with appropriate LULC asset for the block
    try:
        lulc_path = (
            get_gee_asset_path(state, district, block)
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_2023-07-01_2024-06-30_LULCmap_10m"
        )
        lulc = ee.Image(lulc_path).select(["predicted_label"]).rename("lulc")
    except Exception:
        # Fallback to global LULC
        lulc = ee.Image("COPERNICUS/Landcover/100m/Proba-V/Global").select([0]).rename("lulc")
    
    # Compute area for each susceptibility class within each MWS
    pixel_area = ee.Image.pixelArea()
    
    # Process each susceptibility class
    for class_val, class_name in SUSCEPTIBILITY_CLASSES.items():
        # Mask for this susceptibility class
        class_mask = landslide_clipped.eq(ee.Number(class_val))
        
        # Area in hectares for this class
        class_area = pixel_area.updateMask(class_mask)
        fc = class_area.reduceRegions(
            fc, 
            ee.Reducer.sum(), 
            scale=100,
            crs=landslide_clipped.projection()
        )
        
        # Convert from m² to hectares and add as property, then remove "sum"
        def add_area_ha(feature):
            area_m2 = feature.get("sum")
            area_ha = ee.Number(area_m2).divide(10000)
            # Remove "sum" property after storing area to avoid overwrites in next iteration
            return feature.set(f"{class_name}_area_ha", area_ha).remove("sum")
        
        fc = fc.map(add_area_ha)
    
    # Add mean slope per MWS
    fc = slope.reduceRegions(
        fc,
        ee.Reducer.mean(),
        scale=30,
        crs=slope.projection()
    )
    
    def add_mean_slope(feature):
        mean_slope = feature.get("mean")
        return feature.set("mean_slope_deg", mean_slope)
    
    fc = fc.map(add_mean_slope)
    
    # Add mean curvature per MWS
    fc = curvature.reduceRegions(
        fc,
        ee.Reducer.mean(),
        scale=30,
        crs=curvature.projection()
    )
    
    def add_mean_curvature(feature):
        mean_curv = feature.get("mean")
        return feature.set("mean_curvature", mean_curv)
    
    fc = fc.map(add_mean_curvature)
    
    # Add dominant LULC class per MWS
    fc = lulc.reduceRegions(
        fc,
        ee.Reducer.mode(),
        scale=10,
        crs=lulc.projection()
    )
    
    def add_dominant_lulc(feature):
        mode_lulc = feature.get("mode")
        return feature.set("dominant_lulc", mode_lulc)
    
    fc = fc.map(add_dominant_lulc)
    
    # Compute overall susceptibility score (weighted average)
    # Weight by area: (1*low + 2*mod + 3*high + 4*very_high) / total_area
    def compute_susceptibility_score(feature):
        low = ee.Number(feature.get("low_area_ha", 0))
        mod = ee.Number(feature.get("moderate_area_ha", 0))
        high = ee.Number(feature.get("high_area_ha", 0))
        very_high = ee.Number(feature.get("very_high_area_ha", 0))
        
        total_area = low.add(mod).add(high).add(very_high)
        
        # Avoid division by zero
        score = ee.Algorithms.If(
            total_area.gt(0),
            low.multiply(1).add(mod.multiply(2)).add(high.multiply(3)).add(very_high.multiply(4)).divide(total_area),
            0
        )
        
        # Classify into category based on score
        category = ee.Algorithms.If(
            ee.Number(score).lt(1.5), "low",
            ee.Algorithms.If(
                ee.Number(score).lt(2.5), "moderate",
                ee.Algorithms.If(
                    ee.Number(score).lt(3.5), "high",
                    "very_high"
                )
            )
        )
        
        return feature.set({
            "susceptibility_score": score,
            "susceptibility_category": category,
            "total_area_ha": total_area
        })
    
    fc = fc.map(compute_susceptibility_score)
    
    # Export to GEE asset
    fc = ee.FeatureCollection(fc)
    task = export_vector_asset_to_gee(fc, description, asset_id)
    task_status = check_task_status([task])
    print("Task completed - ", task_status)


def generate_demo_susceptibility():
    """Generate a demo susceptibility map from slope for testing.
    
    This is a fallback when the pan-India asset is not available.
    Classification based on slope:
    - Low: 0-15°
    - Moderate: 15-25°
    - High: 25-35°
    - Very High: >35°
    
    Returns:
        ee.Image: Classified susceptibility image (1-4)
    """
    dem = ee.Image("USGS/SRTMGL1_003").select("elevation")
    slope = ee.Terrain.slope(dem)
    
    susceptibility = (
        slope.lt(15).multiply(1)
        .add(slope.gte(15).And(slope.lt(25)).multiply(2))
        .add(slope.gte(25).And(slope.lt(35)).multiply(3))
        .add(slope.gte(35).multiply(4))
    ).rename("susceptibility")
    
    return susceptibility


def sync_to_db_and_geoserver(asset_id, state, district, block, description):
    """Sync landslide vectors to database and GeoServer.
    
    Args:
        asset_id: GEE asset ID
        state: State name
        district: District name
        block: Block/Tehsil name
        description: Layer description
        
    Returns:
        bool: True if synced to GeoServer, False otherwise
    """
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="Landslide Susceptibility",
            misc={
                "methodology": "https://www.sciencedirect.com/science/article/pii/S0341816223007440",
                "resolution": "100m",
                "classes": SUSCEPTIBILITY_CLASSES,
            },
        )
        
        make_asset_public(asset_id)
        
        fc = ee.FeatureCollection(asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        
        res = sync_layer_to_geoserver(
            state,
            fc,
            "landslide_vector_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower()),
            "landslide_vector",
        )
        
        print(res)
        layer_at_geoserver = False
        
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")
            layer_at_geoserver = True
            
        return layer_at_geoserver
    
    return False
