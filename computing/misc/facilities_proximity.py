"""
Facilities Proximity Layer Generator

Filters village facilities data from GEE by tehsil boundary and exports to GEE asset + GeoServer.
Uses admin boundary clipping (spatial filtering) for fast server-side processing.

Usage:
    python -c "
        import os
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nrm_app.settings')
        import django
        django.setup()
        from computing.misc.facilities_proximity import generate_facilities_proximity
        generate_facilities_proximity('Odisha', 'Koraput', 'Jaypur', gee_account_id=21)
    "

GEE Asset: projects/corestack-datasets/assets/datasets/pan_india_facilities
"""

import logging
import time
from datetime import datetime

import ee
from nrm_app.celery import app

from utilities.constants import GEE_PATHS, GEE_FACILITIES_DATASET_PATH
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    is_gee_asset_exists,
    create_gee_directory,
    get_gee_dir_path,
    get_gee_asset_path,
    export_vector_asset_to_gee,
    make_asset_public,
    check_task_status,
)
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    sync_fc_to_geoserver,
)

logger = logging.getLogger(__name__)

# Constants
FACILITIES_GEOSERVER_WORKSPACE = "testworkspace"
FACILITIES_DATASET_NAME = "Facilities Proximity"


def generate_facilities_proximity(state, district, block, gee_account_id=21):
    """
    Generate facilities proximity layer for a tehsil/block.
    
    Steps:
        1. Initialize GEE
        2. Filter facilities by admin boundary (spatial clipping)
        3. Export to GEE asset
        4. Make asset public
        5. Sync to GeoServer
        6. Update database
    
    Args:
        state: State name (e.g., "Odisha")
        district: District name (e.g., "Koraput")
        block: Block/Tehsil name (e.g., "Jaypur")
        gee_account_id: GEE account ID
    
    Returns:
        bool: True if layer synced to GeoServer successfully
    """
    start_time = datetime.now()
    print(f"[{start_time}] Starting facilities proximity for {state}/{district}/{block}")
    
    try:
        # Step 1: Initialize GEE
        ee_initialize(gee_account_id)
        
        # Verify facilities asset exists
        if not is_gee_asset_exists(GEE_FACILITIES_DATASET_PATH):
            print(f"ERROR: GEE asset not found: {GEE_FACILITIES_DATASET_PATH}")
            return False
        
        # Step 2: Build output asset ID
        asset_suffix = f"facilities_proximity_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        asset_id = get_gee_dir_path([state, district, block], GEE_PATHS["MWS"]["GEE_ASSET_PATH"]) + asset_suffix
        
        print(f"[{datetime.now()}] Asset ID: {asset_id}")
        
        # Step 3: Load admin boundary and filter facilities
        admin_boundary_path = (
            get_gee_asset_path(state, district, block, GEE_PATHS["MWS"]["GEE_ASSET_PATH"])
            + "admin_boundary_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        
        if not is_gee_asset_exists(admin_boundary_path):
            print(f"ERROR: Admin boundary not found: {admin_boundary_path}")
            return False
        
        # Load and filter
        facilities_fc = ee.FeatureCollection(GEE_FACILITIES_DATASET_PATH)
        admin_boundary = ee.FeatureCollection(admin_boundary_path)
        filtered_fc = facilities_fc.filterBounds(admin_boundary.geometry())
        
        # Step 4: Create GEE directory and export
        # create_gee_directory(state, district, block, GEE_PATHS["MWS"]["GEE_ASSET_PATH"])
        
        if not is_gee_asset_exists(asset_id):
            print(f"[{datetime.now()}] Exporting to GEE asset...")
            task_id = export_vector_asset_to_gee(filtered_fc, asset_suffix, asset_id)
            if task_id:
                check_task_status([task_id])
            else:
                print("ERROR: Failed to start export task")
                return False
        else:
            print(f"[{datetime.now()}] Asset already exists")
        
        # Step 5: Make public and save to database
        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)
            layer_name = f"facilities_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
            layer_id = save_layer_info_to_db(
                state, district, block,
                layer_name=layer_name,
                asset_id=asset_id,
                dataset_name=FACILITIES_DATASET_NAME,
            )
            print(f"[{datetime.now()}] Layer saved (ID: {layer_id})")
        
            # Step 6: Sync to GeoServer
            print(f"[{datetime.now()}] Syncing to GeoServer...")
            fc = ee.FeatureCollection(asset_id)
            res = sync_fc_to_geoserver(fc, state, f"facilities_{layer_name}", FACILITIES_GEOSERVER_WORKSPACE)
        
            if res and res.get("status_code") == 201 and layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"[{datetime.now()}] SUCCESS! Completed in {elapsed:.1f} seconds")
                return True
            else:
                print(f"ERROR: GeoServer sync failed")
                return False
    
    except Exception as e:
        print(f"ERROR: {e}")
        return False


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def generate_facilities_proximity_task(self, state, district, block, gee_account_id):
    """Celery task wrapper for generate_facilities_proximity"""
    try:
        return generate_facilities_proximity(state, district, block, gee_account_id)
    except Exception as e:
        logger.error(f"Celery task error: {e}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for {state}/{district}/{block}")
            return False
