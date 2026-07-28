"""Upload historical SPI-1/SPEI-3 timeseries and monsoon onset rasters to
GEE as image assets (plan.md Script 01d).

Only the timeseries (not the fitted params) go to GEE: plan.md's script
04a (computing the current period's SPI-1/SPEI-3) runs locally and reads
the local params files directly - it's script 04b (analog-year similarity
scoring, which runs on GEE) that needs to look up any historical year's
value per pixel, which is what these assets are for.

Reuses this repo's existing GCS-to-GEE-asset pattern
(utilities.gee_utils.upload_tif_from_gcs_to_gee: ee.Image.loadGeoTIFF +
Export.image.toAsset), adapted to write under our own GCS path
(ksheetiz/farm_stress/...) and to tag each asset with properties
(period_label/year/month) so later scripts can query them.
"""

import ee

from nrm_app.settings import GCS_BUCKET_NAME
from utilities.gee_utils import ee_initialize, gcs_config, check_task_status, is_gee_asset_exists
from computing.farm_stress.helper import generate_28day_periods
from computing.farm_stress.config import (
    FARM_STRESS_ASSET_ROOT,
    SPI_SCALE_M,
    LOCAL_DIR_SPI1_TIMESERIES,
    LOCAL_DIR_SPEI3_TIMESERIES,
    LOCAL_DIR_MONSOON_ONSET,
)

GCS_UPLOAD_PREFIX = "ksheetiz/farm_stress/asset_staging"


def ensure_farm_stress_folders(gee_account_id, subfolders):
    """Create FARM_STRESS_ASSET_ROOT and the given subfolders on GEE if they
    don't already exist. Doesn't reuse utilities.gee_utils.create_gee_folder
    - that helper is written for the older "legacy" asset path convention
    and fails on segments like "projects"/the bare project id, which aren't
    valid folder assets under the newer Cloud-project asset system this
    account uses (confirmed empirically).
    """
    ee_initialize(gee_account_id)
    parent = "/".join(FARM_STRESS_ASSET_ROOT.split("/")[:-1])  # .../assets/apps
    to_create = [parent, FARM_STRESS_ASSET_ROOT] + [
        f"{FARM_STRESS_ASSET_ROOT}/{s}" for s in subfolders
    ]
    for path in to_create:
        try:
            ee.data.getAsset(path)
        except Exception:
            ee.data.createAsset({"type": "Folder"}, path)
            print(f"Created folder: {path}")


def upload_local_tif_to_gee_asset(
    local_path,
    asset_id,
    scale,
    gee_account_id,
    band_name="b1",
    properties=None,
):
    """Upload one local GeoTIFF to GCS, then ingest it as a GEE image asset.

    gee_account_id: required, no default - see spi_spei_export.export_gsmap_period.
    Returns the export task id (submitted, not waited on - see
    upload_archive_batch for the submit-all/wait-all/download-all pattern).
    """
    ee_initialize(gee_account_id)

    file_name = local_path.split("/")[-1]
    blob_name = f"{GCS_UPLOAD_PREFIX}/{file_name}"
    bucket = gcs_config(gee_account_id)
    bucket.blob(blob_name).upload_from_filename(local_path)
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_name}"

    image = ee.Image.loadGeoTIFF(gcs_uri)
    image = image.rename([band_name])
    if properties:
        image = image.set(properties)

    task = ee.batch.Export.image.toAsset(
        image=image,
        description=asset_id.split("/")[-1],
        assetId=asset_id,
        scale=scale,
        region=image.geometry(),
        crs="EPSG:4326",
        maxPixels=1e13,
    )
    task.start()
    print(f"Submitted upload -> {asset_id} (task {task.status()['id']})")
    return task.status()["id"]


def upload_spi1_spei3_onset_archive(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    overwrite=False,
    poll_seconds=30,
):
    """Upload the full SPI-1 timeseries, SPEI-3 timeseries, and per-year
    monsoon onset archives to GEE as image assets (plan.md Script 01d).

    Only the timeseries (not params) and per-year onset (not the
    climatological median) go to GEE - see this module's docstring and
    the config.py FARM_STRESS_ASSET_ROOT comment for why.

    Submits every upload up front, then waits for the whole batch once
    (same pattern as export_modis_pet_historical_archive), rather than
    submit-wait-submit-wait one file at a time.

    Safe to interrupt and re-run: assets already on GEE are skipped
    unless overwrite=True.
    """
    ensure_farm_stress_folders(
        gee_account_id, subfolders=["spi1_timeseries", "spei3_timeseries", "onset_doy"]
    )

    periods = generate_28day_periods(start_year, end_year)
    pending = []
    skipped = []

    def submit_if_needed(local_path, asset_id, band_name, properties):
        if not overwrite and is_gee_asset_exists(asset_id):
            skipped.append(asset_id)
            return
        task_id = upload_local_tif_to_gee_asset(
            local_path=local_path,
            asset_id=asset_id,
            scale=SPI_SCALE_M,
            gee_account_id=gee_account_id,
            band_name=band_name,
            properties=properties,
        )
        pending.append((asset_id, task_id))

    for period in periods:
        label = period["label"]
        submit_if_needed(
            local_path=f"{LOCAL_DIR_SPI1_TIMESERIES}/spi1_{label}.tif",
            asset_id=f"{FARM_STRESS_ASSET_ROOT}/spi1_timeseries/spi1_{label}",
            band_name="spi1",
            properties={"period_label": label, "index_type": "spi1"},
        )

    for period in periods:
        label = period["label"]
        submit_if_needed(
            local_path=f"{LOCAL_DIR_SPEI3_TIMESERIES}/spei3_{label}.tif",
            asset_id=f"{FARM_STRESS_ASSET_ROOT}/spei3_timeseries/spei3_{label}",
            band_name="spei3",
            properties={"period_label": label, "index_type": "spei3"},
        )

    for year in range(start_year, end_year + 1):
        submit_if_needed(
            local_path=f"{LOCAL_DIR_MONSOON_ONSET}/onset_doy_{year}.tif",
            asset_id=f"{FARM_STRESS_ASSET_ROOT}/onset_doy/onset_doy_{year}",
            band_name="onset_doy",
            properties={"year": year, "index_type": "onset_doy"},
        )

    print(
        f"Submitted {len(pending)} upload task(s), skipped {len(skipped)} already on GEE. "
        "Waiting for the batch to finish..."
    )
    check_task_status([task_id for _, task_id in pending], sleep_time=poll_seconds)

    print(f"Done. Uploaded {len(pending)}, skipped {len(skipped)}.")
    return {"uploaded": [a for a, _ in pending], "skipped": skipped}
