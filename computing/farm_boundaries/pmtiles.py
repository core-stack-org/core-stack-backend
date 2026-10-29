import logging
import os
import shutil
import subprocess
import tempfile
import time

import boto3
import geopandas as gpd
import requests

from nrm_app.settings import DPR_S3_ACCESS_KEY, DPR_S3_REGION, DPR_S3_SECRET_KEY
from utilities.constants import FARM_BOUNDARIES_PATH

import environ
env = environ.Env()
# reading .env file
environ.Env.read_env()

logger = logging.getLogger(__name__)

LAYER_NAME = "farm_boundaries"

# Only these attributes are carried into the tiles — kept deliberately thin.
# farm_id is the join key back to farm_static/farm_annual/farm_monthly
# parquets, so consumers can look up everything else from there instead of
# duplicating it into the tileset.
# frequency / intensity (+ _mod / _sev variants) are the drought indicators computed into static.parquet
# by et_intersection (Phase 3); farms with no value (NaN) get a null property.
TILE_PROPERTY_COLUMNS = [
    "farm_id", "area_m2",
    "frequency", "frequency_mod", "frequency_sev",
    "intensity", "intensity_mod", "intensity_sev",
]

REQUIRED_BINARIES = ["tippecanoe", "pmtiles"]

# cs-geodata service that rewrites the farm parquets into the optimised,
# partitioned layout. It runs on the same server (docker), so it reads and
# writes the same absolute paths we pass it.
CS_GEODATA_URL = env("CS_GEODATA_URL", default="")
STANDARDISE_TIMEOUT_SEC = 300          # 5 min; a tehsil takes ~2-3 min
STANDARDISE_POLL_SEC = 5
# Copied from the example metadata.json "descriptor.converted_files"; only the
# paths change per tehsil.
STANDARDISE_CONVERTED_FILES = {
    "static":     {"partition_by": None,     "row_group_size": 100000},
    "annual":     {"partition_by": ["year"], "row_group_size": 100000},
    "sub_annual": {"partition_by": ["year"], "row_group_size": 100000},
}
# cs-geodata writes the sub-annual output to <output_path>/sub-annual (hard-coded in the
# service); we store it as sub_annual to match the input parquet name.
CS_GEODATA_SUB_ANNUAL_DIR = "sub-annual"
# Standardised outputs inside <block>/farms/ that get uploaded. The folder also holds the
# raw annual/sub_annual parquets and the trees/ponds/wells files, which are not uploaded.
STANDARDISED_FILES = ["static.parquet", "metadata.json"]
STANDARDISED_DIRS = ["annual", "sub_annual"]
# RQ job statuses reported by GET /api/v1/status
_STANDARDISE_WAITING = {"queued", "started", "deferred", "scheduled"}

# Same AWS account/credentials as DPR's S3 upload (dpr/utils.py), separate
# bucket dedicated to farm boundary tilesets.
FARM_DATA_S3_BUCKET = "corestack-farm-dataset"


# ── path helpers ───────────────────────────────────────────────────────────────


def _output_dir(state, district, block):
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block)


def _farm_parquet_path(state, district, block):
    return os.path.join(_output_dir(state, district, block), "farms/static.parquet")


def _s3_key(state, district, block):
    return f"{state}/{district}/{block}.pmtiles"


def _s3_url(state, district, block):
    return f"https://{FARM_DATA_S3_BUCKET}.s3.{DPR_S3_REGION}.amazonaws.com/{_s3_key(state, district, block)}"


# ── environment check ────────────────────────────────────────────────────────


def _check_binaries_available():
    missing = [b for b in REQUIRED_BINARIES if shutil.which(b) is None]
    if missing:
        raise RuntimeError(
            f"Required binaries not found on PATH: {missing}. "
            "Install tippecanoe (conda install -c conda-forge tippecanoe) "
            "and the go-pmtiles CLI before running Phase 4."
        )


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=DPR_S3_ACCESS_KEY,
        aws_secret_access_key=DPR_S3_SECRET_KEY,
        region_name=DPR_S3_REGION,
    )


# ── pipeline steps ───────────────────────────────────────────────────────────


def _export_geojsonseq(gdf, out_path):
    """
    Write a GeoDataFrame as newline-delimited GeoJSON (RFC 8142), the
    streaming-friendly format tippecanoe consumes most efficiently for
    large feature counts.
    """
    gdf.to_file(out_path, driver="GeoJSONSeq")


def _run_tippecanoe(geojsonseq_path, mbtiles_path, layer_name):
    """
    Build an .mbtiles archive from a GeoJSONSeq file via tippecanoe.

    --force                          overwrite mbtiles_path if it exists
    -l / --layer                     explicit layer name (stable for consumers)
    -Z / -z                          zoom range
    --read-parallel                  faster read of large input files
    --extend-zooms-if-still-dropping raise max zoom automatically rather than
                                      silently dropping features at the
                                      requested max zoom when tiles are dense
    """
    cmd = [
        "tippecanoe",
        "--force",
        "-o",
        mbtiles_path,
        "-l",
        layer_name,
        "-Z",
        "8",
        "-z",
        "16",
        "-D",
        "6",
        "-d",
        "16",
        "--read-parallel",
        "--extend-zooms-if-still-dropping",
        geojsonseq_path,
    ]
    logger.info("Running tippecanoe: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed (exit {result.returncode}):\n{result.stderr}"
        )
    logger.debug("tippecanoe stderr:\n%s", result.stderr)


def _convert_mbtiles_to_pmtiles(mbtiles_path, pmtiles_path):
    """Convert an .mbtiles archive to .pmtiles via the go-pmtiles CLI."""
    cmd = ["pmtiles", "convert", mbtiles_path, pmtiles_path]
    logger.info("Running pmtiles convert: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"pmtiles convert failed (exit {result.returncode}):\n{result.stderr}"
        )
    logger.debug("pmtiles convert stderr:\n%s", result.stderr)


def _upload_pmtiles_to_s3(local_path, state, district, block):
    """
    Upload a local .pmtiles file to corestack-farm-data/<state>/<district>/<block>.pmtiles.
    Mirrors dpr/utils.py's upload_dpr_to_s3 (same credentials/region, different bucket).
    """
    s3_key = _s3_key(state, district, block)

    with open(local_path, "rb") as f:
        _s3_client().upload_fileobj(
            f,
            FARM_DATA_S3_BUCKET,
            s3_key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )

    s3_url = _s3_url(state, district, block)
    logger.info("PMTiles uploaded to S3: %s", s3_url)
    return s3_url


def _standardise_parquets(state, district, block):
    """
    Optimise farms/{static,annual,sub_annual}.parquet via the cs-geodata
    /api/v1/vector/standardise endpoint, writing the result into <block>/farms/
    itself: static.parquet is replaced, and annual/year=*/ and sub_annual/year=*/
    folders are created next to the raw annual.parquet / sub_annual.parquet.

    cs-geodata names the sub-annual folder "sub-annual"; it is renamed to
    "sub_annual" once the job finishes. The job is asynchronous, so poll
    /api/v1/status until it finishes. Raises RuntimeError on a non-200 response,
    a failed job, a timeout, or if the expected output folders are missing.

    Returns the farms directory.
    """
    farms_dir = os.path.abspath(os.path.join(_output_dir(state, district, block), "farms"))
    # Drop outputs of an older run so stale partitions can't get uploaded.
    # static.parquet is the input as well as the output, so it is left alone.
    for stale in ("annual", "sub_annual", CS_GEODATA_SUB_ANNUAL_DIR):
        shutil.rmtree(os.path.join(farms_dir, stale), ignore_errors=True)

    body = {
        "static_input": os.path.join(farms_dir, "static.parquet"),
        "annual_input": os.path.join(farms_dir, "annual.parquet"),
        "sub_annual_input": os.path.join(farms_dir, "sub_annual.parquet"),
        "output_path": farms_dir,
        "key": "farm_id",
        "converted_files": STANDARDISE_CONVERTED_FILES,
    }
    logger.info("Standardising parquets via cs-geodata: %s", body)
    resp = requests.post(f"{CS_GEODATA_URL}/api/v1/vector/standardise", json=body, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"standardise request failed ({resp.status_code}): {resp.text}")
    task_id = resp.json()["task_id"]

    deadline = time.monotonic() + STANDARDISE_TIMEOUT_SEC
    while True:
        status = requests.get(
            f"{CS_GEODATA_URL}/api/v1/status", params={"task_id": task_id}, timeout=30
        ).json().get("status")
        if status == "finished":
            break
        if status not in _STANDARDISE_WAITING:
            raise RuntimeError(f"standardise task {task_id} ended with status '{status}'")
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"standardise task {task_id} still '{status}' after {STANDARDISE_TIMEOUT_SEC}s"
            )
        time.sleep(STANDARDISE_POLL_SEC)

    service_dir = os.path.join(farms_dir, CS_GEODATA_SUB_ANNUAL_DIR)
    if os.path.isdir(service_dir):
        os.rename(service_dir, os.path.join(farms_dir, "sub_annual"))
    missing = [d for d in STANDARDISED_DIRS if not os.path.isdir(os.path.join(farms_dir, d))]
    if missing:
        raise RuntimeError(f"standardise task {task_id} finished but {missing} not found in {farms_dir}")
    logger.info("Standardise task %s finished -> %s", task_id, farms_dir)
    return farms_dir


def _upload_farm_parquets_to_s3(state, district, block, farms_dir):
    """
    Upload the standardised outputs in farms_dir (static.parquet, metadata.json,
    and every file under annual/ and sub_annual/) to
    <bucket>/<state>/<district>/<block>/farms/<path relative to farms_dir>
    (e.g. farms/static.parquet, farms/sub_annual/year=2020/data_0.parquet),
    overwriting whatever is there.

    Returns a dict {relative path: s3_url} for the files uploaded.
    """
    local_paths = [
        os.path.join(farms_dir, name)
        for name in STANDARDISED_FILES
        if os.path.isfile(os.path.join(farms_dir, name))
    ]
    for folder in STANDARDISED_DIRS:
        for root, _, files in os.walk(os.path.join(farms_dir, folder)):
            local_paths += [os.path.join(root, name) for name in sorted(files)]

    s3 = _s3_client()
    uploaded = {}
    for local_path in local_paths:
        rel_path = os.path.relpath(local_path, farms_dir).replace(os.sep, "/")
        s3_key = f"{state}/{district}/{block}/farms/{rel_path}"
        with open(local_path, "rb") as f:
            s3.upload_fileobj(
                f,
                FARM_DATA_S3_BUCKET,
                s3_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
        uploaded[rel_path] = f"https://{FARM_DATA_S3_BUCKET}.s3.{DPR_S3_REGION}.amazonaws.com/{s3_key}"
        logger.info("Parquet uploaded to S3: %s", uploaded[rel_path])
    return uploaded


# ── public entry point ───────────────────────────────────────────────────────


def convert_boundaries_to_pmtiles(
    state: str,
    district: str,
    block: str,
) -> dict:
    """
    Phase 4: convert farm_boundaries.parquet into a PMTiles vector tile
    archive and upload it to S3, always overwriting whatever is already at
    that S3 key. No .pmtiles (or intermediate) file is kept on local disk —
    everything is built in a temp directory and discarded once uploaded.

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    min_zoom, max_zoom : int
        Zoom range to generate tiles for.

    Returns
    -------
    dict
        Summary with S3 url, farm count, and file size (or skipped flag if
        there were no farms to tile).
    """
    _check_binaries_available()

    farm_path = _farm_parquet_path(state, district, block)
    if not os.path.exists(farm_path):
        raise FileNotFoundError(
            f"Farm boundaries parquet not found at {farm_path}. Run Phases 1 & 2 first."
        )

    logger.info("Phase 5 — PMTiles conversion: %s/%s/%s", state, district, block)

    gdf = gpd.read_parquet(farm_path)
    logger.info("Loaded %d farm polygons.", len(gdf))

    if gdf.empty:
        logger.warning("No farm polygons to tile — skipping Phase 4.")
        return {"s3_url": None, "farm_count": 0, "skipped": True}

    # Thin down to just the join key + area — everything else (alu_type,
    # plus_code, cell_token, class_confidence, capture_date, ...) is looked
    # up from farm_static/farm_annual/farm_monthly via farm_id instead of
    # being duplicated into the tileset.
    keep_cols = [c for c in TILE_PROPERTY_COLUMNS if c in gdf.columns] + ["geometry"]
    gdf = gdf[keep_cols]

    with tempfile.TemporaryDirectory() as tmp_dir:
        geojsonseq_path = os.path.join(tmp_dir, "farm_boundaries.geojsonl")
        mbtiles_path = os.path.join(tmp_dir, "farm_boundaries.mbtiles")
        pmtiles_path = os.path.join(tmp_dir, "farm_boundaries.pmtiles")

        logger.info("Exporting %d farms to GeoJSONSeq...", len(gdf))
        _export_geojsonseq(gdf, geojsonseq_path)

        _run_tippecanoe(geojsonseq_path, mbtiles_path, LAYER_NAME)
        _convert_mbtiles_to_pmtiles(mbtiles_path, pmtiles_path)

        size_bytes = os.path.getsize(pmtiles_path)
        s3_url = _upload_pmtiles_to_s3(pmtiles_path, state, district, block)
        # tmp_dir (geojsonseq + mbtiles + pmtiles) is removed on context exit —
        # the S3 copy is the only one that persists.

    farms_dir = _standardise_parquets(state, district, block)   # raises on failure → nothing uploaded
    parquet_s3_urls = _upload_farm_parquets_to_s3(state, district, block, farms_dir)

    summary = {
        "state": state,
        "district": district,
        "block": block,
        "farm_count": len(gdf),
        "s3_url": s3_url,
        "size_bytes": size_bytes,
        "parquet_s3_urls": parquet_s3_urls,
    }
    logger.info("Phase 4 complete: %s", summary)
    return summary
