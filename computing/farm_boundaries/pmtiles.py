"""
Phase 4 — Convert farm_boundaries.parquet into a PMTiles vector tile archive
and upload it to S3.

Pipeline:
    farm_boundaries.parquet
        -> newline-delimited GeoJSON (GeoJSONSeq), via GeoPandas/Fiona
        -> tippecanoe                                  -> intermediate .mbtiles
        -> `pmtiles convert` (go-pmtiles CLI)           -> farm_boundaries.pmtiles
        -> boto3 upload                                -> S3

Everything is built inside a temp directory — no .pmtiles (or intermediate
.geojsonl/.mbtiles) file is kept on local disk; the only persistent copy
lives in S3.

Both `tippecanoe` and the `pmtiles` CLI are external binaries, not Python
packages. `tippecanoe` is installed into the project's conda env
(corestackenv); `pmtiles` is a standalone system binary. Both must be
reachable on PATH wherever this runs.

S3 destination (same credentials/region as dpr/utils.py's upload_dpr_to_s3,
different bucket):
    corestack-farm-data/<state>/<district>/<block>.pmtiles

Usage (standalone / debug):
    from computing.farm_boundaries.pmtiles import convert_boundaries_to_pmtiles
    result = convert_boundaries_to_pmtiles("rajasthan", "jaipur", "sanganer")
    print(result)
"""

import logging
import os
import shutil
import subprocess
import tempfile

import boto3
import geopandas as gpd

from nrm_app.settings import DPR_S3_ACCESS_KEY, DPR_S3_REGION, DPR_S3_SECRET_KEY
from utilities.constants import FARM_BOUNDARIES_PATH

logger = logging.getLogger(__name__)

LAYER_NAME = "farm_boundaries"

# Only these attributes are carried into the tiles — kept deliberately thin.
# farm_id is the join key back to farm_static/farm_annual/farm_monthly
# parquets, so consumers can look up everything else from there instead of
# duplicating it into the tileset.
TILE_PROPERTY_COLUMNS = ["farm_id", "area_m2"]

# Zoom range for the generated tileset. Farms are small polygons (often
# sub-hectare), so the max zoom is kept high enough to render individual
# boundaries clearly; adjustable per call via convert_boundaries_to_pmtiles().
DEFAULT_MIN_ZOOM = 8
DEFAULT_MAX_ZOOM = 16

REQUIRED_BINARIES = ["tippecanoe", "pmtiles"]

# Same AWS account/credentials as DPR's S3 upload (dpr/utils.py), separate
# bucket dedicated to farm boundary tilesets.
FARM_DATA_S3_BUCKET = "corestack-farm-data"


# ── path helpers ───────────────────────────────────────────────────────────────

def _output_dir(state, district, block):
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block)

def _farm_parquet_path(state, district, block):
    return os.path.join(_output_dir(state, district, block), "farm_boundaries.parquet")

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


def _run_tippecanoe(geojsonseq_path, mbtiles_path, layer_name, min_zoom, max_zoom):
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
        "-o", mbtiles_path,
        "-l", layer_name,
        "-Z", str(min_zoom),
        "-z", str(max_zoom),
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


# ── public entry point ───────────────────────────────────────────────────────

def convert_boundaries_to_pmtiles(
    state: str,
    district: str,
    block: str,
    min_zoom: int = DEFAULT_MIN_ZOOM,
    max_zoom: int = DEFAULT_MAX_ZOOM,
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
            f"Farm boundaries parquet not found at {farm_path}. "
            "Run Phases 1 & 2 first."
        )

    logger.info("Phase 4 — PMTiles conversion: %s/%s/%s", state, district, block)

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

        _run_tippecanoe(geojsonseq_path, mbtiles_path, LAYER_NAME, min_zoom, max_zoom)
        _convert_mbtiles_to_pmtiles(mbtiles_path, pmtiles_path)

        size_bytes = os.path.getsize(pmtiles_path)
        s3_url = _upload_pmtiles_to_s3(pmtiles_path, state, district, block)
        # tmp_dir (geojsonseq + mbtiles + pmtiles) is removed on context exit —
        # the S3 copy is the only one that persists.

    summary = {
        "state": state, "district": district, "block": block,
        "farm_count": len(gdf),
        "s3_url": s3_url,
        "size_bytes": size_bytes,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
    }
    logger.info("Phase 4 complete: %s", summary)
    return summary
