"""
Phase 4 — Convert farm_boundaries.parquet into a PMTiles vector tile archive.

Pipeline:
    farm_boundaries.parquet
        -> newline-delimited GeoJSON (GeoJSONSeq), via GeoPandas/Fiona
        -> tippecanoe                                  -> intermediate .mbtiles
        -> `pmtiles convert` (go-pmtiles CLI)           -> farm_boundaries.pmtiles

Both `tippecanoe` and the `pmtiles` CLI are external binaries, not Python
packages. `tippecanoe` is installed into the project's conda env
(corestackenv); `pmtiles` is a standalone system binary. Both must be
reachable on PATH wherever this runs.

Output:
    data/farm_boundaries/<state>/<district>/<block>/farm_boundaries.pmtiles

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

import geopandas as gpd

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


# ── path helpers ───────────────────────────────────────────────────────────────

def _output_dir(state, district, block):
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block)

def _farm_parquet_path(state, district, block):
    return os.path.join(_output_dir(state, district, block), "farm_boundaries.parquet")

def _pmtiles_path(state, district, block):
    return os.path.join(_output_dir(state, district, block), "farm_boundaries.pmtiles")


# ── environment check ────────────────────────────────────────────────────────

def _check_binaries_available():
    missing = [b for b in REQUIRED_BINARIES if shutil.which(b) is None]
    if missing:
        raise RuntimeError(
            f"Required binaries not found on PATH: {missing}. "
            "Install tippecanoe (conda install -c conda-forge tippecanoe) "
            "and the go-pmtiles CLI before running Phase 4."
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


# ── public entry point ───────────────────────────────────────────────────────

def convert_boundaries_to_pmtiles(
    state: str,
    district: str,
    block: str,
    overwrite: bool = False,
    min_zoom: int = DEFAULT_MIN_ZOOM,
    max_zoom: int = DEFAULT_MAX_ZOOM,
) -> dict:
    """
    Phase 4: convert farm_boundaries.parquet into a PMTiles vector tile archive.

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    overwrite : bool
        If False (default) and farm_boundaries.pmtiles already exists, skip.
    min_zoom, max_zoom : int
        Zoom range to generate tiles for.

    Returns
    -------
    dict
        Summary with output path, farm count, and file size (or skipped flag).
    """
    out_path = _pmtiles_path(state, district, block)
    if not overwrite and os.path.exists(out_path):
        logger.info("farm_boundaries.pmtiles already exists — skipping Phase 4.")
        return {"path": out_path, "skipped": True}

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
        return {"path": None, "farm_count": 0, "skipped": True}

    # Thin down to just the join key + area — everything else (alu_type,
    # plus_code, cell_token, class_confidence, capture_date, ...) is looked
    # up from farm_static/farm_annual/farm_monthly via farm_id instead of
    # being duplicated into the tileset.
    keep_cols = [c for c in TILE_PROPERTY_COLUMNS if c in gdf.columns] + ["geometry"]
    gdf = gdf[keep_cols]

    with tempfile.TemporaryDirectory() as tmp_dir:
        geojsonseq_path = os.path.join(tmp_dir, "farm_boundaries.geojsonl")
        mbtiles_path = os.path.join(tmp_dir, "farm_boundaries.mbtiles")

        logger.info("Exporting %d farms to GeoJSONSeq...", len(gdf))
        _export_geojsonseq(gdf, geojsonseq_path)

        _run_tippecanoe(geojsonseq_path, mbtiles_path, LAYER_NAME, min_zoom, max_zoom)

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        _convert_mbtiles_to_pmtiles(mbtiles_path, out_path)

    size_bytes = os.path.getsize(out_path)
    summary = {
        "state": state, "district": district, "block": block,
        "farm_count": len(gdf),
        "path": out_path,
        "size_bytes": size_bytes,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
    }
    logger.info("Phase 4 complete: %s", summary)
    return summary
