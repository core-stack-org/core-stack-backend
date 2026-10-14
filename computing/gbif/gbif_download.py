"""
Phase 1 — download GBIF occurrences for one block (UK plan, block-first).

Downloads ALL taxa within the block's bounding box via the GBIF Download API (async: request ->
GBIF prepares a zip -> we fetch it), which has no 100k record cap. The bbox is derived from the
block's MWS GEE asset so there is a single source of truth for "where is this block".

Docs: https://pygbif.readthedocs.io/en/latest/modules/occurrence.html
      https://techdocs.gbif.org/en/data-use/api-downloads
"""

import os
import json
import time
import zipfile

from pygbif import occurrences as occ
from utilities.gee_utils import valid_gee_text

from . import config


def block_cache_dir(state, district, block):
    """
    Per-block download cache dir. Components are normalised exactly like the GEE asset path
    (lowercase, sanitised, spaces -> underscores) so "Chennai"/"chennai" and "Bengaluru Urban"
    resolve to ONE cache entry instead of re-downloading hundreds of MB per spelling.
    """
    return os.path.join(
        config.DATA_DIR, *(valid_gee_text(x.lower()) for x in (state, district, block))
    )


def get_block_bbox_wkt(state, district, block, buffer_deg=0.01):
    """
    Compute the block bounding box as a WKT POLYGON from the MWS GEE asset.
    Requires ee_initialize() to have been called by the caller.

    A bbox (not the exact MWS union) is intentional: GBIF's geometry predicate wants a simple
    polygon, and the precise per-MWS assignment happens later in the GEE spatial join.
    """
    import ee
    from utilities.gee_utils import get_gee_asset_path, valid_gee_text

    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    # bounds() -> rectangle geometry; coordinates is a single ring of [lon, lat] pairs
    ring = roi.geometry().bounds().getInfo()["coordinates"][0]
    lons = [pt[0] for pt in ring]
    lats = [pt[1] for pt in ring]
    minlon, maxlon = min(lons) - buffer_deg, max(lons) + buffer_deg
    minlat, maxlat = min(lats) - buffer_deg, max(lats) + buffer_deg
    return (
        f"POLYGON(({minlon} {minlat},{maxlon} {minlat},"
        f"{maxlon} {maxlat},{minlon} {maxlat},{minlon} {minlat}))"
    )


def request_block_download(bbox_wkt):
    """
    Ask GBIF to prepare a download of ALL occurrences within the block bbox. Returns a download key.

    pygbif parses "KEY OP VALUE" strings where KEY is the camelCase API field name (it uppercases
    to the download-predicate key itself). The `in` list must be valid JSON (double-quoted); the
    `geometry within <wkt>` form is parsed to a {"type":"within",...} predicate. Verified against
    pygbif 0.6.6.
    """
    predicates = [
        "hasCoordinate = TRUE",  # record must carry coordinates
        "hasGeospatialIssue = FALSE",  # drop records GBIF flags as geospatially suspect
        "occurrenceStatus = PRESENT",  # presence records only
        "basisOfRecord in " + json.dumps(config.KEEP_BASIS_OF_RECORD),
        f"geometry within {bbox_wkt}",  # inside the block bbox
    ]
    key = occ.download(
        queries=predicates,
        format="SIMPLE_CSV",
        user=config.GBIF_USER,
        pwd=config.GBIF_PWD,
        email=config.GBIF_EMAIL,
    )
    return key[0] if isinstance(key, (list, tuple)) else key


def wait_and_fetch(download_key, dest_dir, poll_seconds=60):
    """Poll until GBIF finishes preparing the file, then download & unzip. Returns the CSV path."""
    while True:
        meta = occ.download_meta(download_key)
        st = meta["status"]  # PREPARING / RUNNING / SUCCEEDED / KILLED / CANCELLED / FAILED
        if st == "SUCCEEDED":
            break
        if st in ("KILLED", "CANCELLED", "FAILED"):
            raise RuntimeError(f"GBIF download {download_key} ended as {st}")
        time.sleep(poll_seconds)  # large downloads take minutes-hours; be polite

    occ.download_get(download_key, path=dest_dir)  # -> <dest_dir>/<key>.zip
    zip_path = os.path.join(dest_dir, f"{download_key}.zip")
    extract_dir = os.path.join(dest_dir, download_key)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(extract_dir)
    # SIMPLE_CSV extracts to <key>.csv (tab-separated)
    return os.path.join(extract_dir, f"{download_key}.csv")


def _read_meta(meta_path):
    meta = {}
    if os.path.exists(meta_path):
        for line in open(meta_path):
            if "=" in line:
                k, v = line.strip().split("=", 1)
                meta[k] = v or None
    if "raw_record_count" in meta and meta["raw_record_count"] is not None:
        try:
            meta["raw_record_count"] = int(meta["raw_record_count"])
        except ValueError:
            meta["raw_record_count"] = None
    return meta


def download_block_occurrences(state, district, block):
    """
    Cached block download: bbox from MWS asset -> GBIF download -> local raw CSV.
    Returns (csv_path, meta) where meta = {download_key, doi, download_date, raw_record_count}
    (provenance stored in Layer.misc). Reuses a prior download for the same block if present.
    """
    os.makedirs(config.DATA_DIR, exist_ok=True)
    block_dir = block_cache_dir(state, district, block)
    cached_csv = os.path.join(block_dir, "occurrences_raw.csv")
    meta_path = os.path.join(block_dir, "meta.txt")
    if os.path.exists(cached_csv):
        print(f"[gbif] block cache hit: {cached_csv}")
        return cached_csv, _read_meta(meta_path)

    if not (config.GBIF_USER and config.GBIF_PWD and config.GBIF_EMAIL):
        raise RuntimeError("GBIF_USER / GBIF_PWD / GBIF_EMAIL must be set.")

    os.makedirs(block_dir, exist_ok=True)
    bbox_wkt = get_block_bbox_wkt(state, district, block)
    print(f"[gbif] block bbox: {bbox_wkt}")
    key = request_block_download(bbox_wkt)
    csv_path = wait_and_fetch(key, block_dir)

    os.replace(csv_path, cached_csv)
    meta = {"download_key": str(key), "doi": None, "download_date": None, "raw_record_count": None}
    try:
        gbif_meta = occ.download_meta(key)
        meta["doi"] = gbif_meta.get("doi")
        # GBIF's 'created' is an ISO timestamp; keep the date part for the report
        created = gbif_meta.get("created")
        meta["download_date"] = created.split("T")[0] if created else None
        meta["raw_record_count"] = gbif_meta.get("totalRecords")
        with open(meta_path, "w") as fh:
            for k, v in meta.items():
                fh.write(f"{k}={'' if v is None else v}\n")
    except Exception as exc:
        print(f"[gbif] could not record download meta: {exc}")
    print(f"[gbif] block downloaded -> {cached_csv} ({meta})")
    return cached_csv, meta
