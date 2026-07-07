"""
Celery task that orchestrates the three-phase farm boundary pipeline:

  Phase 1 — fetch_raw.fetch_raw_boundaries()
      Queries the AnthroKrishi API per S2 cell and saves raw JSON files
      to disk with a crash-safe manifest.

  Phase 2 — convert.convert_to_geoparquet()
      Reads raw JSON via DuckDB, filters farm field polygons, clips to
      the tehsil boundary with GeoPandas, and writes a GeoParquet file.

  Phase 3 — et_intersection.intersect_et_with_farms()  [OPTIONAL]
      Downloads AET raster from Google Earth Engine, computes per-farm
      monthly ET via zonal statistics, and writes an enhanced parquet.

The task is wired to the "nrm" Celery queue (same as all other CoRE Stack
pipelines) and supports automatic retries on transient failures.

Triggered via the Django API:
    POST /api/v1/generate_farm_boundaries/
    {
        "state": "rajasthan",
        "district": "jaipur",
        "block": "sanganer",
        "api_key": "AIzaSy...",
        "year": 2017            ← optional, enables Phase 3
    }
"""

import logging

from nrm_app.celery import app

from .convert import convert_to_geoparquet
from .fetch_raw import fetch_raw_boundaries

logger = logging.getLogger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def build_farm_boundary_map(self, state: str, district: str, block: str, api_key: str, year: int = None):
    """
    Celery task: runs Phase 1, Phase 2, and optionally Phase 3.

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    api_key : str
        AnthroKrishi / Agricultural Understanding API key.
    year : int, optional
        If provided, runs Phase 3 (ET intersection) for the given year.
        Valid range: 2017–2024.

    Returns
    -------
    dict
        Combined summary from all phases.
    """
    logger.info(
        "Farm boundary pipeline started — state=%s district=%s block=%s",
        state, district, block,
    )

    try:
        # ── Phase 1: Fetch ──────────────────────────────────────────────────
        phase1_summary = fetch_raw_boundaries(
            state=state,
            district=district,
            block=block,
            api_key=api_key,
            resume=True,   # safe to retry; already-fetched cells are skipped
        )
        logger.info("Phase 1 done: %s", phase1_summary)

        # ── Phase 2: Convert ────────────────────────────────────────────────
        phase2_summary = convert_to_geoparquet(
            state=state,
            district=district,
            block=block,
            overwrite=False,  # skip if parquet already exists
        )
        logger.info("Phase 2 done: %s", phase2_summary)

        # ── Phase 3: ET Intersection (optional) ─────────────────────────────
        phase3_summary = None
        if year is not None:
            from .et_intersection import intersect_et_with_farms

            logger.info("Phase 3 — ET intersection for year %d", year)
            phase3_summary = intersect_et_with_farms(
                state=state,
                district=district,
                block=block,
                year=year,
            )
            logger.info("Phase 3 done: %s", phase3_summary)

    except Exception as exc:
        logger.exception(
            "Farm boundary pipeline failed for %s/%s/%s: %s",
            state, district, block, exc,
        )
        raise self.retry(exc=exc)

    result = {
        "phase1": phase1_summary,
        "phase2": phase2_summary,
        "phase3": phase3_summary,
    }
    logger.info("Farm boundary pipeline completed successfully: %s", result)
    return result
