"""
IUCN Red List enrichment.

GBIF's SIMPLE_CSV download does NOT include iucnRedListCategory (contrary to the UK output-design
doc). The IUCN category is a per-species attribute served by GBIF's species API:
    GET https://api.gbif.org/v1/species/{taxonKey}/iucnRedListCategory

So we look it up per distinct taxonKey and attach an `iucnRedListCategory` column to the occurrences.
Results are cached on disk (category is global per taxonKey) so repeated species across blocks are free.
"""

import os
import json
import logging

import requests

from . import config

logger = logging.getLogger(__name__)

_IUCN_URL = "https://api.gbif.org/v1/species/{key}/iucnRedListCategory"
_CACHE_FILE = os.path.join(config.CACHE_DIR, "iucn_by_taxonkey.json")

# GBIF returns long-form categories; we store the standard short codes (config matches VU/EN/CR).
_SHORT = {
    "LEAST_CONCERN": "LC",
    "NEAR_THREATENED": "NT",
    "VULNERABLE": "VU",
    "ENDANGERED": "EN",
    "CRITICALLY_ENDANGERED": "CR",
    "EXTINCT_IN_THE_WILD": "EW",
    "EXTINCT": "EX",
    "DATA_DEFICIENT": "DD",
}


def _load_cache():
    try:
        with open(_CACHE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_cache(cache):
    try:
        os.makedirs(config.CACHE_DIR, exist_ok=True)
        with open(_CACHE_FILE, "w") as fh:
            json.dump(cache, fh)
    except Exception as exc:
        logger.warning(f"[gbif] could not persist IUCN cache: {exc}")


def _lookup(taxon_key, timeout=20):
    try:
        r = requests.get(_IUCN_URL.format(key=int(taxon_key)), timeout=timeout)
        if r.status_code == 200:
            return _SHORT.get(r.json().get("category", ""), "")
    except Exception:
        pass
    return ""


def enrich_with_iucn(df):
    """
    Add an `iucnRedListCategory` column to the occurrences DataFrame by looking up each distinct
    taxonKey (cached). Failures/unknowns default to "" (counted as not-threatened). Returns df.
    """
    if "taxonKey" not in df.columns or df.empty:
        df["iucnRedListCategory"] = ""
        return df

    cache = _load_cache()
    keys = [str(int(k)) for k in df["taxonKey"].dropna().unique()]
    missing = [k for k in keys if k not in cache]
    logger.info(f"[gbif] IUCN lookup: {len(keys)} distinct taxa, {len(missing)} not cached")

    for k in missing:
        cache[k] = _lookup(k)
    if missing:
        _save_cache(cache)

    df["iucnRedListCategory"] = (
        df["taxonKey"].dropna().astype(int).astype(str).map(cache).fillna("")
        if len(df)
        else ""
    )
    # reindex back onto the full df (rows with NaN taxonKey get "")
    df["iucnRedListCategory"] = df["iucnRedListCategory"].reindex(df.index).fillna("")
    n_threatened = df["iucnRedListCategory"].isin(config.THREATENED_IUCN_CATEGORIES).sum()
    logger.info(f"[gbif] IUCN: {n_threatened} threatened-category occurrence rows")
    return df
