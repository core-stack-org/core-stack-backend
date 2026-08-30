"""
Village Name Matcher

Performs fuzzy matching between livestock census village names and
CoRE Stack village boundary records. Matching is done hierarchically:

    1. Exact match on state-district-block-village key
    2. Fuzzy match using edit distance (Levenshtein) on village name
       within the same state-district-block

The output maps each livestock census record to a census2011 village ID
(or lgd_village key) from the CoRE Stack boundary dataset.
"""

import pandas as pd
import geopandas as gpd
from difflib import SequenceMatcher
from unidecode import unidecode
import os
import json


def _normalize(text):
    """Normalize village/district name for matching."""
    if not isinstance(text, str):
        return ""
    text = unidecode(text).strip().lower()
    # Remove common suffixes/prefixes that vary across datasets
    for suffix in [" (ct)", " (cb)", " (og)", " (m)", " (tp)", " (np)", " (m cl)"]:
        text = text.replace(suffix, "")
    # Collapse whitespace
    text = " ".join(text.split())
    return text


def _similarity(a, b):
    """Compute similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()


def load_village_boundaries(shapefile_dir, state_name=None):
    """Load village boundary shapefiles from the CoRE Stack dataset.

    The shapefiles are organized by state. Each has columns including
    village name, census2011 id, lgd_village key, district, subdistrict.

    Args:
        shapefile_dir: Path to directory containing state-wise shapefiles
        state_name: If provided, load only this state's boundaries

    Returns:
        pd.DataFrame with boundary village records (without geometry to save memory)
    """
    frames = []

    if state_name:
        # Try to find matching file
        for f in os.listdir(shapefile_dir):
            if f.endswith(".geojson") or f.endswith(".shp"):
                if _normalize(state_name) in _normalize(f):
                    path = os.path.join(shapefile_dir, f)
                    gdf = gpd.read_file(path)
                    # Drop geometry to save memory for matching
                    df = pd.DataFrame(gdf.drop(columns="geometry"))
                    frames.append(df)
    else:
        for f in sorted(os.listdir(shapefile_dir)):
            if f.endswith(".geojson") or f.endswith(".shp"):
                path = os.path.join(shapefile_dir, f)
                try:
                    gdf = gpd.read_file(path)
                    df = pd.DataFrame(gdf.drop(columns="geometry"))
                    frames.append(df)
                    print(f"  Loaded {f}: {len(df)} villages")
                except Exception as e:
                    print(f"  Error loading {f}: {e}")

    if not frames:
        raise FileNotFoundError(f"No boundary files found in {shapefile_dir}")

    boundaries = pd.concat(frames, ignore_index=True)

    # Normalize column names
    boundaries.columns = [c.strip().lower().replace(" ", "_") for c in boundaries.columns]

    return boundaries


def _detect_boundary_columns(boundaries_df):
    """Auto-detect which columns in the boundary dataset correspond to
    state, district, subdistrict, village name and village ID."""
    cols = boundaries_df.columns.tolist()
    mapping = {}

    for col in cols:
        cl = col.lower()
        if "state" in cl and "name" in cl:
            mapping["state"] = col
        elif "district" in cl and "name" in cl:
            mapping["district"] = col
        elif "subdist" in cl and "name" in cl:
            mapping["subdistrict"] = col
        elif "village" in cl and "name" in cl:
            mapping["village"] = col
        elif "census2011" in cl or "census_2011" in cl:
            mapping["census_id"] = col
        elif "lgd_village" in cl or "lgd" in cl:
            mapping["lgd_id"] = col

    # Fallback heuristics
    if "state" not in mapping:
        for col in cols:
            if col.lower() in ["state", "state_ut"]:
                mapping["state"] = col
                break
    if "village" not in mapping:
        for col in cols:
            if "village" in col.lower() and "code" not in col.lower():
                mapping["village"] = col
                break
    if "census_id" not in mapping:
        for col in cols:
            if "census" in col.lower() and "code" in col.lower():
                mapping["census_id"] = col
                break

    return mapping


def match_villages(livestock_df, boundaries_df, similarity_threshold=0.80):
    """Match livestock census villages to boundary dataset villages.

    Strategy:
        1. Build a lookup of boundary villages keyed by
           normalized (state, district, subdistrict/block)
        2. For each livestock record, first try exact match on village name
        3. If no exact match, try fuzzy match above the threshold

    Args:
        livestock_df: Aggregated livestock census DataFrame
        boundaries_df: Village boundaries DataFrame
        similarity_threshold: Minimum similarity score for fuzzy match

    Returns:
        tuple: (matched_df, match_stats)
            matched_df has additional columns: matched_village_name,
            census2011_id, lgd_village_id, match_type, match_score
    """
    col_map = _detect_boundary_columns(boundaries_df)
    print(f"Detected boundary columns: {json.dumps(col_map, indent=2)}")

    if "village" not in col_map:
        raise ValueError("Could not detect village name column in boundary dataset")

    # Build lookup: (state, district, subdistrict) -> list of village records
    boundary_lookup = {}
    for _, row in boundaries_df.iterrows():
        state = _normalize(str(row.get(col_map.get("state", ""), "")))
        district = _normalize(str(row.get(col_map.get("district", ""), "")))
        subdistrict = _normalize(str(row.get(col_map.get("subdistrict", ""), "")))
        village = _normalize(str(row.get(col_map["village"], "")))

        key = (state, district, subdistrict)
        census_id = row.get(col_map.get("census_id", ""), "")
        lgd_id = row.get(col_map.get("lgd_id", ""), "")

        if key not in boundary_lookup:
            boundary_lookup[key] = []
        boundary_lookup[key].append({
            "village": village,
            "original_name": str(row.get(col_map["village"], "")),
            "census_id": census_id,
            "lgd_id": lgd_id,
        })

    # Match each livestock record
    results = []
    match_counts = {"exact": 0, "fuzzy": 0, "unmatched": 0}

    total = len(livestock_df)
    for idx, row in livestock_df.iterrows():
        if idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx}/{total} records...")

        state = _normalize(str(row.get("state_name", "")))
        district = _normalize(str(row.get("district_name", "")))
        block = _normalize(str(row.get("block_name", "")))
        village = _normalize(str(row.get("village_name", "")))

        # Try lookup with (state, district, block)
        key = (state, district, block)
        candidates = boundary_lookup.get(key, [])

        # Also try without block (sometimes block/subdistrict names differ)
        if not candidates:
            for k, v in boundary_lookup.items():
                if k[0] == state and k[1] == district:
                    candidates.extend(v)

        matched_name = ""
        census_id = ""
        lgd_id = ""
        match_type = "unmatched"
        match_score = 0.0

        if candidates:
            # Exact match first
            for c in candidates:
                if c["village"] == village:
                    matched_name = c["original_name"]
                    census_id = c["census_id"]
                    lgd_id = c["lgd_id"]
                    match_type = "exact"
                    match_score = 1.0
                    break

            # Fuzzy match if no exact
            if match_type == "unmatched" and village:
                best_score = 0
                best_candidate = None
                for c in candidates:
                    score = _similarity(village, c["village"])
                    if score > best_score:
                        best_score = score
                        best_candidate = c

                if best_score >= similarity_threshold and best_candidate:
                    matched_name = best_candidate["original_name"]
                    census_id = best_candidate["census_id"]
                    lgd_id = best_candidate["lgd_id"]
                    match_type = "fuzzy"
                    match_score = best_score

        match_counts[match_type] = match_counts.get(match_type, 0) + 1

        results.append({
            "matched_village_name": matched_name,
            "census2011_id": census_id,
            "lgd_village_id": lgd_id,
            "match_type": match_type,
            "match_score": round(match_score, 3),
        })

    result_df = pd.DataFrame(results)
    matched_df = pd.concat([livestock_df.reset_index(drop=True), result_df], axis=1)

    # Compute stats
    match_stats = {
        "total_records": total,
        "exact_matches": match_counts["exact"],
        "fuzzy_matches": match_counts["fuzzy"],
        "unmatched": match_counts["unmatched"],
        "exact_match_pct": round(100 * match_counts["exact"] / max(total, 1), 2),
        "fuzzy_match_pct": round(100 * match_counts["fuzzy"] / max(total, 1), 2),
        "total_match_pct": round(
            100 * (match_counts["exact"] + match_counts["fuzzy"]) / max(total, 1), 2
        ),
    }

    return matched_df, match_stats


def match_stats_by_state(matched_df):
    """Compute match statistics per state.

    Returns:
        pd.DataFrame with columns: state, total, exact, fuzzy, unmatched,
        match_pct
    """
    stats = []
    for state, group in matched_df.groupby("state_name"):
        total = len(group)
        exact = (group["match_type"] == "exact").sum()
        fuzzy = (group["match_type"] == "fuzzy").sum()
        unmatched = (group["match_type"] == "unmatched").sum()
        stats.append({
            "state": state,
            "total": total,
            "exact": exact,
            "fuzzy": fuzzy,
            "unmatched": unmatched,
            "match_pct": round(100 * (exact + fuzzy) / max(total, 1), 2),
        })

    return pd.DataFrame(stats).sort_values("match_pct", ascending=False)
