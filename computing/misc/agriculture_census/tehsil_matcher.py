"""
Tehsil Name Matcher

Matches scraped agriculture census tehsil/district names to the CoRE Stack
administrative boundary dataset using edit distance and phonetic matching.

The CoRE Stack uses SOI (Survey of India) tehsil boundaries which may have
different spellings compared to the Agriculture Census website.
"""

import pandas as pd
from difflib import SequenceMatcher
from unidecode import unidecode


def _normalize(text):
    """Normalize a tehsil/district name for matching."""
    if not isinstance(text, str):
        return ""
    text = unidecode(text).strip().lower()
    # Remove common variations
    for token in ["district", "tehsil", "taluk", "mandal", "block"]:
        text = text.replace(token, "").strip()
    text = " ".join(text.split())
    return text


def _similarity(a, b):
    """Compute similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()


def match_tehsils(
    census_df,
    boundary_df,
    state_col="STATE",
    district_col="District",
    tehsil_col="TEHSIL",
    similarity_threshold=0.75,
):
    """Match agriculture census tehsil names to SOI boundary tehsil names.

    Args:
        census_df: DataFrame with scraped agriculture census data
            Expected columns: state, district, tehsil
        boundary_df: DataFrame of SOI tehsil boundaries
            Expected columns: STATE, District, TEHSIL (configurable)
        similarity_threshold: Minimum score for a fuzzy match

    Returns:
        tuple: (matched_df, stats_dict)
    """
    # Build lookup from boundary data
    boundary_lookup = {}
    for _, row in boundary_df.iterrows():
        state = _normalize(str(row.get(state_col, "")))
        district = _normalize(str(row.get(district_col, "")))
        tehsil = _normalize(str(row.get(tehsil_col, "")))

        key = (state, district)
        if key not in boundary_lookup:
            boundary_lookup[key] = []
        boundary_lookup[key].append({
            "tehsil": tehsil,
            "original": str(row.get(tehsil_col, "")),
        })

    results = []
    counts = {"exact": 0, "fuzzy": 0, "unmatched": 0}

    for _, row in census_df.iterrows():
        state = _normalize(str(row.get("state", "")))
        district = _normalize(str(row.get("district", "")))
        tehsil = _normalize(str(row.get("tehsil", "")))

        key = (state, district)
        candidates = boundary_lookup.get(key, [])

        # Also search across all districts in the state if no match
        if not candidates:
            for k, v in boundary_lookup.items():
                if k[0] == state:
                    candidates.extend(v)

        matched_tehsil = ""
        match_type = "unmatched"
        match_score = 0.0

        if candidates and tehsil:
            # Exact match
            for c in candidates:
                if c["tehsil"] == tehsil:
                    matched_tehsil = c["original"]
                    match_type = "exact"
                    match_score = 1.0
                    break

            # Fuzzy match
            if match_type == "unmatched":
                best_score = 0
                best = None
                for c in candidates:
                    score = _similarity(tehsil, c["tehsil"])
                    if score > best_score:
                        best_score = score
                        best = c
                if best and best_score >= similarity_threshold:
                    matched_tehsil = best["original"]
                    match_type = "fuzzy"
                    match_score = best_score

        counts[match_type] = counts.get(match_type, 0) + 1
        results.append({
            "matched_tehsil": matched_tehsil,
            "match_type": match_type,
            "match_score": round(match_score, 3),
        })

    result_df = pd.DataFrame(results)
    matched_df = pd.concat([census_df.reset_index(drop=True), result_df], axis=1)

    total = len(census_df)
    stats = {
        "total": total,
        "exact": counts["exact"],
        "fuzzy": counts["fuzzy"],
        "unmatched": counts["unmatched"],
        "match_pct": round(100 * (counts["exact"] + counts["fuzzy"]) / max(total, 1), 2),
    }

    return matched_df, stats
