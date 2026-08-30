"""
Livestock Census Data Loader

Downloads and parses the 20th Livestock Census (DAHD) village and ward level
data. The source xlsx contains four sheets:
    - Rural Male Population
    - Rural Female Population
    - Urban Male Population
    - Urban Female Population

Each sheet has columns: state_name, district_name, block_name/town_name,
village_name/ward_name, cattle, buffalo, sheep, goat, pig, ...

This module consolidates all four sheets into a single DataFrame with
standardized columns and an area_type (rural/urban) indicator.
"""

import os
import pandas as pd
import requests
from io import BytesIO

DAHD_URL = (
    "https://www.dahd.gov.in/sites/default/files/2023-07/"
    "VillageAndWardLevelDataMale-Female.xlsx"
)

LIVESTOCK_TYPES = ["cattle", "buffalo", "sheep", "goat", "pig"]

# Sheet names in the xlsx file
SHEET_CONFIG = {
    0: {"area_type": "rural", "sex": "male"},
    1: {"area_type": "rural", "sex": "female"},
    2: {"area_type": "urban", "sex": "male"},
    3: {"area_type": "urban", "sex": "female"},
}


def download_livestock_data(url=DAHD_URL, cache_dir=None):
    """Download the DAHD livestock census xlsx file.

    Args:
        url: URL of the xlsx file
        cache_dir: If provided, cache the file locally

    Returns:
        BytesIO object containing the xlsx data
    """
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, "livestock_census_20th.xlsx")
        if os.path.exists(cache_path):
            print(f"Using cached file: {cache_path}")
            with open(cache_path, "rb") as f:
                return BytesIO(f.read())

    print(f"Downloading livestock census data from {url}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    data = BytesIO(resp.content)

    if cache_dir:
        with open(cache_path, "wb") as f:
            f.write(resp.content)
        print(f"Cached to {cache_path}")

    return data


def _normalize_columns(df, area_type):
    """Standardize column names across rural and urban sheets."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    rename_map = {}
    for col in df.columns:
        if "state" in col:
            rename_map[col] = "state_name"
        elif "district" in col:
            rename_map[col] = "district_name"
        elif "block" in col:
            rename_map[col] = "block_name"
        elif "town" in col:
            rename_map[col] = "block_name"
        elif "village" in col:
            rename_map[col] = "village_name"
        elif "ward" in col:
            rename_map[col] = "village_name"

    df = df.rename(columns=rename_map)
    df["area_type"] = area_type
    return df


def _clean_text(series):
    """Normalize text: strip, lowercase, collapse whitespace."""
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )


def load_livestock_data(url=DAHD_URL, cache_dir=None):
    """Load and consolidate all four sheets of the livestock census.

    Returns:
        pd.DataFrame with columns:
            state_name, district_name, block_name, village_name,
            area_type, sex, cattle, buffalo, sheep, goat, pig
    """
    data = download_livestock_data(url, cache_dir)

    frames = []
    for sheet_idx, config in SHEET_CONFIG.items():
        print(f"Parsing sheet {sheet_idx}: {config['area_type']} {config['sex']}...")
        df = pd.read_excel(data, sheet_name=sheet_idx)
        df = _normalize_columns(df, config["area_type"])
        df["sex"] = config["sex"]

        # Ensure livestock columns are numeric
        for col in LIVESTOCK_TYPES:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        frames.append(df)
        data.seek(0)  # Reset stream for next sheet

    combined = pd.concat(frames, ignore_index=True)

    # Clean text fields
    for col in ["state_name", "district_name", "block_name", "village_name"]:
        if col in combined.columns:
            combined[col] = _clean_text(combined[col])

    # Keep only relevant columns
    keep_cols = [
        "state_name", "district_name", "block_name", "village_name",
        "area_type", "sex",
    ] + [c for c in LIVESTOCK_TYPES if c in combined.columns]

    combined = combined[[c for c in keep_cols if c in combined.columns]]

    print(f"Loaded {len(combined)} records across {combined['state_name'].nunique()} states")
    return combined


def aggregate_livestock_data(df):
    """Aggregate male + female counts per village to get total livestock.

    Args:
        df: DataFrame from load_livestock_data()

    Returns:
        pd.DataFrame with total counts per village (male + female combined)
    """
    group_cols = ["state_name", "district_name", "block_name", "village_name", "area_type"]
    livestock_cols = [c for c in LIVESTOCK_TYPES if c in df.columns]

    agg = df.groupby(group_cols, as_index=False)[livestock_cols].sum()
    agg["total_livestock"] = agg[livestock_cols].sum(axis=1)

    print(f"Aggregated to {len(agg)} unique village/ward entries")
    return agg
