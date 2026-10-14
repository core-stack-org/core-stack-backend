"""
Phase 2 — clean GBIF occurrences.

Dirty coordinates INFLATE species richness (see README.md §3), so this step is not optional.
We replicate the core CoordinateCleaner ideas with explicit, dependency-light pandas filters.
"""

import pandas as pd

from . import config

# Only the columns the pipeline uses. iucnRedListCategory is NOT in SIMPLE_CSV (see gbif_iucn.py).
_USECOLS = [
    "gbifID",
    "species",
    "taxonKey",
    "kingdom",
    "class",
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
    "basisOfRecord",
]


def clean_occurrences(csv_path, out_path=None):
    """
    Read the raw SIMPLE_CSV, drop untrustworthy records, and return a cleaned DataFrame.
    Logs how many records were dropped (data provenance). Writes to out_path if given.
    """
    df = pd.read_csv(
        csv_path,
        sep="\t",
        on_bad_lines="skip",
        usecols=lambda c: c in _USECOLS,  # tolerate columns missing in some downloads
        low_memory=False,  # infer dtypes from the whole file; per-block downloads fit in RAM
    )
    n0 = len(df)  # rows before cleaning (for the provenance log)

    # 1. must have a real species id and coordinates
    df = df.dropna(subset=["species", "taxonKey", "decimalLatitude", "decimalLongitude"])

    # 2. inside India bbox and not (0,0)
    minlon, minlat, maxlon, maxlat = config.INDIA_BBOX
    df = df[
        df.decimalLatitude.between(minlat, maxlat)
        & df.decimalLongitude.between(minlon, maxlon)
    ]
    df = df[~((df.decimalLatitude == 0) & (df.decimalLongitude == 0))]

    # 3. drop imprecise coordinates (unknown uncertainty is allowed through)
    if "coordinateUncertaintyInMeters" in df.columns:
        unc = df["coordinateUncertaintyInMeters"]
        df = df[unc.isna() | (unc <= config.MAX_COORD_UNCERTAINTY_M)]

    # 4. drop suspected country/province centroids (huge pile on one exact coordinate)
    dup = df.groupby(["decimalLatitude", "decimalLongitude"]).size()
    suspicious = dup[dup > config.CENTROID_DUP_THRESHOLD].index
    if len(suspicious):
        df = df[
            ~df.set_index(["decimalLatitude", "decimalLongitude"]).index.isin(suspicious)
        ]

    # 5. de-duplicate identical species-at-coordinate records
    df = df.drop_duplicates(subset=["species", "decimalLatitude", "decimalLongitude"])

    print(f"[gbif] cleaned {n0} -> {len(df)} records ({n0 - len(df)} dropped)")
    if out_path:
        df.to_csv(out_path, index=False)
    return df
