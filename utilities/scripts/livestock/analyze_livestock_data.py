#!/usr/bin/env python3
"""Create descriptive QA tables for the livestock GeoPackage.

Run with:

    uv run --with pandas --with numpy python utilities/scripts/analyze_livestock_gpkg.py

The script intentionally uses the GeoPackage's native R-tree table for quick
bounding-box summaries, so it does not require GDAL/geopandas.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


SPECIES = ["cattle", "buffalo", "sheep", "goat", "pig"]
COUNT_COLUMNS = [f"{species}_{sex}" for species in SPECIES for sex in ["male", "female", "total"]]
PERCENTILE_PROBS = [0.02, 0.10, 0.25, 0.50, 0.75, 0.90, 0.98]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=root / "data/livestock/pan_india_livestock.gpkg",
        help="Final livestock GeoPackage to analyse.",
    )
    parser.add_argument(
        "--processed-csv",
        type=Path,
        default=root / "data/livestock/processed/livestock_pan_india.csv",
        help="Authoritative processed village-level livestock CSV.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=root / "data/livestock/processed",
        help="Directory for analysis outputs.",
    )
    return parser.parse_args()


def rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def clean_float(value: float | int | None, ndigits: int = 6) -> float | int | None:
    if value is None or pd.isna(value) or np.isinf(value):
        return None
    value = float(value)
    if abs(value - round(value)) < 1e-9:
        return int(round(value))
    return round(value, ndigits)


def csv_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        value = float(value)
        if not math.isfinite(value):
            return ""
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def write_clean_csv(df: pd.DataFrame, output: Path) -> None:
    cleaned = df.copy()
    for column in cleaned.columns:
        cleaned[column] = cleaned[column].map(csv_value)
    cleaned.to_csv(output, index=False)


def gini(values: pd.Series) -> float | None:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    arr = arr[arr >= 0]
    if arr.size == 0:
        return None
    total = arr.sum()
    if total == 0:
        return 0.0
    arr.sort()
    n = arr.size
    return float((2 * np.arange(1, n + 1) @ arr) / (n * total) - (n + 1) / n)


def top_share(values: pd.Series, fraction: float = 0.10) -> float | None:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    arr = arr[arr >= 0]
    if arr.size == 0:
        return None
    total = arr.sum()
    if total == 0:
        return 0.0
    k = max(1, int(math.ceil(arr.size * fraction)))
    return float(np.sort(arr)[-k:].sum() / total)


def read_gpkg(gpkg: Path) -> tuple[pd.DataFrame, dict, int, int]:
    con = sqlite3.connect(gpkg)
    summary_row = con.execute("select value from livestock_join_metadata where key = 'summary'").fetchone()
    join_summary = json.loads(summary_row[0]) if summary_row else {}
    rtree_count = con.execute("select count(*) from rtree_livestock_geom").fetchone()[0]
    feature_count = con.execute("select count(*) from livestock").fetchone()[0]

    query_columns = [
        "l.fid",
        "l.state_name",
        "l.district_name",
        "l.TEHSIL as subdistrict_name",
        "l.pc11_village_id as village_code",
        "l.NAME as village_name",
    ]
    query_columns += [f"l.{column}" for column in COUNT_COLUMNS]
    query_columns += ["r.minx", "r.maxx", "r.miny", "r.maxy"]

    query = f"""
        select {", ".join(query_columns)}
        from livestock l
        left join rtree_livestock_geom r on l.fid = r.id
        where l.cattle_total is not null
           or l.buffalo_total is not null
           or l.sheep_total is not null
           or l.goat_total is not null
           or l.pig_total is not null
    """
    df = pd.read_sql_query(query, con)
    con.close()
    return df, join_summary, int(feature_count), int(rtree_count)


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    for column in COUNT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype("int64")

    for species in SPECIES:
        expected = df[f"{species}_male"] + df[f"{species}_female"]
        if not (expected == df[f"{species}_total"]).all():
            raise ValueError(f"{species}_male + {species}_female does not equal {species}_total")

    df["total_livestock"] = df[[f"{species}_total" for species in SPECIES]].sum(axis=1)
    df["male_total"] = df[[f"{species}_male" for species in SPECIES]].sum(axis=1)
    df["female_total"] = df[[f"{species}_female" for species in SPECIES]].sum(axis=1)
    df["bovine_total"] = df["cattle_total"] + df["buffalo_total"]
    df["small_ruminant_total"] = df["sheep_total"] + df["goat_total"]

    with np.errstate(divide="ignore", invalid="ignore"):
        df["female_share"] = np.where(df["total_livestock"] > 0, df["female_total"] / df["total_livestock"], np.nan)
        for species in SPECIES:
            df[f"{species}_share"] = np.where(
                df["total_livestock"] > 0,
                df[f"{species}_total"] / df["total_livestock"],
                np.nan,
            )

    mid_lat = ((df["miny"] + df["maxy"]) / 2.0).astype(float)
    width_km = (df["maxx"] - df["minx"]).clip(lower=0).astype(float) * 111.320 * np.cos(np.deg2rad(mid_lat))
    height_km = (df["maxy"] - df["miny"]).clip(lower=0).astype(float) * 110.574
    df["bbox_area_sq_km_approx"] = (width_km * height_km).replace([np.inf, -np.inf], np.nan)
    df["bbox_width_km_approx"] = width_km.replace([np.inf, -np.inf], np.nan)
    df["bbox_height_km_approx"] = height_km.replace([np.inf, -np.inf], np.nan)
    return df


def write_metric_percentiles(df: pd.DataFrame, output: Path) -> pd.DataFrame:
    metric_defs = []
    for species in SPECIES:
        metric_defs.extend(
            [
                (f"{species}_male", "count", f"{species.title()} male population count."),
                (f"{species}_female", "count", f"{species.title()} female population count."),
                (f"{species}_total", "count", f"{species.title()} total population count."),
            ]
        )
    metric_defs.extend(
        [
            ("male_total", "count", "All five livestock groups, male count."),
            ("female_total", "count", "All five livestock groups, female count."),
            ("total_livestock", "count", "All five livestock groups combined."),
            ("bovine_total", "count", "Cattle plus buffalo; cattle and buffalo remain separate source categories."),
            ("small_ruminant_total", "count", "Sheep plus goat."),
            ("female_share", "share", "Female share of all counted livestock in the village."),
        ]
    )
    metric_defs.extend(
        (f"{species}_share", "share", f"{species.title()} share of all counted livestock in the village.")
        for species in SPECIES
    )
    metric_defs.extend(
        [
            ("bbox_area_sq_km_approx", "approx_sq_km", "Approximate area of the village bounding box stored in the GeoPackage R-tree."),
            ("bbox_width_km_approx", "approx_km", "Approximate east-west width of the village bounding box."),
            ("bbox_height_km_approx", "approx_km", "Approximate north-south height of the village bounding box."),
        ]
    )

    rows = []
    for metric, unit, description in metric_defs:
        values = pd.to_numeric(df[metric], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        quantiles = values.quantile(PERCENTILE_PROBS) if len(values) else pd.Series(dtype=float)
        mean = values.mean() if len(values) else np.nan
        std = values.std(ddof=0) if len(values) else np.nan
        rows.append(
            {
                "scope": "matched_gpkg_villages",
                "metric": metric,
                "unit": unit,
                "records": int(len(df)),
                "non_null_records": int(values.shape[0]),
                "nonzero_records": int((values != 0).sum()) if len(values) else 0,
                "total": clean_float(values.sum()) if unit == "count" else None,
                "min": clean_float(values.min()) if len(values) else None,
                "p02": clean_float(quantiles.loc[0.02]) if len(values) else None,
                "p10": clean_float(quantiles.loc[0.10]) if len(values) else None,
                "p25": clean_float(quantiles.loc[0.25]) if len(values) else None,
                "median": clean_float(quantiles.loc[0.50]) if len(values) else None,
                "mean": clean_float(mean),
                "p75": clean_float(quantiles.loc[0.75]) if len(values) else None,
                "p90": clean_float(quantiles.loc[0.90]) if len(values) else None,
                "p98": clean_float(quantiles.loc[0.98]) if len(values) else None,
                "max": clean_float(values.max()) if len(values) else None,
                "std": clean_float(std),
                "coefficient_of_variation": clean_float(std / mean) if mean and not pd.isna(mean) else None,
                "comments": description,
            }
        )

    result = pd.DataFrame(rows)
    write_clean_csv(result, output)
    return result


def district_row(state_name: str, district_name: str, group: pd.DataFrame) -> dict:
    values = group["total_livestock"]
    quantiles = values.quantile(PERCENTILE_PROBS)
    mean = values.mean()
    std = values.std(ddof=0)
    minx = group["minx"].min()
    maxx = group["maxx"].max()
    miny = group["miny"].min()
    maxy = group["maxy"].max()
    mid_lat = (miny + maxy) / 2.0
    envelope_width = max(0.0, (maxx - minx) * 111.320 * math.cos(math.radians(mid_lat)))
    envelope_height = max(0.0, (maxy - miny) * 110.574)
    envelope_area = envelope_width * envelope_height
    p10 = quantiles.loc[0.10]
    p02 = quantiles.loc[0.02]
    return {
        "scope": "matched_gpkg_villages_by_district",
        "state_name": state_name,
        "district_name": district_name,
        "matched_village_count": int(len(group)),
        "total_livestock": int(values.sum()),
        "total_livestock_p02": clean_float(quantiles.loc[0.02]),
        "total_livestock_p10": clean_float(quantiles.loc[0.10]),
        "total_livestock_p25": clean_float(quantiles.loc[0.25]),
        "total_livestock_median": clean_float(quantiles.loc[0.50]),
        "total_livestock_mean": clean_float(mean),
        "total_livestock_p75": clean_float(quantiles.loc[0.75]),
        "total_livestock_p90": clean_float(quantiles.loc[0.90]),
        "total_livestock_p98": clean_float(quantiles.loc[0.98]),
        "total_livestock_min": int(values.min()),
        "total_livestock_max": int(values.max()),
        "total_livestock_std": clean_float(std),
        "coefficient_of_variation": clean_float(std / mean) if mean else None,
        "gini_total_livestock": clean_float(gini(values)),
        "p90_p10_ratio": clean_float(quantiles.loc[0.90] / p10) if p10 else None,
        "p98_p02_ratio": clean_float(quantiles.loc[0.98] / p02) if p02 else None,
        "top_10pct_villages_livestock_share": clean_float(top_share(values, 0.10)),
        "rtree_bbox_village_count": int(group["minx"].notna().sum()),
        "rtree_envelope_width_km_approx": clean_float(envelope_width),
        "rtree_envelope_height_km_approx": clean_float(envelope_height),
        "rtree_envelope_area_sq_km_approx": clean_float(envelope_area),
        "matched_villages_per_1000_sq_km_envelope": clean_float(len(group) * 1000 / envelope_area) if envelope_area else None,
        "mean_village_bbox_area_sq_km_approx": clean_float(group["bbox_area_sq_km_approx"].mean()),
        "median_village_bbox_area_sq_km_approx": clean_float(group["bbox_area_sq_km_approx"].median()),
    }


def write_district_spatial_variation(df: pd.DataFrame, output: Path) -> pd.DataFrame:
    rows = [
        district_row(state_name, district_name, group)
        for (state_name, district_name), group in df.groupby(["state_name", "district_name"], dropna=False, sort=True)
    ]
    result = pd.DataFrame(rows)
    write_clean_csv(result, output)
    return result


def livestock_totals(df: pd.DataFrame) -> dict:
    return {
        species: {
            "male": int(df[f"{species}_male"].sum()),
            "female": int(df[f"{species}_female"].sum()),
            "total": int(df[f"{species}_total"].sum()),
        }
        for species in SPECIES
    }


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    args.output_dir.mkdir(parents=True, exist_ok=True)

    percentiles_csv = args.output_dir / "livestock_metric_percentiles.csv"
    district_csv = args.output_dir / "livestock_district_spatial_variation_metrics.csv"
    analysis_json = args.output_dir / "livestock_gpkg_analysis.json"

    gpkg_df, join_summary, feature_count, rtree_count = read_gpkg(args.gpkg)
    gpkg_df = add_derived_metrics(gpkg_df)
    percentiles_df = write_metric_percentiles(gpkg_df, percentiles_csv)
    district_df = write_district_spatial_variation(gpkg_df, district_csv)

    csv_df = pd.read_csv(args.processed_csv, usecols=["village_code"] + COUNT_COLUMNS)
    for column in COUNT_COLUMNS:
        csv_df[column] = pd.to_numeric(csv_df[column], errors="coerce").fillna(0).astype("int64")

    analysis = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": {
            "gpkg": rel(args.gpkg, root),
            "gpkg_layer": "livestock",
            "gpkg_rtree_table": "rtree_livestock_geom",
            "processed_csv": rel(args.processed_csv, root),
        },
        "outputs": {
            "metric_percentiles_csv": rel(percentiles_csv, root),
            "district_spatial_variation_csv": rel(district_csv, root),
        },
        "gpkg_summary": {
            "geometry_features_total": int(feature_count),
            "rtree_bbox_features_total": int(rtree_count),
            "features_with_livestock_attributes": int(len(gpkg_df)),
            "features_without_livestock_attributes": int(feature_count - len(gpkg_df)),
            "livestock_attribute_join_rate_against_gpkg_features": round(len(gpkg_df) / feature_count, 6) if feature_count else None,
            "distinct_states_with_matched_livestock": int(gpkg_df["state_name"].nunique(dropna=True)),
            "distinct_districts_with_matched_livestock": int(gpkg_df[["state_name", "district_name"]].drop_duplicates().shape[0]),
            "distinct_subdistricts_with_matched_livestock": int(
                gpkg_df[["state_name", "district_name", "subdistrict_name"]].drop_duplicates().shape[0]
            ),
        },
        "processed_csv_summary": {
            "rows": int(len(csv_df)),
            "unique_village_codes": int(csv_df["village_code"].nunique(dropna=True)),
            "duplicate_village_codes": int(len(csv_df) - csv_df["village_code"].nunique(dropna=True)),
        },
        "csv_vs_gpkg": {
            "gpkg_features_with_livestock_attributes_as_share_of_processed_csv_rows": round(len(gpkg_df) / len(csv_df), 6)
            if len(csv_df)
            else None,
            "note": (
                "The processed CSV is the authoritative matched village table. The current GPKG contains the geometry "
                "layer and livestock attributes where the admin geometry key joined successfully."
            ),
        },
        "livestock_totals_processed_csv": livestock_totals(csv_df),
        "livestock_totals_gpkg_joined_features": livestock_totals(gpkg_df),
        "spatial_metric_notes": [
            "R-tree metrics use GeoPackage bounding boxes, not exact polygon areas.",
            "Bounding boxes are useful for quick spatial spread and density summaries, but they should not be interpreted as surveyed village area.",
            "Approximate kilometre conversions use WGS84 longitude/latitude degree distances and are intended for descriptive QA, not legal area measurement.",
        ],
        "join_metadata_from_gpkg": join_summary,
    }
    analysis_json.write_text(json.dumps(analysis, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"wrote {rel(percentiles_csv, root)} ({len(percentiles_df)} rows)")
    print(f"wrote {rel(district_csv, root)} ({len(district_df)} rows)")
    print(f"wrote {rel(analysis_json, root)}")
    print(f"gpkg features with livestock attributes: {len(gpkg_df)} of {feature_count}")


if __name__ == "__main__":
    main()
