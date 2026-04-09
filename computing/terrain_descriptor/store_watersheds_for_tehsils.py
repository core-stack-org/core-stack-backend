import argparse
import csv
import os
import re
import time

import geopandas as gpd


DEFAULT_MICROWATERSHED_PATH = (
    "data/base_layers/Microwatershed_v2_with_details.geojson"
)
DEFAULT_TEHSIL_PATH = "data/base_layers/SOI_tehsil.geojson"
DEFAULT_OUTPUT_DIR = "data/base_layers/tehsil_watersheds"

STATE_COLUMN_CANDIDATES = ["STATE", "state", "state_name", "State"]
DISTRICT_COLUMN_CANDIDATES = ["District", "district", "district_name", "DISTRICT"]
TEHSIL_COLUMN_CANDIDATES = ["TEHSIL", "tehsil", "tehsil_name", "block", "block_name"]
MWS_UID_COLUMN_CANDIDATES = ["uid", "UID", "Uid"]
MWS_OPTIONAL_COLUMNS = ["area_in_ha", "bacode", "sbcode", "wsconc"]

OUTPUT_FORMATS = {
    "geojson": ("GeoJSON", ".geojson"),
    "gpkg": ("GPKG", ".gpkg"),
}


def valid_gee_text(description):
    description = re.sub(r"[^a-zA-Z0-9 ,:;_-]", "", description)
    return description.replace(" ", "_")


def _normalize_text(value):
    return valid_gee_text(str(value).strip().lower())

def _find_matching_column(columns, candidates):
    columns_list = list(columns)
    lower_lookup = {col.lower(): col for col in columns_list}
    for candidate in candidates:
        if candidate.lower() in lower_lookup:
            return lower_lookup[candidate.lower()]

    compact_lookup = {
        re.sub(r"[^a-z0-9]", "", col.lower()): col for col in columns_list
    }
    for candidate in candidates:
        compact = re.sub(r"[^a-z0-9]", "", candidate.lower())
        if compact in compact_lookup:
            return compact_lookup[compact]
    return None


def _validate_geometry(gdf, fix_invalid=False):
    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        return gdf

    gdf = gdf[~gdf.geometry.is_empty].copy()
    if gdf.empty:
        return gdf

    if fix_invalid:
        invalid = ~gdf.is_valid
        if invalid.any():
            gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)
            gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf


def _prepare_tehsil_boundaries(tehsil_path, state=None, district=None, tehsil=None):
    tehsil_gdf = gpd.read_file(tehsil_path)

    state_col = "STATE" if "STATE" in tehsil_gdf.columns else None
    district_col = "District" if "District" in tehsil_gdf.columns else None
    tehsil_col = "TEHSIL" if "TEHSIL" in tehsil_gdf.columns else None

    if state_col is None:
        state_col = _find_matching_column(tehsil_gdf.columns, STATE_COLUMN_CANDIDATES)
    if district_col is None:
        district_col = _find_matching_column(tehsil_gdf.columns, DISTRICT_COLUMN_CANDIDATES)
    if tehsil_col is None:
        tehsil_col = _find_matching_column(tehsil_gdf.columns, TEHSIL_COLUMN_CANDIDATES)

    if not all([state_col, district_col, tehsil_col]):
        raise ValueError(
            "Could not identify STATE/District/TEHSIL columns in tehsil file. "
            f"Available columns: {list(tehsil_gdf.columns)}"
        )

    if state:
        tehsil_gdf = tehsil_gdf[
            tehsil_gdf[state_col].fillna("").map(_normalize_text)
            == _normalize_text(state)
        ]
    if district:
        tehsil_gdf = tehsil_gdf[
            tehsil_gdf[district_col].fillna("").map(_normalize_text)
            == _normalize_text(district)
        ]
    if tehsil:
        tehsil_gdf = tehsil_gdf[
            tehsil_gdf[tehsil_col].fillna("").map(_normalize_text)
            == _normalize_text(tehsil)
        ]

    tehsil_gdf = _validate_geometry(tehsil_gdf, fix_invalid=True)
    if tehsil_gdf.empty:
        raise ValueError("No matching tehsil records found after applying filters.")

    if tehsil_gdf.crs is None:
        tehsil_gdf = tehsil_gdf.set_crs("EPSG:4326")

    tehsil_gdf["__state_norm"] = tehsil_gdf[state_col].map(_normalize_text)
    tehsil_gdf["__district_norm"] = tehsil_gdf[district_col].map(_normalize_text)
    tehsil_gdf["__tehsil_norm"] = tehsil_gdf[tehsil_col].map(_normalize_text)
    tehsil_gdf["__key"] = (
        tehsil_gdf["__state_norm"]
        + "||"
        + tehsil_gdf["__district_norm"]
        + "||"
        + tehsil_gdf["__tehsil_norm"]
    )

    meta = (
        tehsil_gdf.groupby("__key", as_index=False)
        .agg(
            {
                state_col: "first",
                district_col: "first",
                tehsil_col: "first",
            }
        )
        .rename(
            columns={
                state_col: "STATE",
                district_col: "District",
                tehsil_col: "TEHSIL",
            }
        )
    )

    dissolved = tehsil_gdf[["__key", "geometry"]].dissolve(by="__key").reset_index()
    dissolved = dissolved.merge(meta, on="__key", how="left")
    dissolved = _validate_geometry(dissolved, fix_invalid=True)

    return dissolved


def _load_microwatersheds(microwatershed_path, fix_invalid=False):
    microwatershed_gdf = gpd.read_file(microwatershed_path)
    if microwatershed_gdf.empty:
        raise ValueError("Microwatershed file is empty.")

    uid_col = _find_matching_column(
        microwatershed_gdf.columns, MWS_UID_COLUMN_CANDIDATES
    )
    if not uid_col:
        raise ValueError(
            "Could not identify microwatershed UID column. "
            f"Available columns: {list(microwatershed_gdf.columns)}"
        )

    keep_columns = ["geometry", uid_col]
    for col in MWS_OPTIONAL_COLUMNS:
        if col in microwatershed_gdf.columns:
            keep_columns.append(col)

    microwatershed_gdf = microwatershed_gdf[keep_columns].copy()
    microwatershed_gdf = microwatershed_gdf.rename(columns={uid_col: "uid"})
    microwatershed_gdf = _validate_geometry(microwatershed_gdf, fix_invalid=fix_invalid)

    if microwatershed_gdf.crs is None:
        microwatershed_gdf = microwatershed_gdf.set_crs("EPSG:4326")

    if microwatershed_gdf.empty:
        raise ValueError("No valid microwatershed geometry found.")

    return microwatershed_gdf


def _save_subset(subset_gdf, output_path, driver):
    if driver == "GPKG":
        subset_gdf.to_file(output_path, driver=driver, layer="watersheds")
    else:
        subset_gdf.to_file(output_path, driver=driver)


def generate_tehsil_watershed_copies(
    microwatershed_path=DEFAULT_MICROWATERSHED_PATH,
    tehsil_path=DEFAULT_TEHSIL_PATH,
    output_dir=DEFAULT_OUTPUT_DIR,
    output_format="gpkg",
    overwrite=False,
    include_empty=False,
    fix_invalid_mws=False,
    state=None,
    district=None,
    tehsil=None,
):
    if output_format not in OUTPUT_FORMATS:
        raise ValueError(
            f"Unsupported format: {output_format}. Supported: {sorted(OUTPUT_FORMATS)}"
        )

    driver, extension = OUTPUT_FORMATS[output_format]

    if not os.path.exists(microwatershed_path):
        raise FileNotFoundError(f"Microwatershed file not found: {microwatershed_path}")
    if not os.path.exists(tehsil_path):
        raise FileNotFoundError(f"Tehsil file not found: {tehsil_path}")

    os.makedirs(output_dir, exist_ok=True)
    start_time = time.time()

    print("Loading tehsil boundaries...")
    tehsil_boundaries = _prepare_tehsil_boundaries(
        tehsil_path=tehsil_path,
        state=state,
        district=district,
        tehsil=tehsil,
    )
    print(f"Prepared {len(tehsil_boundaries)} unique tehsil boundaries.")

    print("Loading microwatersheds (this can take time once)...")
    microwatersheds = _load_microwatersheds(
        microwatershed_path=microwatershed_path,
        fix_invalid=fix_invalid_mws,
    )
    print(f"Loaded {len(microwatersheds)} microwatershed features.")

    if tehsil_boundaries.crs != microwatersheds.crs:
        tehsil_boundaries = tehsil_boundaries.to_crs(microwatersheds.crs)

    print("Building microwatershed spatial index...")
    mws_sindex = microwatersheds.sindex

    manifest_rows = []
    total = len(tehsil_boundaries)
    written_count = 0
    skipped_count = 0

    for idx, row in enumerate(tehsil_boundaries.itertuples(index=False), start=1):
        state_name = row.STATE
        district_name = row.District
        tehsil_name = row.TEHSIL
        tehsil_geom = row.geometry

        try:
            candidate_ids = mws_sindex.query(tehsil_geom, predicate="intersects")
        except TypeError:
            candidate_ids = list(mws_sindex.intersection(tehsil_geom.bounds))

        if len(candidate_ids) == 0:
            intersection_gdf = microwatersheds.iloc[0:0].copy()
        else:
            candidates = microwatersheds.iloc[list(candidate_ids)]
            intersection_gdf = candidates[candidates.intersects(tehsil_geom)].copy()

        feature_count = len(intersection_gdf)
        state_dir = valid_gee_text(str(state_name).strip().lower()) or "unknown_state"
        district_dir = (
            valid_gee_text(str(district_name).strip().lower()) or "unknown_district"
        )
        tehsil_file = (
            valid_gee_text(str(tehsil_name).strip().lower())
            or f"unknown_tehsil_{idx:05d}"
        )

        output_subdir = os.path.join(output_dir, state_dir, district_dir)
        os.makedirs(output_subdir, exist_ok=True)
        out_path = os.path.join(output_subdir, f"{tehsil_file}{extension}")

        row_status = "empty"
        if feature_count > 0 or include_empty:
            if feature_count > 0:
                intersection_gdf["STATE"] = state_name
                intersection_gdf["District"] = district_name
                intersection_gdf["TEHSIL"] = tehsil_name

            if overwrite or not os.path.exists(out_path):
                _save_subset(intersection_gdf, out_path, driver)
                row_status = "written"
                written_count += 1
            else:
                row_status = "exists"
                skipped_count += 1
        else:
            skipped_count += 1

        manifest_rows.append(
            {
                "state": state_name,
                "district": district_name,
                "tehsil": tehsil_name,
                "feature_count": feature_count,
                "status": row_status,
                "output_path": out_path if (feature_count > 0 or include_empty) else "",
            }
        )

        if idx % 100 == 0 or idx == total:
            print(f"Processed {idx}/{total} tehsils...")

    manifest_path = os.path.join(output_dir, "tehsil_watershed_manifest.csv")
    with open(manifest_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "state",
                "district",
                "tehsil",
                "feature_count",
                "status",
                "output_path",
            ],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    elapsed = time.time() - start_time
    print("Done.")
    print(f"Output directory: {output_dir}")
    print(f"Manifest: {manifest_path}")
    print(f"Tehsils processed: {total}")
    print(f"Files written: {written_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Elapsed time: {elapsed:.2f} seconds")

    return manifest_path


def _build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Precompute and store microwatersheds for each tehsil based on spatial "
            "intersection with tehsil boundaries."
        )
    )
    parser.add_argument(
        "--microwatershed-path",
        default=DEFAULT_MICROWATERSHED_PATH,
        help="Path to Microwatershed GeoJSON",
    )
    parser.add_argument(
        "--tehsil-path",
        default=DEFAULT_TEHSIL_PATH,
        help="Path to tehsil boundary GeoJSON",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to save per-tehsil watershed files",
    )
    parser.add_argument(
        "--format",
        choices=sorted(OUTPUT_FORMATS.keys()),
        default="gpkg",
        help="Output file format",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Also write empty files for tehsils with no intersecting microwatersheds",
    )
    parser.add_argument(
        "--fix-invalid-mws",
        action="store_true",
        help="Fix invalid microwatershed geometries (slower, use only if needed)",
    )
    parser.add_argument("--state", help="Optional state filter")
    parser.add_argument("--district", help="Optional district filter")
    parser.add_argument("--tehsil", help="Optional tehsil filter")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    generate_tehsil_watershed_copies(
        microwatershed_path=args.microwatershed_path,
        tehsil_path=args.tehsil_path,
        output_dir=args.output_dir,
        output_format=args.format,
        overwrite=args.overwrite,
        include_empty=args.include_empty,
        fix_invalid_mws=args.fix_invalid_mws,
        state=args.state,
        district=args.district,
        tehsil=args.tehsil,
    )
