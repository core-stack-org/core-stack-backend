import os
import json
import datetime
import pandas as pd
import geopandas as gpd
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    PROJECT_ROOT,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    get_watershed_areas_in_hectares,
    load_precomputed_watersheds,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
    fix_invalid_geometry_in_gdf,
)

RIVER_VECTOR_PATH = PROJECT_ROOT / "data/river/River_pan_india.geojson"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/river/river_local"
GEOSERVER_WORKSPACE = "river"

from shapely.ops import unary_union


def _extract_lines(geom):
    """
    Extract only line geometries from any Shapely geometry.
    Handles GeometryCollection, Polygon boundaries, pure lines.
    Returns a LineString/MultiLineString or None if nothing linear found.
    """
    line_types = {"LineString", "MultiLineString", "LinearRing"}

    if geom is None or geom.is_empty:
        return None

    if geom.geom_type in line_types:
        return geom

    # Polygon/MultiPolygon → take boundary
    if geom.geom_type in {"Polygon", "MultiPolygon"}:
        b = geom.boundary
        return b if not b.is_empty else None

    # GeometryCollection → recurse and collect lines
    if geom.geom_type == "GeometryCollection":
        lines = []
        for part in geom.geoms:
            extracted = _extract_lines(part)
            if extracted and not extracted.is_empty:
                lines.append(extracted)
        if not lines:
            return None
        return unary_union(lines)

    return None


def _compute_river_properties_for_watersheds(watersheds_gdf, rivers_gdf):

    watersheds_gdf = validate_geometry(watersheds_gdf).reset_index(drop=True)
    rivers_gdf = validate_geometry(rivers_gdf).reset_index(drop=True)

    # ── Convert river polygons → boundary lines ───────────────────────────
    # The River pan-India dataset stores rivers as Polygon/MultiPolygon
    # (flood-plain buffers). Extract the boundary so intersection with
    # watershed polygons produces LineStrings, not Polygons.
    polygon_types = {"Polygon", "MultiPolygon"}
    if rivers_gdf.geometry.geom_type.isin(polygon_types).any():
        print("River geometries are Polygons — converting to boundary lines...")
        rivers_gdf = rivers_gdf.copy()
        rivers_gdf["geometry"] = rivers_gdf.geometry.boundary
        # boundary of a Polygon → LinearRing (subclass of LineString, fine for GeoServer)
        # boundary of a MultiPolygon → MultiLineString
        rivers_gdf = rivers_gdf[
            rivers_gdf.geometry.geom_type.isin(
                ["LineString", "MultiLineString", "LinearRing"]
            )
        ]
        print(f"After boundary conversion: {len(rivers_gdf)} river features")

    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Coarse filter ─────────────────────────────────────────────
    rivers_in_roi = rivers_gdf[rivers_gdf.intersects(outer_boundary)].copy()

    if rivers_in_roi.empty:
        print("No rivers found within the outer boundary.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    print(f"Rivers within outer boundary: {len(rivers_in_roi)}")

    # ── Step 2: Spatial join ──────────────────────────────────────────────
    watersheds_indexed = watersheds_gdf[["uid", "area_in_ha", "geometry"]].copy()

    joined = gpd.sjoin(
        rivers_in_roi,
        watersheds_indexed,
        how="inner",
        predicate="intersects",
    )

    matched_river_indices = joined.index.unique()
    gap_rivers = rivers_in_roi.loc[
        ~rivers_in_roi.index.isin(matched_river_indices)
    ].copy()

    print(f"Matched river-watershed pairs : {len(joined)}")
    print(f"Gap rivers (no watershed hit) : {len(gap_rivers)}")

    result_segments = []

    # ── Step 3: Clip matched rivers to their watershed ────────────────────
    if not joined.empty:
        clipped_rows = []

        for idx, row in joined.iterrows():
            try:
                ws_idx = int(row["index_right"])
                ws_geom = watersheds_gdf.loc[ws_idx, "geometry"]
                river_geom = row.geometry

                if not river_geom.is_valid:
                    river_geom = river_geom.buffer(0)
                if not ws_geom.is_valid:
                    ws_geom = ws_geom.buffer(0)

                clipped = river_geom.intersection(ws_geom)

                if clipped is None or clipped.is_empty:
                    continue

                # Extract lines from any geometry type
                clipped = _extract_lines(clipped)
                if clipped is None or clipped.is_empty:
                    continue

                new_row = row.copy()
                new_row["geometry"] = clipped
                clipped_rows.append(new_row)

            except Exception as e:
                print(f"Clip error river idx={idx}: {e}")
                continue

        if clipped_rows:
            matched_fc = gpd.GeoDataFrame(clipped_rows, crs=rivers_gdf.crs)
            result_segments.append(matched_fc)
            print(f"Valid matched segments: {len(clipped_rows)}")

    # ── Step 4: Gap rivers ────────────────────────────────────────────────
    if not gap_rivers.empty:
        clipped_gaps = []

        for idx, row in gap_rivers.iterrows():
            try:
                clipped = row.geometry.intersection(outer_boundary)
                if clipped is None or clipped.is_empty:
                    continue

                clipped = _extract_lines(clipped)
                if clipped is None or clipped.is_empty:
                    continue

                new_row = row.copy()
                new_row["geometry"] = clipped
                new_row["uid"] = ""
                new_row["area_in_ha"] = ""
                clipped_gaps.append(new_row)

            except Exception as e:
                print(f"Gap clip error river idx={idx}: {e}")
                continue

        if clipped_gaps:
            gap_fc = gpd.GeoDataFrame(clipped_gaps, crs=rivers_gdf.crs)
            result_segments.append(gap_fc)
            print(f"Valid gap segments: {len(clipped_gaps)}")

    # ── Step 5: Merge ─────────────────────────────────────────────────────
    if not result_segments:
        print("No valid river segments after clipping.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    final_gdf = gpd.GeoDataFrame(
        pd.concat(result_segments, ignore_index=True),
        crs=rivers_gdf.crs,
    )

    # ── Step 6: Final cleanup ─────────────────────────────────────────────
    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    for col in ["index_right"]:
        if col in final_gdf.columns:
            final_gdf = final_gdf.drop(columns=[col])

    final_gdf = final_gdf[~final_gdf.geometry.is_empty]
    final_gdf = final_gdf[final_gdf.geometry.is_valid]
    final_gdf = final_gdf[final_gdf.geometry.notna()]
    final_gdf = fix_invalid_geometry_in_gdf(final_gdf)

    final_gdf = final_gdf[
        final_gdf.geometry.apply(
            lambda g: g is not None
            and not g.is_empty
            and g.bounds[0] <= g.bounds[2]
            and g.bounds[1] <= g.bounds[3]
        )
    ]

    print(f"Final valid river segments: {len(final_gdf)}")
    return final_gdf


def run_river_vector_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    river_vector_path=RIVER_VECTOR_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Orchestrates the local river vector generation.
    """
    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_river_vector"
        )
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_river_vector".lower()
        watersheds_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
        print(f"ROI source: {roi}")

    if not os.path.exists(river_vector_path):
        raise FileNotFoundError(f"River source file not found: {river_vector_path}")

    print(f"Loading river source: {river_vector_path}")
    rivers_gdf = read_validated_vector_file(
        river_vector_path,
        f"River source file has no valid geometries: {river_vector_path}",
    )

    result_gdf = _compute_river_properties_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        rivers_gdf=rivers_gdf,
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local river vector: {asset_id}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="River Vector",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for river vector")

    return True


@app.task(bind=True)
def river_vector(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    """
    Celery task for local river vector generation.
    Matches signature of river_layer.py
    """
    _ = self, asset_folder_list, app_type, gee_account_id
    return run_river_vector_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )
