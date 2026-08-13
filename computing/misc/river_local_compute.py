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

from computing.config_loader import (
    PAN_INDIA_RIVER_PATH,
    LOCAL_RIVER_OUTPUT,
)
from shapely.ops import unary_union

GEOSERVER_WORKSPACE = "river"

def _extract_lines(geom):
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
    polygon_types = {"Polygon", "MultiPolygon"}

    if rivers_gdf.geometry.geom_type.isin(polygon_types).any():
        rivers_gdf = rivers_gdf.copy()
        rivers_gdf["geometry"] = rivers_gdf.geometry.boundary
        rivers_gdf = rivers_gdf[
            rivers_gdf.geometry.geom_type.isin(
                ["LineString", "MultiLineString", "LinearRing"]
            )
        ]
        print(f"After boundary conversion: {len(rivers_gdf)} river features")

    outer_boundary = watersheds_gdf.geometry.unary_union
    rivers_in_roi = rivers_gdf[rivers_gdf.intersects(outer_boundary)].copy()

    if rivers_in_roi.empty:
        print("No rivers found within the outer boundary.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

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

    result_segments = []
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

    if not result_segments:
        print("No valid river segments after clipping.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    final_gdf = gpd.GeoDataFrame(
        pd.concat(result_segments, ignore_index=True),
        crs=rivers_gdf.crs,
    )

    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    rename_cols = {}
    if "st_area(sh" in final_gdf.columns:
        rename_cols["st_area(sh"] = "st_area"
    if "st_length(" in final_gdf.columns:
        rename_cols["st_length("] = "st_length"
    
    if rename_cols:
        final_gdf = final_gdf.rename(columns=rename_cols)

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
    river_vector_path=PAN_INDIA_RIVER_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Celery task for local river vector generation.
    """
    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_river_vector"
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
        output_base_dir=LOCAL_RIVER_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local river vector: {asset_id}")

    if push_to_geoserver:
        push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="River Vector",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for river vector")

    return True


