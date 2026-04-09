import os

import pandas as pd
from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
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
)


AQUIFER_VECTOR_PATH = PROJECT_ROOT / "data/base_layers/Aquifer_vector.geojson"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/misc/aquifer_vector_local"
GEOSERVER_WORKSPACE = "aquifer"

YIELD_VALUE_MAP = {
    "": None,
    "-": None,
    "Upto 2%": 0.02,
    "1-2%": 0.02,
    "Upto 1.5%": 0.015,
    "Upto 3%": 0.03,
    "Upto 2.5%": 0.025,
    "6 - 8%": 0.08,
    "1-1.5%": 0.015,
    "2-3%": 0.03,
    "Upto 4%": 0.04,
    "Upto 5%": 0.05,
    "Upto -3.5%": 0.035,
    "Upto 3 %": 0.03,
    "Upto 9%": 0.09,
    "1-2.5": 0.025,
    "Upto 1.2%": 0.012,
    "Upto 5-2%": 0.05,
    "Upto 1%": 0.01,
    "Up to 1.5%": 0.015,
    "Upto 8%": 0.08,
    "Upto 6%": 0.06,
    "0.08": 0.08,
    "8 - 16%": 0.16,
    "Not Explored": None,
    "8 - 15%": 0.15,
    "6 - 10%": 0.10,
    "6 - 15%": 0.15,
    "8 - 20%": 0.20,
    "8 - 10%": 0.10,
    "6 - 12%": 0.12,
    "6 - 16%": 0.16,
    "8 - 12%": 0.12,
    "8 - 18%": 0.18,
    "Upto 3.5%": 0.035,
    "Upto 15%": 0.15,
    "1.5-2%": 0.02,
}

PRINCIPAL_AQUIFERS = [
    "Laterite",
    "Basalt",
    "Sandstone",
    "Shale",
    "Limestone",
    "Granite",
    "Schist",
    "Quartzite",
    "Charnockite",
    "Khondalite",
    "Banded Gneissic Complex",
    "Gneiss",
    "Intrusive",
    "Alluvium",
    "None",
]

PRINCIPAL_AQUIFER_PERCENT_COLUMNS = [
    f"principle_aq_{aquifer_name}_percent"
    for aquifer_name in PRINCIPAL_AQUIFERS
]


def _safe_string(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_int(value):
    if pd.isna(value):
        return None
    return int(float(value))


def _normalize_principal_name(value):
    principal_name = _safe_string(value)
    return principal_name or "None"


def _map_yield_value(value):
    return YIELD_VALUE_MAP.get(_safe_string(value))


def _build_empty_aquifer_properties(watershed_row, area_in_ha):
    properties = {
        "uid": watershed_row.get("uid"),
        "id": watershed_row.get("id", watershed_row.get("uid")),
        "area_in_ha": float(area_in_ha) if pd.notna(area_in_ha) else 0.0,
        "total_weighted_yield": 0.0,
        "%_area_aquifer": 0.0,
        "aquifer_count": 0,
        "aquifer_class": "No Data",
        "Age": "",
        "Lithology_": None,
        "Major_Aq_1": "",
        "Major_Aqui": "",
        "Principal_": "",
        "Recommende": None,
        "yeild__": "",
        "zone_m": "",
        "y_value": None,
    }
    properties.update(
        {
            column_name: 0.0
            for column_name in PRINCIPAL_AQUIFER_PERCENT_COLUMNS
        }
    )
    return properties


def _build_aquifer_properties(watershed_row, area_in_ha, intersections_df):
    properties = _build_empty_aquifer_properties(watershed_row, area_in_ha)
    dominant_aquifer = intersections_df.sort_values(
        "intersection_area_m2",
        ascending=False,
    ).iloc[0]
    pct_by_aquifer = intersections_df.groupby("principal_name")[
        "percent_area_aquifer"
    ].sum()

    for aquifer_name in PRINCIPAL_AQUIFERS:
        properties[f"principle_aq_{aquifer_name}_percent"] = float(
            pct_by_aquifer.get(aquifer_name, 0.0)
        )

    principal_value = _safe_string(dominant_aquifer["Principal_"])
    properties.update(
        {
            "total_weighted_yield": float(
                intersections_df["weighted_contribution"].sum()
            ),
            "%_area_aquifer": float(dominant_aquifer["percent_area_aquifer"]),
            "aquifer_count": int(len(intersections_df)),
            "aquifer_class": (
                "Alluvium"
                if principal_value == "Alluvium"
                else "Hard-Rock"
            ),
            "Age": _safe_string(dominant_aquifer["Age"]),
            "Lithology_": _safe_int(dominant_aquifer["Lithology_"]),
            "Major_Aq_1": _safe_string(dominant_aquifer["Major_Aq_1"]),
            "Major_Aqui": _safe_string(dominant_aquifer["Major_Aqui"]),
            "Principal_": principal_value,
            "Recommende": _safe_int(dominant_aquifer["Recommende"]),
            "yeild__": _safe_string(dominant_aquifer["yeild__"]),
            "zone_m": _safe_string(dominant_aquifer["zone_m"]),
            "y_value": (
                float(dominant_aquifer["y_value"])
                if pd.notna(dominant_aquifer["y_value"])
                else None
            ),
        }
    )
    return properties


def _compute_aquifer_properties_for_watersheds(watersheds_gdf, aquifers_gdf):
    watersheds_gdf = validate_geometry(watersheds_gdf)
    if watersheds_gdf.empty:
        raise ValueError("No valid watershed geometries found for local processing.")
    if watersheds_gdf.crs is None:
        raise ValueError("Watershed CRS is missing; cannot compute aquifer overlaps.")

    aquifers_gdf = validate_geometry(aquifers_gdf)
    if aquifers_gdf.empty:
        raise ValueError("Aquifer source file has no valid geometries.")
    if aquifers_gdf.crs is None:
        raise ValueError("Aquifer source CRS is missing; cannot compute overlaps.")

    watersheds_result = watersheds_gdf.copy()
    watersheds_result["area_in_ha"] = get_watershed_areas_in_hectares(
        watersheds_result
    ).astype(float)

    aquifers_with_yield = aquifers_gdf.copy()
    aquifers_with_yield["y_value"] = aquifers_with_yield["yeild__"].apply(
        _map_yield_value
    )
    aquifers_with_yield = aquifers_with_yield.loc[
        aquifers_with_yield["y_value"].notna()
    ].copy()
    if aquifers_with_yield.empty:
        raise ValueError("Aquifer source has no records with valid yield values.")

    watersheds_projected = watersheds_result.to_crs("EPSG:6933")
    aquifers_projected = aquifers_with_yield.to_crs("EPSG:6933")

    computed_rows = []
    total = len(watersheds_projected)

    for index, watershed_idx in enumerate(watersheds_projected.index, start=1):
        watershed_geometry = watersheds_projected.at[watershed_idx, "geometry"]
        watershed_row = watersheds_result.loc[watershed_idx]
        area_in_ha = watersheds_result.at[watershed_idx, "area_in_ha"]

        if watershed_geometry is None or watershed_geometry.is_empty:
            computed_rows.append(
                _build_empty_aquifer_properties(watershed_row, area_in_ha)
            )
            continue

        watershed_area_m2 = float(watershed_geometry.area)
        if watershed_area_m2 <= 0:
            computed_rows.append(
                _build_empty_aquifer_properties(watershed_row, area_in_ha)
            )
            continue

        intersecting_aquifers = aquifers_projected.loc[
            aquifers_projected.intersects(watershed_geometry)
        ]

        intersections = []
        for _, aquifer_row in intersecting_aquifers.iterrows():
            intersection_geometry = watershed_geometry.intersection(
                aquifer_row.geometry
            )
            if intersection_geometry.is_empty:
                continue

            intersection_area_m2 = float(intersection_geometry.area)
            if intersection_area_m2 <= 0:
                continue

            fraction = intersection_area_m2 / watershed_area_m2
            intersections.append(
                {
                    "intersection_area_m2": intersection_area_m2,
                    "percent_area_aquifer": fraction * 100.0,
                    "weighted_contribution": fraction * float(aquifer_row["y_value"]),
                    "principal_name": _normalize_principal_name(
                        aquifer_row["Principal_"]
                    ),
                    "Age": aquifer_row["Age"],
                    "Lithology_": aquifer_row["Lithology_"],
                    "Major_Aq_1": aquifer_row["Major_Aq_1"],
                    "Major_Aqui": aquifer_row["Major_Aqui"],
                    "Principal_": aquifer_row["Principal_"],
                    "Recommende": aquifer_row["Recommende"],
                    "yeild__": aquifer_row["yeild__"],
                    "zone_m": aquifer_row["zone_m"],
                    "y_value": aquifer_row["y_value"],
                }
            )

        if intersections:
            intersections_df = pd.DataFrame(intersections)
            computed_rows.append(
                _build_aquifer_properties(
                    watershed_row,
                    area_in_ha,
                    intersections_df,
                )
            )
        else:
            computed_rows.append(
                _build_empty_aquifer_properties(watershed_row, area_in_ha)
            )

        if index % 200 == 0 or index == total:
            print(f"Computed aquifer properties for {index}/{total} watersheds")

    computed_df = pd.DataFrame(computed_rows)
    for column in computed_df.columns:
        watersheds_result[column] = computed_df[column].values
    return watersheds_result


def run_aquifer_vector_local(
    state,
    district,
    block,
    aquifer_vector_path=AQUIFER_VECTOR_PATH,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    aquifers_gdf = read_validated_vector_file(
        aquifer_vector_path,
        f"Aquifer source file has no valid geometries: {aquifer_vector_path}",
    )

    result_gdf = _compute_aquifer_properties_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        aquifers_gdf=aquifers_gdf,
    )

    layer_name = (
        f"aquifer_vector_{valid_gee_text(district.lower())}_"
        f"{valid_gee_text(block.lower())}"
    )
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        block_fallback="unknown_block",
    )
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local aquifer vector: {asset_id}")
    print(f"Watershed boundary source: {watershed_source}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")
        if not isinstance(geoserver_response, dict) or geoserver_response.get(
            "status_code"
        ) not in (200, 201):
            return False

    if sync_layer_metadata:
        from computing.STAC_specs import generate_STAC_layerwise

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Aquifer",
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_stac_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state,
                district=district,
                block=block,
                layer_name="aquifer_vector",
            )
            update_layer_sync_status(
                layer_id=layer_id,
                is_stac_specs_generated=layer_stac_generated,
            )

    return True


def _generate_aquifer_vector_local_task(
    state,
    district,
    block,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_aquifer_vector_local(
        state=state,
        district=district,
        block=block,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def generate_aquifer_vector(
    self,
    state,
    district,
    block,
    gee_account_id=None,
):
    _ = self
    return _generate_aquifer_vector_local_task(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
    )
