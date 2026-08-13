import csv
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from computing.config_loader import PRECOMPUTED_TEHSIL_WATERSHED_DIR, PROJECT_ROOT

DEFAULT_WATERSHED_ROOT = PRECOMPUTED_TEHSIL_WATERSHED_DIR
DEFAULT_BOUNDARY_OUTPUT_ROOT = PROJECT_ROOT / "data" / "hydrology_gpu" / "boundaries"
DEFAULT_PAN_INDIA_DOWNLOAD_BOUNDARY = PROJECT_ROOT / "data" / "base_layers" / "PanIndia_Boundaries" / "india_state_outer_no_islands.geojson"
PAN_INDIA_SLUG = "pan_india"


def normalize_name(value: str) -> str:
    text = str(value).strip().lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def slugify(value: str) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def default_output_path(state: str, district: str, tehsil: str) -> Path:
    return (
        DEFAULT_BOUNDARY_OUTPUT_ROOT
        / slugify(state)
        / slugify(district)
        / f"{slugify(tehsil)}.geojson"
    )


def default_district_output_path(state: str, district: str) -> Path:
    return (
        DEFAULT_BOUNDARY_OUTPUT_ROOT
        / slugify(state)
        / f"{slugify(district)}.geojson"
    )


def default_state_output_path(state: str) -> Path:
    return (
        DEFAULT_BOUNDARY_OUTPUT_ROOT
        / slugify(state)
        / f"{slugify(state)}.geojson"
    )


def default_pan_india_output_path() -> Path:
    return DEFAULT_BOUNDARY_OUTPUT_ROOT / PAN_INDIA_SLUG / f"{PAN_INDIA_SLUG}.geojson"


def download_boundary_path(boundary_path: str | Path) -> Path:
    path = Path(boundary_path)
    return path.with_name(f"{path.stem}_download_boundary{path.suffix}")


def manifest_path(root: Path) -> Path:
    return root / "tehsil_watershed_manifest.csv"


def load_manifest(root: Path) -> list[dict]:
    path = manifest_path(root)
    if not path.exists():
        raise FileNotFoundError(f"Watershed manifest not found: {path}")

    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def manifest_relative_output(root: Path, output_path: str) -> Path | None:
    if not output_path:
        return None

    raw_path = Path(output_path)
    if raw_path.is_absolute() and raw_path.exists():
        return raw_path

    parts = raw_path.parts
    if "tehsil_watersheds" in parts:
        idx = parts.index("tehsil_watersheds")
        candidate = root.joinpath(*parts[idx + 1 :])
        if candidate.exists():
            return candidate

    candidate = root / raw_path
    if candidate.exists():
        return candidate

    return None


def fallback_gpkg_path(root: Path, state: str, district: str, tehsil: str) -> Path:
    return root / slugify(state) / slugify(district) / f"{slugify(tehsil)}.gpkg"


def find_tehsil_watershed(root: Path, state: str, district: str, tehsil: str) -> tuple[Path, dict]:
    wanted_state = normalize_name(state)
    wanted_district = normalize_name(district)
    wanted_tehsil = normalize_name(tehsil)

    for row in load_manifest(root):
        if normalize_name(row.get("state", "")) != wanted_state:
            continue
        if normalize_name(row.get("district", "")) != wanted_district:
            continue
        if normalize_name(row.get("tehsil", "")) != wanted_tehsil:
            continue

        if row.get("status") != "written":
            raise ValueError(f"Watershed boundary is not available for {state}/{district}/{tehsil}: {row}")

        path = manifest_relative_output(root, row.get("output_path", ""))
        if path is None:
            path = fallback_gpkg_path(root, state, district, tehsil)
        if not path.exists():
            raise FileNotFoundError(f"Manifest matched, but watershed file does not exist: {path}")
        return path, row

    path = fallback_gpkg_path(root, state, district, tehsil)
    if path.exists():
        return path, {"state": state, "district": district, "tehsil": tehsil, "status": "written"}

    raise FileNotFoundError(
        "Could not find tehsil watershed for "
        f"state={state!r}, district={district!r}, tehsil={tehsil!r} under {root}"
    )


def find_district_watersheds(root: Path, state: str, district: str) -> list[tuple[Path, dict]]:
    wanted_state = normalize_name(state)
    wanted_district = normalize_name(district)
    matches = []

    for row in load_manifest(root):
        if normalize_name(row.get("state", "")) != wanted_state:
            continue
        if normalize_name(row.get("district", "")) != wanted_district:
            continue
        if row.get("status") != "written":
            continue

        path = manifest_relative_output(root, row.get("output_path", ""))
        if path is None:
            path = fallback_gpkg_path(root, row.get("state", state), row.get("district", district), row.get("tehsil", ""))
        if path.exists():
            matches.append((path, row))

    if not matches:
        raise FileNotFoundError(
            f"Could not find written district watersheds for state={state!r}, district={district!r} under {root}"
        )

    return matches


def find_state_watersheds(root: Path, state: str) -> list[tuple[Path, dict]]:
    wanted_state = normalize_name(state)
    matches = []

    for row in load_manifest(root):
        if normalize_name(row.get("state", "")) != wanted_state:
            continue
        if row.get("status") != "written":
            continue

        path = manifest_relative_output(root, row.get("output_path", ""))
        if path is None:
            path = fallback_gpkg_path(
                root,
                row.get("state", state),
                row.get("district", ""),
                row.get("tehsil", ""),
            )
        if path.exists():
            matches.append((path, row))

    if not matches:
        raise FileNotFoundError(f"Could not find written state watersheds for state={state!r} under {root}")

    return matches


def find_pan_india_watersheds(root: Path) -> list[tuple[Path, dict]]:
    matches = []

    for row in load_manifest(root):
        if row.get("status") != "written":
            continue

        path = manifest_relative_output(root, row.get("output_path", ""))
        if path is None:
            path = fallback_gpkg_path(
                root,
                row.get("state", ""),
                row.get("district", ""),
                row.get("tehsil", ""),
            )
        if path.exists():
            matches.append((path, row))

    if not matches:
        raise FileNotFoundError(f"Could not find written pan-India watersheds under {root}")

    return matches


def prepare_boundary_gdf(gdf, state: str, district: str, tehsil: str | None, source_path: Path):
    if gdf.empty:
        raise ValueError(f"Watershed file has no features: {source_path}")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if gdf.empty:
        raise ValueError(f"Watershed file has no valid geometries: {source_path}")

    if "id" in gdf.columns:
        gdf = gdf.rename(columns={"id": "source_id"})
    if "uid" in gdf.columns and "watershed_uid" not in gdf.columns:
        gdf["watershed_uid"] = gdf["uid"].astype(str)

    gdf["selected_state"] = state
    gdf["selected_district"] = district
    if tehsil is not None:
        gdf["selected_tehsil"] = tehsil
    elif "TEHSIL" in gdf.columns:
        gdf["selected_tehsil"] = gdf["TEHSIL"].astype(str)
    gdf["source_gpkg"] = str(source_path)
    return gdf


def polygon_parts(geometry):
    if isinstance(geometry, Polygon):
        return [geometry]
    if isinstance(geometry, MultiPolygon):
        return list(geometry.geoms)
    if isinstance(geometry, GeometryCollection):
        parts = []
        for part in geometry.geoms:
            parts.extend(polygon_parts(part))
        return parts
    return []


def strip_inner_rings(geometry):
    polygons = []
    for polygon in polygon_parts(geometry):
        if not polygon.is_empty:
            polygons.append(Polygon(polygon.exterior))

    if not polygons:
        raise ValueError("Could not build an outer boundary from the watershed geometries")
    if len(polygons) == 1:
        return polygons[0]
    return MultiPolygon(polygons)


def write_download_boundary(gdf, destination: str | Path, state: str, district: str | None = None) -> Path:
    destination = Path(destination)
    union_geometry = gdf.geometry.unary_union
    outer_geometry = strip_inner_rings(union_geometry)
    properties = {
        "id": 1,
        "selected_state": state,
        "boundary_role": "download_outer_boundary",
    }
    if district is not None:
        properties["selected_district"] = district

    outer_gdf = gpd.GeoDataFrame(
        [properties],
        geometry=[outer_geometry],
        crs=gdf.crs,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(outer_gdf.to_json())
    return destination


def materialize_tehsil_boundary(
    state: str,
    district: str,
    tehsil: str,
    watershed_root: str | Path = DEFAULT_WATERSHED_ROOT,
    output_path: str | Path | None = None,
    overwrite: bool = True,
) -> tuple[Path, Path, int]:
    root = Path(watershed_root)
    source_path, _ = find_tehsil_watershed(root, state, district, tehsil)
    destination = Path(output_path) if output_path else default_output_path(state, district, tehsil)

    if destination.exists() and not overwrite:
        gdf_existing = gpd.read_file(destination)
        return destination, source_path, len(gdf_existing)

    gdf = prepare_boundary_gdf(gpd.read_file(source_path), state, district, tehsil, source_path)
    gdf["id"] = range(1, len(gdf) + 1)

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(gdf.to_json())

    return destination, source_path, len(gdf)


def materialize_district_boundary(
    state: str,
    district: str,
    watershed_root: str | Path = DEFAULT_WATERSHED_ROOT,
    output_path: str | Path | None = None,
    overwrite: bool = True,
) -> tuple[Path, list[Path], int]:
    root = Path(watershed_root)
    matches = find_district_watersheds(root, state, district)
    destination = Path(output_path) if output_path else default_district_output_path(state, district)

    if destination.exists() and not overwrite:
        gdf_existing = gpd.read_file(destination)
        return destination, [path for path, _ in matches], len(gdf_existing)

    frames = []
    source_paths = []
    for source_path, row in matches:
        gdf = prepare_boundary_gdf(
            gpd.read_file(source_path),
            state=state,
            district=district,
            tehsil=row.get("tehsil") or None,
            source_path=source_path,
        )
        frames.append(gdf)
        source_paths.append(source_path)

    if not frames:
        raise ValueError(f"No non-empty watershed files found for {state}/{district}")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    combined["id"] = range(1, len(combined) + 1)

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(combined.to_json())
    write_download_boundary(combined, download_boundary_path(destination), state, district)

    return destination, source_paths, len(combined)


def materialize_state_boundary(
    state: str,
    watershed_root: str | Path = DEFAULT_WATERSHED_ROOT,
    output_path: str | Path | None = None,
    overwrite: bool = True,
) -> tuple[Path, list[Path], int]:
    root = Path(watershed_root)
    matches = find_state_watersheds(root, state)
    destination = Path(output_path) if output_path else default_state_output_path(state)

    if destination.exists() and not overwrite:
        gdf_existing = gpd.read_file(destination)
        return destination, [path for path, _ in matches], len(gdf_existing)

    frames = []
    source_paths = []
    for source_path, row in matches:
        district = row.get("district") or source_path.parent.name
        tehsil = row.get("tehsil") or None
        gdf = prepare_boundary_gdf(
            gpd.read_file(source_path),
            state=state,
            district=district,
            tehsil=tehsil,
            source_path=source_path,
        )
        frames.append(gdf)
        source_paths.append(source_path)

    if not frames:
        raise ValueError(f"No non-empty watershed files found for {state}")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    combined["id"] = range(1, len(combined) + 1)

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(combined.to_json())
    write_download_boundary(combined, download_boundary_path(destination), state)

    return destination, source_paths, len(combined)


def materialize_pan_india_boundary(
    watershed_root: str | Path = DEFAULT_WATERSHED_ROOT,
    output_path: str | Path | None = None,
    overwrite: bool = True,
    download_boundary_source: str | Path = DEFAULT_PAN_INDIA_DOWNLOAD_BOUNDARY,
) -> tuple[Path, list[Path], int]:
    root = Path(watershed_root)
    matches = find_pan_india_watersheds(root)
    destination = Path(output_path) if output_path else default_pan_india_output_path()

    if destination.exists() and not overwrite:
        download_destination = download_boundary_path(destination)
        download_boundary_source = Path(download_boundary_source)
        if not download_destination.exists() and download_boundary_source.exists():
            download_destination.parent.mkdir(parents=True, exist_ok=True)
            download_destination.write_text(download_boundary_source.read_text())
        gdf_existing = gpd.read_file(destination)
        return destination, [path for path, _ in matches], len(gdf_existing)

    frames = []
    source_paths = []
    for source_path, row in matches:
        state = row.get("state") or source_path.parent.parent.name
        district = row.get("district") or source_path.parent.name
        tehsil = row.get("tehsil") or None
        gdf = prepare_boundary_gdf(
            gpd.read_file(source_path),
            state=state,
            district=district,
            tehsil=tehsil,
            source_path=source_path,
        )
        frames.append(gdf)
        source_paths.append(source_path)

    if not frames:
        raise ValueError("No non-empty watershed files found for pan-India")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    combined["id"] = range(1, len(combined) + 1)

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(combined.to_json())

    download_destination = download_boundary_path(destination)
    download_boundary_source = Path(download_boundary_source)
    if download_boundary_source.exists():
        download_destination.write_text(download_boundary_source.read_text())
    else:
        write_download_boundary(combined, download_destination, PAN_INDIA_SLUG)

    return destination, source_paths, len(combined)
