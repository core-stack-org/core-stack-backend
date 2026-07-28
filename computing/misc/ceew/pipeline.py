"""Read-only helpers for exposing generated CEEW/CRAVIS district profiles.

The heavy CEEW/CRAVIS processing pipeline writes district profile artifacts
under data/ceew_cravis/output/district_profiles. This module intentionally does
not rebuild those artifacts; it provides a lean backend-facing access layer.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import unicodedata
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CEEW_ROOT = PROJECT_ROOT / "data" / "ceew_cravis"
CEEW_OUTPUT_DIR = CEEW_ROOT / "output"
CEEW_DISTRICT_PROFILE_ROOT = CEEW_OUTPUT_DIR / "district_profiles"
CEEW_GRID_BBOX_PROFILE_ROOT = CEEW_OUTPUT_DIR / "grid_bbox_profiles"
CEEW_GRID_POINT_PROFILE_ROOT = CEEW_OUTPUT_DIR / "grid_point_profiles"
CEEW_GLOBAL_METADATA_CONFIG = CEEW_OUTPUT_DIR / "ceew_cravis_metadata_config.json"
CEEW_DISTRICT_MAP_INDEX = CEEW_OUTPUT_DIR / "map_layers" / "district_map_index.json"


class CEEWProfileNotFound(FileNotFoundError):
    """Raised when a requested CEEW district profile artifact is unavailable."""


def slugify_location(value: str) -> str:
    """Return the file-system slug used by CEEW district profile outputs."""

    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_value = ascii_value.lower().replace("&", " ")
    ascii_value = re.sub(r"[^a-z0-9]+", "_", ascii_value)
    return re.sub(r"_+", "_", ascii_value).strip("_")


def _relative(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def ceew_district_paths(state: str, district: str) -> dict[str, Path]:
    """Return expected CEEW output paths for a state/district pair."""

    state_slug = slugify_location(state)
    district_slug = slugify_location(district)
    prefix = f"{state_slug}_{district_slug}"
    folder = CEEW_DISTRICT_PROFILE_ROOT / state_slug / district_slug
    return {
        "folder": folder,
        "profile": folder / f"{prefix}_profile.json",
        "metadata_config": folder / f"{prefix}_metadata_config.json",
        "boundary_gpkg": folder / f"{prefix}_boundary.gpkg",
    }


def _slug_coordinate(value: float | str) -> str:
    text = str(value).strip().replace("-", "minus_").replace(".", "_")
    return re.sub(r"[^0-9a-zA-Z_]+", "_", text).strip("_")


def _bbox_slug(bbox: list[float] | tuple[float, float, float, float] | str) -> str:
    if isinstance(bbox, str):
        parts = [part.strip() for part in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError("bbox must contain min_x,min_y,max_x,max_y")
        values = [float(part) for part in parts]
    else:
        values = [float(value) for value in bbox]
        if len(values) != 4:
            raise ValueError("bbox must contain four values")
    return "_".join(_slug_coordinate(value) for value in values)


def ceew_grid_bbox_paths(bbox: list[float] | tuple[float, float, float, float] | str) -> dict[str, Path]:
    """Return expected generated grid-bbox profile paths."""

    slug = _bbox_slug(bbox)
    folder = CEEW_GRID_BBOX_PROFILE_ROOT / slug
    prefix = f"grid_bbox_{slug}"
    return {
        "folder": folder,
        "profile": folder / f"{prefix}_profile.json",
        "metadata_config": folder / f"{prefix}_metadata_config.json",
        "grid_cells_gpkg": folder / f"{prefix}_grid_cells.gpkg",
    }


def ceew_grid_point_paths(latitude: float | str, longitude: float | str) -> dict[str, Path]:
    """Return generated grid-point profile paths for a point query."""

    lat_slug = _slug_coordinate(latitude)
    lon_slug = _slug_coordinate(longitude)
    folder = CEEW_GRID_POINT_PROFILE_ROOT / lat_slug / lon_slug
    profile_paths = sorted(folder.glob("*_profile.json"))
    profile_path = profile_paths[0] if profile_paths else folder / "profile.json"
    prefix = profile_path.name.removesuffix("_profile.json") if profile_path.name.endswith("_profile.json") else profile_path.stem
    return {
        "folder": folder,
        "profile": profile_path,
        "metadata_config": folder / f"{prefix}_metadata_config.json",
        "grid_cells_gpkg": folder / f"{prefix}_grid_cells.gpkg",
    }


def _compact_lookup_key(slug_value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", slug_value)


def _candidate_zonal_ids(state: str, district: str) -> list[str]:
    state_slug = slugify_location(state)
    district_slug = slugify_location(district)
    base = f"{district_slug}_{state_slug}"
    candidates = [base]
    without_and = base.replace("_and_", "_")
    if without_and not in candidates:
        candidates.append(without_and)
    return candidates


def resolve_ceew_district_paths(state: str, district: str) -> dict[str, Path]:
    """Return existing district paths, using path-slug fallbacks if needed."""

    paths = ceew_district_paths(state, district)
    if paths["profile"].exists():
        return paths

    lookup = _district_artifact_lookup_from_paths()
    for zonal_id in _candidate_zonal_ids(state, district):
        fallback = lookup.get(zonal_id) or lookup.get(f"compact:{_compact_lookup_key(zonal_id)}")
        if fallback and fallback["profile"].exists():
            return fallback
    return paths


def ceew_file_info(paths: dict[str, Path]) -> dict[str, Any]:
    """Return relative paths, existence, and sizes for district artifacts."""

    info: dict[str, Any] = {}
    for key, path in paths.items():
        exists = path.exists()
        info[key] = {
            "path": _relative(path),
            "exists": exists,
            "bytes": path.stat().st_size if exists and path.is_file() else None,
        }
    return info


def _count_records(collection: Any) -> int:
    if isinstance(collection, list):
        return len(collection)
    if isinstance(collection, dict):
        return sum(_count_records(value) for value in collection.values())
    return 0


def summarize_ceew_profile(
    profile: dict[str, Any],
    *,
    file_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact quality and content summary for a loaded profile."""

    climate = profile.get("climate") if isinstance(profile.get("climate"), dict) else {}
    risk = profile.get("risk") if isinstance(profile.get("risk"), dict) else {}
    sectoral = profile.get("sectoral") if isinstance(profile.get("sectoral"), dict) else {}
    zonal = sectoral.get("zonal_observations") if isinstance(sectoral.get("zonal_observations"), dict) else {}
    point = sectoral.get("point_features") if isinstance(sectoral.get("point_features"), dict) else {}
    risk_keys = sorted(risk.keys())

    has_climate = bool(climate)
    has_risk = bool(risk)
    data_status = "full" if has_climate and has_risk else "sparse" if profile else "missing"

    return {
        "data_status": data_status,
        "has_climate": has_climate,
        "has_risk": has_risk,
        "climate_frameworks": sorted(climate.keys()),
        "risk_index_count": len(risk_keys),
        "risk_indices": risk_keys,
        "sectoral_zonal_layer_count": len(zonal),
        "sectoral_zonal_record_count": _count_records(zonal),
        "sectoral_point_layer_count": len(point),
        "sectoral_point_feature_count": _count_records(point),
        "file_info": file_info or {},
    }


def get_ceew_district_profile(state: str, district: str) -> dict[str, Any]:
    """Load the generated primary CEEW district profile JSON."""

    paths = resolve_ceew_district_paths(state, district)
    profile_path = paths["profile"]
    if not profile_path.exists():
        raise CEEWProfileNotFound(f"CEEW profile not found: {_relative(profile_path)}")
    return _load_json(profile_path)


def get_ceew_district_metadata_config(state: str, district: str) -> dict[str, Any]:
    """Load the generated district metadata/config JSON."""

    paths = resolve_ceew_district_paths(state, district)
    metadata_path = paths["metadata_config"]
    if not metadata_path.exists():
        raise CEEWProfileNotFound(f"CEEW metadata config not found: {_relative(metadata_path)}")
    return _load_json(metadata_path)


def _load_profile_bundle(
    paths: dict[str, Path],
    *,
    include_metadata: bool = True,
    include_summary: bool = True,
    include_file_info: bool = True,
) -> dict[str, Any]:
    profile_path = paths["profile"]
    if not profile_path.exists():
        raise CEEWProfileNotFound(f"CEEW profile not found: {_relative(profile_path)}")
    file_info = ceew_file_info(paths) if include_file_info else {}
    profile = _load_json(profile_path)

    bundle: dict[str, Any] = {"profile": profile}
    metadata_path = paths.get("metadata_config")
    if include_metadata and metadata_path and metadata_path.exists():
        bundle["metadata_config"] = _load_json(metadata_path)
    if include_summary:
        if profile.get("profile_kind") in {"grid", "grid_collection"}:
            grids = profile.get("grids") if isinstance(profile.get("grids"), dict) else {}
            bundle["summary"] = {
                "data_status": "full" if grids else "sparse",
                "profile_kind": profile.get("profile_kind"),
                "grid_count": len(grids),
                "file_info": file_info,
            }
        else:
            bundle["summary"] = summarize_ceew_profile(profile, file_info=file_info)
    return bundle


def get_ceew_data(
    state: str | None = None,
    district: str | None = None,
    *,
    latitude: float | str | None = None,
    longitude: float | str | None = None,
    bbox: list[float] | tuple[float, float, float, float] | str | None = None,
    include_metadata: bool = True,
    include_summary: bool = True,
    include_file_info: bool = True,
) -> dict[str, Any]:
    """Load generated CEEW data for a district, grid point, or grid bbox."""

    if state and district:
        return _load_profile_bundle(
            resolve_ceew_district_paths(state, district),
            include_metadata=include_metadata,
            include_summary=include_summary,
            include_file_info=include_file_info,
        )
    if bbox is not None:
        return _load_profile_bundle(
            ceew_grid_bbox_paths(bbox),
            include_metadata=include_metadata,
            include_summary=include_summary,
            include_file_info=include_file_info,
        )
    if latitude is not None and longitude is not None:
        return _load_profile_bundle(
            ceew_grid_point_paths(latitude, longitude),
            include_metadata=include_metadata,
            include_summary=include_summary,
            include_file_info=include_file_info,
        )
    raise ValueError("Provide either state+district, latitude+longitude, or bbox.")


def iter_ceew_district_profile_paths(state: str | None = None) -> list[Path]:
    """List generated district profile paths, optionally filtered by state."""

    if state:
        root = CEEW_DISTRICT_PROFILE_ROOT / slugify_location(state)
        return sorted(root.glob("*/*_profile.json"))
    return sorted(CEEW_DISTRICT_PROFILE_ROOT.glob("*/*/*_profile.json"))


def _district_artifact_lookup_from_paths() -> dict[str, dict[str, Path]]:
    """Build a lightweight zonal_id lookup from profile path slugs."""

    lookup: dict[str, dict[str, Path]] = {}
    for profile_path in iter_ceew_district_profile_paths():
        district_slug = profile_path.parent.name
        state_slug = profile_path.parent.parent.name
        prefix = f"{state_slug}_{district_slug}"
        zonal_id = f"{district_slug}_{state_slug}"
        folder = profile_path.parent
        lookup[zonal_id] = {
            "folder": folder,
            "profile": profile_path,
            "metadata_config": folder / f"{prefix}_metadata_config.json",
            "boundary_gpkg": folder / f"{prefix}_boundary.gpkg",
        }
        lookup[f"compact:{_compact_lookup_key(zonal_id)}"] = lookup[zonal_id]
    return lookup


def list_ceew_district_profiles(
    *,
    state: str | None = None,
    include_sparse: bool = True,
) -> list[dict[str, Any]]:
    """Return a lightweight inventory of generated district profile files."""

    rows: list[dict[str, Any]] = []
    for profile_path in iter_ceew_district_profile_paths(state):
        try:
            profile = _load_json(profile_path)
        except json.JSONDecodeError:
            continue
        location = profile.get("location") if isinstance(profile.get("location"), dict) else {}
        state_slug = profile_path.parent.parent.name
        district_slug = profile_path.parent.name
        prefix = f"{state_slug}_{district_slug}"
        paths = {
            "folder": profile_path.parent,
            "profile": profile_path,
            "metadata_config": profile_path.parent / f"{prefix}_metadata_config.json",
            "boundary_gpkg": profile_path.parent / f"{prefix}_boundary.gpkg",
        }
        summary = summarize_ceew_profile(profile, file_info=ceew_file_info(paths))
        if not include_sparse and summary["data_status"] != "full":
            continue
        rows.append(
            {
                "state_name": location.get("state_name"),
                "district_name": location.get("district_name"),
                "zonal_id": location.get("zonal_id"),
                "profile_path": _relative(profile_path),
                "metadata_config_path": _relative(paths["metadata_config"]) if paths["metadata_config"].exists() else None,
                "boundary_gpkg_path": _relative(paths["boundary_gpkg"]) if paths["boundary_gpkg"].exists() else None,
                "summary": {key: value for key, value in summary.items() if key != "file_info"},
            }
        )
    return rows


def get_ceew_global_metadata_config() -> dict[str, Any]:
    """Load global labels, legends, and source metadata for CEEW display."""

    if not CEEW_GLOBAL_METADATA_CONFIG.exists():
        raise CEEWProfileNotFound(f"CEEW global metadata config not found: {_relative(CEEW_GLOBAL_METADATA_CONFIG)}")
    return _load_json(CEEW_GLOBAL_METADATA_CONFIG)


def get_ceew_district_map_index(*, resolve_file_links: bool = True) -> dict[str, Any]:
    """Load the pan-India district map index.

    When resolve_file_links is true, stale/missing file path fields are corrected
    in memory by checking generated district profile folders.
    """

    if not CEEW_DISTRICT_MAP_INDEX.exists():
        raise CEEWProfileNotFound(f"CEEW district map index not found: {_relative(CEEW_DISTRICT_MAP_INDEX)}")
    index = _load_json(CEEW_DISTRICT_MAP_INDEX)
    districts = index.get("districts")
    if not resolve_file_links or not isinstance(districts, list):
        return index

    path_lookup = _district_artifact_lookup_from_paths()
    profile_count = 0
    metadata_count = 0
    boundary_gpkg_count = 0
    for district in districts:
        if not isinstance(district, dict):
            continue
        state_name = district.get("state_name")
        district_name = district.get("district_name")
        if not state_name or not district_name:
            continue
        paths = ceew_district_paths(state_name, district_name)
        if not paths["profile"].exists():
            fallback_paths = None
            raw_zonal_id = str(district.get("zonal_id") or "")
            for candidate in [slugify_location(raw_zonal_id), *_candidate_zonal_ids(state_name, district_name)]:
                fallback_paths = path_lookup.get(candidate) or path_lookup.get(
                    f"compact:{_compact_lookup_key(candidate)}"
                )
                if fallback_paths:
                    break
            if fallback_paths:
                paths = fallback_paths
        profile_path = paths["profile"] if paths["profile"].exists() else None
        metadata_path = paths["metadata_config"] if paths["metadata_config"].exists() else None
        boundary_path = paths["boundary_gpkg"] if paths["boundary_gpkg"].exists() else None
        district["profile_path"] = _relative(profile_path)
        district["metadata_config_path"] = _relative(metadata_path)
        district["district_boundary_gpkg"] = _relative(boundary_path)
        profile_count += 1 if profile_path else 0
        metadata_count += 1 if metadata_path else 0
        boundary_gpkg_count += 1 if boundary_path else 0

    index["profile_count"] = profile_count
    index["metadata_config_count"] = metadata_count
    index["boundary_gpkg_count"] = boundary_gpkg_count
    index["file_links_resolved_in_memory"] = True
    return index


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expose generated CEEW/CRAVIS district profile artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    district_parser = subparsers.add_parser("district", help="Load one district profile bundle.")
    district_parser.add_argument("--state", required=True, help="State name, for example Bihar.")
    district_parser.add_argument("--district", required=True, help="District name, for example Jamui.")
    district_parser.add_argument("--include-metadata", action="store_true", help="Include metadata/config in output.")
    district_parser.add_argument("--summary-only", action="store_true", help="Print only compact summary and file info.")

    list_parser = subparsers.add_parser("list", help="List generated district profiles.")
    list_parser.add_argument("--state", help="Optional state filter.")
    list_parser.add_argument("--full-only", action="store_true", help="Skip sparse profiles.")

    map_parser = subparsers.add_parser("map-index", help="Load pan-India district map index.")
    map_parser.add_argument("--raw", action="store_true", help="Do not resolve stale file links in memory.")

    args = parser.parse_args()
    if args.command == "district":
        bundle = get_ceew_data(
            args.state,
            args.district,
            include_metadata=args.include_metadata,
            include_summary=True,
            include_file_info=True,
        )
        _print_json(bundle["summary"] if args.summary_only else bundle)
    elif args.command == "list":
        _print_json(list_ceew_district_profiles(state=args.state, include_sparse=not args.full_only))
    elif args.command == "map-index":
        _print_json(get_ceew_district_map_index(resolve_file_links=not args.raw))


if __name__ == "__main__":
    main()
