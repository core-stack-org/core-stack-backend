"""
Single source of truth for where per-block ("tehsil") local-compute layers live.

Target layout (``NEW``)::

    {DATA_DIR}/layers_output/{state}/{district}/{block}/{layer_type}/{filename}

Legacy layout (``LEGACY``) is whatever each local-compute module wrote to before
the reorganisation, e.g. ``{DATA_DIR}/layers/aquifer_vector/{state}/{district}/{block}/...``
or ``{DATA_DIR}/lulc/lulc_vector_local/{state}/{district}/{block}/...``.

Every layer is described once by a :class:`LayerSpec`. ``layer_type`` is the entry
name in ``layers/layer_dependency/local/local_layer_map.json`` and is also the
folder name in the new layout (so layers sharing a map entry share a folder).

This module is deliberately free of Django / geopandas imports so that it can be
used from scripts (coverage report, migration) as well as from the app.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_dir(data_dir: Optional[os.PathLike] = None) -> Path:
    if data_dir:
        return Path(data_dir).expanduser().resolve()
    return Path(os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data"))).expanduser().resolve()


# New root of the organised tree, relative to DATA_DIR.
NEW_ROOT = "layers_output"

# State directories that were written with a corrupted slug in the past.
# legacy dir name -> canonical state slug
STATE_ALIASES = {":_west_bengal": "west_bengal"}

# Placeholder -> regex, used to turn a filename template into a matcher.
_PLACEHOLDERS = {
    "year": r"\d{4}",
    "param": r"[A-Za-z]+",
    "band": r"[A-Za-z_]+?",
}


@dataclass(frozen=True)
class Part:
    """One kind of file inside a layer folder (a layer may have a raster and a vector part)."""

    kind: str  # "vector" | "raster"
    templates: Tuple[str, ...]  # filename templates ({district}, {block}, {year}, {param}, {band})
    expected_min: int = 1  # files needed for this part to count as present


@dataclass(frozen=True)
class LayerSpec:
    layer_type: str  # == entry name in local_layer_map.json == folder name in the new layout
    legacy_dir: Optional[str]  # relative to DATA_DIR; {state}/{district}/{block} follow. None = no local output yet
    parts: Tuple[Part, ...]
    note: str = ""

    @property
    def local_computable(self) -> bool:
        return self.legacy_dir is not None


def _L(layer_type: str, legacy_dir: Optional[str], *parts: Part, note: str = "") -> LayerSpec:
    return LayerSpec(layer_type, legacy_dir, tuple(parts), note)


def _V(*templates: str, n: int = 1) -> Part:
    return Part("vector", tuple(templates), n)


def _R(*templates: str, n: int = 1) -> Part:
    return Part("raster", tuple(templates), n)


# --------------------------------------------------------------------------
# Registry. ``layer_type`` is exactly the name used in local_layer_map.json.
# Order follows the map (dynamic, static, gee) so it is easy to diff.
# --------------------------------------------------------------------------
LAYERS: Tuple[LayerSpec, ...] = (
    # ---- dynamic layers -------------------------------------------------
    _L("generate_nrega_layer", "layers/nrega_assets",
       _V("{district}_{block}.gpkg", "nrega_{district}_{block}.gpkg")),
    _L("lulc_v3", "lulc/lulc_v3_local",
       _R("{district}_{block}_{year}-07-01_{year}-06-30_LULCmap_10m.tif")),
    _L("lulc_vector", "lulc/lulc_vector_local", _V("lulc_vector_{district}_{block}.gpkg")),
    _L("change_detection", "change_detection/change_detection_local",
       _R("change_{district}_{block}_{param}_{year}_{year}.tif", n=6)),
    _L("change_detection_vector", "change_detection/change_detection_vector_local",
       _V("change_vector_{district}_{block}_{param}_{year}_{year}.gpkg", n=6)),
    _L("generate_terrain_raster", "terrain/fabdem_local", _R("{district}_{block}_terrain_raster.tif")),
    _L("generate_terrain_descriptor", "terrain/terrain_clusters_local", _V("{district}_{block}_cluster.gpkg")),
    _L("terrain_lulc_plain_cluster", "lulc_X_terrain/lulc_plain_clusters_local",
       _V("{district}_{block}_lulc_plain.gpkg")),
    _L("terrain_lulc_slope_cluster", "lulc_X_terrain/lulc_slope_clusters_local",
       _V("{district}_{block}_lulc_slope.gpkg")),
    _L("generate_ci_layer", "cropping_intensity/cropping_intensity_local",
       _V("{district}_{block}_intensity.gpkg")),
    _L("generate_swb", "surface_water_bodies", _V("surface_waterbodies_{district}_{block}.gpkg"),
       _V("surface_waterbodies_{district}_{block}_local.gpkg", n=0)),
    _L("generate_zoi", "zoi", _V("waterbodies_zoi_{district}_{block}.gpkg")),
    _L("tree_health_ch_raster", "tree_health", _R("ch_raster_{district}_{block}_{year}.tif")),
    _L("tree_health_ch_vector", "tree_health", _V("ch_vector_{district}_{block}_{year}_{year}.gpkg")),
    _L("tree_health_ccd_raster", "tree_health", _R("ccd_raster_{district}_{block}_{year}.tif")),
    _L("tree_health_ccd_vector", "tree_health", _V("ccd_vector_{district}_{block}_{year}_{year}.gpkg")),
    _L("tree_health_overall_change_raster", "tree_health",
       _R("overall_change_raster_{district}_{block}.tif")),
    _L("tree_health_overall_change_vector", "tree_health",
       _V("overall_change_vector_{district}_{block}.gpkg")),
    _L("soil_health", "soil_health",
       _R("{district}_{block}_soil_health_raster_{band}.tif", n=5),
       _V("{district}_{block}_soil_health_vector.gpkg"),
       # per-nutrient vectors written by an older run; optional
       _V("{district}_{block}_soil_health_vector_{band}.gpkg", n=0)),
    _L("hydrology_fortnightly", "hydrology/hydrology_local", _V("deltaG_fortnight_{district}_{block}.gpkg")),
    _L("hydrology_annual", "hydrology/hydrology_local", _V("deltaG_well_depth_{district}_{block}.gpkg")),
    _L("site_suitability", "layers/site_suitability", _R("{district}_{block}_site_suitability_raster.tif")),
    # ---- static layers --------------------------------------------------
    _L("aquifer_vector", "layers/aquifer_vector", _V("aquifer_vector_{district}_{block}.gpkg")),
    _L("generate_livestocks", "livestock/output/livestock_local", _V("livestocks_{district}_{block}.gpkg")),
    _L("generate_antyodaya", "antyodaya/output/antyodaya_local", _V("antyodaya20_{district}_{block}.gpkg")),
    _L("generate_density_vector", "drainage_density", _V("{district}_{block}_drainage_density.gpkg")),
    _L("generate_river_data", "river/river_local", _V("{district}_{block}_river_vector.gpkg")),
    _L("generate_canal_vector", "canal/canal_local", _V("{district}_{block}_canal_vector.gpkg")),
    _L("generate_dem_raster_vector", "fabdem/fabdem_local",
       _R("{district}_{block}_dem_raster.tif"), _V("{district}_{block}_dem_vector.gpkg")),
    _L("generate_facilities_proximity", "layers/facilities", _V("facilities_{district}_{block}.gpkg")),
    _L("generate_drainage_layer", "layers/drainage_lines/drainage_lines_local", _V("{district}_{block}.gpkg")),
    _L("restoration_opportunity", "layers/restoration_opportunity",
       _R("restoration_{district}_{block}_raster.tif"), _V("restoration_{district}_{block}_vector.gpkg")),
    _L("soge_vector", "layers/SOGE_vector",
       _V("soge_vector_{district}_{block}.gpkg", "soge_vector_{district}_{block}_{year}.gpkg")),
    _L("generate_lcw", "layers/lcw_conflict", _V("{district}_{block}_lcw_conflict.gpkg")),
    _L("generate_agroecological", "layers/agroecological", _V("{district}_{block}_agroecological.gpkg")),
    _L("generate_factory_csr", "layers/factory_csr", _V("{district}_{block}_factory_csr.gpkg")),
    _L("generate_green_credit", "layers/green_credit", _V("{district}_{block}_green_credit.gpkg")),
    _L("generate_mining", "layers/mining", _V("{district}_{block}_mining.gpkg")),
    _L("generate_natural_depression", "layers/natural_depression",
       _R("natural_depression_{district}_{block}_raster.tif")),
    _L("generate_distance_nearest_DL", "layers/distance_nearest_upstream_DL",
       _R("distance_to_drainage_line_{district}_{block}_raster.tif")),
    _L("generate_catchment_area_singleflow", "layers/catchment_area_singleflow",
       _R("catchment_area_{district}_{block}_raster.tif")),
    _L("generate_slope_percentage", "layers/slope_percentage",
       _R("{district}_{block}_slope_percentage_raster.tif", "slope_percentage_{district}_{block}.tif")),
    _L("generate_mws_connectivity_data", "layers/mws_connectivity/mws_connectivity_local",
       _V("{district}_{block}_mws_connectivity.gpkg")),
    _L("generate_mws_centroid", "layers/mws_centroid", _V("{district}_{block}_mws_centroid.gpkg")),
    _L("soil_type", "layers/soil_type", _V("soil_type_{district}_{block}.gpkg")),
    _L("forest_fringe", "forest_fringe/output", _V("{district}_{block}_forest_fringe.gpkg")),
    _L("tree_in_grassland", "tree_in_grassland/output", _V("{district}_{block}_tree_in_grassland.gpkg")),
    # Not in local_layer_map.json, but written by local compute next to forest_fringe.
    _L("forest_fire", "forest_fringe/output", _V("{district}_{block}_forest_fire.gpkg"),
       note="not in local_layer_map.json"),
    # ---- gee layers: no local output tree yet ---------------------------
    _L("ndvi_timeseries", None, _V("ndvi_timeseries_{district}_{block}_{band}.gpkg", n=3),
       note="only fc_to_shape samples exist"),
    _L("calculate_drought", None, _V("{district}_{block}_drought.gpkg"), note="only fc_to_shape samples exist"),
    _L("drought_causality", None, _V("{district}_{block}_drought_causality.gpkg"),
       note="only fc_to_shape samples exist"),
    _L("generate_stream_order", None, _R("stream_order_{district}_{block}_raster.tif")),
    _L("generate_clart_layer", None, _R("{district}_{block}_clart.tif")),
)

LAYER_BY_TYPE: Dict[str, LayerSpec] = {s.layer_type: s for s in LAYERS}
assert len(LAYER_BY_TYPE) == len(LAYERS), "duplicate layer_type in registry"


def map_layer_names(data_dir=None) -> List[str]:
    """All layer names in local_layer_map.json (dynamic, static, gee; children included)."""
    import json

    path = get_data_dir(data_dir) / "layers" / "layer_dependency" / "local" / "local_layer_map.json"
    with open(path) as fh:
        data = json.load(fh)
    names: List[str] = []

    def walk(items):
        for item in items:
            names.append(item["name"])
            walk(item.get("children", []))

    for items in data.values():
        walk(items)
    return names


def validate_registry(data_dir=None) -> Dict[str, List[str]]:
    """Compare the registry with local_layer_map.json. Both lists should be empty
    (``forest_fire`` is the one known extra)."""
    names = set(map_layer_names(data_dir))
    return {
        "in_map_not_in_registry": sorted(names - set(LAYER_BY_TYPE)),
        "in_registry_not_in_map": sorted(set(LAYER_BY_TYPE) - names),
    }


# --------------------------------------------------------------------------
# Name handling
# --------------------------------------------------------------------------
def slug(value: str) -> str:
    """Same normalisation as the local-compute helpers (lower-case, ``_`` for spaces)."""
    return re.sub(r"\s+", "_", str(value).strip().lower())


def _flex(name: str) -> str:
    """Regex for a district/block name; ``-`` and ``_`` are interchangeable on disk."""
    return re.sub(r"[-_]", "[-_]", re.escape(name).replace(r"\-", "-"))


def compile_template(template: str, district: str, block: str) -> "re.Pattern[str]":
    pattern = re.escape(template)
    # Some blocks were written with a doubled separator ("bid__parli-vai"), so the
    # underscore joining district and block may repeat.
    pattern = pattern.replace(r"\{district\}_\{block\}", _flex(district) + "_+" + _flex(block))
    pattern = pattern.replace(r"\{district\}", _flex(district)).replace(r"\{block\}", _flex(block))
    for key, rx in _PLACEHOLDERS.items():
        pattern = pattern.replace(r"\{%s\}" % key, rx)
    return re.compile("^" + pattern + "$")


def matching_files(part: Part, filenames: Iterable[str], district: str, block: str) -> List[str]:
    regexes = [compile_template(t, district, block) for t in part.templates]
    return sorted(f for f in filenames if any(r.match(f) for r in regexes))


# --------------------------------------------------------------------------
# Path builders
# --------------------------------------------------------------------------
def block_dir(state: str, district: str, block: str, data_dir=None) -> Path:
    return get_data_dir(data_dir) / NEW_ROOT / slug(state) / slug(district) / slug(block)


def layer_dir(layer_type: str, state: str, district: str, block: str, data_dir=None) -> Path:
    if layer_type not in LAYER_BY_TYPE:
        raise KeyError(f"Unknown layer_type: {layer_type}")
    return block_dir(state, district, block, data_dir) / layer_type


def layer_path(layer_type: str, state: str, district: str, block: str, filename: Optional[str] = None,
               part: int = 0, data_dir=None) -> Path:
    """Path of a layer file in the NEW layout.

    ``filename`` defaults to the first template of ``part`` (only valid for
    templates without year/param/band placeholders)."""
    spec = LAYER_BY_TYPE[layer_type]
    if filename is None:
        template = spec.parts[part].templates[0]
        if re.search(r"\{(year|param|band)\}", template):
            raise ValueError(f"{layer_type} has several files per block; pass filename explicitly")
        filename = template.format(district=slug(district), block=slug(block))
    return layer_dir(layer_type, state, district, block, data_dir) / filename


def legacy_block_dir(spec: LayerSpec, state: str, district: str, block: str, data_dir=None) -> Optional[Path]:
    if not spec.legacy_dir:
        return None
    return get_data_dir(data_dir) / spec.legacy_dir / slug(state) / slug(district) / slug(block)


# --------------------------------------------------------------------------
# Lookup (new layout first, legacy as fallback during the transition)
# --------------------------------------------------------------------------
def _listdir(path: Optional[Path]) -> List[str]:
    if path is None:
        return []
    try:
        return os.listdir(path)
    except (FileNotFoundError, NotADirectoryError):
        return []


@dataclass
class LayerLookup:
    files: List[Path]
    where: str  # "new" | "legacy" | "missing"
    complete: bool  # every part of the layer has at least ``expected_min`` files
    parts_found: int


def _lookup_in(spec: LayerSpec, directory: Path, district: str, block: str) -> Tuple[List[Path], int, bool]:
    names = _listdir(directory)
    files: List[Path] = []
    found = 0
    complete = True
    for part in spec.parts:
        hits = matching_files(part, names, district, block)
        files += [directory / f for f in hits]
        found += bool(hits)
        complete &= len(hits) >= part.expected_min
    return files, found, complete


def find_layer_files(layer_type: str, state: str, district: str, block: str, data_dir=None,
                     include_legacy: bool = True) -> LayerLookup:
    """Look in the new layout first, then (during the transition) the legacy one."""
    spec = LAYER_BY_TYPE[layer_type]
    state, district, block = slug(state), slug(district), slug(block)

    best = LayerLookup([], "missing", False, 0)
    candidates = [("new", layer_dir(layer_type, state, district, block, data_dir))]
    if include_legacy and spec.legacy_dir:
        for sd in [state] + [a for a, s in STATE_ALIASES.items() if s == state]:
            candidates.append(("legacy", get_data_dir(data_dir) / spec.legacy_dir / sd / district / block))

    for where, directory in candidates:
        files, found, complete = _lookup_in(spec, directory, district, block)
        if found and (complete, found) > (best.complete, best.parts_found):
            best = LayerLookup(files, where, complete, found)
        if best.complete:
            break
    return best


# --------------------------------------------------------------------------
# Block universe + coverage
# --------------------------------------------------------------------------
def iter_blocks(data_dir=None, active_only: bool = False) -> Iterator[Tuple[str, str, str]]:
    """Yield (state, district, block) from the admin-boundary tree (or the
    activated-locations json when ``active_only``)."""
    root = get_data_dir(data_dir)
    if active_only:
        import json

        with open(root / "activated_locations" / "active_locations.json") as fh:
            data = json.load(fh)
        for st in data:
            for d in st.get("district", []):
                for b in d.get("blocks", []):
                    yield slug(st["label"]), slug(d["label"]), slug(b["label"])
        return

    boundary = root / "base_layers" / "admin-boundary" / "output"
    for state in sorted(os.listdir(boundary)):
        for district in sorted(_listdir(boundary / state)):
            for block in sorted(_listdir(boundary / state / district)):
                if (boundary / state / district / block).is_dir():
                    yield state, district, block


def block_coverage(state: str, district: str, block: str, data_dir=None, include_legacy: bool = True,
                   layer_types: Optional[Iterable[str]] = None) -> Dict[str, dict]:
    out = {}
    for spec in LAYERS:
        if layer_types and spec.layer_type not in layer_types:
            continue
        r = find_layer_files(spec.layer_type, state, district, block, data_dir, include_legacy)
        out[spec.layer_type] = {
            "present": r.complete,
            "partial": bool(r.files) and not r.complete,
            "where": r.where,
            "n_files": len(r.files),
        }
    return out
