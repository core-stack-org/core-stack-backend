"""
Read stats-excel input layers from local compute outputs instead of GeoServer.

Where each layer lives on disk comes from `derived_layers` in
computing/config.yaml: an entry's `geoserver_workspace` + `geoserver_layer`
identify the LayerInfo row it backs, and `local_path` / `filename` say where
local compute wrote it.

Per layer, `LocalLayerSource.fetch` returns a GeoServer-shaped GeoJSON dict
(EPSG:4326, with a `crs` member) so the existing create_excel_* functions can
consume it unchanged. Lookup order:

    1. the local file, if present;
    2. GeoServer, if the tehsil is active and fallback is enabled;
    3. None -> the caller writes the layer's sheets with blank columns.

Blank sheets reuse the column headers the same layer produced for another
tehsil; those headers are kept in a small JSON store (SheetTemplateStore).
"""

import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests

from computing.config_loader import DATA_DIR, derived_layers
from nrm_app.settings import EXCEL_PATH, GEOSERVER_URL
from utilities.gee_utils import valid_gee_text

logger = logging.getLogger(__name__)

ORIGIN_LOCAL = "local"
ORIGIN_GEOSERVER = "geoserver"
ORIGIN_MISSING = "missing"

GEOSERVER_TIMEOUT_SECONDS = 300
_GEOJSON_CRS = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_+]*)\}")
STATS_EXCEL_BASE_DIR = Path(EXCEL_PATH) / "data" / "stats_excel_files"
SHEET_TEMPLATES_PATH = STATS_EXCEL_BASE_DIR / "_meta" / "sheet_templates.json"


def slug(value) -> str:
    """Slug used for local output dirs and GeoServer layer names."""
    return valid_gee_text(str(value).strip().lower())


def stats_excel_path(state, district, block) -> Path:
    """Same path get_vector_layer_geoserver writes to (state/DISTRICT/district_block.xlsx)."""
    return (
        STATS_EXCEL_BASE_DIR
        / state.replace(" ", "_").upper()
        / district.replace(" ", "_").upper()
        / f"{district}_{block}.xlsx"
    )


def layer_key(workspace, layer_template) -> str:
    """Location-independent id of a LayerInfo row, e.g. 'mining|{district}_{block}_mining'."""
    return f"{workspace}|{layer_template}"


# ---------------------------------------------------------------------------
# config.yaml lookup
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LocalLayerSpec:
    name: str
    workspace: str
    layer_template: str
    filename: str | None
    local_path: str | None
    per_year: bool

    @property
    def is_local(self) -> bool:
        return bool(self.local_path)


@lru_cache(maxsize=1)
def local_layer_specs() -> tuple[LocalLayerSpec, ...]:
    specs = []
    for layer in derived_layers():
        if layer.get("layer_type") != "vector" or not layer.get("geoserver_layer"):
            continue
        specs.append(
            LocalLayerSpec(
                name=layer["name"],
                workspace=layer["geoserver_workspace"],
                layer_template=layer["geoserver_layer"],
                filename=layer.get("filename"),
                local_path=layer.get("local_path"),
                per_year=bool(layer.get("per_year")),
            )
        )
    return tuple(specs)


def _template_regex(template, district, block, per_year):
    """
    Regex matching a concrete GeoServer layer name against a config template.
    {district}/{block} must equal the given slugs; any other placeholder
    (param_name, start_year, ...) is captured so it can fill the filename.
    """
    parts, pos = [], 0
    for match in _PLACEHOLDER.finditer(template):
        parts.append(re.escape(template[pos : match.start()]))
        key = match.group(1)
        if key == "district":
            parts.append(re.escape(district))
        elif key == "block":
            parts.append(re.escape(block))
        else:
            parts.append(f"(?P<{key}>.+?)")
        pos = match.end()
    parts.append(re.escape(template[pos:]))
    if per_year:
        parts.append(r"(?:_(?P<year>\d{4}))?")
    return re.compile("".join(parts) + "$")


def find_spec(workspace, layer_name, district, block):
    """(spec, captured params) for a concrete layer name, or (None, {})."""
    for spec in local_layer_specs():
        if spec.workspace != workspace:
            continue
        match = _template_regex(
            spec.layer_template, district, block, spec.per_year
        ).match(layer_name)
        if match:
            return spec, {k: v for k, v in match.groupdict().items() if v}
    return None, {}


def _fill(template, values):
    """Substitute known placeholders; unknown ones become glob wildcards."""
    return _PLACEHOLDER.sub(lambda m: values.get(m.group(1), "*"), template)


def _name_variants(value):
    # Boundary datasets name hyphenated places either way (see
    # computing.base_layer_setup.tehsil_file_candidates).
    return list(dict.fromkeys([value, value.replace("-", "_")]))


def local_paths_for(spec, params, state, district, block) -> list[Path]:
    """Existing local files for a spec + tehsil (several for per-year layers)."""
    if not spec.is_local:
        return []
    found = []
    for s in _name_variants(state):
        for d in _name_variants(district):
            for b in _name_variants(block):
                values = {**params, "state": s, "district": d, "block": b}
                filename = _fill(spec.filename or "", values)
                pattern = spec.local_path.replace("{DATA_DIR}", str(DATA_DIR))
                pattern = pattern.replace("{filename}", filename)
                pattern = _fill(pattern, values)
                if "*" in pattern:
                    root = Path(pattern.split("*")[0]).parent
                    rel = os.path.relpath(pattern, root)
                    matches = sorted(root.glob(rel)) if root.is_dir() else []
                else:
                    matches = [Path(pattern)] if Path(pattern).is_file() else []
                for path in matches:
                    if path not in found:
                        found.append(path)
        if found:
            break
    return found


def sample_location_for(spec, params, max_checked=500):
    """
    Some (state, district, block) whose local file for this layer has
    features, for learning the layer's sheet headers. Many pan-India layers
    (mining, lcw, ...) are empty for most tehsils, so empty files are skipped.
    """
    if not spec.is_local:
        return None
    import pyogrio

    pattern = spec.local_path.replace("{DATA_DIR}", str(DATA_DIR))
    pattern = pattern.replace("{filename}", spec.filename or "*")
    pattern = _fill(pattern, params)  # state/district/block -> "*"
    root = Path(pattern.split("*")[0]).parent
    if not root.is_dir():
        return None
    for checked, path in enumerate(root.glob(os.path.relpath(pattern, root))):
        if checked >= max_checked:
            break
        state, district, block = path.parts[-4:-1]
        # The district_block part of the filename must belong to this dir.
        if district not in path.stem or block not in path.stem:
            continue
        try:
            if pyogrio.read_info(path).get("features", 0) > 0:
                return state, district, block
        except Exception:
            continue
    return None


def year_of(path: Path):
    match = re.search(r"_(\d{4})$", path.stem)
    return int(match.group(1)) if match else None


def _integral_floats_to_int(gdf):
    """
    GeoJSON/GPKG outputs often store integer fields (vill_ID, census codes,
    counts) as float; GeoServer serves them as integers. Convert float
    columns whose values are all whole numbers so ids render as 244706,
    not 244706.0, matching the GeoServer-based excels.
    """
    for col in gdf.columns:
        if col == gdf.geometry.name or not pd.api.types.is_float_dtype(gdf[col]):
            continue
        values = gdf[col].dropna()
        if values.empty or not np.isfinite(values).all():
            continue
        if (values % 1 == 0).all() and values.abs().max() < 2**53:
            gdf[col] = gdf[col].astype("Int64")
    return gdf


def _read_vector(path):
    # pyogrio is ~50x faster than the fiona default of geopandas 0.14 on wide
    # layers (SWB: 0.5s vs 34s) and returns the same data.
    try:
        return gpd.read_file(path, engine="pyogrio")
    except Exception:
        logger.debug("pyogrio could not read %s; falling back to default engine", path)
        return gpd.read_file(path)


def read_layer_as_geojson(path) -> dict:
    gdf = _read_vector(path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    gdf = _integral_floats_to_int(gdf)
    data = json.loads(gdf.to_json(na="null", drop_id=True))
    data["crs"] = _GEOJSON_CRS
    return data


def geoserver_wfs_url(workspace, layer_name):
    return (
        f"{GEOSERVER_URL}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature"
        f"&typeName={workspace}:{layer_name}&outputFormat=application/json"
    )


def active_location_slugs() -> set[tuple[str, str, str]]:
    """(state, district, block) slugs of active tehsils in the local DB."""
    from computing.bulk_layer_generation import get_active_locations

    return {
        (slug(loc.state), slug(loc.district), slug(loc.block))
        for loc in get_active_locations()
    }


def is_active_location(state, district, block) -> bool:
    return (slug(state), slug(district), slug(block)) in active_location_slugs()


# ---------------------------------------------------------------------------
# Per-tehsil source
# ---------------------------------------------------------------------------


class LocalLayerSource:
    """Serves layers for one tehsil from local files (GeoServer fallback when active)."""

    def __init__(self, state, district, block, *, is_active=False, geoserver_fallback=True):
        self.state = slug(state)
        self.district = slug(district)
        self.block = slug(block)
        self.is_active = bool(is_active)
        self.geoserver_fallback = bool(geoserver_fallback)
        self.origins: dict[str, str] = {}  # layer_name -> origin
        self.paths: dict[str, str] = {}  # layer_name -> local path / URL
        self._cache: dict[tuple, dict | None] = {}
        self._warned: set[str] = set()

    @property
    def label(self):
        return f"{self.state}/{self.district}/{self.block}"

    def _from_geoserver(self, workspace, layer_name):
        if not (self.is_active and self.geoserver_fallback):
            return None
        url = geoserver_wfs_url(workspace, layer_name)
        started = time.monotonic()
        try:
            response = requests.get(url, timeout=GEOSERVER_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "[%s] GeoServer fallback failed for %s:%s: %s",
                self.label, workspace, layer_name, exc,
            )
            return None
        logger.info(
            "[%s] %s:%s <- GeoServer (%d features, %.1fs)",
            self.label, workspace, layer_name,
            len(data.get("features", [])), time.monotonic() - started,
        )
        self.paths[layer_name] = url
        return data

    def _resolve(self, workspace, layer_name):
        spec, params = find_spec(workspace, layer_name, self.district, self.block)
        if spec is None and workspace not in self._warned:
            self._warned.add(workspace)
            logger.warning(
                "[%s] No derived_layers entry in config.yaml for %s:%s; "
                "local lookup skipped",
                self.label, workspace, layer_name,
            )
        return spec, params

    def fetch(self, workspace, layer_name):
        """GeoJSON dict for one layer, or None when unavailable."""
        key = (workspace, layer_name)
        if key in self._cache:
            return self._cache[key]

        data, origin = None, ORIGIN_MISSING
        spec, params = self._resolve(workspace, layer_name)
        paths = local_paths_for(spec, params, self.state, self.district, self.block) if spec else []
        if paths:
            path = paths[-1]
            started = time.monotonic()
            try:
                data = read_layer_as_geojson(path)
                origin = ORIGIN_LOCAL
                self.paths[layer_name] = str(path)
                logger.info(
                    "[%s] %s:%s <- %s (%d features, %.1fs)",
                    self.label, workspace, layer_name, path,
                    len(data["features"]), time.monotonic() - started,
                )
            except Exception:
                logger.exception(
                    "[%s] Could not read local file %s for %s:%s",
                    self.label, path, workspace, layer_name,
                )
        else:
            logger.info(
                "[%s] %s:%s not found locally%s",
                self.label, workspace, layer_name,
                "" if spec is None or spec.is_local else " (GEE-only layer)",
            )

        if data is None:
            data = self._from_geoserver(workspace, layer_name)
            if data is not None:
                origin = ORIGIN_GEOSERVER

        self.origins[layer_name] = origin
        self._cache[key] = data
        return data

    def fetch_years(self, workspace, layer_name):
        """[(year, geojson), ...] for a per-year layer (e.g. SOGE), oldest first."""
        spec, params = self._resolve(workspace, layer_name)
        results = []
        if spec is not None and spec.per_year:
            for path in local_paths_for(spec, params, self.state, self.district, self.block):
                year = year_of(path)
                if year is None:
                    continue
                try:
                    results.append((year, read_layer_as_geojson(path)))
                except Exception:
                    logger.exception("[%s] Could not read local file %s", self.label, path)
            if results:
                logger.info(
                    "[%s] %s:%s <- %d local yearly files (%s)",
                    self.label, workspace, layer_name, len(results),
                    ", ".join(str(y) for y, _ in sorted(results, key=lambda r: r[0])),
                )
                self.origins[layer_name] = ORIGIN_LOCAL
                self.paths[layer_name] = str(
                    local_paths_for(spec, params, self.state, self.district, self.block)[0].parent
                )
                return sorted(results, key=lambda r: r[0])

        data = self.fetch(workspace, layer_name)
        if data is None:
            return []
        year = params.get("year")
        return [(int(year) if year else None, data)]

    def village_gdf(self):
        """Panchayat/village boundaries as a GeoDataFrame (local or GeoServer)."""
        data = self.fetch("panchayat_boundaries", f"{self.district}_{self.block}")
        if data is None:
            raise FileNotFoundError(f"[{self.label}] panchayat boundaries not available")
        return gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")

    def mws_uids(self):
        """Sorted MWS UIDs of the tehsil (to pre-fill UID in blank sheets)."""
        data = self.fetch("mws", f"mws_{self.district}_{self.block}")
        if data is None:
            data = self.fetch("mws_layers", f"deltaG_well_depth_{self.district}_{self.block}")
        if data is None:
            return []
        uids = {f["properties"].get("uid") for f in data.get("features", [])}
        return sorted(u for u in uids if u)


# ---------------------------------------------------------------------------
# Sheet header templates for blank sheets
# ---------------------------------------------------------------------------


class SheetTemplateStore:
    """
    Maps layer_key -> {sheet_name: [column headers]}, learned from sheets
    written for other tehsils. Persisted as JSON so later runs (and worker
    processes) can write blank sheets with the right columns.
    """

    def __init__(self, path=SHEET_TEMPLATES_PATH):
        self.path = Path(path)
        self.templates: dict[str, dict[str, list]] = {}
        self._loaded = False

    def load(self):
        if not self._loaded:
            try:
                self.templates = json.loads(self.path.read_text())
            except FileNotFoundError:
                self.templates = {}
            except (OSError, ValueError):
                logger.exception("Could not read sheet templates at %s", self.path)
                self.templates = {}
            self._loaded = True
        return self.templates

    def get(self, key):
        return self.load().get(key, {})

    def learn(self, key, sheets: dict[str, list]):
        self.load()
        if not sheets:
            return
        current = self.templates.get(key, {})
        merged = {**current, **sheets}
        if merged == current:
            return
        self.templates[key] = merged
        logger.debug("Learned sheet template %s -> %s", key, sorted(sheets))
        self._save()

    def _save(self):
        # Merge with what other processes may have written, then replace atomically.
        try:
            on_disk = json.loads(self.path.read_text())
        except (OSError, ValueError):
            on_disk = {}
        for key, sheets in self.templates.items():
            on_disk[key] = {**on_disk.get(key, {}), **sheets}
        self.templates = on_disk
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".json.tmp")
        with os.fdopen(fd, "w") as fh:
            json.dump(on_disk, fh, indent=1, sort_keys=True)
        os.replace(tmp, self.path)


sheet_templates = SheetTemplateStore()


def sheet_snapshot(writer):
    """{sheet_name: worksheet object}; a replaced sheet gets a new object."""
    book = writer.book
    return dict(zip(book.sheetnames, book.worksheets))


def sheets_written_since(before, writer):
    return [
        name for name, ws in sheet_snapshot(writer).items() if before.get(name) is not ws
    ]


def sheet_header(writer, sheet_name):
    ws = writer.book[sheet_name]
    first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), ())
    return ["" if value is None else value for value in first_row]
