"""Original GEE assets as a layer source for the stats Excel generator.

GeoServer now serves the new locally-computed layers. To rebuild the
workbooks from the ORIGINAL GEE-computed layers, ``GEELayerSource`` maps each
GeoServer workspace/layer used by ``get_vector_layer_geoserver`` to the
FeatureCollection asset its GEE pipeline in ``computing/`` exported, and
returns it as GeoJSON with GeoServer-compatible property names.

Asset names are rebuilt from the pipeline naming conventions rather than read
from ``computing.models.Layer.gee_asset_path``, which local pipelines
overwrite with local file paths. Each rule cites the pipeline it mirrors.

Layers that were never produced as a GEE vector raise ``LayerUnavailable``,
so they are skipped (reported) instead of silently falling back to
GeoServer, which would compare the new local output against itself.
"""

import logging
import re

from .layer_sources import LayerUnavailable


logger = logging.getLogger(__name__)

DEFAULT_GEE_PROJECT = "ee-corestackdev-1"

# Workspaces with no original GEE vector asset (see docstring).
NOT_FROM_GEE = {
    "drought_causality": "computed in pandas and pushed straight to GeoServer; no GEE vector "
    "(computing/drought/drought_causality.py, GEE export commented out)",
    "drainage_density": "only a GEE raster exists; the vector is local-compute only "
    "(computing/misc/drainage_density_local_compute.py)",
    "river": "local-compute only (computing/misc/river_local_compute.py)",
    "canal": "GEE path not wired in the API and its asset has no 'uid' "
    "(computing/misc/canal_layer.py)",
    "antyodaya_2020": "local CSV join, no GEE asset",
    "livestocks": "local pipeline, no GEE asset",
    "soil_type": "local pipeline, no GEE asset",
    "soil_health_vector": "local rasterio pipeline, no GEE asset",
    "facilities_proximity": "old GEE asset has a different schema from the current Excel builder",
}

_YEAR_SUFFIX = re.compile(r"_(\d{4})_(\d{4})$")


def _vt(value):
    # Imported lazily: utilities.gee_utils needs Django; this module's
    # name-resolution logic is unit-tested without it.
    from utilities.gee_utils import valid_gee_text

    return valid_gee_text(str(value).lower())


class GEELayerSource:
    """Callable layer source: (workspace, layer_name, start_year, end_year) -> GeoJSON."""

    def __init__(self, state, district, block, project=DEFAULT_GEE_PROJECT):
        self.state = _vt(state)
        self.district = _vt(district)
        self.block = _vt(block)
        self.project = project
        self.folder = (
            f"projects/{project}/assets/apps/mws/"
            f"{self.state}/{self.district}/{self.block}"
        )
        self.stem = f"{self.district}_{self.block}"  # "S" in the pipelines
        self._asset_ids = None
        self.used_assets = {}  # layer_name -> asset id, for the run report

    # ---------- asset discovery ----------

    def asset_ids(self):
        """All asset ids in the block folder (listed once)."""
        if self._asset_ids is None:
            import ee

            ids = set()
            params = {"parent": self.folder}
            while True:
                response = ee.data.listAssets(params)
                for asset in response.get("assets", []):
                    ids.add(asset.get("id") or asset["name"])
                token = response.get("nextPageToken")
                if not token:
                    break
                params = {"parent": self.folder, "pageToken": token}
            self._asset_ids = ids
        return self._asset_ids

    def _path(self, name):
        return f"{self.folder}/{name}"

    def _first_existing(self, names):
        existing = self.asset_ids()
        for name in names:
            asset_id = self._path(name)
            if asset_id in existing:
                return asset_id
        return None

    def _year_suffixed(self, base, start_year, end_year):
        """Resolve ``{base}_{start}_{end}``; fall back to the widest year range present."""
        if start_year and end_year:
            exact = self._first_existing([f"{base}_{start_year}_{end_year}"])
            if exact:
                return exact
        prefix = self._path(base) + "_"
        options = []
        for asset_id in self.asset_ids():
            if not asset_id.startswith(prefix):
                continue
            match = _YEAR_SUFFIX.search(asset_id)
            if match and asset_id[: match.start()] == self._path(base):
                options.append((int(match.group(2)), -int(match.group(1)), asset_id))
        if not options:
            return None
        chosen = max(options)[2]
        if start_year and end_year:
            logger.warning(
                "No %s_%s_%s; using %s instead", base, start_year, end_year, chosen
            )
        return chosen

    # ---------- workspace -> asset rules ----------

    def resolve(self, workspace, layer_name, start_year=None, end_year=None):
        """Return the GEE asset id for a GeoServer layer, or raise LayerUnavailable."""
        if workspace in NOT_FROM_GEE:
            raise LayerUnavailable(f"not available from GEE: {NOT_FROM_GEE[workspace]}")

        S = self.stem
        names = None  # candidate names in priority order
        year_base = None  # for assets with _{start}_{end} suffixes

        if workspace == "mws_layers":
            # computing/mws/generate_hydrology.py -> calculateG.py
            if layer_name.startswith("deltaG_well_depth"):
                names = [f"deltaG_well_depth_{S}"]
            elif layer_name.startswith("deltaG_fortnight"):
                names = [f"deltaG_fortnight_{S}"]
        elif workspace == "mws":
            names = [f"filtered_mws_{S}_uid"]  # computing/mws/mws.py
        elif workspace == "swb":
            # computing/surface_water_bodies/swb.py. swb4 (enriched, has
            # water_body_name) was the published layer before 2026-04;
            # current code publishes swb3.
            names = [f"swb4_{S}", f"swb3_{S}"]
        elif workspace == "crop_intensity":
            names = [f"cropping_intensity_{S}"]  # cropping_intensity.py
        elif workspace in ("drought", "cropping_drought"):
            names = [f"drought_{S}"]  # drought/merge_layers.py
        elif workspace == "mws_connectivity":
            names = [f"{S}_mws_connectivity"]  # mws/mws_connectivity.py
        elif workspace == "stream_order":
            names = [f"stream_order_{S}_vector"]  # misc/stream_order.py
        elif workspace == "terrain":
            names = [f"{S}_terrain_clusters"]  # terrain_descriptor/terrain_clusters.py
        elif workspace == "terrain_lulc":
            if layer_name.endswith("_lulc_slope"):
                names = [f"{S}_lulcXslopes_clusters"]  # lulc_on_slope_cluster.py
            elif layer_name.endswith("_lulc_plain"):
                names = [f"{S}_lulcXplains_clusters"]  # lulc_on_plain_cluster.py
        elif workspace == "lulc_vector":
            names = [f"lulc_vector_{S}"]  # lulc/lulc_vector.py
        elif workspace == "tree_ccd_vector":
            year_base = f"ccd_vector_{S}"  # tree_health/gee/ccd_vector.py
        elif workspace == "canopy_height_vector":
            year_base = f"ch_vector_{S}"  # tree_health/gee/canopy_height_vector.py
        elif workspace == "tree_overall_vector":
            names = [f"overall_change_vector_{S}"]  # overall_change_vector.py
        elif workspace == "change_detection":
            # change_detection/change_detection_vector.py:
            # GeoServer change_vector_{S}_{param}; GEE adds _{start}_{end}.
            match = re.match(rf"change_vector_{re.escape(S)}_(\w+)$", layer_name)
            if match:
                year_base = f"change_vector_{S}_{match.group(1)}"
        elif workspace == "restoration":
            names = [f"restoration_{S}_vector"]  # misc/restoration_opportunity.py
        elif workspace == "ndvi_timeseries":
            names = [layer_name]  # misc/ndvi_time_series.py (same name)
        elif workspace == "forest_fire":
            names = [f"forest_fire_{S}"]
        elif workspace == "forest_fringes":
            names = [f"forest_fringe_{S}"]  # singular in GEE
        elif workspace == "tree_in_grassland":
            year_base = f"tree_in_grassland_{S}"
        elif workspace == "nrega_assets":
            names = [f"nrega_{S}"]  # misc/nrega.py
        elif workspace == "panchayat_boundaries":
            names = [f"admin_boundary_{S}"]  # misc/admin_boundary.py
        elif workspace == "aquifer":
            names = [f"aquifer_vector_{S}"]
        elif workspace == "soge":
            # misc/soge_vector.py: a single year (SOGE_vector_2020) only.
            year = start_year or _trailing_year(layer_name)
            if year and int(year) != 2020:
                raise LayerUnavailable(f"GEE SOGE asset only has 2020, not {year}")
            names = [f"soge_vector_{S}"]
        elif workspace in (
            "lcw",
            "agroecological",
            "factory_csr",
            "green_credit",
            "mining",
        ):
            suffix = "lcw_conflict" if workspace == "lcw" else workspace
            names = [f"{S}_{suffix}"]  # misc/<workspace>.py
        elif workspace == "dem":
            names = [f"{S}_dem_vector"]  # misc/digital_elevation_model.py

        if names is None and year_base is None:
            raise LayerUnavailable(f"no GEE mapping for {workspace}:{layer_name}")

        asset_id = (
            self._year_suffixed(year_base, start_year, end_year)
            if year_base
            else self._first_existing(names)
        )
        if not asset_id:
            tried = [f"{year_base}_<start>_<end>"] if year_base else names
            raise LayerUnavailable(
                f"GEE asset not found in {self.folder}: {', '.join(tried)}"
            )
        return asset_id

    # ---------- fetching ----------

    def __call__(self, workspace, layer_name, start_year=None, end_year=None):
        asset_id = self.resolve(workspace, layer_name, start_year, end_year)
        logger.info("%s:%s <- %s", workspace, layer_name, asset_id)
        self.used_assets[layer_name] = asset_id
        geojson = fetch_feature_collection(asset_id)
        return _to_geoserver_schema(workspace, geojson)


def _trailing_year(layer_name):
    match = re.search(r"_(\d{4})$", layer_name or "")
    return int(match.group(1)) if match else None


def fetch_feature_collection(asset_id, page_size=2000):
    """Download a FeatureCollection asset as GeoJSON.

    Uses a download URL (handles large collections); falls back to paged
    getInfo if the download fails.
    """
    import ee
    import requests

    collection = ee.FeatureCollection(asset_id)
    try:
        url = collection.getDownloadURL(filetype="geojson")
        response = requests.get(url, timeout=600)
        response.raise_for_status()
        data = response.json()
        if data.get("type") == "FeatureCollection":
            return data
    except Exception as exc:  # noqa: BLE001 - fall back to paged getInfo
        logger.warning("Download URL failed for %s (%s); paging getInfo", asset_id, exc)

    total = collection.size().getInfo()
    features = []
    for offset in range(0, total, page_size):
        page = ee.FeatureCollection(collection.toList(page_size, offset)).getInfo()
        features.extend(page.get("features", []))
    return {"type": "FeatureCollection", "features": features}


def _to_geoserver_schema(workspace, geojson):
    """Adapt GEE property names/ordering to what the Excel builders expect."""
    for feature in geojson.get("features", []):
        properties = feature.get("properties") or {}
        if workspace == "soge" and "nat_discha" in properties:
            # SOGE_vector_2020 uses the shapefile-truncated name.
            properties.setdefault("nat_discharge", properties.pop("nat_discha"))
        elif workspace == "ndvi_timeseries":
            # The Mann-Kendall trend depends on column order: keep dates sorted.
            properties = {
                key: properties[key]
                for key in sorted(properties, key=lambda k: (k != "uid", k))
            }
        feature["properties"] = properties
    return geojson
