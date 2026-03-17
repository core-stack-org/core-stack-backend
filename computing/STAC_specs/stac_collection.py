from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import os
import re
import subprocess
import urllib
import xml.etree.ElementTree as ET

import pandas as pd
import pystac
import requests
from shapely.geometry import mapping, Polygon

from computing.STAC_specs import constants
from nrm_app.celery import app
from nrm_app.settings import (
    BASE_DIR, S3_ACCESS_KEY, S3_SECRET_KEY,
    GEOSERVER_USERNAME, GEOSERVER_PASSWORD,
)


def sanitize_text(text):
    text = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", text)
    return text.replace(" ", "_")


_STAC_DATA = os.path.join(BASE_DIR, "data", "STAC_specs")

JAVA_TYPE_MAP = {
    "java.lang.String": "object",
    "java.lang.Integer": "int64",
    "java.lang.Long": "int64",
    "java.lang.Short": "int64",
    "java.lang.Double": "float64",
    "java.lang.Float": "float64",
    "java.math.BigDecimal": "float64",
    "java.lang.Boolean": "bool",
    "java.sql.Date": "datetime64",
    "java.sql.Timestamp": "datetime64",
}


@dataclass
class STACConfig:
    geoserver_base_url: str = constants.GEOSERVER_BASE_URL
    thumbnail_data_url: str = constants.S3_STAC_BUCKET_URL
    local_data_dir: str = _STAC_DATA
    stac_files_dir: str = os.path.join(_STAC_DATA, "CorestackCatalogs_merged_collection")
    thumbnail_dir: str = os.path.join(_STAC_DATA, "STAC_output_merged_collection")
    style_file_dir: str = os.path.join(_STAC_DATA, "input", "style_files")
    tehsil_dirname: str = "tehsil_wise"
    s3_bucket_name: str = constants.S3_STAC_BUCKET_NAME
    s3_uri: str = constants.S3_STAC_URI
    layer_map_csv: str = os.path.join(_STAC_DATA, "input", "metadata", "layer_mapping.csv")
    layer_desc_csv: str = os.path.join(_STAC_DATA, "input", "metadata", "layer_descriptions.csv")
    column_desc_csv: str = os.path.join(_STAC_DATA, "input", "metadata", "vector_column_descriptions.csv")


# ---------------------------------------------------------------------------
# GeoServer interaction — metadata only, zero data download
# ---------------------------------------------------------------------------

class GeoServerClient:
    def __init__(self, base_url, username=None, password=None):
        self.base_url = base_url
        self.auth = (username, password) if username else None

    def _get(self, url, **kwargs):
        kwargs.setdefault("verify", False)
        return requests.get(url, auth=self.auth, **kwargs)

    # -- URL builders --------------------------------------------------------

    def raster_describe_url(self, workspace, layer_name):
        return (
            f"{self.base_url}/{workspace}/wcs?"
            f"service=WCS&version=2.0.1&request=DescribeCoverage&"
            f"coverageId={workspace}:{layer_name}"
        )

    def raster_data_url(self, workspace, layer_name, fmt="geotiff"):
        return (
            f"{self.base_url}/{workspace}/wcs?"
            f"service=WCS&version=2.0.1&request=GetCoverage&"
            f"CoverageId={workspace}:{layer_name}&"
            f"format={fmt}&compression=LZW"
        )

    def vector_data_url(self, workspace, layer_name):
        return (
            f"{self.base_url}/{workspace}/ows?"
            f"service=WFS&version=1.0.0&request=GetFeature&"
            f"typeName={workspace}:{layer_name}&outputFormat=application/json"
        )

    def wms_thumbnail_url(self, workspace, layer_name, bbox):
        bbox_str = ",".join(map(str, bbox))
        return (
            f"{self.base_url}/{workspace}/wms?"
            f"service=WMS&version=1.1.0&request=GetMap&"
            f"layers={workspace}:{layer_name}&"
            f"bbox={bbox_str}&"
            f"width=300&height=300&srs=EPSG:4326&styles=&format=image/png"
        )

    # -- raster metadata via WCS DescribeCoverage ----------------------------

    def fetch_raster_metadata(self, describe_url):
        response = self._get(describe_url)
        if response.status_code != 200:
            return None

        root = ET.fromstring(response.content)
        ns = {
            "gml": "http://www.opengis.net/gml/3.2",
            "wcs": "http://www.opengis.net/wcs/2.0",
        }

        envelope = root.find(".//gml:Envelope", ns)
        if envelope is None:
            return None

        lower = envelope.find("gml:lowerCorner", ns).text.split()
        upper = envelope.find("gml:upperCorner", ns).text.split()
        bbox = [float(lower[1]), float(lower[0]), float(upper[1]), float(upper[0])]
        footprint = Polygon([
            [bbox[0], bbox[1]], [bbox[0], bbox[3]],
            [bbox[2], bbox[3]], [bbox[2], bbox[1]],
        ])

        grid_low = root.find(".//gml:low", ns)
        grid_high = root.find(".//gml:high", ns)
        if grid_low is not None and grid_high is not None:
            low_c = grid_low.text.split()
            high_c = grid_high.text.split()
            shape = [int(high_c[1]) - int(low_c[1]) + 1,
                     int(high_c[0]) - int(low_c[0]) + 1]
        else:
            shape = [0, 0]

        return bbox, mapping(footprint), "EPSG:4326", shape

    # -- vector metadata via REST API ----------------------------------------
    # Single lightweight JSON call replaces downloading the full GeoJSON.

    def fetch_vector_metadata(self, workspace, layer_name):
        url = (
            f"{self.base_url}/rest/workspaces/{workspace}"
            f"/featuretypes/{layer_name}.json"
        )
        response = self._get(url)
        if response.status_code != 200:
            return None

        ft = response.json()["featureType"]
        ll = ft["latLonBoundingBox"]
        bbox = [ll["minx"], ll["miny"], ll["maxx"], ll["maxy"]]
        footprint = Polygon([
            [bbox[0], bbox[1]], [bbox[0], bbox[3]],
            [bbox[2], bbox[3]], [bbox[2], bbox[1]],
            [bbox[0], bbox[1]],
        ])

        columns = []
        for attr in ft.get("attributes", {}).get("attribute", []):
            binding = attr.get("binding", "")
            if "geom" in binding.lower() or "geometry" in binding.lower():
                dtype = "geometry"
            else:
                dtype = JAVA_TYPE_MAP.get(binding, "object")
            columns.append({"name": attr["name"], "type": dtype})

        return bbox, mapping(footprint), columns

    # -- thumbnail download (WMS GetMap — works for both raster & vector) ----

    def download_thumbnail(self, url, output_path):
        response = self._get(url)
        if response.status_code != 200:
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(response.content)
        return True


# ---------------------------------------------------------------------------
# Metadata from CSVs
# ---------------------------------------------------------------------------

class MetadataProvider:
    def __init__(self, config):
        self.config = config

    def get_layer_description(self, layer_name, overwrite=False):
        path = self.config.layer_desc_csv
        if os.path.exists(path) and not overwrite:
            df = pd.read_csv(path)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df = pd.read_csv(constants.LAYER_DESC_GITHUB_URL)
            df.to_csv(path)

        match = df[df["layer_name"] == layer_name]
        return match["layer_description"].iloc[0] if not match.empty else ""

    def get_layer_mapping(self, layer_name, district, block, start_year=""):
        df = pd.read_csv(self.config.layer_map_csv)
        row = df[df["layer_name"] == layer_name].iloc[0]
        gs_layer = row["geoserver_layer_name"]

        if layer_name == "land_use_land_cover_raster":
            gs_layer = gs_layer.format(
                start_year=str(int(start_year) % 100),
                end_year=str((int(start_year) + 1) % 100),
                district=district, block=block,
            )
        elif layer_name in ("tree_canopy_cover_density_raster", "tree_canopy_height_raster"):
            gs_layer = gs_layer.format(start_year=start_year, district=district, block=block)
        else:
            gs_layer = gs_layer.format(district=district, block=block)

        return {
            "workspace": row["geoserver_workspace_name"],
            "layer_name": gs_layer,
            "style_file_url": row["style_file_url"],
            "display_name": row["display_name"],
            "ee_layer_name": row["ee_layer_name"],
            "gsd": row["spatial_resolution_in_meters"],
            "theme": row["theme"],
        }

    def get_vector_column_descriptions(self, ee_layer_name, overwrite=False):
        path = self.config.column_desc_csv
        if os.path.exists(path) and not overwrite:
            df = pd.read_csv(path)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df = pd.read_csv(constants.VECTOR_COLUMN_DESC_GITHUB_URL)
            df.to_csv(path)

        return df[df["ee_layer_name"] == ee_layer_name].rename(
            {"column_name_description": "column_description"}, axis=1
        )


# ---------------------------------------------------------------------------
# Style file parsing (raster palette for classification extension)
# ---------------------------------------------------------------------------

class StyleParser:
    def __init__(self, style_file_dir):
        self.style_file_dir = style_file_dir

    def _ensure_local(self, url):
        local_path = os.path.join(self.style_file_dir, os.path.basename(url))
        if not os.path.exists(local_path):
            os.makedirs(self.style_file_dir, exist_ok=True)
            urllib.request.urlretrieve(url, local_path)
        return local_path

    def parse_raster_style(self, url):
        local = self._ensure_local(url)
        root = ET.parse(local).getroot()
        classes = []
        for tag in ("paletteEntry", "item"):
            for entry in root.findall(f".//{tag}"):
                info = {}
                for k, v in entry.attrib.items():
                    if k == "value":
                        try:
                            info[k] = int(v)
                        except ValueError:
                            info[k] = v
                    else:
                        info[k] = v
                classes.append(info)
            if classes:
                break
        return classes


# ---------------------------------------------------------------------------
# STAC catalog hierarchy management
# ---------------------------------------------------------------------------

class CatalogManager:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def _merge_bboxes(a, b):
        if not a or a == [0, 0, 0, 0]:
            return b
        return [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]

    def _default_extent(self, bbox):
        return pystac.Extent(
            spatial=pystac.SpatialExtent([bbox]),
            temporal=pystac.TemporalExtent(
                [[constants.DEFAULT_START_DATE, constants.DEFAULT_END_DATE]]
            ),
        )

    @staticmethod
    def _provider():
        return pystac.Provider(
            name="CoRE Stack",
            roles=[
                pystac.ProviderRole.PRODUCER, pystac.ProviderRole.PROCESSOR,
                pystac.ProviderRole.HOST, pystac.ProviderRole.LICENSOR,
            ],
            url="https://core-stack.org/",
        )

    def update(self, state, district, block, item):
        bbox = item.bbox

        block_existed = self._upsert_block(state, district, block, bbox, item)
        if block_existed:
            return True

        block_coll = self._new_block_collection(state, district, block, bbox, item)

        district_existed = self._upsert_district(state, district, bbox, block_coll)
        if district_existed:
            return True

        district_coll = self._new_district_collection(state, district, bbox, block_coll)

        state_existed = self._upsert_state(state, bbox, district_coll)
        if state_existed:
            return True

        state_coll = self._new_state_collection(state, bbox, district_coll)
        self._upsert_tehsil(state_coll)
        self._upsert_root()
        return True

    # -- block ---------------------------------------------------------------

    def _block_dir(self, state, district, block):
        return os.path.join(
            self.config.stac_files_dir, self.config.tehsil_dirname,
            state, district, block,
        )

    def _upsert_block(self, state, district, block, bbox, item):
        d = self._block_dir(state, district, block)
        path = os.path.join(d, "collection.json")
        if not os.path.exists(path):
            return False

        coll = pystac.read_file(path)
        coll.extent.spatial.bboxes = [
            self._merge_bboxes(coll.extent.spatial.bboxes[0], bbox)
        ]
        if item.id in [i.id for i in coll.get_all_items()]:
            coll.remove_item(item.id)
        coll.add_item(item)
        coll.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)
        return True

    def _new_block_collection(self, state, district, block, bbox, item):
        d = self._block_dir(state, district, block)
        os.makedirs(d, exist_ok=True)
        coll = pystac.Collection(
            id=block, title=block,
            description=f"STAC collection for {block} block data in {district}, {state}",
            license="CC-BY-4.0",
            extent=self._default_extent(bbox),
            providers=[self._provider()],
            keywords=["social-ecological", "sustainability", "CoRE stack",
                       block, district, state],
        )
        coll.add_item(item)
        return coll

    # -- district ------------------------------------------------------------

    def _district_dir(self, state, district):
        return os.path.join(
            self.config.stac_files_dir, self.config.tehsil_dirname,
            state, district,
        )

    def _upsert_district(self, state, district, bbox, block_coll):
        d = self._district_dir(state, district)
        path = os.path.join(d, "collection.json")
        if not os.path.exists(path):
            return False

        coll = pystac.read_file(path)
        coll.extent.spatial.bboxes = [
            self._merge_bboxes(coll.extent.spatial.bboxes[0], bbox)
        ]
        coll.add_child(block_coll)
        coll.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)
        return True

    def _new_district_collection(self, state, district, bbox, block_coll):
        d = self._district_dir(state, district)
        os.makedirs(d, exist_ok=True)
        coll = pystac.Collection(
            id=district, title=district,
            description=f"STAC collection for data of {district} district",
            license="CC-BY-4.0",
            extent=self._default_extent(bbox),
            providers=[self._provider()],
            keywords=["social-ecological", "sustainability", "CoRE stack",
                       district, state],
        )
        coll.add_child(block_coll)
        coll.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)
        return coll

    # -- state ---------------------------------------------------------------

    def _state_dir(self, state):
        return os.path.join(
            self.config.stac_files_dir, self.config.tehsil_dirname, state,
        )

    def _upsert_state(self, state, bbox, district_coll):
        d = self._state_dir(state)
        path = os.path.join(d, "collection.json")
        if not os.path.exists(path):
            return False

        coll = pystac.read_file(path)
        coll.extent.spatial.bboxes = [
            self._merge_bboxes(coll.extent.spatial.bboxes[0], bbox)
        ]
        coll.add_child(district_coll)
        coll.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)
        return True

    def _new_state_collection(self, state, bbox, district_coll):
        d = self._state_dir(state)
        os.makedirs(d, exist_ok=True)
        coll = pystac.Collection(
            id=state, title=state,
            description=f"STAC Collection for data of {state} state.",
            license="CC-BY-4.0",
            extent=self._default_extent(bbox),
            providers=[self._provider()],
            keywords=["social-ecological", "sustainability", "CoRE stack", state],
        )
        coll.add_link(pystac.Link(
            rel=pystac.RelType.LICENSE,
            target="https://spdx.org/licenses/CC-BY-4.0.html",
            media_type="text/html",
        ))
        for target, title in [
            ("https://core-stack.org/", "CoRE stack"),
            ("https://drive.google.com/file/d/1ZxovdpPThkN09cB1TcUYSE2BImI7M3k_/view", "Technical Manual"),
            ("https://github.com/orgs/core-stack-org/repositories", "Github link"),
        ]:
            coll.add_link(pystac.Link(
                rel="documentation", target=target,
                title=title, media_type="application/pdf",
            ))
        coll.add_child(district_coll)
        coll.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)
        return coll

    # -- tehsil catalog ------------------------------------------------------

    def _upsert_tehsil(self, state_coll):
        d = os.path.join(self.config.stac_files_dir, self.config.tehsil_dirname)
        path = os.path.join(d, "catalog.json")

        if os.path.exists(path):
            catalog = pystac.read_file(path)
        else:
            catalog = pystac.Catalog(
                id=self.config.tehsil_dirname,
                title=constants.TEHSIL_CATALOG_TITLE,
                description=constants.TEHSIL_CATALOG_DESCRIPTION,
            )

        if state_coll.id not in [c.id for c in catalog.get_children()]:
            catalog.add_child(state_coll)
        catalog.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # -- root catalog --------------------------------------------------------

    def _upsert_root(self):
        d = self.config.stac_files_dir
        root_path = os.path.join(d, "catalog.json")
        tehsil_path = os.path.join(d, self.config.tehsil_dirname, "catalog.json")

        if os.path.exists(root_path):
            root = pystac.read_file(root_path)
        else:
            root = pystac.Catalog(
                id="corestack_STAC",
                title=constants.ROOT_CATALOG_TITLE,
                description=constants.ROOT_CATALOG_DESCRIPTION,
            )

        tehsil = pystac.read_file(tehsil_path)
        if tehsil.id not in [c.id for c in root.get_children()]:
            root.add_child(tehsil)
        root.normalize_and_save(d, catalog_type=pystac.CatalogType.SELF_CONTAINED)


# ---------------------------------------------------------------------------
# S3 sync
# ---------------------------------------------------------------------------

class S3Syncer:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def sync(self, folder_path, s3_uri):
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_key
        source = os.path.basename(folder_path)
        destination = s3_uri + source + "/"
        result = subprocess.run(
            ["aws", "s3", "sync", source, destination],
            capture_output=True, text=True,
        )
        return result.returncode


# ---------------------------------------------------------------------------
# STAC item builders (template method pattern)
# ---------------------------------------------------------------------------

class BaseSTACItemBuilder(ABC):
    def __init__(self, config, geoserver, metadata, style_parser):
        self.config = config
        self.geoserver = geoserver
        self.metadata = metadata
        self.style_parser = style_parser
        self._ws = None
        self._gs_layer = None

    def build(self, state, district, block, layer_name, overwrite=False, **kwargs):
        description = self.metadata.get_layer_description(layer_name, overwrite)
        layer_map = self.metadata.get_layer_mapping(
            layer_name, district, block, kwargs.get("start_year", ""),
        )
        item = self._create_item(state, district, block, layer_name,
                                 description, layer_map, **kwargs)
        if item is None:
            return None
        item = self._add_data_asset(item, layer_map)
        item = self._add_extensions(item, layer_map, overwrite=overwrite, **kwargs)
        self._add_style_asset(item, layer_map)
        item = self._add_thumbnail(item, state, district, block,
                                   layer_name, layer_map, **kwargs)
        return item

    @abstractmethod
    def _create_item(self, state, district, block, layer_name,
                     description, layer_map, **kw):
        ...

    @abstractmethod
    def _add_data_asset(self, item, layer_map):
        ...

    @abstractmethod
    def _add_extensions(self, item, layer_map, **kw):
        ...

    def _add_thumbnail(self, item, state, district, block,
                       layer_name, layer_map, **kw):
        start_year = kw.get("start_year", "")
        fname = self._thumbnail_filename(state, district, block, layer_name, start_year)
        path = os.path.join(self.config.thumbnail_dir, fname)
        url = self.geoserver.wms_thumbnail_url(self._ws, self._gs_layer, item.bbox)
        if self.geoserver.download_thumbnail(url, path):
            self._add_thumbnail_asset(item, path)
        return item

    @staticmethod
    def _add_style_asset(item, layer_map):
        item.add_asset(
            "style",
            pystac.Asset(
                href=layer_map["style_file_url"],
                media_type=pystac.MediaType.XML,
                roles=["metadata"],
                title="QGIS Style file",
            ),
        )
        return item

    def _add_thumbnail_asset(self, item, thumbnail_path):
        item.add_asset(
            "thumbnail",
            pystac.Asset(
                href=os.path.join(
                    self.config.thumbnail_data_url,
                    os.path.relpath(thumbnail_path, start=self.config.local_data_dir),
                ),
                media_type=pystac.MediaType.PNG,
                roles=["thumbnail"],
                title="Thumbnail",
            ),
        )
        return item

    @staticmethod
    def _item_id(state, district, block, layer_name, start_year=""):
        parts = [state, district, block, layer_name]
        if start_year:
            parts.append(start_year)
        return "_".join(parts)

    @staticmethod
    def _layer_title(display_name, start_year=""):
        if start_year:
            return f"{display_name} : {start_year}"
        return display_name

    @staticmethod
    def _thumbnail_filename(state, district, block, layer_name, start_year=""):
        base = f"{state}_{district}_{block}_{layer_name}"
        if start_year:
            base += f"_{start_year}"
        return base + ".png"


class RasterSTACItemBuilder(BaseSTACItemBuilder):

    def _create_item(self, state, district, block, layer_name,
                     description, layer_map, **kw):
        start_year = kw.get("start_year", "")
        ws, gs_layer = layer_map["workspace"], layer_map["layer_name"]

        desc_url = self.geoserver.raster_describe_url(ws, gs_layer)
        meta = self.geoserver.fetch_raster_metadata(desc_url)
        if meta is None:
            print(f"STAC_Error: Could not fetch metadata for {layer_name}")
            return None

        bbox, footprint, crs, shape = meta
        self._ws = ws
        self._gs_layer = gs_layer

        props = {
            "title": self._layer_title(layer_map["display_name"], start_year),
            "description": description,
            "gsd": layer_map["gsd"],
            "keywords": [layer_map["theme"]],
        }
        if start_year:
            sd = pd.to_datetime(f"{start_year}-{constants.AGRI_YEAR_START_DATE}")
            ed = pd.to_datetime(f"{int(start_year)+1}-{constants.AGRI_YEAR_END_DATE}")
            props["start_datetime"] = sd.isoformat() + "Z"
            props["end_datetime"] = ed.isoformat() + "Z"
        else:
            props["start_datetime"] = constants.DEFAULT_START_DATE.strftime("%Y-%m-%dT%H:%M:%SZ")
            props["end_datetime"] = constants.DEFAULT_END_DATE.strftime("%Y-%m-%dT%H:%M:%SZ")

        item = pystac.Item(
            id=self._item_id(state, district, block, layer_name, start_year),
            geometry=footprint, bbox=bbox,
            datetime=datetime.datetime.now(datetime.timezone.utc),
            properties=props,
        )

        proj = pystac.extensions.projection.ProjectionExtension.ext(
            item, add_if_missing=True
        )
        proj.epsg = crs
        proj.shape = shape
        return item

    def _add_data_asset(self, item, layer_map):
        url = self.geoserver.raster_data_url(layer_map["workspace"], layer_map["layer_name"])
        item.add_asset(
            "data",
            pystac.Asset(href=url, media_type=pystac.MediaType.GEOTIFF,
                         roles=["data"], title="Raster Layer"),
        )
        return item

    def _add_extensions(self, item, layer_map, **kw):
        style_classes = self.style_parser.parse_raster_style(layer_map["style_file_url"])
        cls_ext = pystac.extensions.classification.ClassificationExtension.ext(
            item.assets["data"], add_if_missing=True
        )
        cls_ext.classes = [
            pystac.extensions.classification.Classification.create(
                value=int(c["value"]),
                name=c.get("label") or f"Class {c['value']}",
                description=c.get("label"),
                color_hint=c["color"].replace("#", ""),
            )
            for c in style_classes
        ]
        return item


class VectorSTACItemBuilder(BaseSTACItemBuilder):

    def _create_item(self, state, district, block, layer_name,
                     description, layer_map, **kw):
        ws, gs_layer = layer_map["workspace"], layer_map["layer_name"]

        meta = self.geoserver.fetch_vector_metadata(ws, gs_layer)
        if meta is None:
            print(f"STAC_Error: Could not fetch metadata for {layer_name}")
            return None

        bbox, footprint, columns = meta
        self._ws = ws
        self._gs_layer = gs_layer
        self._columns = columns

        item = pystac.Item(
            id=self._item_id(state, district, block, layer_name),
            geometry=footprint, bbox=bbox,
            datetime=datetime.datetime.now(datetime.timezone.utc),
            properties={
                "title": layer_map["display_name"],
                "description": description,
                "start_datetime": constants.DEFAULT_START_DATE.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_datetime": constants.DEFAULT_END_DATE.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "keywords": [layer_map["theme"]],
            },
        )
        return item

    def _add_data_asset(self, item, layer_map):
        url = self.geoserver.vector_data_url(layer_map["workspace"], layer_map["layer_name"])
        item.add_asset(
            "data",
            pystac.Asset(href=url, media_type=pystac.MediaType.GEOJSON,
                         roles=["data"], title="Vector Layer"),
        )
        return item

    def _add_extensions(self, item, layer_map, **kw):
        col_desc = self.metadata.get_vector_column_descriptions(
            layer_map["ee_layer_name"], kw.get("overwrite", False),
        )
        desc_map = dict(zip(col_desc["column_name"], col_desc["column_description"]))

        tbl = pystac.extensions.table.TableExtension.ext(item, add_if_missing=True)
        tbl.columns = [
            {
                "name": col["name"],
                "type": col["type"],
                "description": desc_map.get(col["name"], ""),
            }
            for col in self._columns
        ]
        return item


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

class STACCollectionGenerator:
    def __init__(self, config=None):
        self.config = config or STACConfig()
        self.geoserver = GeoServerClient(
            self.config.geoserver_base_url,
            username=GEOSERVER_USERNAME,
            password=GEOSERVER_PASSWORD,
        )
        self.metadata = MetadataProvider(self.config)
        self.style_parser = StyleParser(self.config.style_file_dir)
        self.catalog_mgr = CatalogManager(self.config)
        self.s3_syncer = S3Syncer(S3_ACCESS_KEY, S3_SECRET_KEY)

    def _builder_args(self):
        return (self.config, self.geoserver, self.metadata, self.style_parser)

    def generate_raster(self, state, district, block, layer_name,
                        start_year="", upload_to_s3=False, overwrite=False):
        state, district, block = (sanitize_text(x.lower()) for x in (state, district, block))
        builder = RasterSTACItemBuilder(*self._builder_args())
        item = builder.build(state, district, block, layer_name,
                             overwrite=overwrite, start_year=start_year)
        if item is None:
            return False
        self.catalog_mgr.update(state, district, block, item)
        if upload_to_s3:
            self._sync_s3()
        return True

    def generate_vector(self, state, district, block, layer_name,
                        upload_to_s3=False, overwrite=False):
        state, district, block = (sanitize_text(x.lower()) for x in (state, district, block))
        builder = VectorSTACItemBuilder(*self._builder_args())
        item = builder.build(state, district, block, layer_name, overwrite=overwrite)
        if item is None:
            return False
        self.catalog_mgr.update(state, district, block, item)
        if upload_to_s3:
            self._sync_s3()
        return True

    def _sync_s3(self):
        self.s3_syncer.sync(self.config.stac_files_dir, self.config.s3_uri)
        self.s3_syncer.sync(self.config.thumbnail_dir, self.config.s3_uri)


# ---------------------------------------------------------------------------
# Celery task entry point
# ---------------------------------------------------------------------------

@app.task(bind=True)
def generate_stac_collection_task(self, layer_type, state, district, block,
                                  layer_name, start_year="",
                                  upload_to_s3=False, overwrite=False):
    generator = STACCollectionGenerator()
    if layer_type == "raster":
        return generator.generate_raster(
            state, district, block, layer_name,
            start_year=start_year, upload_to_s3=upload_to_s3, overwrite=overwrite,
        )
    elif layer_type == "vector":
        return generator.generate_vector(
            state, district, block, layer_name,
            upload_to_s3=upload_to_s3, overwrite=overwrite,
        )
    else:
        raise ValueError(f"Unknown layer_type: {layer_type}")
