import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pystac

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
import django
django.setup()

from computing.STAC_specs.stac_collection import (
    CatalogManager,
    GeoServerClient,
    MetadataProvider,
    RasterSTACItemBuilder,
    STACCollectionGenerator,
    STACConfig,
    VectorSTACItemBuilder,
    sanitize_text,
)


FAKE_BBOX = [85.155, 25.010, 85.525, 25.430]
FAKE_FOOTPRINT = {
    "type": "Polygon",
    "coordinates": [[
        [85.155, 25.010], [85.155, 25.430],
        [85.525, 25.430], [85.525, 25.010],
        [85.155, 25.010],
    ]],
}
FAKE_VECTOR_COLUMNS = [
    {"name": "the_geom", "type": "geometry"},
    {"name": "District", "type": "object"},
    {"name": "population", "type": "int64"},
    {"name": "area_sqkm", "type": "float64"},
]
FAKE_LAYER_MAP = {
    "workspace": "test_ws",
    "layer_name": "nalanda_hilsa",
    "style_file_url": "https://example.com/style.qml",
    "display_name": "Test Layer",
    "ee_layer_name": "test_layer",
    "gsd": 10,
    "theme": "water",
}
FAKE_RASTER_STYLE_CLASSES = [
    {"value": 1, "label": "Water", "color": "#0000FF"},
    {"value": 2, "label": "Forest", "color": "#00FF00"},
    {"value": 3, "label": "Urban", "color": "#FF0000"},
]
FAKE_COLUMN_DESC_DF = pd.DataFrame({
    "ee_layer_name": ["test_layer"] * 4,
    "column_name": ["the_geom", "District", "population", "area_sqkm"],
    "column_name_description": ["geometry", "district name", "total population", "area in sq km"],
})


def _make_config(tmp_dir):
    return STACConfig(
        geoserver_base_url="https://fake-geoserver/geoserver",
        thumbnail_data_url="https://fake-s3.example.com/",
        local_data_dir=tmp_dir,
        stac_files_dir=os.path.join(tmp_dir, "catalogs"),
        thumbnail_dir=os.path.join(tmp_dir, "thumbnails"),
        style_file_dir=os.path.join(tmp_dir, "styles"),
        tehsil_dirname="tehsil_wise",
        s3_bucket_name="fake-bucket",
        s3_uri="s3://fake-bucket/",
        layer_map_csv=os.path.join(tmp_dir, "layer_mapping.csv"),
        layer_desc_csv=os.path.join(tmp_dir, "layer_descriptions.csv"),
        column_desc_csv=os.path.join(tmp_dir, "column_descriptions.csv"),
    )


def _mock_geoserver():
    gs = MagicMock(spec=GeoServerClient)
    gs.fetch_vector_metadata.return_value = (FAKE_BBOX, FAKE_FOOTPRINT, FAKE_VECTOR_COLUMNS)
    gs.fetch_raster_metadata.return_value = (FAKE_BBOX, FAKE_FOOTPRINT, "EPSG:4326", [100, 200])
    gs.wms_thumbnail_url.return_value = "https://fake-geoserver/wms?fake=1"
    gs.download_thumbnail.return_value = True
    gs.raster_describe_url.return_value = "https://fake-geoserver/wcs?describe"
    gs.raster_data_url.return_value = "https://fake-geoserver/wcs?getdata"
    gs.vector_data_url.return_value = "https://fake-geoserver/wfs?getfeature"
    return gs


def _mock_metadata():
    meta = MagicMock(spec=MetadataProvider)
    meta.get_layer_description.return_value = "A test layer description"
    meta.get_layer_mapping.return_value = FAKE_LAYER_MAP.copy()
    meta.get_vector_column_descriptions.return_value = FAKE_COLUMN_DESC_DF.rename(
        {"column_name_description": "column_description"}, axis=1,
    )
    return meta


def _mock_style_parser():
    sp = MagicMock()
    sp.parse_raster_style.return_value = FAKE_RASTER_STYLE_CLASSES
    return sp


class TestSanitizeText(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(sanitize_text("hello world"), "hello_world")

    def test_special_chars(self):
        self.assertEqual(sanitize_text("Bihar(East)"), "BiharEast")

    def test_preserves_allowed(self):
        self.assertEqual(sanitize_text("a-b_c.d"), "a-b_c.d")


class TestGeoServerClient(unittest.TestCase):
    def setUp(self):
        self.client = GeoServerClient("https://gs.example.com/geoserver", "user", "pass")

    def test_raster_describe_url(self):
        url = self.client.raster_describe_url("ws", "layer")
        self.assertIn("service=WCS", url)
        self.assertIn("DescribeCoverage", url)
        self.assertIn("ws:layer", url)

    def test_vector_data_url(self):
        url = self.client.vector_data_url("ws", "layer")
        self.assertIn("service=WFS", url)
        self.assertIn("ws:layer", url)

    def test_wms_thumbnail_url(self):
        url = self.client.wms_thumbnail_url("ws", "layer", [1, 2, 3, 4])
        self.assertIn("service=WMS", url)
        self.assertIn("bbox=1,2,3,4", url)
        self.assertIn("ws:layer", url)

    @patch("computing.STAC_specs.stac_collection.requests.get")
    def test_fetch_vector_metadata_success(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "featureType": {
                    "latLonBoundingBox": {
                        "minx": 85.0, "miny": 25.0, "maxx": 86.0, "maxy": 26.0,
                    },
                    "attributes": {"attribute": [
                        {"name": "geom", "binding": "org.locationtech.jts.geom.MultiPolygon"},
                        {"name": "name", "binding": "java.lang.String"},
                        {"name": "pop", "binding": "java.lang.Integer"},
                    ]},
                }
            },
        )
        result = self.client.fetch_vector_metadata("ws", "layer")
        self.assertIsNotNone(result)
        bbox, footprint, columns = result
        self.assertEqual(bbox, [85.0, 25.0, 86.0, 26.0])
        self.assertEqual(footprint["type"], "Polygon")
        self.assertEqual(len(columns), 3)
        self.assertEqual(columns[0]["type"], "geometry")
        self.assertEqual(columns[1]["type"], "object")
        self.assertEqual(columns[2]["type"], "int64")

    @patch("computing.STAC_specs.stac_collection.requests.get")
    def test_fetch_vector_metadata_404(self, mock_get):
        mock_get.return_value = MagicMock(status_code=404)
        self.assertIsNone(self.client.fetch_vector_metadata("ws", "missing"))

    @patch("computing.STAC_specs.stac_collection.requests.get")
    def test_fetch_raster_metadata_success(self, mock_get):
        xml_body = b"""<?xml version="1.0" encoding="UTF-8"?>
        <wcs:CoverageDescriptions xmlns:gml="http://www.opengis.net/gml/3.2"
                                   xmlns:wcs="http://www.opengis.net/wcs/2.0">
          <wcs:CoverageDescription>
            <gml:boundedBy>
              <gml:Envelope srsName="EPSG:4326">
                <gml:lowerCorner>25.0 85.0</gml:lowerCorner>
                <gml:upperCorner>26.0 86.0</gml:upperCorner>
              </gml:Envelope>
            </gml:boundedBy>
            <gml:domainSet>
              <gml:RectifiedGrid>
                <gml:limits>
                  <gml:GridEnvelope>
                    <gml:low>0 0</gml:low>
                    <gml:high>99 199</gml:high>
                  </gml:GridEnvelope>
                </gml:limits>
              </gml:RectifiedGrid>
            </gml:domainSet>
          </wcs:CoverageDescription>
        </wcs:CoverageDescriptions>"""
        mock_get.return_value = MagicMock(status_code=200, content=xml_body)
        result = self.client.fetch_raster_metadata("https://gs/wcs?describe")
        self.assertIsNotNone(result)
        bbox, footprint, crs, shape = result
        self.assertEqual(bbox, [85.0, 25.0, 86.0, 26.0])
        self.assertEqual(crs, "EPSG:4326")
        self.assertEqual(shape, [200, 100])


class TestVectorSTACItemBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = _make_config(self.tmp)
        self.gs = _mock_geoserver()
        self.meta = _mock_metadata()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_build_returns_valid_item(self):
        builder = VectorSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_vector")

        self.assertIsNotNone(item)
        self.assertIsInstance(item, pystac.Item)
        self.assertEqual(item.id, "bihar_nalanda_hilsa_test_vector")
        self.assertEqual(item.bbox, FAKE_BBOX)

    def test_item_has_required_assets(self):
        builder = VectorSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_vector")

        self.assertIn("data", item.assets)
        self.assertIn("style", item.assets)
        self.assertIn("thumbnail", item.assets)
        self.assertEqual(item.assets["data"].media_type, pystac.MediaType.GEOJSON)

    def test_item_has_table_extension(self):
        builder = VectorSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_vector")

        self.assertIn(
            "https://stac-extensions.github.io/table/",
            " ".join(item.stac_extensions),
        )
        cols = item.properties.get("table:columns", [])
        self.assertEqual(len(cols), len(FAKE_VECTOR_COLUMNS))
        col_names = [c["name"] for c in cols]
        self.assertIn("District", col_names)

    def test_item_properties(self):
        builder = VectorSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_vector")

        self.assertEqual(item.properties["title"], "Test Layer")
        self.assertEqual(item.properties["description"], "A test layer description")
        self.assertIn("water", item.properties["keywords"])

    def test_returns_none_when_metadata_fails(self):
        self.gs.fetch_vector_metadata.return_value = None
        builder = VectorSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_vector")
        self.assertIsNone(item)


class TestRasterSTACItemBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = _make_config(self.tmp)
        self.gs = _mock_geoserver()
        self.meta = _mock_metadata()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_build_returns_valid_item(self):
        builder = RasterSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_raster")

        self.assertIsNotNone(item)
        self.assertEqual(item.id, "bihar_nalanda_hilsa_test_raster")
        self.assertEqual(item.bbox, FAKE_BBOX)

    def test_item_has_projection_extension(self):
        builder = RasterSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_raster")

        self.assertIn(
            "https://stac-extensions.github.io/projection/",
            " ".join(item.stac_extensions),
        )

    def test_item_has_classification_extension(self):
        builder = RasterSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_raster")

        data_asset = item.assets["data"]
        self.assertIn("data", item.assets)
        self.assertEqual(data_asset.media_type, pystac.MediaType.GEOTIFF)
        cls_ext = pystac.extensions.classification.ClassificationExtension.ext(data_asset)
        self.assertEqual(len(cls_ext.classes), 3)
        self.assertEqual(cls_ext.classes[0].name, "Water")

    def test_with_start_year(self):
        builder = RasterSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_raster", start_year="2023")

        self.assertEqual(item.id, "bihar_nalanda_hilsa_test_raster_2023")
        self.assertIn("2023", item.properties["start_datetime"])
        self.assertIn("2024", item.properties["end_datetime"])

    def test_returns_none_when_metadata_fails(self):
        self.gs.fetch_raster_metadata.return_value = None
        builder = RasterSTACItemBuilder(self.config, self.gs, self.meta, _mock_style_parser())
        item = builder.build("bihar", "nalanda", "hilsa", "test_raster")
        self.assertIsNone(item)


class TestCatalogManager(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = _make_config(self.tmp)
        self.mgr = CatalogManager(self.config)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _dummy_item(self, item_id="test_item"):
        return pystac.Item(
            id=item_id,
            geometry=FAKE_FOOTPRINT,
            bbox=FAKE_BBOX,
            datetime=None,
            properties={"title": "Test", "start_datetime": "2020-01-01T00:00:00Z",
                         "end_datetime": "2024-01-01T00:00:00Z"},
        )

    def test_creates_full_hierarchy(self):
        item = self._dummy_item()
        self.mgr.update("bihar", "nalanda", "hilsa", item)

        root_cat = os.path.join(self.config.stac_files_dir, "catalog.json")
        tehsil_cat = os.path.join(self.config.stac_files_dir, "tehsil_wise", "catalog.json")
        state_coll = os.path.join(self.config.stac_files_dir, "tehsil_wise", "bihar", "collection.json")
        dist_coll = os.path.join(self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "collection.json")
        block_coll = os.path.join(self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "hilsa", "collection.json")

        for p in [root_cat, tehsil_cat, state_coll, dist_coll, block_coll]:
            self.assertTrue(os.path.exists(p), f"Missing: {p}")

        root = pystac.read_file(root_cat)
        self.assertEqual(root.id, "corestack_STAC")

        block = pystac.read_file(block_coll)
        items = list(block.get_all_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "test_item")

    def test_upsert_adds_second_item(self):
        self.mgr.update("bihar", "nalanda", "hilsa", self._dummy_item("item_1"))
        self.mgr.update("bihar", "nalanda", "hilsa", self._dummy_item("item_2"))

        block_coll_path = os.path.join(
            self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "hilsa", "collection.json",
        )
        block = pystac.read_file(block_coll_path)
        items = list(block.get_all_items())
        ids = {i.id for i in items}
        self.assertEqual(ids, {"item_1", "item_2"})

    def test_upsert_replaces_existing_item(self):
        self.mgr.update("bihar", "nalanda", "hilsa", self._dummy_item("item_1"))
        self.mgr.update("bihar", "nalanda", "hilsa", self._dummy_item("item_1"))

        block_coll_path = os.path.join(
            self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "hilsa", "collection.json",
        )
        block = pystac.read_file(block_coll_path)
        items = list(block.get_all_items())
        self.assertEqual(len(items), 1)

    def test_merge_bboxes(self):
        a = [1, 2, 3, 4]
        b = [0, 1, 5, 6]
        self.assertEqual(CatalogManager._merge_bboxes(a, b), [0, 1, 5, 6])

    def test_merge_bboxes_empty(self):
        self.assertEqual(CatalogManager._merge_bboxes([0, 0, 0, 0], [1, 2, 3, 4]), [1, 2, 3, 4])

    def test_all_json_is_valid_stac(self):
        self.mgr.update("bihar", "nalanda", "hilsa", self._dummy_item())
        for root, _, files in os.walk(self.config.stac_files_dir):
            for f in files:
                if f.endswith(".json"):
                    path = os.path.join(root, f)
                    with open(path) as fp:
                        data = json.load(fp)
                    self.assertIn("type", data)
                    self.assertIn(data["type"], ("Catalog", "Collection", "Feature"))
                    if data["type"] in ("Catalog", "Collection"):
                        self.assertIn("links", data)


class TestSTACCollectionGeneratorVector(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = _make_config(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @patch("computing.STAC_specs.stac_collection.GEOSERVER_USERNAME", "user")
    @patch("computing.STAC_specs.stac_collection.GEOSERVER_PASSWORD", "pass")
    @patch("computing.STAC_specs.stac_collection.S3_ACCESS_KEY", "key")
    @patch("computing.STAC_specs.stac_collection.S3_SECRET_KEY", "secret")
    @patch.object(GeoServerClient, "fetch_vector_metadata", return_value=(FAKE_BBOX, FAKE_FOOTPRINT, FAKE_VECTOR_COLUMNS))
    @patch.object(GeoServerClient, "download_thumbnail", return_value=True)
    @patch.object(MetadataProvider, "get_layer_description", return_value="desc")
    @patch.object(MetadataProvider, "get_layer_mapping", return_value=FAKE_LAYER_MAP)
    @patch.object(MetadataProvider, "get_vector_column_descriptions")
    def test_end_to_end_vector(self, mock_col_desc, mock_mapping, mock_desc,
                                mock_thumb, mock_meta):
        mock_col_desc.return_value = FAKE_COLUMN_DESC_DF.rename(
            {"column_name_description": "column_description"}, axis=1,
        )

        gen = STACCollectionGenerator(config=self.config)
        result = gen.generate_vector(
            state="Bihar", district="Nalanda", block="Hilsa",
            layer_name="test_vector",
        )
        self.assertTrue(result)

        root_path = os.path.join(self.config.stac_files_dir, "catalog.json")
        self.assertTrue(os.path.exists(root_path))

        block_dir = os.path.join(
            self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "hilsa",
        )
        self.assertTrue(os.path.isdir(block_dir))

        item_dir = os.path.join(block_dir, "bihar_nalanda_hilsa_test_vector")
        item_files = [f for f in os.listdir(item_dir) if f.endswith(".json")]
        self.assertEqual(len(item_files), 1)

        with open(os.path.join(item_dir, item_files[0])) as fp:
            item_data = json.load(fp)
        self.assertEqual(item_data["type"], "Feature")
        self.assertEqual(item_data["id"], "bihar_nalanda_hilsa_test_vector")
        self.assertIn("data", item_data["assets"])
        self.assertIn("style", item_data["assets"])


class TestSTACCollectionGeneratorRaster(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config = _make_config(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @patch("computing.STAC_specs.stac_collection.GEOSERVER_USERNAME", "user")
    @patch("computing.STAC_specs.stac_collection.GEOSERVER_PASSWORD", "pass")
    @patch("computing.STAC_specs.stac_collection.S3_ACCESS_KEY", "key")
    @patch("computing.STAC_specs.stac_collection.S3_SECRET_KEY", "secret")
    @patch.object(GeoServerClient, "fetch_raster_metadata",
                  return_value=(FAKE_BBOX, FAKE_FOOTPRINT, "EPSG:4326", [100, 200]))
    @patch.object(GeoServerClient, "download_thumbnail", return_value=True)
    @patch.object(MetadataProvider, "get_layer_description", return_value="desc")
    @patch.object(MetadataProvider, "get_layer_mapping", return_value=FAKE_LAYER_MAP)
    def test_end_to_end_raster(self, mock_mapping, mock_desc, mock_thumb, mock_meta):
        sp = _mock_style_parser()
        with patch("computing.STAC_specs.stac_collection.StyleParser", return_value=sp):
            gen = STACCollectionGenerator(config=self.config)
            result = gen.generate_raster(
                state="Bihar", district="Nalanda", block="Hilsa",
                layer_name="test_raster",
            )
        self.assertTrue(result)

        item_dir = os.path.join(
            self.config.stac_files_dir, "tehsil_wise", "bihar", "nalanda", "hilsa",
            "bihar_nalanda_hilsa_test_raster",
        )
        self.assertTrue(os.path.isdir(item_dir))

        item_files = [f for f in os.listdir(item_dir) if f.endswith(".json")]
        with open(os.path.join(item_dir, item_files[0])) as fp:
            item_data = json.load(fp)
        self.assertEqual(item_data["type"], "Feature")
        self.assertIn("data", item_data["assets"])
        self.assertEqual(item_data["assets"]["data"]["type"], "image/tiff; application=geotiff")
