"""Focused tests for the finalized local-pipeline output contract."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from computing.misc.local_pipeline.outputs import OutputBundle, frame_profile
from computing.misc.local_pipeline.publish import publish_gpkg_layers
from computing.misc.local_pipeline.schema import OutputOptions
from computing.misc.local_pipeline.unicode import (
    normalize_unicode_frame,
    normalize_unicode_text,
)


class OutputContractTests(unittest.TestCase):
    def test_output_options_have_no_csv_or_stac_contract(self):
        options = OutputOptions.from_mapping({"csv": True, "stac": True})
        self.assertFalse(hasattr(options, "csv"))
        self.assertFalse(hasattr(options, "stac"))
        self.assertFalse(hasattr(OutputBundle, "write_csv"))

    def test_unicode_normalization_preserves_indic_and_duplicate_columns(self):
        frame = pd.DataFrame([["हिंदी\x00", "অসমীয়া", "bad\ud800"]], columns=["text", "text", "other"])
        normalized = normalize_unicode_frame(frame)
        self.assertEqual(list(normalized.columns), ["text", "text", "other"])
        self.assertEqual(normalized.iloc[0, 0], "हिंदी")
        self.assertEqual(normalized.iloc[0, 1], "অসমীয়া")
        self.assertEqual(normalized.iloc[0, 2], "bad�")
        self.assertEqual(normalize_unicode_text("தமிழ்"), "தமிழ்")

    def test_metadata_includes_column_descriptions_and_rename_mapping(self):
        frame = pd.DataFrame({"l3_phc_distance_km": [1.25], "village_name": ["पलनपुर"]})
        profile = frame_profile(
            frame,
            {"l3_phc_distance_km": "Distance to the nearest PHC."},
            {"l3_phc_distance_km": "nearest_phc_distance_km"},
        )
        self.assertEqual(
            profile["column_rename_mapping"],
            {"l3_phc_distance_km": "nearest_phc_distance_km"},
        )
        self.assertEqual(profile["columns"][0]["rename_to"], "nearest_phc_distance_km")
        self.assertIn("eda", profile)

    def test_links_manifest_is_single_utf8_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            bundle = OutputBundle(temporary, "demo")
            path = bundle.write_links({"local": {"title": "सुविधाएँ"}})
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["local"]["title"], "सुविधाएँ")
            self.assertEqual(path.name, "demo.links.json")

    @patch("computing.misc.local_pipeline.publish.geoserver_wms_url", side_effect=lambda w, n: f"wms:{w}:{n}")
    @patch("computing.misc.local_pipeline.publish.geoserver_wfs_url", side_effect=lambda w, n: f"wfs:{w}:{n}")
    @patch("computing.misc.local_pipeline.publish.verify_wfs_layer")
    @patch("computing.misc.local_pipeline.publish._publish_feature_type")
    @patch("computing.misc.local_pipeline.publish._upload_gpkg_store")
    @patch("computing.misc.local_pipeline.publish.delete_datastore")
    @patch("computing.misc.local_pipeline.publish.ensure_workspace_ready")
    @patch("computing.misc.local_pipeline.publish._gpkg_row_count", return_value=2)
    @patch("computing.misc.local_pipeline.publish._gpkg_columns", return_value=["facility_uid", "name"])
    @patch("computing.misc.local_pipeline.publish._resolve_source_layer", side_effect=lambda _p, source: source)
    def test_multi_layer_publish_uploads_one_store_and_verifies_each_layer(
        self,
        _resolve,
        _columns,
        _count,
        _workspace,
        _delete,
        upload,
        publish,
        verify,
        _wfs,
        _wms,
    ):
        upload.return_value = {"ok": True}
        publish.return_value = {"ok": True}
        verify.return_value = {
            "feature_count": 2,
            "properties": ["facility_uid", "name"],
        }
        with tempfile.TemporaryDirectory() as temporary:
            gpkg = Path(temporary) / "points.gpkg"
            gpkg.touch()
            results = publish_gpkg_layers(
                gpkg,
                workspace="testworkspace",
                store_name="facilities_points",
                layers={
                    "facilities_tehsil": "tehsil_facility_collection",
                    "facilities_nearest": "village_nearest_facility_collection",
                },
            )
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(publish.call_count, 2)
        self.assertEqual(verify.call_count, 2)
        self.assertEqual(set(results), {"facilities_tehsil", "facilities_nearest"})


if __name__ == "__main__":
    unittest.main()
