"""Focused tests for the Core Stack GeoLibre output adapter."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from computing.misc.local_pipeline.admin import AdminScope
from computing.misc.local_pipeline.geolibre import (
    GeoLibreAWSOptions,
    GeoLibreOptions,
    GeoServerFeatureType,
    create_geolibre_outputs,
    parse_wfs_capabilities,
    publish_artifacts_to_s3,
    remove_geolibre_outputs,
    select_scope_feature_types,
)
from computing.misc.local_pipeline.schema import OutputOptions, api_request_payload


WFS_URL = (
    "https://geo.example/geoserver/testworkspace/ows?service=WFS&version=1.0.0"
    "&request=GetFeature&typeName=testworkspace:facilities_banas_kantha_palanpur"
    "&outputFormat=application/json"
)
SCOPE = AdminScope(
    level="tehsil",
    state_name="gujarat",
    district_name="banas kantha",
    tehsil_name="palanpur",
)


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        return {"ETag": '"test-etag"'}


class GeoLibreTests(unittest.TestCase):
    def test_default_output_and_simple_api_aliases(self):
        self.assertTrue(OutputOptions().geolibre)
        payload = api_request_payload(
            {
                "state": "gujarat",
                "district": "banas kantha",
                "block": "palanpur",
                "include_tehsil_layers": True,
                "publish_to_aws": False,
            }
        )
        self.assertTrue(payload["geolibre"]["include_tehsil_layers"])
        self.assertFalse(payload["geolibre"]["publish_to_aws"])

    def test_request_cannot_override_aws_destination(self):
        options = GeoLibreOptions.from_mappings(
            {
                "aws": {
                    "bucket": "trusted-bucket",
                    "prefix": "trusted-prefix",
                    "endpoint_url": "https://s3.ap-south-1.amazonaws.com",
                }
            },
            {
                "aws": {
                    "enabled": True,
                    "bucket": "caller-bucket",
                    "prefix": "caller-prefix",
                    "endpoint_url": "http://127.0.0.1:9999",
                }
            },
        )
        self.assertTrue(options.aws.enabled)
        self.assertEqual(options.aws.bucket, "trusted-bucket")
        self.assertEqual(options.aws.prefix, "trusted-prefix")
        self.assertEqual(
            options.aws.endpoint_url, "https://s3.ap-south-1.amazonaws.com"
        )

    def test_capabilities_parser_and_scope_selection(self):
        xml = b"""<?xml version='1.0'?>
        <wfs:WFS_Capabilities xmlns:wfs='http://www.opengis.net/wfs/2.0'
          xmlns:ows='http://www.opengis.net/ows/1.1'>
          <wfs:FeatureTypeList>
            <wfs:FeatureType><wfs:Name>testworkspace:facilities_banas_kantha_palanpur</wfs:Name>
              <wfs:Title>Facilities</wfs:Title><ows:WGS84BoundingBox>
                <ows:LowerCorner>72.1 23.8</ows:LowerCorner><ows:UpperCorner>72.9 24.5</ows:UpperCorner>
              </ows:WGS84BoundingBox></wfs:FeatureType>
            <wfs:FeatureType><wfs:Name>testworkspace:livestock_banas_kantha_palanpur</wfs:Name>
              <wfs:Title>Livestock</wfs:Title></wfs:FeatureType>
            <wfs:FeatureType><wfs:Name>testworkspace:facilities_dumka_masalia</wfs:Name></wfs:FeatureType>
            <wfs:FeatureType><wfs:Name>testworkspace:facilities_other_district_palanpur</wfs:Name></wfs:FeatureType>
          </wfs:FeatureTypeList>
        </wfs:WFS_Capabilities>"""
        parsed = parse_wfs_capabilities(xml)
        self.assertEqual(len(parsed), 4)
        self.assertEqual(parsed[0].bbox, (72.1, 23.8, 72.9, 24.5))
        selected = select_scope_feature_types(
            parsed,
            current_qualified_name="testworkspace:facilities_banas_kantha_palanpur",
            scope=SCOPE,
            include_scope_layers=True,
            max_layers=10,
        )
        self.assertEqual(
            {item.qualified_name for item in selected},
            {
                "testworkspace:facilities_banas_kantha_palanpur",
                "testworkspace:livestock_banas_kantha_palanpur",
            },
        )

    def test_local_outputs_reference_wfs_without_embedding_features(self):
        features = [
            GeoServerFeatureType(
                qualified_name="testworkspace:facilities_banas_kantha_palanpur",
                workspace="testworkspace",
                layer_name="facilities_banas_kantha_palanpur",
                title="Facilities",
                bbox=(72.1, 23.8, 72.9, 24.5),
            ),
            GeoServerFeatureType(
                qualified_name="testworkspace:livestock_banas_kantha_palanpur",
                workspace="testworkspace",
                layer_name="livestock_banas_kantha_palanpur",
                title="Livestock",
                bbox=(72.2, 23.9, 72.8, 24.4),
            ),
        ]
        with tempfile.TemporaryDirectory() as temporary, patch(
            "computing.misc.local_pipeline.geolibre.fetch_wfs_feature_types",
            return_value=features,
        ):
            result = create_geolibre_outputs(
                output_dir=temporary,
                output_name="facilities_banas_kantha_palanpur",
                scope=SCOPE,
                geoserver={
                    "workspace": "testworkspace",
                    "layer_name": "facilities_banas_kantha_palanpur",
                    "wfs_url": WFS_URL,
                },
                requested={"include_tehsil_layers": True},
            )
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["layer_count"], 2)
            project = json.loads(Path(result["project_path"]).read_text())
            self.assertIn("bbox", project["mapView"])
            for layer in project["layers"]:
                self.assertNotIn("geojson", layer)
                self.assertNotIn("embeddedGeoJSON", layer["metadata"])
                self.assertEqual(layer["metadata"]["sourceKind"], "maplibre-gl-vector")
                self.assertIn("request=GetFeature", layer["source"]["url"])
            html_text = Path(result["html_path"]).read_text()
            self.assertIn("geolibre:load-project", html_text)
            self.assertIn("embed=1&amp;welcome=0", html_text)

    def test_s3_upload_is_canonical_json_only_by_default(self):
        fake = FakeS3Client()
        with tempfile.TemporaryDirectory() as temporary:
            project_path = Path(temporary) / "map.geolibre.json"
            html_path = Path(temporary) / "map.geolibre.html"
            project_path.write_text("{}\n")
            html_path.write_text("<html></html>")
            result = publish_artifacts_to_s3(
                project_path=project_path,
                html_path=html_path,
                scope=SCOPE,
                options=GeoLibreAWSOptions(
                    enabled=True,
                    bucket="corestack-geolibre",
                    public_base_url="https://maps.example.org",
                ),
                viewer_url="https://web.geolibre.app/",
                s3_client=fake,
            )
        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0]["ContentType"], "application/json; charset=utf-8")
        self.assertIn("gujarat/banas_kantha/palanpur/map.geolibre.json", result["project_key"])
        self.assertIn("web.geolibre.app", result["viewer_url"])
        self.assertIn("maps.example.org", result["viewer_url"])

    def test_optional_aws_failure_keeps_local_outputs(self):
        current = GeoServerFeatureType(
            qualified_name="testworkspace:facilities_banas_kantha_palanpur",
            workspace="testworkspace",
            layer_name="facilities_banas_kantha_palanpur",
            title="Facilities",
        )
        with tempfile.TemporaryDirectory() as temporary, patch(
            "computing.misc.local_pipeline.geolibre.fetch_wfs_feature_types",
            return_value=[current],
        ):
            result = create_geolibre_outputs(
                output_dir=temporary,
                output_name=current.layer_name,
                scope=SCOPE,
                geoserver={
                    "workspace": current.workspace,
                    "layer_name": current.layer_name,
                    "wfs_url": WFS_URL,
                },
                requested={"publish_to_aws": True},
            )
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["aws"]["status"], "upload_failed")
            self.assertTrue(Path(result["project_path"]).exists())
            self.assertTrue(Path(result["html_path"]).exists())

    def test_stale_local_outputs_are_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary) / "map.geolibre.json"
            launcher = Path(temporary) / "map.geolibre.html"
            project.write_text("{}\n")
            launcher.write_text("<html></html>")
            removed = remove_geolibre_outputs(temporary, "map")
            self.assertEqual(set(removed), {project.as_posix(), launcher.as_posix()})
            self.assertFalse(project.exists())
            self.assertFalse(launcher.exists())


if __name__ == "__main__":
    unittest.main()
