from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from installation import public_api_client


class PublicAPIClientTests(unittest.TestCase):
    def sample_active_locations(self) -> list[dict]:
        return [
            {
                "label": "Assam",
                "state_id": "1",
                "district": [
                    {
                        "label": "Cachar",
                        "district_id": "11",
                        "blocks": [
                            {"label": "Lakhipur", "block_id": "111", "tehsil_id": "111"},
                            {"label": "Sonai", "block_id": "112", "tehsil_id": "112"},
                        ],
                    }
                ],
            }
        ]

    def test_normalize_base_url_appends_api_v1(self) -> None:
        self.assertEqual(
            public_api_client.normalize_base_url("https://geoserver.core-stack.org"),
            "https://geoserver.core-stack.org/api/v1",
        )

    def test_extract_mws_ids_preserves_unique_order(self) -> None:
        geojson = {
            "features": [
                {"properties": {"uid": "12_101"}},
                {"properties": {"uid": "12_102"}},
                {"properties": {"uid": "12_101"}},
            ]
        }
        self.assertEqual(public_api_client.extract_mws_ids(geojson), ["12_101", "12_102"])

    def test_resolve_location_uses_latlon_lookup_when_admin_args_missing(self) -> None:
        args = argparse.Namespace(
            state=None,
            district=None,
            tehsil=None,
            latitude=25.5,
            longitude=92.8,
            timeout=30,
        )

        with mock.patch.object(
            public_api_client,
            "request_json",
            return_value={"State": "Assam", "District": "Cachar", "Tehsil": "Lakhipur"},
        ) as request_json:
            location = public_api_client.resolve_location(
                args,
                api_key="secret",
                base_url="https://geoserver.core-stack.org/api/v1",
            )

        self.assertEqual(location["state"], "Assam")
        self.assertEqual(location["district"], "Cachar")
        self.assertEqual(location["tehsil"], "Lakhipur")
        request_json.assert_called_once()

    def test_runtime_config_reads_env_file_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            env_file = Path(tmp_dir) / ".env"
            env_file.write_text(
                '\n'.join(
                    [
                        'PUBLIC_API_X_API_KEY="abc123"',
                        'PUBLIC_API_BASE_URL="https://example.com"',
                    ]
                ),
                encoding="utf-8",
            )

            api_key, base_url = public_api_client.resolve_runtime_config(
                env_file=env_file,
                api_key=None,
                base_url=None,
            )

        self.assertEqual(api_key, "abc123")
        self.assertEqual(base_url, "https://example.com/api/v1")

    def test_matcher_backend_falls_back_when_optional_module_breaks(self) -> None:
        with mock.patch.object(
            public_api_client.importlib,
            "import_module",
            side_effect=ImportError("broken matcher"),
        ):
            exports, backend_name, backend_error = public_api_client._load_hierarchy_matching_backend()

        self.assertEqual(backend_name, "internal_fallback")
        self.assertIsNotNone(backend_error)
        self.assertEqual(exports["normalize_match_text"]("Andhra Pradesh"), "andhra pradesh")

        resolution = exports["resolve_best_candidate"](
            "kachar",
            ["Cachar", "Kamrup", "Dima Hasao"],
            limit=3,
            auto_accept_score=0.6,
            min_margin=0.01,
        )
        self.assertIsNotNone(resolution.best_match)
        self.assertEqual(resolution.best_match.candidate, "Cachar")

    def test_validate_active_location_returns_exact_path(self) -> None:
        active_locations = [
            {
                "label": "Bihar",
                "state_id": "1",
                "district": [
                    {
                        "label": "Jamui",
                        "district_id": "11",
                        "blocks": [{"label": "Jamui", "block_id": "111", "tehsil_id": "111"}],
                    }
                ],
            }
        ]

        matched = public_api_client.validate_active_location(
            active_locations=active_locations,
            state="bihar",
            district="jamui",
            tehsil="jamui",
        )

        self.assertEqual(matched.state, "Bihar")
        self.assertEqual(matched.district, "Jamui")
        self.assertEqual(matched.tehsil, "Jamui")

    def test_resolve_active_location_auto_accepts_high_confidence_fuzzy_match(self) -> None:
        active_locations = self.sample_active_locations()

        resolution = public_api_client.resolve_active_location(
            active_locations=active_locations,
            state="assam",
            district="kachar",
            tehsil="lakhipur",
        )

        self.assertEqual(resolution.path.district, "Cachar")
        self.assertEqual(resolution.matched_via, "active_locations_fuzzy")
        self.assertGreater(resolution.score, 0.9)

    def test_validate_active_location_surfaces_fuzzy_suggestions(self) -> None:
        active_locations = [
            {
                "label": "Bihar",
                "state_id": "1",
                "district": [
                    {
                        "label": "Jamui",
                        "district_id": "11",
                        "blocks": [{"label": "Jamui", "block_id": "111", "tehsil_id": "111"}],
                    }
                ],
            }
        ]

        with self.assertRaises(public_api_client.PublicAPIError) as context:
            public_api_client.validate_active_location(
                active_locations=active_locations,
                state="bihar",
                district="jamu",
                tehsil="jami",
                strict_location_match=True,
            )

        self.assertIn("Closest districts", str(context.exception))
        self.assertIn("Jamui", str(context.exception))

    def test_expand_requested_datasets_supports_bundle(self) -> None:
        datasets = public_api_client.expand_requested_datasets(
            raw_datasets=None,
            raw_streams=None,
            bundle="watersheds",
        )

        self.assertEqual(
            datasets,
            {"mws_geometries", "mws_data", "mws_kyl", "mws_report"},
        )

    def test_resolve_download_plan_expands_district_to_tehsils(self) -> None:
        args = argparse.Namespace(
            state="assam",
            district="kachar",
            tehsil=None,
            latitude=None,
            longitude=None,
            strict_location_match=False,
            tehsil_limit=None,
            allow_unlisted_location=False,
        )

        plan = public_api_client.resolve_download_plan(
            args,
            api_key="dummy",
            base_url="https://geoserver.core-stack.org/api/v1",
            active_locations=self.sample_active_locations(),
        )

        self.assertEqual(plan.scope, "district")
        self.assertEqual(plan.root_location["district"], "Cachar")
        self.assertEqual(len(plan.tehsil_targets), 2)
        self.assertEqual(plan.tehsil_targets[0]["tehsil"], "Lakhipur")

    def test_resolve_download_plan_expands_state_to_all_tehsils(self) -> None:
        args = argparse.Namespace(
            state="assam",
            district=None,
            tehsil=None,
            latitude=None,
            longitude=None,
            strict_location_match=False,
            tehsil_limit=1,
            allow_unlisted_location=False,
        )

        plan = public_api_client.resolve_download_plan(
            args,
            api_key="dummy",
            base_url="https://geoserver.core-stack.org/api/v1",
            active_locations=self.sample_active_locations(),
        )

        self.assertEqual(plan.scope, "state")
        self.assertEqual(plan.root_location["state"], "Assam")
        self.assertEqual(len(plan.tehsil_targets), 1)

    def test_subset_active_locations_requires_state_for_district_filter(self) -> None:
        with self.assertRaises(public_api_client.PublicAPIError) as context:
            public_api_client.subset_active_locations(
                self.sample_active_locations(),
                state=None,
                district="kachar",
            )

        self.assertIn("--state", str(context.exception))

    def test_resolve_download_plan_rejects_bulk_scope_with_mws_id(self) -> None:
        args = argparse.Namespace(
            state="assam",
            district="kachar",
            tehsil=None,
            latitude=None,
            longitude=None,
            strict_location_match=False,
            tehsil_limit=None,
            allow_unlisted_location=False,
            mws_id="10_1234",
        )

        with self.assertRaises(public_api_client.PublicAPIError) as context:
            public_api_client.resolve_download_plan(
                args,
                api_key="dummy",
                base_url="https://geoserver.core-stack.org/api/v1",
                active_locations=self.sample_active_locations(),
            )

        self.assertIn("--mws-id", str(context.exception))

    def test_resolve_smoke_test_target_accepts_district_scope(self) -> None:
        args = argparse.Namespace(
            state="assam",
            district="kachar",
            tehsil=None,
            latitude=None,
            longitude=None,
            strict_location_match=False,
            tehsil_limit=1,
            allow_unlisted_location=False,
            mws_id=None,
        )

        location, requested_scope, notes = public_api_client.resolve_smoke_test_target(
            args,
            api_key="dummy",
            base_url="https://geoserver.core-stack.org/api/v1",
            active_locations=self.sample_active_locations(),
        )

        self.assertEqual(requested_scope, "district")
        self.assertEqual(location["district"], "Cachar")
        self.assertEqual(location["tehsil"], "Lakhipur")
        self.assertTrue(any("from 2 activated tehsil(s)" in note for note in notes))

    def test_resolve_smoke_test_target_uses_default_sample_when_no_args(self) -> None:
        args = argparse.Namespace(
            state=None,
            district=None,
            tehsil=None,
            latitude=None,
            longitude=None,
            strict_location_match=False,
            tehsil_limit=1,
            allow_unlisted_location=False,
            mws_id=None,
        )

        location, requested_scope, notes = public_api_client.resolve_smoke_test_target(
            args,
            api_key="dummy",
            base_url="https://geoserver.core-stack.org/api/v1",
            active_locations=self.sample_active_locations(),
        )

        self.assertEqual(requested_scope, "tehsil")
        self.assertEqual(location["state"], public_api_client.DEFAULT_SAMPLE_STATE)
        self.assertEqual(location["district"], public_api_client.DEFAULT_SAMPLE_DISTRICT)
        self.assertEqual(location["tehsil"], public_api_client.DEFAULT_SAMPLE_TEHSIL)
        self.assertEqual(notes, [])

    def test_aggregate_bulk_download_outputs_deduplicates_mws_and_layers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root_output_dir = Path(tmp_dir) / "district"
            tehsil_one_dir = root_output_dir / "lakhipur"
            tehsil_two_dir = root_output_dir / "sonai"

            for base_dir, tehsil_name, layer_name in [
                (tehsil_one_dir, "Lakhipur", "shared_layer"),
                (tehsil_two_dir, "Sonai", "shared_layer"),
            ]:
                metadata_dir = base_dir / "metadata"
                metadata_dir.mkdir(parents=True, exist_ok=True)
                (base_dir / "mws" / "10_1").mkdir(parents=True, exist_ok=True)
                (base_dir / "mws" / "10_1" / "mws_data.json").write_text(
                    json.dumps({"uid": "10_1"}),
                    encoding="utf-8",
                )
                (metadata_dir / "generated_layer_urls.json").write_text(
                    json.dumps(
                        [
                            {
                                "layer_name": layer_name,
                                "layer_url": f"https://example.com/{tehsil_name.lower()}",
                                "dataset_name": "lulc",
                            }
                        ]
                    ),
                    encoding="utf-8",
                )
                (metadata_dir / "mws_geometries.json").write_text(
                    json.dumps(
                        {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "properties": {"uid": "10_1"},
                                    "geometry": {"type": "Point", "coordinates": [1, 2]},
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )

            tehsil_summaries = [
                {
                    "output_dir": str(tehsil_one_dir),
                    "location": {"state": "Assam", "district": "Cachar", "tehsil": "Lakhipur"},
                },
                {
                    "output_dir": str(tehsil_two_dir),
                    "location": {"state": "Assam", "district": "Cachar", "tehsil": "Sonai"},
                },
            ]

            aggregates = public_api_client.aggregate_bulk_download_outputs(
                root_output_dir=root_output_dir,
                root_location={"state": "Assam", "district": "Cachar", "scope": "district"},
                tehsil_summaries=tehsil_summaries,
                streams={"layer_catalog", "mws_geometries", "mws_data"},
            )

            self.assertEqual(aggregates["selected_tehsil_count"], 2)
            self.assertEqual(aggregates["unique_layer_count"], 2)
            self.assertEqual(aggregates["duplicate_layer_count"], 0)
            self.assertEqual(aggregates["unique_mws_feature_count"], 1)
            self.assertEqual(aggregates["duplicate_mws_feature_count"], 1)
            self.assertEqual(aggregates["unique_mws_payload_count"], 1)

            aggregated_geojson = json.loads(
                (root_output_dir / "metadata" / "mws_geometries_aggregated.geojson").read_text(encoding="utf-8")
            )
            self.assertEqual(len(aggregated_geojson["features"]), 1)


if __name__ == "__main__":
    unittest.main()
