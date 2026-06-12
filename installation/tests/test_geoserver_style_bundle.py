from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from installation.geoserver_style_bundle import (
    StyleEntry,
    fetch_styles,
    load_manifest,
    normalize_geoserver_url,
    relative_style_file,
    sync_styles,
    unwrap_collection,
    write_manifest,
)


class GeoServerStyleBundleTests(unittest.TestCase):
    def test_normalize_geoserver_url_strips_trailing_slash(self) -> None:
        self.assertEqual(
            normalize_geoserver_url("https://example.com/geoserver/"),
            "https://example.com/geoserver",
        )

    def test_unwrap_collection_handles_single_item(self) -> None:
        payload = {"styles": {"style": {"name": "default_style"}}}
        self.assertEqual(
            unwrap_collection(payload, "styles", "style"),
            [{"name": "default_style"}],
        )

    def test_load_manifest_from_sld_files_when_manifest_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            styles_dir = Path(tmp_dir)
            (styles_dir / "default_style.sld").write_text("<StyledLayerDescriptor/>", encoding="utf-8")
            workspace_dir = styles_dir / "workspaces" / "corestack"
            workspace_dir.mkdir(parents=True)
            (workspace_dir / "layer_style.sld").write_text("<StyledLayerDescriptor/>", encoding="utf-8")

            entries = load_manifest(styles_dir)
            self.assertEqual(len(entries), 2)
            self.assertEqual(entries[0].name, "default_style")
            self.assertEqual(entries[1].workspace, "corestack")

    def test_write_and_load_manifest_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            styles_dir = Path(tmp_dir)
            entries = [
                StyleEntry("default_style", None, "default_style.sld"),
                StyleEntry("slope_percentage", "slope_percentage", "workspaces/slope_percentage/slope_percentage.sld"),
            ]
            write_manifest(styles_dir, source_url="https://example.com/geoserver", entries=entries)

            loaded = load_manifest(styles_dir)
            self.assertEqual(loaded, entries)

            manifest = json.loads((styles_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["style_count"], 2)

    def test_relative_style_file_paths(self) -> None:
        self.assertEqual(relative_style_file("default_style", None), "default_style.sld")
        self.assertEqual(
            relative_style_file("slope_percentage", "slope_percentage"),
            "workspaces/slope_percentage/slope_percentage.sld",
        )

    @patch("installation.geoserver_style_bundle.requests.Session")
    def test_fetch_styles_downloads_global_styles(self, session_cls: MagicMock) -> None:
        session = session_cls.return_value
        session.verify = True

        def request_json_side_effect(
            _session: MagicMock,
            _method: str,
            url: str,
            *,
            timeout: int = 60,
        ):
            if url.endswith("/rest/styles.json"):
                return {"styles": {"style": [{"name": "default_style"}, {"name": "polygon"}]}}
            if url.endswith("/rest/workspaces.json"):
                return {"workspaces": {"workspace": []}}
            raise AssertionError(f"Unexpected URL: {url}")

        def get_side_effect(url: str, timeout: int = 60):
            response = MagicMock()
            response.status_code = 200
            if url.endswith("/rest/styles.json"):
                return MagicMock(
                    status_code=200,
                    content=b'{"styles":{"style":[{"name":"default_style"}]}}',
                    json=lambda: {"styles": {"style": [{"name": "default_style"}]}},
                )
            if url.endswith("default_style.sld"):
                response.content = b"<StyledLayerDescriptor/>"
                return response
            if url.endswith("polygon.sld"):
                response.content = b"<StyledLayerDescriptor version='1.0.0'/>"
                return response
            raise AssertionError(f"Unexpected GET: {url}")

        with patch("installation.geoserver_style_bundle.request_json", side_effect=request_json_side_effect):
            session.get.side_effect = get_side_effect

            with tempfile.TemporaryDirectory() as tmp_dir:
                styles_dir = Path(tmp_dir)
                entries = fetch_styles(
                    "https://example.com/geoserver",
                    "admin",
                    "secret",
                    styles_dir,
                    include_workspace_styles=False,
                )

                self.assertEqual(len(entries), 2)
                self.assertTrue((styles_dir / "default_style.sld").exists())
                self.assertTrue((styles_dir / "manifest.json").exists())

    @patch("installation.geoserver_style_bundle.style_exists", return_value=False)
    @patch("installation.geoserver_style_bundle.create_style")
    @patch("installation.geoserver_style_bundle.upload_style_sld")
    def test_sync_styles_creates_missing_styles(
        self,
        upload_style_sld: MagicMock,
        create_style: MagicMock,
        style_exists: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            styles_dir = Path(tmp_dir)
            (styles_dir / "default_style.sld").write_text("<StyledLayerDescriptor/>", encoding="utf-8")
            write_manifest(
                styles_dir,
                source_url="https://example.com/geoserver",
                entries=[StyleEntry("default_style", None, "default_style.sld")],
            )

            counts = sync_styles(
                "http://localhost:8080/geoserver",
                "admin",
                "geoserver",
                styles_dir,
            )

            self.assertEqual(counts["created"], 1)
            create_style.assert_called_once()
            upload_style_sld.assert_called_once()


if __name__ == "__main__":
    unittest.main()
