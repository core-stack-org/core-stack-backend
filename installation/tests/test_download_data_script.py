from __future__ import annotations

import subprocess
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_SCRIPT = REPO_ROOT / "installation" / "docker" / "download-data.sh"


def run_bash(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-lc", script],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


class DownloadDataScriptTests(unittest.TestCase):
    def test_skip_layer_setup_does_not_call_manage_py(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                DATA_DIR="$(mktemp -d)"
                source "{DOWNLOAD_SCRIPT}"
                SKIP_LAYER_SETUP=1
                download_local_compute_layers
                download_tehsil_watersheds
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Skipping local compute base-layer downloads", result.stdout)
        self.assertIn("Skipping GeoServer tehsil watershed setup", result.stdout)

    def test_large_downloads_have_independent_skip_flags(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                DATA_DIR="$(mktemp -d)"
                source "{DOWNLOAD_SCRIPT}"
                SKIP_ADMIN_BOUNDARY_DOWNLOAD=1
                SKIP_BASE_LAYER_DOWNLOAD=1
                SKIP_TEHSIL_WATERSHEDS=1
                download_admin_boundary
                download_local_compute_layers
                download_tehsil_watersheds
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("SKIP_ADMIN_BOUNDARY_DOWNLOAD=1", result.stdout)
        self.assertIn("Skipping local compute base-layer downloads", result.stdout)
        self.assertIn("Skipping GeoServer tehsil watershed setup", result.stdout)

    def test_script_invokes_requested_layer_setup_commands(self) -> None:
        script = DOWNLOAD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("local_compute_layer_setup terrain mws lulc_v3", script)
        self.assertIn("local_compute_layer_setup static_layers", script)
        self.assertIn("local_compute_layer_setup tehsil_level", script)
        self.assertIn("local_compute_layer_setup --ensure-soi-tehsil", script)
        self.assertIn(
            "local_compute_layer_setup --ensure-tehsil-watersheds --geoserver",
            script,
        )
        self.assertNotIn(
            "local_compute_layer_setup --ensure-tehsil-watersheds --skip-checks",
            script,
        )
        self.assertIn('DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"', script)
