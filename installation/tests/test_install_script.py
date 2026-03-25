from __future__ import annotations

import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_SCRIPT = REPO_ROOT / "installation" / "install.sh"


def run_bash(script: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-lc", script],
        cwd=cwd or REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


class InstallScriptTests(unittest.TestCase):
    def test_list_steps_exposes_new_shortcuts(self) -> None:
        result = subprocess.run(
            ["bash", "installation/install.sh", "--list-steps"],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("gee_configuration", result.stdout)
        self.assertIn("admin_boundary_data", result.stdout)
        self.assertIn("initialisation_check", result.stdout)

    def test_from_and_skip_select_expected_steps(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                parse_args --from gee_configuration --skip initialisation_check
                for step in "${{STEP_ORDER[@]}}"; do
                    if should_execute_step "$step"; then
                        echo "$step"
                    fi
                done
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip().splitlines(),
            ["gee_configuration", "admin_boundary_data", "public_api_check"],
        )

    def test_normalize_user_path_supports_windows_input(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                INSTALL_INVOCATION_DIR="/tmp/example"
                normalize_user_path 'C:\\Users\\name\\Downloads\\gee.json'
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "/mnt/c/Users/name/Downloads/gee.json")

    def test_cli_optional_input_sets_gee_json_value(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                parse_args --input gee_json=./service-account.json
                echo "$GEE_JSON_PATH_ARG"
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "./service-account.json")

    def test_prompt_style_optional_input_accepts_cli_like_windows_gee_path(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                parse_optional_input_entry '- gee-json "Y:\\core-stack-org\\core-stack-backend\\data\\gee_confs\\file.json"'
                echo "$GEE_JSON_PATH_ARG"
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            r"Y:\core-stack-org\core-stack-backend\data\gee_confs\file.json",
        )

    def test_public_api_optional_inputs_set_runtime_values(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                parse_args \
                  --input public_api_key=test-key-1234 \
                  --input public_api_base_url=https://example.com
                echo "$PUBLIC_API_X_API_KEY_ARG"
                echo "$PUBLIC_API_BASE_URL_ARG"
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip().splitlines(),
            ["test-key-1234", "https://example.com"],
        )

    def test_geoserver_optional_inputs_set_runtime_values(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                parse_args \
                  --input geoserver_url=https://maps.example.com/geoserver \
                  --input geoserver_username=admin \
                  --input geoserver_password=secret
                echo "$GEOSERVER_URL_ARG"
                echo "$GEOSERVER_USERNAME_ARG"
                echo "$GEOSERVER_PASSWORD_ARG"
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip().splitlines(),
            ["https://maps.example.com/geoserver", "admin", "secret"],
        )

    def test_finalize_admin_boundary_extraction_flattens_nested_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            backend_dir = tmp_path / "backend"
            extract_root = tmp_path / "extract"
            nested_input_dir = extract_root / "admin-boundary" / "input" / "assam"
            nested_output_dir = extract_root / "admin-boundary" / "output"

            nested_input_dir.mkdir(parents=True)
            nested_output_dir.mkdir(parents=True)
            (extract_root / "admin-boundary" / "input" / "soi_tehsil.geojson").write_text("{}", encoding="utf-8")
            (nested_input_dir / "baksa.geojson").write_text("{}", encoding="utf-8")

            result = run_bash(
                textwrap.dedent(
                    f"""
                    source "{INSTALL_SCRIPT}"
                    BACKEND_DIR="{backend_dir}"
                    INSTALL_STATE_DIR="$BACKEND_DIR/.installation_state"
                    mkdir -p "$BACKEND_DIR/data"
                    finalize_admin_boundary_extraction "{extract_root}"
                    test -f "$BACKEND_DIR/data/admin-boundary/input/soi_tehsil.geojson"
                    test -f "$BACKEND_DIR/data/admin-boundary/input/assam/baksa.geojson"
                    test ! -d "$BACKEND_DIR/data/admin-boundary/admin-boundary"
                    echo ok
                    """
                )
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("ok", result.stdout)

    def test_path_detection_accepts_relative_json_paths(self) -> None:
        result = run_bash(
            textwrap.dedent(
                f"""
                source "{INSTALL_SCRIPT}"
                if looks_like_user_path_input "./service-account.json"; then
                    echo yes
                else
                    echo no
                fi
                """
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "yes")
