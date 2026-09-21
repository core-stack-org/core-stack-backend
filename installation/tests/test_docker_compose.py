from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENTRYPOINT = REPO_ROOT / "installation" / "docker" / "entrypoint.sh"
DATABASE_INIT = REPO_ROOT / "installation" / "docker" / "database-init.sh"
APP_INIT = REPO_ROOT / "installation" / "docker" / "app-init.sh"
COMPOSE_WRAPPER = REPO_ROOT / "installation" / "docker" / "compose.sh"


class DockerComposeArchitectureTests(unittest.TestCase):
    def test_compose_uses_separate_durable_services_and_host_data(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("postgres_data:/var/lib/postgresql/data", compose)
        self.assertIn("geoserver_data:/opt/geoserver/data_dir", compose)
        self.assertIn(
            "${CORESTACK_DATA_DIR:-./data}:/var/tmp/core-stack-data",
            compose,
        )
        self.assertIn("${BACKEND_CODE_DIR:-.}:/app", compose)

    def test_startup_uses_one_shot_initialisation_services(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("database-init:", compose)
        self.assertIn("data-download:", compose)
        self.assertIn("tehsil-watershed-setup:", compose)
        self.assertIn("condition: service_completed_successfully", compose)

    def test_runtime_entrypoint_does_not_mutate_database_schema(self) -> None:
        entrypoint = ENTRYPOINT.read_text(encoding="utf-8")
        database_init = DATABASE_INIT.read_text(encoding="utf-8")

        self.assertNotIn("manage.py makemigrations", entrypoint)
        self.assertNotIn("manage.py migrate", entrypoint)
        self.assertNotIn("runserver", entrypoint)
        self.assertIn("manage.py makemigrations --skip-checks", database_init)
        self.assertIn("migrate --plan", database_init)
        self.assertIn("migrate --fake-initial --noinput", database_init)

    def test_named_compose_env_and_django_env_are_wired_explicitly(self) -> None:
        wrapper = COMPOSE_WRAPPER.read_text(encoding="utf-8")
        app_init = APP_INIT.read_text(encoding="utf-8")

        self.assertIn(".env.core-stack-docker", wrapper)
        self.assertIn('--env-file "$COMPOSE_ENV_FILE"', wrapper)
        for key in ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"):
            self.assertIn(key, app_init)

    def test_tehsil_bootstrap_explicitly_uses_geoserver(self) -> None:
        download_script = (
            REPO_ROOT / "installation" / "docker" / "download-data.sh"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "local_compute_layer_setup --ensure-tehsil-watersheds --geoserver",
            download_script,
        )



if __name__ == "__main__":
    unittest.main()
