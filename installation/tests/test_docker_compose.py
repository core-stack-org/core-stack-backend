from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
DOCKER_GUIDE = REPO_ROOT / "installation" / "DOCKER.md"
ENTRYPOINT = REPO_ROOT / "installation" / "docker" / "entrypoint.sh"
DATABASE_INIT = REPO_ROOT / "installation" / "docker" / "database-init.sh"
APP_INIT = REPO_ROOT / "installation" / "docker" / "app-init.sh"
REMOVED_COMPOSE_WRAPPER = REPO_ROOT / "installation" / "docker" / "compose.sh"


class DockerComposeArchitectureTests(unittest.TestCase):
    def test_compose_uses_separate_durable_services_and_host_data(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("postgres_data:/var/lib/postgresql/data", compose)
        self.assertIn("geoserver_data:/opt/geoserver/data_dir", compose)
        self.assertIn(
            "${CORESTACK_HOST_DATA_DIR:-.}/data:/var/tmp/core-stack-data",
            compose,
        )
        self.assertIn(
            "${CORESTACK_HOST_DATA_DIR:-.}/gee_confs:/app/data/gee_confs:ro",
            compose,
        )
        self.assertIn(
            "${CORESTACK_HOST_DATA_DIR:-.}/backups/postgres:/backups",
            compose,
        )
        self.assertIn(
            "${CORESTACK_HOST_DATA_DIR:-.}/backups/geoserver:/backups",
            compose,
        )
        for old_variable in (
            "CORESTACK_DATA_DIR",
            "GEE_CONFS_DIR:-",
            "POSTGRES_BACKUP_DIR",
            "GEOSERVER_BACKUP_DIR",
        ):
            self.assertNotIn(old_variable, compose)
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
        docker_guide = DOCKER_GUIDE.read_text(encoding="utf-8")
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        app_init = APP_INIT.read_text(encoding="utf-8")

        self.assertFalse(REMOVED_COMPOSE_WRAPPER.exists())
        self.assertNotIn("compose.sh", docker_guide)
        self.assertIn(
            "docker compose --env-file nrm_app/.env",
            docker_guide,
        )
        self.assertNotIn("${POSTGRES_DB:-", compose)
        self.assertNotIn("${POSTGRES_USER:-", compose)
        self.assertNotIn("${POSTGRES_PASSWORD:-", compose)
        self.assertIn("POSTGRES_DB: ${DB_NAME:-corestack_db}", compose)
        self.assertIn("POSTGRES_USER: ${DB_USER:-corestack_admin}", compose)
        for key in ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"):
            self.assertIn(key, app_init)
        # Compose reads the same file from the host on every command, so
        # app-init must not leave it owned by root after rewriting it.
        self.assertIn('chmod 600 "$APP_ENV_FILE"', app_init)
        self.assertIn('chown "$checkout_owner" "$APP_ENV_FILE"', app_init)

    def test_tehsil_bootstrap_explicitly_uses_geoserver(self) -> None:
        download_script = (
            REPO_ROOT / "installation" / "docker" / "download-data.sh"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "local_compute_layer_setup --ensure-tehsil-watersheds --geoserver",
            download_script,
        )

    def test_heavy_worker_is_opt_in_and_serialized(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        template = (
            REPO_ROOT / "installation" / "docker" / "env.template"
        ).read_text(encoding="utf-8")

        heavy = compose.split("celery-heavy:", 1)[1].split("\n\n", 1)[0]
        self.assertIn('profiles: ["heavy"]', heavy)
        self.assertIn("--queues=heavy", heavy)
        self.assertIn("--pool=solo", heavy)
        self.assertIn("driver: nvidia", heavy)

        nrm = compose.split("celery-nrm:", 1)[1].split("\n\n", 1)[0]
        self.assertIn("--concurrency=${CELERY_NRM_CONCURRENCY:-3}", nrm)
        self.assertNotIn("--pool=solo", nrm)
        self.assertNotIn("driver: nvidia", nrm)

        # A GPU-less host needs no settings: both flags stay off until the
        # heavy profile is selected, and then follow it automatically.
        self.assertIn(
            "GPU_AVAILABLE: ${GPU_AVAILABLE:-${COMPOSE_PROFILES:+True}}",
            compose,
        )
        self.assertIn(
            "HEAVY_WORKER_ENABLED: "
            "${HEAVY_WORKER_ENABLED:-${COMPOSE_PROFILES:+True}}",
            compose,
        )
        self.assertIn("# COMPOSE_PROFILES=heavy", template)
        for key in ("GPU_AVAILABLE", "HEAVY_WORKER_ENABLED"):
            self.assertNotIn(f"\n{key}=", template)

    def test_proxy_is_wired_into_build_and_runtime(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("x-proxy-environment: &proxy-environment", compose)
        self.assertIn("args: *proxy-environment", compose)
        backend_env = compose.split("x-backend-environment:", 1)[1]
        self.assertTrue(
            backend_env.split("\n", 2)[1].strip() == "<<: *proxy-environment"
        )
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            self.assertIn(f"  {key}: ", compose)
        for key in ("NO_PROXY", "no_proxy"):
            line = next(
                l for l in compose.splitlines() if l.startswith(f"  {key}: ")
            )
            for service in ("postgres", "redis", "geoserver", "backend", "core-stack"):
                self.assertIn(service, line.split(","))



if __name__ == "__main__":
    unittest.main()
