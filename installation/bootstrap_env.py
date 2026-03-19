#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import os
import re
import secrets
import shutil
from pathlib import Path


ENV_ASSIGNMENT_PATTERN = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
ENV_LOOKUP_PATTERN = re.compile(r'env(?:\.[A-Za-z_]+)?\(\s*"([A-Za-z_][A-Za-z0-9_]*)"')
EXTRA_ENV_KEYS = {
    "EXCEL_DIR",
    "DB_HOST",
    "DB_PORT",
    "CELERY_BROKER_URL",
    "CELERY_RESULT_BACKEND",
    "CELERY_WORKER_POOL",
    "ASSET_DIR",
    "MEDIA_ROOT",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate or update nrm_app/.env with cross-platform defaults."
    )
    parser.add_argument("--backend-dir", required=True)
    parser.add_argument("--env-file")
    parser.add_argument("--db-name", default="corestack_db")
    parser.add_argument("--db-user", default="corestack_admin")
    parser.add_argument("--db-password", default="corestack@123")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", default="5432")
    parser.add_argument(
        "--celery-broker-url",
        default="amqp://guest:guest@127.0.0.1:5672//",
    )
    parser.add_argument("--celery-result-backend", default="rpc://")
    parser.add_argument("--celery-worker-pool", default="solo")
    parser.add_argument("--debug", default="True")
    return parser.parse_args()


def to_env_path(path: Path) -> str:
    return path.resolve(strict=False).as_posix()


def generate_secret_key() -> str:
    return secrets.token_urlsafe(50)


def generate_fernet_key() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def collect_env_keys(settings_file: Path) -> list[str]:
    env_keys = set(EXTRA_ENV_KEYS)
    env_keys.update(ENV_LOOKUP_PATTERN.findall(settings_file.read_text(encoding="utf-8")))
    return sorted(env_keys)


def read_env_lines(env_file: Path) -> list[str]:
    if env_file.exists():
        return env_file.read_text(encoding="utf-8").splitlines()
    return []


def write_env_lines(env_file: Path, lines: list[str]) -> None:
    env_file.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines).rstrip() + "\n"
    env_file.write_text(content, encoding="utf-8")


def normalise_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def quote_env_value(value: str) -> str:
    value = str(value)
    if any(char.isspace() for char in value) or "#" in value:
        return f'"{value}"'
    return value


def ensure_assignment(lines: list[str], key: str, value: str) -> None:
    assignment = f"{key}={quote_env_value(value)}"

    for index, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[index] = assignment
            return

    if lines and lines[-1] != "":
        lines.append("")
    lines.append(assignment)


def ensure_key(lines: list[str], key: str) -> None:
    for line in lines:
        if line.startswith(f"{key}="):
            return
    lines.append(f'{key}=""')


def get_env_value(lines: list[str], key: str) -> str | None:
    for line in lines:
        match = ENV_ASSIGNMENT_PATTERN.match(line)
        if match and match.group(1) == key:
            return normalise_env_value(match.group(2))
    return None


def ensure_default_if_blank(lines: list[str], key: str, value: str) -> None:
    current_value = get_env_value(lines, key)
    if current_value in (None, ""):
        ensure_assignment(lines, key, value)


def ensure_required_directories(backend_dir: Path) -> None:
    directories = [
        backend_dir / "logs",
        backend_dir / "data",
        backend_dir / "data" / "activated_locations",
        backend_dir / "data" / "stats_excel_files",
        backend_dir / "tmp",
        backend_dir / "bot_interface" / "whatsapp_media",
        backend_dir / "assets",
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    for log_name in ("app.log", "nrm_app.log"):
        (backend_dir / "logs" / log_name).touch(exist_ok=True)


def migrate_legacy_env(root_env_file: Path, app_env_file: Path) -> None:
    if app_env_file.exists() or not root_env_file.exists():
        return

    app_env_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root_env_file, app_env_file)


def main() -> int:
    args = parse_args()

    backend_dir = Path(args.backend_dir).resolve(strict=False)
    env_file = Path(args.env_file).resolve(strict=False) if args.env_file else backend_dir / "nrm_app" / ".env"
    settings_file = backend_dir / "nrm_app" / "settings.py"
    root_env_file = backend_dir / ".env"

    if not settings_file.exists():
        raise FileNotFoundError(f"settings.py was not found at {settings_file}")

    ensure_required_directories(backend_dir)
    migrate_legacy_env(root_env_file, env_file)

    lines = read_env_lines(env_file)
    if not lines:
        lines = [
            "# Auto-generated by installation/bootstrap_env.py",
            "",
        ]

    env_keys = collect_env_keys(settings_file)
    for env_key in env_keys:
        ensure_key(lines, env_key)

    defaults = {
        "DEBUG": args.debug,
        "SECRET_KEY": generate_secret_key(),
        "FERNET_KEY": generate_fernet_key(),
        "DB_NAME": args.db_name,
        "DB_USER": args.db_user,
        "DB_PASSWORD": args.db_password,
        "DB_HOST": args.db_host,
        "DB_PORT": args.db_port,
        "DEPLOYMENT_DIR": to_env_path(backend_dir),
        "TMP_LOCATION": to_env_path(backend_dir / "tmp"),
        "WHATSAPP_MEDIA_PATH": to_env_path(backend_dir / "bot_interface" / "whatsapp_media"),
        "EXCEL_DIR": to_env_path(backend_dir / "data" / "stats_excel_files"),
        "EXCEL_PATH": to_env_path(backend_dir),
        "ASSET_DIR": to_env_path(backend_dir / "assets"),
        "MEDIA_ROOT": to_env_path(backend_dir / "data"),
        "CELERY_BROKER_URL": args.celery_broker_url,
        "CELERY_RESULT_BACKEND": args.celery_result_backend,
        "CELERY_WORKER_POOL": args.celery_worker_pool,
    }

    for key, value in defaults.items():
        ensure_default_if_blank(lines, key, value)

    write_env_lines(env_file, lines)

    print(f".env ready at {env_file}")
    print(f"Backend directory: {backend_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
