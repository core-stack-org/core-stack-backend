#!/usr/bin/env python
"""Small terminal utility for inspecting CoRE Stack GEE accounts.

Examples:
    python utilities/scripts/gee_account_util.py --list
    python utilities/scripts/gee_account_util.py --probe 1
    python utilities/scripts/gee_account_util.py --add-json data/gee_confs/key.json --name corestackdev
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def bootstrap_django() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    try:
        from nrm_app.runtime import configure_runtime_environment

        configure_runtime_environment()
    except ModuleNotFoundError:
        pass

    import django

    django.setup()


def list_accounts() -> None:
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, name, service_account_email, helper_account_id,
                   is_visible, credentials_encrypted IS NOT NULL AS has_credentials
            FROM gee_computing_geeaccount
            ORDER BY id
            """
        )
        rows = cursor.fetchall()

    if not rows:
        print("No GEE accounts found.")
        return

    headers = [
        "gee_account_id",
        "name",
        "service_account_email",
        "helper_account_id",
        "visible",
        "has_credentials",
    ]
    widths = [len(header) for header in headers]
    formatted_rows = []
    for row in rows:
        values = ["" if value is None else str(value) for value in row]
        formatted_rows.append(values)
        widths = [max(width, len(value)) for width, value in zip(widths, values)]

    print("  ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("  ".join("-" * width for width in widths))
    for values in formatted_rows:
        print("  ".join(value.ljust(width) for value, width in zip(values, widths)))


def add_account(json_path: str, name: str | None, helper_account_id: int | None) -> None:
    from utilities.gee_utils import upsert_gee_account_from_json

    account = upsert_gee_account_from_json(
        Path(json_path).expanduser().resolve(),
        account_name=name,
        helper_account_id=helper_account_id,
    )
    print(
        "Saved GEE account: "
        f"gee_account_id={account.id}, name={account.name}, "
        f"service_account_email={account.service_account_email}"
    )


def probe_account(gee_account_id: int) -> None:
    from utilities.gee_utils import probe_gcs_upload_access, probe_gee_connection

    print(f"Probing GEE account id={gee_account_id}")
    gee_ok = probe_gee_connection(gee_account_id)
    print(f"Earth Engine API: {'ok' if gee_ok else 'failed'}")
    gcs_result = probe_gcs_upload_access(gee_account_id=gee_account_id)
    print(
        "GCS staging bucket: ok "
        f"bucket={gcs_result['bucket_name']} "
        f"service_account={gcs_result['service_account_email']}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List stored GEE accounts.")
    parser.add_argument("--probe", type=int, help="Probe Earth Engine and GCS access for an account id.")
    parser.add_argument("--add-json", help="Add or update an account from a service-account JSON file.")
    parser.add_argument("--name", help="Name to use with --add-json.")
    parser.add_argument("--helper-account-id", type=int, help="Helper account id to set with --add-json.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    bootstrap_django()

    if args.add_json:
        add_account(args.add_json, args.name, args.helper_account_id)
    if args.probe:
        probe_account(args.probe)
    if args.list or not (args.add_json or args.probe):
        list_accounts()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
