#!/usr/bin/env python3
"""Fetch GeoServer SLD styles from a source instance and sync them to a target."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests
from requests.auth import HTTPBasicAuth

DEFAULT_SOURCE_URL = "https://geoserver.core-stack.org:8443/geoserver"
DEFAULT_STYLES_DIR = Path(__file__).resolve().parent / "geoserver" / "styles"
MANIFEST_FILENAME = "manifest.json"
SLD_CONTENT_TYPE = "application/vnd.ogc.sld+xml"
STYLE_XML_CONTENT_TYPE = "text/xml"


@dataclass(frozen=True)
class StyleEntry:
    name: str
    workspace: str | None
    file: str

    @property
    def key(self) -> str:
        if self.workspace:
            return f"{self.workspace}:{self.name}"
        return self.name


def normalize_geoserver_url(url: str) -> str:
    return url.strip().rstrip("/")


def basic_auth(username: str, password: str) -> HTTPBasicAuth:
    return HTTPBasicAuth(username, password)


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int = 60,
) -> Any:
    response = session.request(method, url, timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"{method.upper()} {url} failed with HTTP {response.status_code}: {response.text[:500]}")
    if not response.content:
        return None
    return response.json()


def unwrap_collection(
    payload: dict[str, Any],
    collection_key: str,
    item_key: str,
) -> list[dict[str, Any]]:
    container = payload.get(collection_key) or {}
    items = container.get(item_key, [])
    if isinstance(items, dict):
        return [items]
    return items or []


def list_global_styles(session: requests.Session, base_url: str) -> list[str]:
    payload = request_json(session, "get", f"{base_url}/rest/styles.json")
    return [
        item["name"]
        for item in unwrap_collection(payload, "styles", "style")
        if item.get("name")
    ]


def list_workspaces(session: requests.Session, base_url: str) -> list[str]:
    payload = request_json(session, "get", f"{base_url}/rest/workspaces.json")
    return [
        item["name"]
        for item in unwrap_collection(payload, "workspaces", "workspace")
        if item.get("name")
    ]


def list_workspace_styles(session: requests.Session, base_url: str, workspace: str) -> list[str]:
    payload = request_json(
        session,
        "get",
        f"{base_url}/rest/workspaces/{workspace}/styles.json",
    )
    return [
        item["name"]
        for item in unwrap_collection(payload, "styles", "style")
        if item.get("name")
    ]


def fetch_style_sld(
    session: requests.Session,
    base_url: str,
    style_name: str,
    workspace: str | None,
) -> bytes:
    if workspace:
        url = f"{base_url}/rest/workspaces/{workspace}/styles/{style_name}.sld"
    else:
        url = f"{base_url}/rest/styles/{style_name}.sld"

    response = session.get(url, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(
            f"GET {url} failed with HTTP {response.status_code}: {response.text[:500]}"
        )
    return response.content


def style_file_path(styles_dir: Path, style_name: str, workspace: str | None) -> Path:
    if workspace:
        return styles_dir / "workspaces" / workspace / f"{style_name}.sld"
    return styles_dir / f"{style_name}.sld"


def relative_style_file(style_name: str, workspace: str | None) -> str:
    if workspace:
        return f"workspaces/{workspace}/{style_name}.sld"
    return f"{style_name}.sld"


def write_manifest(
    styles_dir: Path,
    *,
    source_url: str,
    entries: list[StyleEntry],
) -> None:
    manifest = {
        "source_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "style_count": len(entries),
        "styles": [asdict(entry) for entry in entries],
    }
    manifest_path = styles_dir / MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def load_manifest(styles_dir: Path) -> list[StyleEntry]:
    manifest_path = styles_dir / MANIFEST_FILENAME
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return [
            StyleEntry(
                name=item["name"],
                workspace=item.get("workspace"),
                file=item["file"],
            )
            for item in payload.get("styles", [])
        ]

    entries: list[StyleEntry] = []
    for sld_path in sorted(styles_dir.glob("*.sld")):
        entries.append(
            StyleEntry(
                name=sld_path.stem,
                workspace=None,
                file=sld_path.name,
            )
        )
    for sld_path in sorted(styles_dir.glob("workspaces/*/*.sld")):
        workspace = sld_path.parent.name
        entries.append(
            StyleEntry(
                name=sld_path.stem,
                workspace=workspace,
                file=f"workspaces/{workspace}/{sld_path.name}",
            )
        )
    return entries


def fetch_styles(
    source_url: str,
    username: str,
    password: str,
    styles_dir: Path,
    *,
    include_workspace_styles: bool = True,
    verify_ssl: bool = True,
) -> list[StyleEntry]:
    base_url = normalize_geoserver_url(source_url)
    styles_dir.mkdir(parents=True, exist_ok=True)
    (styles_dir / "workspaces").mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.auth = basic_auth(username, password)
    session.verify = verify_ssl

    entries: list[StyleEntry] = []
    seen: set[str] = set()

    for style_name in sorted(list_global_styles(session, base_url)):
        key = style_name
        if key in seen:
            continue
        seen.add(key)

        sld_bytes = fetch_style_sld(session, base_url, style_name, None)
        target_path = style_file_path(styles_dir, style_name, None)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(sld_bytes)
        entries.append(
            StyleEntry(
                name=style_name,
                workspace=None,
                file=relative_style_file(style_name, None),
            )
        )

    if include_workspace_styles:
        for workspace in sorted(list_workspaces(session, base_url)):
            for style_name in sorted(list_workspace_styles(session, base_url, workspace)):
                key = f"{workspace}:{style_name}"
                if key in seen:
                    continue
                seen.add(key)

                sld_bytes = fetch_style_sld(session, base_url, style_name, workspace)
                target_path = style_file_path(styles_dir, style_name, workspace)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_bytes(sld_bytes)
                entries.append(
                    StyleEntry(
                        name=style_name,
                        workspace=workspace,
                        file=relative_style_file(style_name, workspace),
                    )
                )

    write_manifest(styles_dir, source_url=base_url, entries=entries)
    return entries


def style_exists(
    session: requests.Session,
    base_url: str,
    entry: StyleEntry,
) -> bool:
    if entry.workspace:
        url = f"{base_url}/rest/workspaces/{entry.workspace}/styles/{entry.name}.json"
    else:
        url = f"{base_url}/rest/styles/{entry.name}.json"
    response = session.get(url, timeout=30)
    return response.status_code == 200


def create_style(
    session: requests.Session,
    base_url: str,
    entry: StyleEntry,
) -> None:
    style_xml = (
        f"<style><name>{entry.name}</name>"
        f"<filename>{entry.name}.sld</filename></style>"
    )
    headers = {"Content-Type": STYLE_XML_CONTENT_TYPE}

    if entry.workspace:
        url = f"{base_url}/rest/workspaces/{entry.workspace}/styles"
    else:
        url = f"{base_url}/rest/styles"

    response = session.post(url, data=style_xml, headers=headers, timeout=30)
    if response.status_code not in {201, 202}:
        raise RuntimeError(
            f"POST {url} failed with HTTP {response.status_code}: {response.text[:500]}"
        )


def upload_style_sld(
    session: requests.Session,
    base_url: str,
    entry: StyleEntry,
    sld_bytes: bytes,
) -> None:
    headers = {"Content-Type": SLD_CONTENT_TYPE}
    if entry.workspace:
        url = f"{base_url}/rest/workspaces/{entry.workspace}/styles/{entry.name}"
    else:
        url = f"{base_url}/rest/styles/{entry.name}"

    response = session.put(url, data=sld_bytes, headers=headers, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(
            f"PUT {url} failed with HTTP {response.status_code}: {response.text[:500]}"
        )


def sync_styles(
    target_url: str,
    username: str,
    password: str,
    styles_dir: Path,
    *,
    update_existing: bool = True,
    verify_ssl: bool = True,
) -> dict[str, int]:
    base_url = normalize_geoserver_url(target_url)
    entries = load_manifest(styles_dir)
    if not entries:
        return {"created": 0, "updated": 0, "skipped": 0, "failed": 0}

    session = requests.Session()
    session.auth = basic_auth(username, password)
    session.verify = verify_ssl

    counts = {"created": 0, "updated": 0, "skipped": 0, "failed": 0}

    for entry in entries:
        sld_path = styles_dir / entry.file
        if not sld_path.exists():
            print(f"WARNING: Missing SLD file for {entry.key}: {sld_path}", file=sys.stderr)
            counts["failed"] += 1
            continue

        sld_bytes = sld_path.read_bytes()
        try:
            exists = style_exists(session, base_url, entry)
            if exists and not update_existing:
                counts["skipped"] += 1
                continue

            if not exists:
                create_style(session, base_url, entry)
                upload_style_sld(session, base_url, entry, sld_bytes)
                counts["created"] += 1
                print(f"Created style {entry.key}")
                continue

            upload_style_sld(session, base_url, entry, sld_bytes)
            counts["updated"] += 1
            print(f"Updated style {entry.key}")
        except Exception as exc:
            counts["failed"] += 1
            print(f"ERROR: Failed to sync style {entry.key}: {exc}", file=sys.stderr)

    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Download styles from a GeoServer instance")
    fetch_parser.add_argument("--url", default=DEFAULT_SOURCE_URL)
    fetch_parser.add_argument("--username", required=True)
    fetch_parser.add_argument("--password", required=True)
    fetch_parser.add_argument("--styles-dir", type=Path, default=DEFAULT_STYLES_DIR)
    fetch_parser.add_argument(
        "--skip-workspace-styles",
        action="store_true",
        help="Only fetch global styles",
    )
    fetch_parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification",
    )

    sync_parser = subparsers.add_parser("sync", help="Upload bundled styles to a GeoServer instance")
    sync_parser.add_argument("--url", required=True)
    sync_parser.add_argument("--username", required=True)
    sync_parser.add_argument("--password", required=True)
    sync_parser.add_argument("--styles-dir", type=Path, default=DEFAULT_STYLES_DIR)
    sync_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Do not overwrite styles that already exist",
    )
    sync_parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    verify_ssl = not args.insecure

    if args.command == "fetch":
        entries = fetch_styles(
            args.url,
            args.username,
            args.password,
            args.styles_dir,
            include_workspace_styles=not args.skip_workspace_styles,
            verify_ssl=verify_ssl,
        )
        print(
            f"Fetched {len(entries)} styles into {args.styles_dir} "
            f"(manifest: {args.styles_dir / MANIFEST_FILENAME})"
        )
        return 0

    if args.command == "sync":
        counts = sync_styles(
            args.url,
            args.username,
            args.password,
            args.styles_dir,
            update_existing=not args.skip_existing,
            verify_ssl=verify_ssl,
        )
        print(
            "Style sync complete: "
            f"created={counts['created']} updated={counts['updated']} "
            f"skipped={counts['skipped']} failed={counts['failed']}"
        )
        return 1 if counts["failed"] else 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
