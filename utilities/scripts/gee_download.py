"""General Earth Engine vector download helpers.

This module downloads Earth Engine table assets (FeatureCollections / tables)
directly via signed download URLs. It supports:

- single-asset downloads via ``--asset-id``
- batch downloads from one or more EE folders via ``--asset-parent``
- regex filtering on the full asset id via ``--asset-id-pattern``

Usage examples:

`single asset download`
python -m utilities.scripts.gee_download \
  --asset-id projects/ee-corestackdev/assets/apps/mws/odisha/koraput/jaypur/test_facilities_proximity_koraput_jaypur \
  --service-account-json data/gee_confs/core-stack-learn-1234.json \
  --output-dir data/fc_to_shape/facilities_proximity_downloads


`pattern download from a folder tree`
python -m utilities.scripts.gee_download \
  --asset-parent projects/ee-corestackdev/assets/apps/mws \
  --asset-id-pattern '(^|/)test_facilities_proximity_[^/]+_[^/]+$' \
  --service-account-json data/gee_confs/core-stack-learn-1234.json \
  --output-dir data/fc_to_shape/facilities_proximity_downloads
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Pattern, Sequence

import requests

if __package__ in (None, ""):
    REPO_ROOT_FOR_SCRIPT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT_FOR_SCRIPT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT_FOR_SCRIPT))

from utilities.scripts.gee_upload import (  # noqa: E402
    bootstrap_django_for_cli,
    initialize_gee_session,
    log_progress,
    normalize_gee_asset_parent,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "fc_to_shape" / "gee_download"
DEFAULT_SUMMARY_FILENAME = "gee_download_summary.json"
SUPPORTED_DOWNLOAD_FORMATS = {
    "csv": ".csv",
    "json": ".json",
    "geojson": ".geojson",
    "kml": ".kml",
    "kmz": ".kmz",
}
DOWNLOADABLE_ASSET_TYPES = {"TABLE"}
CONTAINER_ASSET_TYPES = {"FOLDER"}


class GEEDownloadError(Exception):
    """Raised when Earth Engine download preparation or retrieval fails."""


def normalize_asset_id(asset_id: str) -> str:
    normalized = str(asset_id).strip().rstrip("/")
    if not normalized.startswith("projects/") or "/assets/" not in normalized:
        raise GEEDownloadError(
            "Earth Engine asset id must look like "
            "'projects/<project-id>/assets/<folder>/<asset-name>'."
        )
    return normalized


def compile_asset_id_patterns(patterns: Optional[Sequence[str]]) -> List[Pattern[str]]:
    compiled: List[Pattern[str]] = []
    for raw_pattern in patterns or []:
        try:
            compiled.append(re.compile(raw_pattern))
        except re.error as exc:
            raise GEEDownloadError(
                f"Invalid --asset-id-pattern regex '{raw_pattern}': {exc}"
            ) from exc
    return compiled


def asset_matches_patterns(asset_id: str, compiled_patterns: Sequence[Pattern[str]]) -> bool:
    if not compiled_patterns:
        return True
    return any(pattern.search(asset_id) for pattern in compiled_patterns)


def infer_asset_id(asset_record: Dict[str, Any]) -> str:
    asset_id = asset_record.get("id") or asset_record.get("name")
    if not asset_id:
        raise GEEDownloadError(f"Earth Engine asset record is missing id/name: {asset_record}")
    return str(asset_id).rstrip("/")


def list_assets_under_parent(
    ee_module,
    asset_parent: str,
    *,
    recursive: bool = True,
) -> List[Dict[str, Any]]:
    parent = normalize_gee_asset_parent(asset_parent)
    queue = [parent]
    visited = set()
    collected: List[Dict[str, Any]] = []

    while queue:
        current_parent = queue.pop(0)
        if current_parent in visited:
            continue
        visited.add(current_parent)

        page_token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"parent": current_parent, "pageSize": 1000}
            if page_token:
                request["pageToken"] = page_token

            response = ee_module.data.listAssets(request) or {}
            for asset in response.get("assets", []):
                asset_id = infer_asset_id(asset)
                asset_type = str(asset.get("type") or "").upper()
                if recursive and asset_type in CONTAINER_ASSET_TYPES:
                    queue.append(asset_id)
                    continue
                collected.append(asset)

            page_token = response.get("nextPageToken") or response.get("next_page_token")
            if not page_token:
                break

    return collected


def resolve_assets_to_download(
    ee_module,
    *,
    asset_ids: Optional[Sequence[str]] = None,
    asset_parents: Optional[Sequence[str]] = None,
    compiled_patterns: Optional[Sequence[Pattern[str]]] = None,
    recursive: bool = True,
) -> List[Dict[str, Any]]:
    compiled_patterns = compiled_patterns or []
    deduped: Dict[str, Dict[str, Any]] = {}

    for raw_asset_id in asset_ids or []:
        asset_id = normalize_asset_id(raw_asset_id)
        asset_record = ee_module.data.getAsset(asset_id)
        deduped[infer_asset_id(asset_record)] = asset_record

    for raw_parent in asset_parents or []:
        for asset_record in list_assets_under_parent(
            ee_module,
            raw_parent,
            recursive=recursive,
        ):
            deduped[infer_asset_id(asset_record)] = asset_record

    filtered: List[Dict[str, Any]] = []
    for asset_id in sorted(deduped):
        asset_record = deduped[asset_id]
        asset_type = str(asset_record.get("type") or "").upper()
        if asset_type not in DOWNLOADABLE_ASSET_TYPES:
            continue
        if asset_matches_patterns(asset_id, compiled_patterns):
            filtered.append(asset_record)
    return filtered


def choose_output_anchor(asset_id: str, asset_parents: Optional[Sequence[str]]) -> Optional[str]:
    matching_parents = [
        normalize_gee_asset_parent(parent)
        for parent in asset_parents or []
        if asset_id == normalize_gee_asset_parent(parent)
        or asset_id.startswith(normalize_gee_asset_parent(parent) + "/")
    ]
    if not matching_parents:
        return None
    return max(matching_parents, key=len)


def asset_relative_output_path(asset_id: str, asset_parents: Optional[Sequence[str]]) -> Path:
    anchor = choose_output_anchor(asset_id, asset_parents)
    if anchor:
        relative = asset_id[len(anchor):].lstrip("/")
        if relative:
            return Path(relative)

    assets_marker = "/assets/"
    if assets_marker in asset_id:
        relative = asset_id.split(assets_marker, 1)[1]
        return Path(relative)

    return Path(asset_id.replace("/", "_"))


def build_output_path_for_asset(
    asset_id: str,
    *,
    output_dir: Path,
    asset_parents: Optional[Sequence[str]],
    file_format: str,
) -> Path:
    extension = SUPPORTED_DOWNLOAD_FORMATS[file_format]
    relative_path = asset_relative_output_path(asset_id, asset_parents)
    return (output_dir / relative_path).with_suffix(extension)


def parse_selectors(raw_selectors: Optional[Sequence[str]]) -> Optional[List[str]]:
    selectors = [str(selector).strip() for selector in raw_selectors or [] if str(selector).strip()]
    return selectors or None


def download_table_asset(
    ee_module,
    asset_record: Dict[str, Any],
    *,
    output_path: Path,
    file_format: str,
    selectors: Optional[Sequence[str]] = None,
    request_timeout_seconds: int = 600,
    overwrite: bool = False,
) -> Dict[str, Any]:
    asset_id = infer_asset_id(asset_record)
    output_path = output_path.expanduser().resolve()

    if output_path.exists() and not overwrite:
        return {
            "ok": True,
            "asset_id": asset_id,
            "asset_type": asset_record.get("type"),
            "output_path": str(output_path),
            "skipped": True,
            "reason": "already_exists",
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    selectors = list(selectors) if selectors else None
    file_basename = output_path.stem
    fc = ee_module.FeatureCollection(asset_id)
    download_url = fc.getDownloadURL(
        filetype=file_format,
        selectors=selectors,
        filename=file_basename,
    )

    log_progress(f"Downloading {asset_id} -> {output_path}")
    started = time.monotonic()
    with requests.get(download_url, stream=True, timeout=(30, request_timeout_seconds)) as response:
        response.raise_for_status()
        bytes_written = 0
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                bytes_written += len(chunk)

    elapsed = time.monotonic() - started
    rate = bytes_written / elapsed if elapsed > 0 else 0.0
    log_progress(
        f"Finished {asset_id}: {bytes_written:,} bytes in {elapsed:.1f}s "
        f"({rate:,.0f} bytes/s)"
    )
    return {
        "ok": True,
        "asset_id": asset_id,
        "asset_type": asset_record.get("type"),
        "output_path": str(output_path),
        "download_url": download_url,
        "bytes_written": bytes_written,
        "selectors": selectors,
        "skipped": False,
    }


def print_cli_result(result: Dict[str, Any]) -> None:
    if not result.get("ok"):
        print(
            f"FAILED  {result.get('asset_id')}: {result.get('error')}",
            file=sys.stderr,
        )
        return

    message = f"OK      {result['asset_id']} -> {result['output_path']}"
    if result.get("skipped"):
        message += " | skipped=already_exists"
    elif result.get("bytes_written") is not None:
        message += f" | bytes={result['bytes_written']}"
    print(message)


def write_summary(summary_path: Path, summary: Dict[str, Any]) -> None:
    summary_path = summary_path.expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download Earth Engine table assets (FeatureCollections / tables) "
            "by exact asset id, folder scan, or full-id regex matching."
        )
    )
    parser.add_argument(
        "--asset-id",
        action="append",
        dest="asset_ids",
        help="Exact Earth Engine asset id to download. Repeat as needed.",
    )
    parser.add_argument(
        "--asset-parent",
        action="append",
        dest="asset_parents",
        help=(
            "Earth Engine asset folder to scan, e.g. "
            "projects/ee-corestackdev/assets/apps/mws"
        ),
    )
    parser.add_argument(
        "--asset-id-pattern",
        action="append",
        dest="asset_id_patterns",
        help=(
            "Python regex matched against the full asset id. Repeat to OR multiple patterns."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for downloaded files. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--format",
        dest="file_format",
        choices=sorted(SUPPORTED_DOWNLOAD_FORMATS),
        default="geojson",
        help="Download format. Default: geojson.",
    )
    parser.add_argument(
        "--selector",
        action="append",
        dest="selectors",
        help="Optional property name to include in the download. Repeat as needed.",
    )
    parser.add_argument(
        "--gee-account-id",
        type=int,
        help="Use credentials stored in the Django GEEAccount model.",
    )
    parser.add_argument(
        "--service-account-json",
        help="Path to a service-account JSON file to use instead of Django GEEAccount credentials.",
    )
    parser.add_argument(
        "--recursive",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Recursively scan child folders under each --asset-parent. Default: true.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing local file if present.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Abort the batch on the first failed asset.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the resolved downloadable assets without downloading them.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of matched assets to process.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=600,
        help="HTTP read timeout per asset download. Default: 600.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        help="Optional JSON summary path. Defaults to <output-dir>/gee_download_summary.json.",
    )
    return parser


def cli_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)

    asset_ids = args.asset_ids or []
    asset_parents = args.asset_parents or []
    if not asset_ids and not asset_parents:
        parser.error("Provide at least one --asset-id or --asset-parent.")

    if args.gee_account_id and args.service_account_json:
        parser.error("Use either --gee-account-id or --service-account-json, not both.")

    if args.gee_account_id or not args.service_account_json:
        bootstrap_django_for_cli()

    try:
        compiled_patterns = compile_asset_id_patterns(args.asset_id_patterns)
        selectors = parse_selectors(args.selectors)
        output_dir = args.output_dir.expanduser().resolve()
        summary_path = (
            args.summary_path.expanduser().resolve()
            if args.summary_path
            else output_dir / DEFAULT_SUMMARY_FILENAME
        )
    except GEEDownloadError as exc:
        parser.error(str(exc))

    ee_module, _credentials, _key_dict = initialize_gee_session(
        gee_account_id=args.gee_account_id,
        service_account_json_path=args.service_account_json,
    )

    try:
        matched_assets = resolve_assets_to_download(
            ee_module,
            asset_ids=asset_ids,
            asset_parents=asset_parents,
            compiled_patterns=compiled_patterns,
            recursive=args.recursive,
        )
    except Exception as exc:
        print(f"FAILED  asset resolution: {exc}", file=sys.stderr)
        return 1

    if args.limit is not None:
        matched_assets = matched_assets[: args.limit]

    log_progress(f"Resolved {len(matched_assets)} downloadable asset(s)")
    if args.dry_run:
        for asset_record in matched_assets:
            asset_id = infer_asset_id(asset_record)
            output_path = build_output_path_for_asset(
                asset_id,
                output_dir=output_dir,
                asset_parents=asset_parents,
                file_format=args.file_format,
            )
            print(f"MATCH   {asset_id} -> {output_path}")
        return 0

    results: List[Dict[str, Any]] = []
    ok_count = 0
    failed_count = 0
    skipped_count = 0

    for asset_record in matched_assets:
        asset_id = infer_asset_id(asset_record)
        output_path = build_output_path_for_asset(
            asset_id,
            output_dir=output_dir,
            asset_parents=asset_parents,
            file_format=args.file_format,
        )
        try:
            result = download_table_asset(
                ee_module,
                asset_record,
                output_path=output_path,
                file_format=args.file_format,
                selectors=selectors,
                request_timeout_seconds=args.request_timeout_seconds,
                overwrite=args.overwrite,
            )
        except Exception as exc:
            result = {
                "ok": False,
                "asset_id": asset_id,
                "asset_type": asset_record.get("type"),
                "output_path": str(output_path),
                "error": str(exc),
            }

        results.append(result)
        print_cli_result(result)

        if result.get("ok"):
            ok_count += 1
            if result.get("skipped"):
                skipped_count += 1
        else:
            failed_count += 1
            if args.stop_on_error:
                break

    summary = {
        "asset_id_patterns": args.asset_id_patterns or [],
        "asset_ids": asset_ids,
        "asset_parents": asset_parents,
        "dry_run": args.dry_run,
        "file_format": args.file_format,
        "limit": args.limit,
        "matched_asset_count": len(matched_assets),
        "ok_count": ok_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "output_dir": str(output_dir),
        "results": results,
    }
    write_summary(summary_path, summary)

    print(
        f"Completed downloads: {ok_count} succeeded, {failed_count} failed, "
        f"{skipped_count} skipped"
    )
    print(f"Summary written to: {summary_path}")
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
