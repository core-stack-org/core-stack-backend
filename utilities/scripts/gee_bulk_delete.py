"""Safe Earth Engine bulk delete helper.

This CLI resolves Earth Engine assets by exact id or folder scan plus regex
matching, prints a dry-run listing, and only deletes after a mandatory
interactive confirmation step.

Default behavior with no arguments:
- scans ``projects/ee-corestackdev/assets/apps/mws``
- matches ``(^|/)(test_)?facilities_proximity_[^/]+_[^/]+$``
- prints the dry-run listing only

Examples:

`default dry run`
python utilities/scripts/gee_bulk_delete.py

`delete after listing and confirmation`
python utilities/scripts/gee_bulk_delete.py --execute

`delete a specific exact asset`
python utilities/scripts/gee_bulk_delete.py \
  --asset-id projects/ee-corestackdev/assets/apps/mws/odisha/koraput/jaypur/test_facilities_proximity_koraput_jaypur \
  --execute

`delete matched assets under specific parents`
python utilities/scripts/gee_bulk_delete.py \
  --asset-parent projects/ee-corestackdev/assets/apps/mws/odisha/koraput/jaypur \
  --asset-id-pattern '(^|/)(test_)?facilities_proximity_[^/]+_[^/]+$' \
  --service-account-json 'data/gee_confs/core-stack-learn-1234.json' \
  --execute
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Sequence

if __package__ in (None, ""):
    REPO_ROOT_FOR_SCRIPT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT_FOR_SCRIPT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT_FOR_SCRIPT))

from utilities.scripts.gee_download import (  # noqa: E402
    asset_matches_patterns,
    compile_asset_id_patterns,
    infer_asset_id,
    list_assets_under_parent,
    normalize_asset_id,
)
from utilities.scripts.gee_upload import (  # noqa: E402
    bootstrap_django_for_cli,
    initialize_gee_session,
    log_progress,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ASSET_PARENT = "projects/ee-corestackdev/assets/apps/mws"
DEFAULT_ASSET_ID_PATTERN = r"(^|/)(test_)?facilities_proximity_[^/]+_[^/]+$"
DEFAULT_SUMMARY_PATH = (
    REPO_ROOT / "data" / "fc_to_shape" / "gee_bulk_delete" / "gee_bulk_delete_summary.json"
)


class GEEBulkDeleteError(Exception):
    """Raised when bulk delete setup or execution fails."""


def resolve_assets_to_delete(
    ee_module,
    *,
    asset_ids: Optional[Sequence[str]] = None,
    asset_parents: Optional[Sequence[str]] = None,
    compiled_patterns: Optional[Sequence[Pattern[str]]] = None,
    recursive: bool = True,
) -> List[Dict[str, Any]]:
    compiled_patterns = compiled_patterns or []
    deduped: Dict[str, Dict[str, Any]] = {}
    exact_asset_ids = {normalize_asset_id(asset_id) for asset_id in asset_ids or []}

    for asset_id in sorted(exact_asset_ids):
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
        if asset_id in exact_asset_ids or asset_matches_patterns(asset_id, compiled_patterns):
            filtered.append(asset_record)
    return filtered


def sort_assets_for_deletion(asset_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        asset_records,
        key=lambda record: (
            infer_asset_id(record).count("/"),
            infer_asset_id(record),
        ),
        reverse=True,
    )


def confirmation_phrase_for(count: int) -> str:
    return f"DELETE {count}"


def confirm_deletion_interactively(asset_records: Sequence[Dict[str, Any]]) -> bool:
    if not sys.stdin.isatty():
        raise GEEBulkDeleteError(
            "Interactive confirmation is required for deletions. "
            "Run this command from a terminal."
        )

    phrase = confirmation_phrase_for(len(asset_records))
    typed = input(
        "\nType "
        f"'{phrase}'"
        " to permanently delete the listed Earth Engine assets: "
    ).strip()
    return typed == phrase


def delete_assets(
    ee_module,
    asset_records: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for asset_record in asset_records:
        asset_id = infer_asset_id(asset_record)
        try:
            ee_module.data.deleteAsset(asset_id)
            result = {
                "ok": True,
                "asset_id": asset_id,
                "asset_type": asset_record.get("type"),
            }
            log_progress(f"Deleted {asset_id}")
        except Exception as exc:
            result = {
                "ok": False,
                "asset_id": asset_id,
                "asset_type": asset_record.get("type"),
                "error": str(exc),
            }
        results.append(result)
    return results


def print_asset_listing(asset_records: Sequence[Dict[str, Any]]) -> None:
    print(f"\nFound {len(asset_records)} matching asset(s):")
    for asset_record in asset_records:
        print(f"[MATCH] {infer_asset_id(asset_record)} | type={asset_record.get('type')}")


def print_delete_result(result: Dict[str, Any]) -> None:
    if result.get("ok"):
        print(f"DELETED {result['asset_id']}")
    else:
        print(f"FAILED  {result['asset_id']}: {result.get('error')}", file=sys.stderr)


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
            "List and optionally delete Earth Engine assets by exact id or by "
            "folder scan plus regex matching. A dry-run listing is always shown "
            "before any deletion, and interactive confirmation is mandatory."
        )
    )
    parser.add_argument(
        "--asset-id",
        action="append",
        dest="asset_ids",
        help="Exact Earth Engine asset id to include. Repeat as needed.",
    )
    parser.add_argument(
        "--asset-parent",
        action="append",
        dest="asset_parents",
        help="Earth Engine asset folder to scan.",
    )
    parser.add_argument(
        "--asset-id-pattern",
        action="append",
        dest="asset_id_patterns",
        help="Regex matched against the full asset id. Repeat to OR multiple patterns.",
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
        "--match-all-under-parent",
        action="store_true",
        help=(
            "Allow every asset discovered under --asset-parent to match even when "
            "no --asset-id-pattern is provided."
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="After the dry-run listing, prompt for confirmation and delete the matches.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of matched assets to process after sorting.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        default=DEFAULT_SUMMARY_PATH,
        help=f"JSON summary output path. Default: {DEFAULT_SUMMARY_PATH}",
    )
    return parser


def cli_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)

    asset_ids = args.asset_ids or []
    asset_parents = args.asset_parents or []
    asset_id_patterns = args.asset_id_patterns or []

    if not asset_ids and not asset_parents:
        asset_parents = [DEFAULT_ASSET_PARENT]
        asset_id_patterns = [DEFAULT_ASSET_ID_PATTERN]

    if args.gee_account_id and args.service_account_json:
        parser.error("Use either --gee-account-id or --service-account-json, not both.")

    if asset_parents and not asset_id_patterns and not args.match_all_under_parent:
        parser.error(
            "When using --asset-parent, provide --asset-id-pattern or "
            "--match-all-under-parent."
        )

    if args.gee_account_id or not args.service_account_json:
        bootstrap_django_for_cli()

    try:
        compiled_patterns = compile_asset_id_patterns(asset_id_patterns)
    except Exception as exc:
        parser.error(str(exc))

    ee_module, _credentials, _key_dict = initialize_gee_session(
        gee_account_id=args.gee_account_id,
        service_account_json_path=args.service_account_json,
    )

    log_progress("Resolving matching Earth Engine assets")
    try:
        matched_assets = resolve_assets_to_delete(
            ee_module,
            asset_ids=asset_ids,
            asset_parents=asset_parents,
            compiled_patterns=compiled_patterns,
            recursive=args.recursive,
        )
    except Exception as exc:
        print(f"FAILED  asset resolution: {exc}", file=sys.stderr)
        return 1

    matched_assets = sort_assets_for_deletion(matched_assets)
    if args.limit is not None:
        matched_assets = matched_assets[: args.limit]

    print_asset_listing(matched_assets)

    summary: Dict[str, Any] = {
        "asset_ids": asset_ids,
        "asset_parents": asset_parents,
        "asset_id_patterns": asset_id_patterns,
        "execute": args.execute,
        "limit": args.limit,
        "matched_asset_count": len(matched_assets),
        "matched_assets": [
            {
                "asset_id": infer_asset_id(asset_record),
                "asset_type": asset_record.get("type"),
            }
            for asset_record in matched_assets
        ],
        "deleted_count": 0,
        "failed_count": 0,
        "results": [],
    }

    if not matched_assets:
        print("No matching assets found.")
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {args.summary_path.resolve()}")
        return 0

    if not args.execute:
        print(
            "\nDry run only. No assets were deleted. "
            "Rerun with --execute to enable the confirmation prompt."
        )
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {args.summary_path.resolve()}")
        return 0

    try:
        confirmed = confirm_deletion_interactively(matched_assets)
    except GEEBulkDeleteError as exc:
        print(f"FAILED  confirmation: {exc}", file=sys.stderr)
        return 1

    if not confirmed:
        print("Deletion cancelled.")
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {args.summary_path.resolve()}")
        return 0

    delete_results = delete_assets(ee_module, matched_assets)
    for result in delete_results:
        print_delete_result(result)

    summary["results"] = delete_results
    summary["deleted_count"] = sum(1 for result in delete_results if result.get("ok"))
    summary["failed_count"] = sum(1 for result in delete_results if not result.get("ok"))
    write_summary(args.summary_path, summary)

    print(
        f"Completed deletions: {summary['deleted_count']} succeeded, "
        f"{summary['failed_count']} failed"
    )
    print(f"Summary written to: {args.summary_path.resolve()}")
    return 1 if summary["failed_count"] else 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
