"""General CoRE Stack Earth Engine asset manager.

This utility is for operational GEE maintenance across CoRE Stack asset trees.
It can list, make public, delete, update asset metadata properties, and create
renamed-column replacement table assets for exact assets, folder scans, or
known pattern classes.

Fast Antyodaya examples:

1. List Antyodaya assets without crawling the full ``apps/mws`` tree. This
   derives exact expected asset ids from the admin combination cache and probes
   those ids in parallel:

   python utilities/scripts/gee_asset_manager.py \
     --pattern-name antyodaya20 --state "uttar pradesh" --district lucknow \
     --gee-account-id 26 --workers 32

2. The same fast path is used when an Antyodaya-looking regex is provided with
   no explicit parent:

   python utilities/scripts/gee_asset_manager.py \
     --asset-id-pattern antyodaya --gee-account-id 26 --workers 32

3. Make all matched Antyodaya assets public. Mutating operations require
   ``--execute``; deletion has extra guards.

   python utilities/scripts/gee_asset_manager.py \
     --operation make-public --pattern-name antyodaya20 \
     --gee-account-id 26 --workers 32 --execute

4. Start replacement-table exports with renamed columns. Earth Engine tables
   cannot be renamed in place safely, so this creates sibling assets with the
   suffix ``_renamed`` unless another destination option is supplied.

   python utilities/scripts/gee_asset_manager.py \
     --operation rename-columns --pattern-name antyodaya20 \
     --rename-column "legacy_column:standard_column" \
     --destination-suffix _village_name_fixed \
     --gee-account-id 26 --execute

5. Conservative deletion. This requires ``--execute``,
   ``--i-understand-delete``, and an interactive confirmation phrase.

   python utilities/scripts/gee_asset_manager.py \
     --operation delete --pattern-name antyodaya20 --state bihar --district jamui \
     --gee-account-id 26 --execute --i-understand-delete

Folder scan examples:

6. Scan a narrow folder recursively:

   python utilities/scripts/gee_asset_manager.py \
     --asset-parent projects/ee-corestackdev/assets/apps/mws/uttar_pradesh/lucknow \
     --asset-id-pattern antyodaya20 --gee-account-id 26

7. Use a local scan cache for repeated broad arbitrary searches:

   python utilities/scripts/gee_asset_manager.py \
     --asset-parent projects/ee-corestackdev/assets/apps/mws \
     --asset-id-pattern facilities_proximity --use-cache --refresh-cache \
     --gee-account-id 26
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Pattern, Sequence

if __package__ in (None, ""):
    REPO_ROOT_FOR_SCRIPT = Path(__file__).resolve().parents[3]
    if str(REPO_ROOT_FOR_SCRIPT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT_FOR_SCRIPT))

from utilities.scripts.gee.gee_download import (  # noqa: E402
    asset_matches_patterns,
    compile_asset_id_patterns,
    infer_asset_id,
    normalize_asset_id,
)
from utilities.scripts.gee.gee_upload import (  # noqa: E402
    bootstrap_django_for_cli,
    gee_asset_exists,
    initialize_gee_session,
    log_progress,
    normalize_gee_asset_parent,
    sanitize_gee_asset_name,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ASSET_PARENT = "projects/ee-corestackdev/assets/apps/mws"
DEFAULT_SUMMARY_PATH = (
    REPO_ROOT / "data" / "gee_asset_manager" / "gee_asset_manager_summary.json"
)
DEFAULT_CACHE_PATH = REPO_ROOT / "data" / "gee_asset_manager" / "asset_cache.jsonl"
DEFAULT_ANTYODAYA_COMBINATIONS_CACHE = (
    REPO_ROOT
    / "data"
    / "antyodaya"
    / "integration-analysis"
    / "admin_tehsil_combinations_cache.json"
)
DEFAULT_OPERATION = "list"
CONTAINER_ASSET_TYPES = {"FOLDER"}
MUTATING_OPERATIONS = {"make-public", "set-asset-property", "rename-columns", "delete"}


class GEEAssetManagerError(Exception):
    """Raised when GEE asset manager setup or execution fails."""


@dataclass(frozen=True)
class PatternDefinition:
    name: str
    description: str
    asset_id_pattern: str
    default_parents: tuple[str, ...] = (DEFAULT_ASSET_PARENT,)
    candidate_builder: Optional[Callable[[argparse.Namespace], List[str]]] = None


def normalize_location(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def load_antyodaya_combinations(cache_path: Path) -> List[Dict[str, Any]]:
    if not cache_path.exists():
        raise GEEAssetManagerError(
            f"Antyodaya combination cache does not exist: {cache_path}. "
            "Build it with utilities/scripts/antyodaya_api_batch_generate.py "
            "--build-combinations-cache --force-refresh-combinations --dry-run."
        )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    combinations = payload.get("combinations")
    if not isinstance(combinations, list):
        raise GEEAssetManagerError(
            f"Antyodaya combination cache is missing 'combinations': {cache_path}"
        )
    return combinations


def build_antyodaya_expected_asset_ids(args: argparse.Namespace) -> List[str]:
    state_target = normalize_location(args.state) if args.state else None
    district_target = normalize_location(args.district) if args.district else None
    block_target = normalize_location(args.block) if args.block else None
    cache_path = Path(args.antyodaya_combinations_cache).expanduser().resolve()

    asset_ids: List[str] = []
    for combo in load_antyodaya_combinations(cache_path):
        state_slug = combo.get("state_slug") or normalize_location(combo.get("state"))
        district_slug = combo.get("district_slug") or normalize_location(combo.get("district"))
        block_slug = combo.get("block_slug") or normalize_location(combo.get("block"))
        if state_target and state_slug != state_target:
            continue
        if district_target and district_slug != district_target:
            continue
        if block_target and block_slug != block_target:
            continue
        asset_ids.append(
            f"{DEFAULT_ASSET_PARENT}/{state_slug}/{district_slug}/{block_slug}/"
            f"antyodaya20_{district_slug}_{block_slug}"
        )
    return sorted(set(asset_ids))


PATTERN_REGISTRY: Dict[str, PatternDefinition] = {
    "facilities_proximity": PatternDefinition(
        name="facilities_proximity",
        description="Facilities proximity table assets under apps/mws.",
        asset_id_pattern=r"(^|/)(test_)?facilities_proximity_[^/]+_[^/]+$",
    ),
    "antyodaya20": PatternDefinition(
        name="antyodaya20",
        description=(
            "Mission Antyodaya tehsil table assets, expected at "
            "apps/mws/{state}/{district}/{block}/antyodaya20_{district}_{block}."
        ),
        asset_id_pattern=r"(^|/)antyodaya20_[^/]+_[^/]+$",
        candidate_builder=build_antyodaya_expected_asset_ids,
    ),
    "antyodaya": PatternDefinition(
        name="antyodaya",
        description="Alias for the Antyodaya v20 table asset pattern.",
        asset_id_pattern=r"(^|/)antyodaya20_[^/]+_[^/]+$",
        candidate_builder=build_antyodaya_expected_asset_ids,
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def pattern_names_from_args(args: argparse.Namespace) -> List[str]:
    names = list(args.pattern_names or [])
    if not names:
        for raw_pattern in args.asset_id_patterns or []:
            if "antyodaya" in raw_pattern.lower():
                names.append("antyodaya20")
                break
    return list(dict.fromkeys(names))


def load_pattern_definitions(names: Sequence[str]) -> List[PatternDefinition]:
    definitions: List[PatternDefinition] = []
    for name in names:
        key = str(name).strip().lower()
        if key not in PATTERN_REGISTRY:
            raise GEEAssetManagerError(
                f"Unknown --pattern-name '{name}'. Use --list-patterns to see options."
            )
        definitions.append(PATTERN_REGISTRY[key])
    return definitions


def list_known_patterns() -> None:
    print("Known pattern names:")
    for definition in PATTERN_REGISTRY.values():
        print(f"- {definition.name}: {definition.description}")
        print(f"  regex: {definition.asset_id_pattern}")


def parse_key_value(raw_value: str, *, separator: str = "=") -> tuple[str, str]:
    if separator not in raw_value:
        raise GEEAssetManagerError(
            f"Expected KEY{separator}VALUE, got '{raw_value}'."
        )
    key, value = raw_value.split(separator, 1)
    key = key.strip()
    value = value.strip()
    if not key:
        raise GEEAssetManagerError(f"Blank key in '{raw_value}'.")
    return key, value


def parse_key_value_pairs(raw_values: Optional[Sequence[str]]) -> Dict[str, str]:
    pairs: Dict[str, str] = {}
    for raw_value in raw_values or []:
        key, value = parse_key_value(raw_value)
        pairs[key] = value
    return pairs


def parse_rename_pairs(raw_values: Optional[Sequence[str]]) -> Dict[str, str]:
    pairs: Dict[str, str] = {}
    for raw_value in raw_values or []:
        key, value = parse_key_value(raw_value, separator=":")
        pairs[key] = value
    return pairs


def list_direct_child_assets(ee_module, parent: str) -> List[Dict[str, Any]]:
    normalized_parent = normalize_gee_asset_parent(parent)
    assets: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        request: Dict[str, Any] = {"parent": normalized_parent, "pageSize": 1000}
        if page_token:
            request["pageToken"] = page_token
        response = ee_module.data.listAssets(request) or {}
        assets.extend(response.get("assets", []))
        page_token = response.get("nextPageToken") or response.get("next_page_token")
        if not page_token:
            return assets


def list_assets_under_parent_fast(
    ee_module,
    asset_parent: str,
    *,
    recursive: bool = True,
    max_depth: Optional[int] = None,
    workers: int = 8,
    progress_every_folders: int = 100,
) -> List[Dict[str, Any]]:
    parent = normalize_gee_asset_parent(asset_parent)
    collected: List[Dict[str, Any]] = []
    visited: set[str] = set()
    scheduled: set[str] = {parent}
    completed_folders = 0
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        pending = {executor.submit(list_direct_child_assets, ee_module, parent): (parent, 0)}
        while pending:
            done, _remaining = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                current_parent, depth = pending.pop(future)
                if current_parent in visited:
                    continue
                visited.add(current_parent)
                completed_folders += 1
                if completed_folders == 1 or completed_folders % progress_every_folders == 0:
                    elapsed = time.monotonic() - started
                    log_progress(
                        f"Scanned {completed_folders:,} folder(s), "
                        f"collected {len(collected):,} asset(s) in {elapsed:.1f}s"
                    )

                for asset_record in future.result():
                    asset_id = infer_asset_id(asset_record)
                    asset_type = str(asset_record.get("type") or "").upper()
                    should_descend = (
                        recursive
                        and asset_type in CONTAINER_ASSET_TYPES
                        and (max_depth is None or depth < max_depth)
                    )
                    if should_descend and asset_id not in scheduled:
                        scheduled.add(asset_id)
                        pending[executor.submit(list_direct_child_assets, ee_module, asset_id)] = (
                            asset_id,
                            depth + 1,
                        )
                    else:
                        collected.append(asset_record)

    return collected


def fetch_asset_record(
    ee_module,
    asset_id: str,
    *,
    include_missing: bool = False,
) -> Optional[Dict[str, Any]]:
    normalized_asset_id = normalize_asset_id(asset_id)
    try:
        return ee_module.data.getAsset(normalized_asset_id)
    except Exception as exc:
        if include_missing:
            return {
                "id": normalized_asset_id,
                "type": "MISSING",
                "missing": True,
                "error": str(exc),
            }
        return None


def fetch_asset_records_parallel(
    ee_module,
    asset_ids: Sequence[str],
    *,
    workers: int,
    include_missing: bool = False,
) -> List[Dict[str, Any]]:
    deduped = sorted(set(normalize_asset_id(asset_id) for asset_id in asset_ids))
    if not deduped:
        return []

    records: List[Dict[str, Any]] = []
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(
                fetch_asset_record,
                ee_module,
                asset_id,
                include_missing=include_missing,
            )
            for asset_id in deduped
        ]
        for index, future in enumerate(futures, start=1):
            record = future.result()
            if record is not None:
                records.append(record)
            if index == 1 or index % 250 == 0 or index == len(futures):
                elapsed = time.monotonic() - started
                log_progress(
                    f"Probed {index:,}/{len(futures):,} exact asset id(s), "
                    f"found {len([r for r in records if not r.get('missing')]):,} "
                    f"in {elapsed:.1f}s"
                )
    return records


def load_asset_cache(cache_path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with cache_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def write_asset_cache(cache_path: Path, asset_records: Sequence[Dict[str, Any]]) -> None:
    cache_path = cache_path.expanduser().resolve()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        for asset_record in asset_records:
            handle.write(json.dumps(asset_record, default=str, ensure_ascii=False) + "\n")


def is_cache_fresh(cache_path: Path, ttl_hours: Optional[float]) -> bool:
    if not cache_path.exists():
        return False
    if ttl_hours is None:
        return True
    age_seconds = time.time() - cache_path.stat().st_mtime
    return age_seconds <= ttl_hours * 3600.0


def narrow_default_parent_from_location(args: argparse.Namespace) -> Optional[str]:
    parts = []
    for value in (args.state, args.district, args.block):
        if not value:
            break
        parts.append(normalize_location(value))
    if not parts:
        return None
    return f"{DEFAULT_ASSET_PARENT}/{'/'.join(parts)}"


def build_resolution_inputs(args: argparse.Namespace) -> tuple[List[str], List[str], List[str]]:
    pattern_definitions = load_pattern_definitions(pattern_names_from_args(args))
    asset_ids = list(args.asset_ids or [])
    asset_parents = list(args.asset_parents or [])
    asset_id_patterns = list(args.asset_id_patterns or [])

    for definition in pattern_definitions:
        asset_id_patterns.append(definition.asset_id_pattern)
        if definition.candidate_builder and not asset_parents:
            asset_ids.extend(definition.candidate_builder(args))
        elif not asset_parents:
            asset_parents.extend(definition.default_parents)

    narrowed_parent = narrow_default_parent_from_location(args)
    if narrowed_parent and not asset_ids and not asset_parents:
        asset_parents.append(narrowed_parent)

    if not asset_ids and not asset_parents and not asset_id_patterns:
        default_definition = PATTERN_REGISTRY["facilities_proximity"]
        asset_parents.extend(default_definition.default_parents)
        asset_id_patterns.append(default_definition.asset_id_pattern)

    return asset_ids, asset_parents, asset_id_patterns


def filter_asset_records(
    asset_records: Sequence[Dict[str, Any]],
    *,
    exact_asset_ids: Sequence[str],
    compiled_patterns: Sequence[Pattern[str]],
    asset_types: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    exact_ids = {normalize_asset_id(asset_id) for asset_id in exact_asset_ids}
    allowed_types = {asset_type.upper() for asset_type in asset_types or []}
    filtered: List[Dict[str, Any]] = []
    deduped: Dict[str, Dict[str, Any]] = {}
    for asset_record in asset_records:
        asset_id = infer_asset_id(asset_record)
        deduped[asset_id] = asset_record

    for asset_id in sorted(deduped):
        asset_record = deduped[asset_id]
        asset_type = str(asset_record.get("type") or "").upper()
        if allowed_types and asset_type not in allowed_types:
            continue
        if asset_id in exact_ids or asset_matches_patterns(asset_id, compiled_patterns):
            filtered.append(asset_record)
    return filtered


def resolve_assets(
    ee_module,
    *,
    asset_ids: Sequence[str],
    asset_parents: Sequence[str],
    compiled_patterns: Sequence[Pattern[str]],
    recursive: bool,
    max_depth: Optional[int],
    workers: int,
    include_missing: bool,
    use_cache: bool,
    refresh_cache: bool,
    cache_path: Path,
    cache_ttl_hours: Optional[float],
    asset_types: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    exact_records = fetch_asset_records_parallel(
        ee_module,
        asset_ids,
        workers=workers,
        include_missing=include_missing,
    )

    scanned_records: List[Dict[str, Any]] = []
    if asset_parents:
        if use_cache and not refresh_cache and is_cache_fresh(cache_path, cache_ttl_hours):
            log_progress(f"Using asset scan cache: {cache_path}")
            scanned_records = load_asset_cache(cache_path)
        else:
            for parent in asset_parents:
                log_progress(f"Scanning GEE parent: {parent}")
                scanned_records.extend(
                    list_assets_under_parent_fast(
                        ee_module,
                        parent,
                        recursive=recursive,
                        max_depth=max_depth,
                        workers=workers,
                    )
                )
            if use_cache:
                write_asset_cache(cache_path, scanned_records)
                log_progress(f"Wrote asset scan cache: {cache_path}")

    return filter_asset_records(
        [*exact_records, *scanned_records],
        exact_asset_ids=asset_ids,
        compiled_patterns=compiled_patterns,
        asset_types=asset_types,
    )


def sort_assets_deepest_first(asset_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        asset_records,
        key=lambda record: (infer_asset_id(record).count("/"), infer_asset_id(record)),
        reverse=True,
    )


def confirmation_phrase(operation: str, count: int) -> str:
    if operation == "delete":
        return f"DELETE {count}"
    return f"APPLY {count}"


def confirm_interactively(operation: str, asset_records: Sequence[Dict[str, Any]]) -> bool:
    if not sys.stdin.isatty():
        raise GEEAssetManagerError(
            "Interactive confirmation is required. Run this command from a terminal "
            "or provide --yes for non-delete operations."
        )
    phrase = confirmation_phrase(operation, len(asset_records))
    typed = input(f"\nType '{phrase}' to continue: ").strip()
    return typed == phrase


def confirm_mutation(args: argparse.Namespace, asset_records: Sequence[Dict[str, Any]]) -> bool:
    if args.operation not in MUTATING_OPERATIONS:
        return True
    if not args.execute:
        return False
    if args.operation == "delete" and not args.i_understand_delete:
        raise GEEAssetManagerError(
            "Deletion requires --i-understand-delete in addition to --execute."
        )
    if args.yes and args.operation != "delete":
        return True
    return confirm_interactively(args.operation, asset_records)


def make_asset_public(ee_module, asset_record: Dict[str, Any]) -> Dict[str, Any]:
    asset_id = infer_asset_id(asset_record)
    try:
        acl = ee_module.data.getAssetAcl(asset_id)
        acl["all_users_can_read"] = True
        ee_module.data.setAssetAcl(asset_id, acl)
        updated_acl = ee_module.data.getAssetAcl(asset_id)
        return {
            "ok": bool(updated_acl.get("all_users_can_read")),
            "asset_id": asset_id,
            "operation": "make-public",
        }
    except Exception as exc:
        return {
            "ok": False,
            "asset_id": asset_id,
            "operation": "make-public",
            "error": str(exc),
        }


def delete_asset(ee_module, asset_record: Dict[str, Any]) -> Dict[str, Any]:
    asset_id = infer_asset_id(asset_record)
    try:
        ee_module.data.deleteAsset(asset_id)
        return {"ok": True, "asset_id": asset_id, "operation": "delete"}
    except Exception as exc:
        return {
            "ok": False,
            "asset_id": asset_id,
            "operation": "delete",
            "error": str(exc),
        }


def update_asset_properties(
    ee_module,
    asset_record: Dict[str, Any],
    properties: Dict[str, str],
) -> Dict[str, Any]:
    asset_id = infer_asset_id(asset_record)
    try:
        ee_module.data.updateAsset(asset_id, {"properties": properties}, ["properties"])
        return {
            "ok": True,
            "asset_id": asset_id,
            "operation": "set-asset-property",
            "properties": properties,
        }
    except Exception as exc:
        return {
            "ok": False,
            "asset_id": asset_id,
            "operation": "set-asset-property",
            "error": str(exc),
        }


def destination_asset_id_for(asset_id: str, args: argparse.Namespace) -> str:
    if args.destination_parent:
        parent = normalize_gee_asset_parent(args.destination_parent)
        return f"{parent}/{asset_id.rsplit('/', 1)[-1]}"
    return f"{asset_id}{args.destination_suffix}"


def rename_columns_to_new_asset(
    ee_module,
    asset_record: Dict[str, Any],
    rename_map: Dict[str, str],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    asset_id = infer_asset_id(asset_record)
    destination_id = destination_asset_id_for(asset_id, args)
    try:
        if gee_asset_exists(ee_module, destination_id):
            if not args.replace_destination:
                return {
                    "ok": False,
                    "asset_id": asset_id,
                    "destination_asset_id": destination_id,
                    "operation": "rename-columns",
                    "error": "destination_exists",
                }
            ee_module.data.deleteAsset(destination_id)

        fc = ee_module.FeatureCollection(asset_id)
        property_names = list(ee_module.Feature(fc.first()).propertyNames().getInfo())
        missing_columns = [old for old in rename_map if old not in property_names]
        if missing_columns and not args.skip_missing_columns:
            return {
                "ok": False,
                "asset_id": asset_id,
                "destination_asset_id": destination_id,
                "operation": "rename-columns",
                "error": f"missing columns: {missing_columns}",
            }

        selectors = []
        renamed = []
        for property_name in property_names:
            if property_name in rename_map:
                selectors.append(property_name)
                renamed.append(rename_map[property_name])
            else:
                selectors.append(property_name)
                renamed.append(property_name)

        transformed = fc.select(selectors, renamed, True)
        task = ee_module.batch.Export.table.toAsset(
            collection=transformed,
            description=sanitize_gee_asset_name(destination_id.rsplit("/", 1)[-1]),
            assetId=destination_id,
        )
        task.start()
        return {
            "ok": True,
            "asset_id": asset_id,
            "destination_asset_id": destination_id,
            "operation": "rename-columns",
            "task_id": task.id,
            "renamed_columns": rename_map,
            "source_columns": property_names,
        }
    except Exception as exc:
        return {
            "ok": False,
            "asset_id": asset_id,
            "destination_asset_id": destination_id,
            "operation": "rename-columns",
            "error": str(exc),
        }


def apply_operation(
    ee_module,
    asset_records: Sequence[Dict[str, Any]],
    args: argparse.Namespace,
) -> List[Dict[str, Any]]:
    if args.operation == "list":
        return []
    if args.operation == "make-public":
        return [make_asset_public(ee_module, record) for record in asset_records]
    if args.operation == "delete":
        return [delete_asset(ee_module, record) for record in sort_assets_deepest_first(asset_records)]
    if args.operation == "set-asset-property":
        properties = parse_key_value_pairs(args.asset_properties)
        if not properties:
            raise GEEAssetManagerError(
                "--operation set-asset-property requires --asset-property KEY=VALUE."
            )
        return [update_asset_properties(ee_module, record, properties) for record in asset_records]
    if args.operation == "rename-columns":
        rename_map = parse_rename_pairs(args.rename_columns)
        if not rename_map:
            raise GEEAssetManagerError(
                "--operation rename-columns requires --rename-column OLD:NEW."
            )
        return [rename_columns_to_new_asset(ee_module, record, rename_map, args) for record in asset_records]
    raise GEEAssetManagerError(f"Unsupported operation: {args.operation}")


def print_asset_listing(asset_records: Sequence[Dict[str, Any]]) -> None:
    print(f"\nFound {len(asset_records)} matching asset(s):")
    for asset_record in asset_records:
        asset_id = infer_asset_id(asset_record)
        missing = " missing=true" if asset_record.get("missing") else ""
        print(f"[MATCH] {asset_id} | type={asset_record.get('type')}{missing}")


def write_summary(summary_path: Path, summary: Dict[str, Any]) -> None:
    summary_path = summary_path.expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str),
        encoding="utf-8",
    )


def write_csv_listing(csv_path: Path, asset_records: Sequence[Dict[str, Any]]) -> None:
    csv_path = csv_path.expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["asset_id", "asset_type", "missing"])
        writer.writeheader()
        for asset_record in asset_records:
            writer.writerow(
                {
                    "asset_id": infer_asset_id(asset_record),
                    "asset_type": asset_record.get("type"),
                    "missing": bool(asset_record.get("missing")),
                }
            )


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "List and operate on Earth Engine assets by exact id, known CoRE Stack "
            "pattern, or folder scan. Mutating operations require --execute."
        )
    )
    parser.add_argument(
        "--operation",
        choices=["list", "make-public", "delete", "rename-columns", "set-asset-property"],
        default=DEFAULT_OPERATION,
        help="Operation to perform. Default: list.",
    )
    parser.add_argument("--list-patterns", action="store_true", help="Show known pattern names and exit.")
    parser.add_argument("--pattern-name", action="append", dest="pattern_names", help="Known pattern class to use. Repeat as needed.")
    parser.add_argument("--asset-id", action="append", dest="asset_ids", help="Exact Earth Engine asset id. Repeat as needed.")
    parser.add_argument("--asset-parent", action="append", dest="asset_parents", help="Earth Engine folder to scan. Repeat as needed.")
    parser.add_argument("--asset-id-pattern", action="append", dest="asset_id_patterns", help="Regex matched against the full asset id. Repeat to OR multiple patterns.")
    parser.add_argument("--match-all-under-parent", action="store_true", help="Allow mutating every asset discovered under --asset-parent when no pattern is supplied.")
    parser.add_argument("--asset-type", action="append", dest="asset_types", help="Restrict to asset type, e.g. TABLE or IMAGE. Repeat as needed.")
    parser.add_argument("--state", help="State filter for known CoRE Stack MWS patterns.")
    parser.add_argument("--district", help="District filter for known CoRE Stack MWS patterns.")
    parser.add_argument("--block", help="Block/tehsil filter for known CoRE Stack MWS patterns.")
    parser.add_argument("--gee-account-id", type=int, help="Use credentials stored in the Django GEEAccount model.")
    parser.add_argument("--service-account-json", help="Path to a service-account JSON file.")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=True, help="Recursively scan child folders under --asset-parent. Default: true.")
    parser.add_argument("--max-depth", type=int, help="Maximum folder depth to descend below each --asset-parent.")
    parser.add_argument("--workers", type=int, default=16, help="Parallel GEE metadata requests. Default: 16.")
    parser.add_argument("--limit", type=int, help="Maximum number of matched assets to process after sorting.")
    parser.add_argument("--include-missing", action="store_true", help="Include missing exact candidate ids in listing output.")
    parser.add_argument("--use-cache", action="store_true", help="Use/write a local JSONL cache for folder scan results.")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh folder scan cache instead of reading it.")
    parser.add_argument("--cache-ttl-hours", type=float, default=24.0, help="Folder scan cache TTL in hours. Default: 24.")
    parser.add_argument("--cache-path", type=Path, default=DEFAULT_CACHE_PATH, help=f"Folder scan cache path. Default: {DEFAULT_CACHE_PATH}")
    parser.add_argument("--summary-path", type=Path, default=DEFAULT_SUMMARY_PATH, help=f"JSON summary output path. Default: {DEFAULT_SUMMARY_PATH}")
    parser.add_argument("--csv-path", type=Path, help="Optional CSV listing output path.")
    parser.add_argument("--antyodaya-combinations-cache", default=str(DEFAULT_ANTYODAYA_COMBINATIONS_CACHE), help=f"Admin combination cache for fast Antyodaya exact-id expansion. Default: {DEFAULT_ANTYODAYA_COMBINATIONS_CACHE}")
    parser.add_argument("--execute", action="store_true", help="Actually apply mutating operation after the dry-run listing.")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation for non-delete mutating operations.")
    parser.add_argument("--i-understand-delete", action="store_true", help="Required for --operation delete.")
    parser.add_argument("--rename-column", action="append", dest="rename_columns", help="Column rename OLD:NEW. Repeat as needed.")
    parser.add_argument("--destination-suffix", default="_renamed", help="Suffix for rename-columns destination assets. Default: _renamed.")
    parser.add_argument("--destination-parent", help="Destination parent for rename-columns replacement assets.")
    parser.add_argument("--replace-destination", action="store_true", help="Delete existing rename-columns destination asset before starting export.")
    parser.add_argument("--skip-missing-columns", action="store_true", help="For rename-columns, allow assets missing one or more OLD columns.")
    parser.add_argument("--asset-property", action="append", dest="asset_properties", help="Asset metadata property KEY=VALUE for set-asset-property.")
    return parser


def cli_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)

    if args.list_patterns:
        list_known_patterns()
        return 0

    if args.gee_account_id and args.service_account_json:
        parser.error("Use either --gee-account-id or --service-account-json, not both.")

    if args.operation == "rename-columns" and args.destination_suffix == "":
        parser.error("--destination-suffix cannot be blank for rename-columns.")

    try:
        asset_ids, asset_parents, asset_id_patterns = build_resolution_inputs(args)
        compiled_patterns = compile_asset_id_patterns(asset_id_patterns)
    except Exception as exc:
        parser.error(str(exc))

    if (
        args.operation in MUTATING_OPERATIONS
        and asset_parents
        and not asset_id_patterns
        and not args.match_all_under_parent
    ):
        parser.error(
            "Mutating every asset under --asset-parent requires "
            "--match-all-under-parent or a narrower --asset-id-pattern/--pattern-name."
        )

    if args.gee_account_id or not args.service_account_json:
        bootstrap_django_for_cli()

    ee_module, _credentials, key_dict = initialize_gee_session(
        gee_account_id=args.gee_account_id,
        service_account_json_path=args.service_account_json,
    )

    started = time.monotonic()
    log_progress("Resolving matching Earth Engine assets")
    try:
        matched_assets = resolve_assets(
            ee_module,
            asset_ids=asset_ids,
            asset_parents=asset_parents,
            compiled_patterns=compiled_patterns,
            recursive=args.recursive,
            max_depth=args.max_depth,
            workers=args.workers,
            include_missing=args.include_missing,
            use_cache=args.use_cache,
            refresh_cache=args.refresh_cache,
            cache_path=Path(args.cache_path),
            cache_ttl_hours=args.cache_ttl_hours,
            asset_types=args.asset_types,
        )
    except Exception as exc:
        print(f"FAILED  asset resolution: {exc}", file=sys.stderr)
        return 1

    matched_assets = sort_assets_deepest_first(matched_assets)
    if args.limit is not None:
        matched_assets = matched_assets[: args.limit]

    print_asset_listing(matched_assets)
    if args.csv_path:
        write_csv_listing(args.csv_path, matched_assets)
        print(f"CSV listing written to: {Path(args.csv_path).expanduser().resolve()}")

    summary: Dict[str, Any] = {
        "created_at": utc_now_iso(),
        "operation": args.operation,
        "execute": args.execute,
        "gee_account_id": args.gee_account_id,
        "gee_project_id": key_dict.get("project_id"),
        "service_account_email": key_dict.get("client_email"),
        "asset_ids": asset_ids,
        "asset_parents": asset_parents,
        "asset_id_patterns": asset_id_patterns,
        "pattern_names": pattern_names_from_args(args),
        "state": args.state,
        "district": args.district,
        "block": args.block,
        "limit": args.limit,
        "matched_asset_count": len(matched_assets),
        "matched_assets": [
            {
                "asset_id": infer_asset_id(asset_record),
                "asset_type": asset_record.get("type"),
                "missing": bool(asset_record.get("missing")),
                "error": asset_record.get("error"),
            }
            for asset_record in matched_assets
        ],
        "results": [],
        "ok_count": 0,
        "failed_count": 0,
        "elapsed_seconds": None,
    }

    if not matched_assets:
        print("No matching assets found.")
        summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {Path(args.summary_path).expanduser().resolve()}")
        return 0

    if args.operation == "list":
        summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {Path(args.summary_path).expanduser().resolve()}")
        return 0

    if args.operation in MUTATING_OPERATIONS and not args.execute:
        print("\nDry run only. Rerun with --execute to apply the requested operation.")
        summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_summary(args.summary_path, summary)
        print(f"Summary written to: {Path(args.summary_path).expanduser().resolve()}")
        return 0

    try:
        if not confirm_mutation(args, matched_assets):
            print("Operation cancelled.")
            summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
            write_summary(args.summary_path, summary)
            print(f"Summary written to: {Path(args.summary_path).expanduser().resolve()}")
            return 0
        results = apply_operation(ee_module, matched_assets, args)
    except Exception as exc:
        print(f"FAILED  operation setup: {exc}", file=sys.stderr)
        return 1

    for result in results:
        if result.get("ok"):
            print(f"OK      {result.get('operation')} {result.get('asset_id')}")
            if result.get("destination_asset_id"):
                print(f"        -> {result['destination_asset_id']}")
            if result.get("task_id"):
                print(f"        task_id={result['task_id']}")
        else:
            print(
                f"FAILED  {result.get('operation')} {result.get('asset_id')}: "
                f"{result.get('error')}",
                file=sys.stderr,
            )

    summary["results"] = results
    summary["ok_count"] = sum(1 for result in results if result.get("ok"))
    summary["failed_count"] = sum(1 for result in results if not result.get("ok"))
    summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
    write_summary(args.summary_path, summary)
    print(
        f"Completed {args.operation}: {summary['ok_count']} succeeded, "
        f"{summary['failed_count']} failed"
    )
    print(f"Summary written to: {Path(args.summary_path).expanduser().resolve()}")
    return 1 if summary["failed_count"] else 0


# Backwards-compatible names used by the old gee_bulk_delete helper/tests.
def resolve_assets_to_delete(
    ee_module,
    *,
    asset_ids: Optional[Sequence[str]] = None,
    asset_parents: Optional[Sequence[str]] = None,
    compiled_patterns: Optional[Sequence[Pattern[str]]] = None,
    recursive: bool = True,
) -> List[Dict[str, Any]]:
    return resolve_assets(
        ee_module,
        asset_ids=asset_ids or [],
        asset_parents=asset_parents or [],
        compiled_patterns=compiled_patterns or [],
        recursive=recursive,
        max_depth=None,
        workers=1,
        include_missing=False,
        use_cache=False,
        refresh_cache=False,
        cache_path=DEFAULT_CACHE_PATH,
        cache_ttl_hours=None,
        asset_types=None,
    )


def sort_assets_for_deletion(asset_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sort_assets_deepest_first(asset_records)


def confirm_deletion_interactively(asset_records: Sequence[Dict[str, Any]]) -> bool:
    return confirm_interactively("delete", asset_records)


if __name__ == "__main__":
    raise SystemExit(cli_main())
