#!/usr/bin/env python3
"""Refresh changed district rows in the standard admin GeoPackage.

The first run treats input files newer than the existing GeoPackage as changed.
Successful runs persist an input manifest inside the GeoPackage, so later runs
also detect new, removed, or replaced files. Use ``--force-rebuild`` when a
global standardisation rule or the multipart review file has changed.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
from typing import Any, Sequence

import pyogrio

from build_cs_admin_boundary_standard import (
    DEFAULT_ADMIN_INPUT_DIR,
    DEFAULT_MULTIPART_ANALYSIS,
    DEFAULT_OUTPUT_GPKG,
    DEFAULT_OUTPUT_LAYER,
    DEFAULT_REPORTS_DIR,
    ROOT_DIR,
    build_standard_asset,
    normalize_slug,
    parse_args as parse_builder_args,
    repo_path,
)


DEFAULT_BASE_RESOURCE = Path("data/base_resources/cs_admin_standard.gpkg")
MANIFEST_TABLE = "cs_admin_standard_input_manifest"
REFRESH_TABLE = "cs_admin_standard_refresh_log"
TRACE_TABLE = "cs_admin_standard_source_trace"


def source_file_name(path: Path) -> str:
    return path.relative_to(ROOT_DIR).as_posix() if path.is_relative_to(ROOT_DIR) else path.as_posix()


def current_inputs(input_dir: Path) -> dict[str, dict[str, Any]]:
    inputs: dict[str, dict[str, Any]] = {}
    for path in sorted(input_dir.glob("*/*.geojson")):
        stat = path.stat()
        inputs[source_file_name(path)] = {
            "source_file": source_file_name(path),
            "source_state_slug": normalize_slug(path.parent.name),
            "source_district_slug": normalize_slug(path.stem),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return inputs


def load_manifest(gpkg_path: Path) -> dict[str, dict[str, Any]]:
    if not gpkg_path.exists():
        return {}
    with sqlite3.connect(gpkg_path) as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (MANIFEST_TABLE,),
        ).fetchone()
        if not exists:
            return {}
        columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{MANIFEST_TABLE}")')]
        return {
            row[0]: dict(zip(columns, row))
            for row in connection.execute(f'SELECT * FROM "{MANIFEST_TABLE}"')
        }


def changed_inputs(
    current: dict[str, dict[str, Any]],
    previous: dict[str, dict[str, Any]],
    gpkg_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if previous:
        changed = [
            item
            for name, item in current.items()
            if name not in previous
            or int(previous[name]["size_bytes"]) != item["size_bytes"]
            or int(previous[name]["mtime_ns"]) != item["mtime_ns"]
        ]
        removed = [item for name, item in previous.items() if name not in current]
        return changed, removed

    output_mtime = gpkg_path.stat().st_mtime_ns if gpkg_path.exists() else 0
    return [item for item in current.values() if item["mtime_ns"] > output_mtime], []


def write_manifest(connection: sqlite3.Connection, inputs: dict[str, dict[str, Any]]) -> None:
    connection.execute(f'DROP TABLE IF EXISTS "{MANIFEST_TABLE}"')
    connection.execute(
        f'''CREATE TABLE "{MANIFEST_TABLE}" (
            source_file TEXT PRIMARY KEY,
            source_state_slug TEXT NOT NULL,
            source_district_slug TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL
        )'''
    )
    connection.executemany(
        f'INSERT INTO "{MANIFEST_TABLE}" VALUES (?, ?, ?, ?, ?)',
        [
            (
                item["source_file"],
                item["source_state_slug"],
                item["source_district_slug"],
                item["size_bytes"],
                item["mtime_ns"],
            )
            for item in inputs.values()
        ],
    )


def record_refresh(connection: sqlite3.Connection, details: dict[str, Any]) -> None:
    connection.execute(
        f'''CREATE TABLE IF NOT EXISTS "{REFRESH_TABLE}" (
            refreshed_at_utc TEXT NOT NULL,
            mode TEXT NOT NULL,
            changed_files INTEGER NOT NULL,
            removed_files INTEGER NOT NULL,
            details_json TEXT NOT NULL
        )'''
    )
    connection.execute(
        f'INSERT INTO "{REFRESH_TABLE}" VALUES (datetime("now"), ?, ?, ?, ?)',
        (
            details["mode"],
            len(details.get("changed_files", [])),
            len(details.get("removed_files", [])),
            json.dumps(details, sort_keys=True),
        ),
    )


def copy_atomically(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def wait_for_file(path: Path, *, timeout_seconds: float = 30.0) -> None:
    """Wait for a replaced file to become visible on mounted/network filesystems."""

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.25)
    raise FileNotFoundError(f"Replaced file did not become visible within {timeout_seconds}s: {path}")


def layer_labels_for_scope(
    connection: sqlite3.Connection,
    state_slug: str,
    district_slug: str,
) -> set[tuple[str, str]]:
    return {
        (state_name, district_name)
        for state_name, district_name in connection.execute(
            f'''SELECT DISTINCT state_name, district_name
                FROM "{TRACE_TABLE}"
                WHERE source_state_slug = ? AND source_district_slug = ?
                  AND state_name IS NOT NULL AND district_name IS NOT NULL''',
            (state_slug, district_slug),
        )
    }


def replace_scope(
    target: Path,
    replacement: Path | None,
    *,
    layer: str,
    state_slug: str,
    district_slug: str,
) -> tuple[int, int]:
    with sqlite3.connect(target) as connection:
        labels = layer_labels_for_scope(connection, state_slug, district_slug)

    replacement_frame = None
    replacement_trace_rows: list[tuple[Any, ...]] = []
    trace_columns: list[str] = []
    if replacement is not None:
        replacement_frame = pyogrio.read_dataframe(replacement, layer=layer)
        labels.update(
            (str(state_name), str(district_name))
            for state_name, district_name in replacement_frame[["state_name", "district_name"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        with sqlite3.connect(replacement) as source:
            trace_columns = [row[1] for row in source.execute(f'PRAGMA table_info("{TRACE_TABLE}")')]
            replacement_trace_rows = source.execute(f'SELECT * FROM "{TRACE_TABLE}"').fetchall()

    with sqlite3.connect(target) as connection:
        deleted_rows = 0
        for state_name, district_name in labels:
            cursor = connection.execute(
                f'DELETE FROM "{layer}" WHERE state_name = ? AND district_name = ?',
                (state_name, district_name),
            )
            deleted_rows += cursor.rowcount
        connection.execute(
            f'DELETE FROM "{TRACE_TABLE}" WHERE source_state_slug = ? AND source_district_slug = ?',
            (state_slug, district_slug),
        )
        connection.commit()

    inserted_rows = 0
    if replacement_frame is not None and not replacement_frame.empty:
        pyogrio.write_dataframe(
            replacement_frame,
            target,
            layer=layer,
            driver="GPKG",
            append=True,
        )
        inserted_rows = len(replacement_frame)
    if replacement_trace_rows:
        quoted = ", ".join(f'"{column}"' for column in trace_columns)
        placeholders = ", ".join("?" for _ in trace_columns)
        with sqlite3.connect(target) as connection:
            connection.executemany(
                f'INSERT INTO "{TRACE_TABLE}" ({quoted}) VALUES ({placeholders})',
                replacement_trace_rows,
            )
            connection.commit()
    return deleted_rows, inserted_rows


def build_scope(
    input_dir: Path,
    multipart_csv: Path,
    state_slug: str,
    district_slug: str,
    output_dir: Path,
    layer: str,
) -> Path:
    output = output_dir / f"{state_slug.lower()}_{district_slug.lower()}.gpkg"
    args = parse_builder_args(
        [
            "--admin-input-dir", str(input_dir),
            "--multipart-analysis-csv", str(multipart_csv),
            "--output-gpkg", str(output),
            "--output-layer", layer,
            "--reports-dir", str(output_dir / "reports" / state_slug.lower() / district_slug.lower()),
            "--state", state_slug,
            "--district", district_slug,
            "--skip-geojson",
            "--skip-csv",
            "--overwrite",
        ]
    )
    build_standard_asset(args)
    return output


def force_rebuild(args: argparse.Namespace, inputs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    target = repo_path(args.output_gpkg)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cs_admin_full_rebuild_", dir=target.parent) as directory:
        staging = Path(directory) / target.name
        builder_args = parse_builder_args(
            [
                "--admin-input-dir", str(args.admin_input_dir),
                "--multipart-analysis-csv", str(args.multipart_analysis_csv),
                "--output-gpkg", str(staging),
                "--output-layer", args.output_layer,
                "--reports-dir", str(args.reports_dir),
                "--skip-geojson",
                "--skip-csv",
                "--overwrite",
            ]
        )
        result = build_standard_asset(builder_args)
        details = {"mode": "force_rebuild", "changed_files": sorted(inputs), "removed_files": []}
        with sqlite3.connect(staging) as connection:
            write_manifest(connection, inputs)
            record_refresh(connection, details)
            connection.commit()
        os.replace(staging, target)
        wait_for_file(target)
    copy_atomically(target, repo_path(args.base_resource))
    return {**details, "summary": result["summary"]}


def refresh(args: argparse.Namespace) -> dict[str, Any]:
    input_dir = repo_path(args.admin_input_dir)
    multipart_csv = repo_path(args.multipart_analysis_csv)
    target = repo_path(args.output_gpkg)
    current = current_inputs(input_dir)
    previous = load_manifest(target)
    changed, removed = changed_inputs(current, previous, target)

    if args.force_rebuild or not target.exists():
        return force_rebuild(args, current)

    scopes = sorted(
        {
            (item["source_state_slug"], item["source_district_slug"])
            for item in [*changed, *removed]
        }
    )
    details: dict[str, Any] = {
        "mode": "incremental" if scopes else "unchanged",
        "changed_files": sorted(item["source_file"] for item in changed),
        "removed_files": sorted(item["source_file"] for item in removed),
        "scopes": [],
    }
    if args.dry_run:
        details["scopes"] = [f"{state}/{district}" for state, district in scopes]
        return details

    if scopes:
        with tempfile.TemporaryDirectory(prefix="cs_admin_refresh_", dir=target.parent) as directory:
            temporary_dir = Path(directory)
            staging = temporary_dir / target.name
            shutil.copy2(target, staging)
            for state_slug, district_slug in scopes:
                scope_exists = any(
                    item["source_state_slug"] == state_slug
                    and item["source_district_slug"] == district_slug
                    for item in current.values()
                )
                replacement = (
                    build_scope(input_dir, multipart_csv, state_slug, district_slug, temporary_dir, args.output_layer)
                    if scope_exists
                    else None
                )
                deleted, inserted = replace_scope(
                    staging,
                    replacement,
                    layer=args.output_layer,
                    state_slug=state_slug,
                    district_slug=district_slug,
                )
                details["scopes"].append(
                    {
                        "state": state_slug,
                        "district": district_slug,
                        "deleted_rows": deleted,
                        "inserted_rows": inserted,
                    }
                )
            with sqlite3.connect(staging) as connection:
                write_manifest(connection, current)
                record_refresh(connection, details)
                connection.commit()
            os.replace(staging, target)
            wait_for_file(target)
    elif not previous:
        with sqlite3.connect(target) as connection:
            write_manifest(connection, current)
            record_refresh(connection, details)
            connection.commit()

    copy_atomically(target, repo_path(args.base_resource))
    return details


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-input-dir", type=Path, default=DEFAULT_ADMIN_INPUT_DIR)
    parser.add_argument("--multipart-analysis-csv", type=Path, default=DEFAULT_MULTIPART_ANALYSIS)
    parser.add_argument("--output-gpkg", type=Path, default=DEFAULT_OUTPUT_GPKG)
    parser.add_argument("--output-layer", default=DEFAULT_OUTPUT_LAYER)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--base-resource", type=Path, default=DEFAULT_BASE_RESOURCE)
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild the full national GeoPackage")
    parser.add_argument("--dry-run", action="store_true", help="Report detected changes without writing outputs")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    started = time.perf_counter()
    result = refresh(parse_args(argv))
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
