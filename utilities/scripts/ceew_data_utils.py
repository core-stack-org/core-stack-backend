#!/usr/bin/env python3
"""Discover and profile CEEW CRAVIS data before bulk download.

The CRAVIS visualisation page does not expose data through the page URL itself.
Its frontend calls a JSON API at ``https://test-api.cravis.ai``. This utility
keeps the first pass deliberately read-only and lightweight:

- collect available climate, risk-index, and sectoral dataset metadata
- probe static file URLs referenced by the app bundle
- fetch a couple of small scoped samples for schema/usage analysis
- write an inventory report that can guide a later storage/download plan

Examples:

  python3 utilities/ceew_data_utils.py inventory

  python3 utilities/ceew_data_utils.py sample \
      --metric temp_mean_mean_of_mean_temperature_annual \
      --state BIHAR --district JAMUI

  python3 utilities/ceew_data_utils.py analyse \
      --output data/ceew_discovery/ceew_analysis_report.json \
      --markdown-output data/ceew_discovery/ceew_analysis_report.md

  # Resolve a long run without downloading yet.
  python3 utilities/ceew_data_utils.py download --dry-run \
      --all-metrics --representations zonalData,gridData \
      --state BIHAR --district JAMUI \
      --tasks-output data/ceew_discovery/ceew_tasks_preview.json

  # Download with retry, atomic writes, and a manifest checkpoint.
  python3 utilities/ceew_data_utils.py download \
      --output-dir data/ceew_downloads \
      --metrics temp_mean_mean_of_mean_temperature_annual \
      --state BIHAR --district JAMUI

  # Full traceable all-data download. This keeps the API JSON structure intact,
  # validates every payload, writes readable JSON, and records every file in a
  # manifest. Empty state/district means all-India wherever the API supports it.
  python3 utilities/ceew_data_utils.py --timeout 120 download \
      --output-dir data/ceew_downloads_full_traceable \
      --manifest data/ceew_downloads_full_traceable/ceew_download_manifest.json \
      --all-metrics \
      --representations zonalData,gridData \
      --scenario-modes rcpssp,globalWarming \
      --rcps all \
      --warming-levels 1c,1_5c,2c,3c \
      --state "" --district "" \
      --include-metadata \
      --include-risk-indices --risk-indices all \
      --include-sectoral --sectoral-keys all \
      --allow-unscoped-grid \
      --retries 6 --backoff 5 --chunk-size 1048576 \
      --pretty-json \
      --progress-every 10 \
      --tasks-output data/ceew_downloads_full_traceable/resolved_download_tasks.json

  # Run embedded offline downloader tests.
  python3 utilities/ceew_data_utils.py self-test
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import multiprocessing
from pathlib import Path
import re
import sys
import tempfile
import time
from typing import Any, Iterable
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_APP_BASE_URL = "https://test.cravis.ai"
DEFAULT_API_BASE_URL = "https://test-api.cravis.ai"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_RETRIES = 4
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_DOWNLOAD_LIMIT_BYTES = 10 * 1024 * 1024 * 1024

DEFAULT_METRIC = "temp_mean_mean_of_mean_temperature_annual"
DEFAULT_RISK_INDEX = "risk_heat"
DEFAULT_STATE = "BIHAR"
DEFAULT_DISTRICT = "JAMUI"

CLIMATE_REPRESENTATIONS = ("zonalData", "gridData")
SCENARIO_MODES = ("rcpssp", "globalWarming")
RCPS = ("RCP45", "RCP85")
SCENARIO_TYPE_VALUES = ("imd", "RCP45", "RCP85")
GLOBAL_WARMING_LEVELS = ("1c", "1_5c", "2c", "3c")
NON_RETRYABLE_HTTP_STATUS_CODES = {400, 401, 403, 404, 405, 409, 410, 422}


@dataclass(frozen=True)
class HTTPProbe:
    url: str
    status: int | None
    content_type: str | None = None
    content_length: int | None = None
    content_encoding: str | None = None
    last_modified: str | None = None
    etag: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class SampleRequest:
    name: str
    kind: str
    path: str
    params: dict[str, str]


@dataclass(frozen=True)
class DownloadTask:
    name: str
    kind: str
    url: str
    relative_path: Path


@dataclass(frozen=True)
class DownloadResult:
    name: str
    kind: str
    url: str
    path: str
    status: str
    attempts: int
    bytes_written: int = 0
    sha256: str | None = None
    duration_seconds: float = 0.0
    error: str | None = None


@dataclass(frozen=True)
class SizeProbe:
    name: str
    kind: str
    url: str
    relative_path: str
    status: int | None
    content_length: int | None = None
    content_type: str | None = None
    content_encoding: str | None = None
    error: str | None = None


class CEEWDataExtractor:
    """Small client for CRAVIS/CEEW discovery and scoped sample analysis."""

    def __init__(
        self,
        app_base_url: str = DEFAULT_APP_BASE_URL,
        api_base_url: str = DEFAULT_API_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.app_base_url = app_base_url.rstrip("/")
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch_metric(
        self,
        metric_name: str,
        rcp: str = "RCP85",
        representation: str = "zonalData",
        scenario_mode: str = "rcpssp",
        focus_state: str = "",
        focus_district: str = "",
        focus_grid: str = "",
        slope: bool = False,
    ) -> dict[str, Any]:
        """Fetch a scoped metric payload from the CRAVIS API.

        This is the API path exposed by the current FastAPI schema:
        /api/v1/metrics/{representation}/{scenario}/{scenario_type}/{metric}
        """

        params = {
            "focusState": focus_state.strip().upper(),
            "focusDistrict": focus_district.strip().upper(),
            "focusGrid": focus_grid.strip(),
            "slope": str(slope).lower(),
        }
        path = f"/api/v1/metrics/{representation}/{scenario_mode}/{rcp}/{metric_name}"
        return self._get_json(self._api_url(path, params))

    def fetch_metric_metadata(self) -> dict[str, Any]:
        return self._get_json(self._api_url("/api/v1/metrics/metadata"))

    def fetch_sectoral_metadata(self) -> dict[str, Any]:
        return self._get_json(self._api_url("/api/v1/metrics/sectoral_data/metadata"))

    def fetch_risk_index_metadata(self) -> dict[str, Any]:
        return self._get_json(self._api_url("/api/v1/metrics/risk_index/metadata"))

    def fetch_risk_index(
        self,
        risk_key: str = DEFAULT_RISK_INDEX,
        focus_state: str = "",
        focus_district: str = "",
    ) -> dict[str, Any]:
        params = {
            "focusState": focus_state.strip().upper(),
            "focusDistrict": focus_district.strip().upper(),
        }
        return self._get_json(self._api_url(f"/api/v1/metrics/risk_index/{risk_key}", params))

    def fetch_sectoral_data(self, sectoral_key: str) -> Any:
        return self._get_json(self._api_url(f"/api/v1/metrics/sectoral_data/{sectoral_key}"))

    def build_inventory(self, probe_static_files: bool = True) -> dict[str, Any]:
        metrics = self.fetch_metric_metadata()
        sectoral = self.fetch_sectoral_metadata()
        risk = self.fetch_risk_index_metadata()

        climate_resources = []
        for key, metadata in sorted(metrics.items()):
            climate_resources.append(
                {
                    "key": key,
                    "label": metadata.get("label"),
                    "category": metadata.get("category"),
                    "subcategory": metadata.get("subcategory"),
                    "period": metadata.get("period"),
                    "unit": metadata.get("unit"),
                    "file_indicator": metadata.get("fileIndicator"),
                    "threshold": metadata.get("threshold"),
                    "endpoint_template": (
                        f"{self.api_base_url}/api/v1/metrics/"
                        "{representation}/{scenario}/{scenario_type}/"
                        f"{key}?focusState={{STATE}}&focusDistrict={{DISTRICT}}&focusGrid=&slope=false"
                    ),
                    "candidate_representations": list(CLIMATE_REPRESENTATIONS),
                    "scenario_modes": list(SCENARIO_MODES),
                    "scenario_type_values": list(SCENARIO_TYPE_VALUES),
                    "rcps": list(RCPS),
                    "warming_levels": [],
                    "warming_level_note": (
                        "The current API schema exposes globalWarming as a scenario with scenario_type "
                        "values imd/RCP45/RCP85; it does not expose a warming-level path or query selector."
                    ),
                }
            )

        sectoral_resources = []
        for group, group_items in sorted(sectoral.items()):
            for key, metadata in sorted(group_items.items()):
                sectoral_resources.append(
                    {
                        "key": key,
                        "group": group,
                        "dataset": metadata.get("dataset"),
                        "datatype": metadata.get("datatype"),
                        "category": metadata.get("category"),
                        "source": metadata.get("source"),
                        "metadata": metadata.get("metadata"),
                        "endpoint": f"{self.api_base_url}/api/v1/metrics/sectoral_data/{key}",
                    }
                )

        risk_resources = []
        for key, metadata in sorted(risk.items()):
            risk_resources.append(
                {
                    "key": key,
                    "label": metadata.get("label"),
                    "description": metadata.get("description"),
                    "filename": metadata.get("filename"),
                    "source": metadata.get("source"),
                    "endpoint_template": (
                        f"{self.api_base_url}/api/v1/metrics/risk_index/"
                        f"{key}?focusState={{STATE}}&focusDistrict={{DISTRICT}}"
                    ),
                    "legend": metadata.get("legend"),
                }
            )

        static_files = self.discover_app_static_files() if probe_static_files else []
        accessible_static_files = [item for item in static_files if item.get("status") == 200]

        return {
            "generated_at_utc": _utc_now(),
            "app_base_url": self.app_base_url,
            "api_base_url": self.api_base_url,
            "summary": {
                "climate_metric_count": len(climate_resources),
                "sectoral_resource_count": len(sectoral_resources),
                "risk_index_count": len(risk_resources),
                "static_file_reference_count": len(static_files),
                "accessible_static_file_count": len(accessible_static_files),
            },
            "climate_metrics": climate_resources,
            "sectoral_resources": sectoral_resources,
            "risk_indices": risk_resources,
            "static_files_referenced_by_app": static_files,
            "notes": [
                "The visualisation page URL is not the data API.",
                "Climate metric payloads are fetched from /api/v1/metrics/{representation}/{scenarioMode}/{rcp_or_warming}/{metric}.",
                "The climateModel query parameter in the frontend defaults to ceew25k but is not part of the current metric API path.",
                "The active frontend climate controls expose zonalData and gridData; watershed constants exist but should be validated metric-by-metric before storage planning.",
                "Some static /data/*.json paths are referenced in older/lazy frontend chunks but currently return 404; treat the API as canonical.",
            ],
        }

    def discover_app_static_files(self) -> list[dict[str, Any]]:
        html = self._get_text(f"{self.app_base_url}/visualisation-tool")
        js_paths = sorted(set(re.findall(r'(?:src|href)="([^"]+\.js)"', html)))
        data_paths: set[str] = set()

        for js_path in js_paths:
            js_url = urllib.parse.urljoin(self.app_base_url, js_path)
            try:
                script = self._get_text(js_url)
            except RuntimeError:
                continue
            data_paths.update(
                re.findall(
                    r"/data/[A-Za-z0-9_./-]+\.(?:json|geojson|csv|tif|tiff|parquet|xlsx|nc)",
                    script,
                )
            )

        probes = []
        for path in sorted(data_paths):
            probe = self._probe_url(urllib.parse.urljoin(self.app_base_url, path))
            item = asdict(probe)
            item["path"] = path
            probes.append(item)
        return probes

    def analyse_samples(
        self,
        metric: str = DEFAULT_METRIC,
        risk_index: str = DEFAULT_RISK_INDEX,
        focus_state: str = DEFAULT_STATE,
        focus_district: str = DEFAULT_DISTRICT,
    ) -> list[dict[str, Any]]:
        sample_requests = [
            SampleRequest(
                name=f"climate_metric_zonal_{metric}",
                kind="climate_metric",
                path=f"/api/v1/metrics/zonalData/rcpssp/RCP85/{metric}",
                params={
                    "focusState": focus_state.strip().upper(),
                    "focusDistrict": focus_district.strip().upper(),
                    "focusGrid": "",
                    "slope": "false",
                },
            ),
            SampleRequest(
                name=f"risk_index_{risk_index}",
                kind="risk_index",
                path=f"/api/v1/metrics/risk_index/{risk_index}",
                params={
                    "focusState": focus_state.strip().upper(),
                    "focusDistrict": focus_district.strip().upper(),
                },
            ),
        ]

        analyses = []
        for request in sample_requests:
            url = self._api_url(request.path, request.params)
            payload = self._get_json(url)
            analyses.append(self._profile_payload(request, url, payload))
        return analyses

    def build_download_tasks(
        self,
        metrics: list[str],
        all_metrics: bool,
        representations: list[str],
        scenario_modes: list[str],
        rcps: list[str],
        warming_levels: list[str],
        focus_state: str,
        focus_district: str,
        focus_grid: str,
        slope: bool,
        include_metadata: bool,
        include_climate: bool,
        include_risk_indices: bool,
        risk_indices: list[str],
        include_sectoral: bool,
        sectoral_keys: list[str],
        allow_unscoped_grid: bool,
    ) -> list[DownloadTask]:
        """Resolve CLI options into concrete API download tasks."""

        focus_state = focus_state.strip().upper()
        focus_district = focus_district.strip().upper()
        focus_grid = focus_grid.strip()
        scenario_type_values = _expand_scenario_type_values(rcps)
        metric_metadata: dict[str, Any] | None = None
        risk_metadata: dict[str, Any] | None = None
        sectoral_metadata: dict[str, Any] | None = None

        tasks: list[DownloadTask] = []
        if include_metadata:
            tasks.extend(
                [
                    DownloadTask(
                        name="metrics_metadata",
                        kind="metadata",
                        url=self._api_url("/api/v1/metrics/metadata"),
                        relative_path=Path("metadata") / "metrics_metadata.json",
                    ),
                    DownloadTask(
                        name="sectoral_metadata",
                        kind="metadata",
                        url=self._api_url("/api/v1/metrics/sectoral_data/metadata"),
                        relative_path=Path("metadata") / "sectoral_metadata.json",
                    ),
                    DownloadTask(
                        name="risk_index_metadata",
                        kind="metadata",
                        url=self._api_url("/api/v1/metrics/risk_index/metadata"),
                        relative_path=Path("metadata") / "risk_index_metadata.json",
                    ),
                ]
            )

        if include_climate:
            if all_metrics or any(metric.lower() == "all" for metric in metrics):
                metric_metadata = self.fetch_metric_metadata()
                selected_metrics = sorted(metric_metadata)
            else:
                selected_metrics = metrics or [DEFAULT_METRIC]

            for representation in representations:
                if representation == "gridData" and not allow_unscoped_grid and not (focus_state or focus_district or focus_grid):
                    raise ValueError(
                        "Refusing unscoped gridData download. Provide --state/--district/--focus-grid "
                        "or pass --allow-unscoped-grid explicitly."
                    )
                for scenario_mode in scenario_modes:
                    if scenario_mode not in SCENARIO_MODES:
                        raise ValueError(f"Unknown scenario mode {scenario_mode!r}; expected one of {SCENARIO_MODES}")
                    for scenario_type in scenario_type_values:
                        for metric in selected_metrics:
                            params = {
                                "focusState": focus_state,
                                "focusDistrict": focus_district,
                                "focusGrid": focus_grid,
                                "slope": str(slope).lower(),
                            }
                            path = f"/api/v1/metrics/{representation}/{scenario_mode}/{scenario_type}/{metric}"
                            scope = _scope_path(focus_state, focus_district, focus_grid)
                            tasks.append(
                                DownloadTask(
                                    name=(
                                        f"climate_{representation}_{scenario_mode}_{scenario_type}_"
                                        f"{_safe_filename(metric)}"
                                    ),
                                    kind="climate_metric",
                                    url=self._api_url(path, params),
                                    relative_path=(
                                        Path("climate")
                                        / scenario_mode
                                        / scenario_type
                                        / representation
                                        / scope
                                        / f"{_safe_filename(metric)}.json"
                                    ),
                                )
                            )

        if include_risk_indices:
            if not risk_indices or any(risk_index.lower() == "all" for risk_index in risk_indices):
                risk_metadata = self.fetch_risk_index_metadata()
                selected_risk_indices = sorted(risk_metadata)
            else:
                selected_risk_indices = risk_indices

            for risk_index in selected_risk_indices:
                params = {"focusState": focus_state, "focusDistrict": focus_district}
                tasks.append(
                    DownloadTask(
                        name=f"risk_index_{risk_index}",
                        kind="risk_index",
                        url=self._api_url(f"/api/v1/metrics/risk_index/{risk_index}", params),
                        relative_path=(
                            Path("risk_index")
                            / _scope_path(focus_state, focus_district, "")
                            / f"{_safe_filename(risk_index)}.json"
                        ),
                    )
                )

        if include_sectoral:
            if not sectoral_keys or any(sectoral_key.lower() == "all" for sectoral_key in sectoral_keys):
                sectoral_metadata = self.fetch_sectoral_metadata()
                selected_sectoral = [
                    (group, key)
                    for group, group_items in sorted(sectoral_metadata.items())
                    for key in sorted(group_items)
                ]
            else:
                selected_sectoral = [("selected", key) for key in sectoral_keys]

            for group, key in selected_sectoral:
                tasks.append(
                    DownloadTask(
                        name=f"sectoral_{key}",
                        kind="sectoral",
                        url=self._api_url(f"/api/v1/metrics/sectoral_data/{key}"),
                        relative_path=Path("sectoral") / _safe_filename(group) / f"{_safe_filename(key)}.json",
                    )
                )

        return tasks

    def download_tasks(
        self,
        tasks: list[DownloadTask],
        output_dir: Path,
        manifest_path: Path | None = None,
        overwrite: bool = False,
        retries: int = DEFAULT_RETRIES,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        validate_json: bool = True,
        pretty_json: bool = False,
        progress_every: int = 10,
        retry_unavailable: bool = False,
    ) -> dict[str, Any]:
        """Download tasks with retries, atomic writes, and a manifest checkpoint."""

        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_path or output_dir / "ceew_download_manifest.json"
        manifest = _load_manifest(manifest_path)
        manifest.setdefault("created_at_utc", _utc_now())
        manifest["updated_at_utc"] = _utc_now()
        manifest["output_dir"] = str(output_dir)
        manifest["task_count"] = len(tasks)
        manifest.setdefault("results", {})

        results: list[DownloadResult] = []
        for index, task in enumerate(tasks, start=1):
            destination = output_dir / task.relative_path
            existing = manifest["results"].get(task.name)
            if not overwrite and destination.exists() and _existing_download_is_reusable(destination, validate_json):
                result = DownloadResult(
                    name=task.name,
                    kind=task.kind,
                    url=task.url,
                    path=str(destination),
                    status="skipped",
                    attempts=0,
                    bytes_written=destination.stat().st_size,
                    sha256=(existing or {}).get("sha256") or _sha256_file(destination),
                )
            elif (
                not overwrite
                and not retry_unavailable
                and existing
                and existing.get("status") == "unavailable"
                and existing.get("url") == task.url
            ):
                result = DownloadResult(
                    name=task.name,
                    kind=task.kind,
                    url=task.url,
                    path=str(destination),
                    status="unavailable",
                    attempts=0,
                    error=existing.get("error"),
                )
            else:
                print(f"[{index}/{len(tasks)}] Downloading {task.name}")
                raw_result = self._download_url_to_json_file(
                    task.url,
                    destination,
                    retries=retries,
                    backoff_seconds=backoff_seconds,
                    chunk_size=chunk_size,
                    validate_json=validate_json,
                    pretty_json=pretty_json,
                )
                result = DownloadResult(
                    name=task.name,
                    kind=task.kind,
                    url=task.url,
                    path=raw_result.path,
                    status=raw_result.status,
                    attempts=raw_result.attempts,
                    bytes_written=raw_result.bytes_written,
                    sha256=raw_result.sha256,
                    duration_seconds=raw_result.duration_seconds,
                    error=raw_result.error,
                )

            results.append(result)
            manifest["results"][task.name] = asdict(result)
            manifest["updated_at_utc"] = _utc_now()
            _write_manifest(manifest, manifest_path)

            if progress_every and (index % progress_every == 0 or index == len(tasks)):
                ok_count = sum(1 for item in results if item.status in {"ok", "skipped"})
                unavailable_count = sum(1 for item in results if item.status == "unavailable")
                failed_count = sum(1 for item in results if item.status == "failed")
                print(
                    f"Progress: {index}/{len(tasks)} tasks, {ok_count} ok/skipped, "
                    f"{unavailable_count} unavailable, {failed_count} failed"
                )

        ok_count = sum(1 for item in results if item.status in {"ok", "skipped"})
        downloaded_count = sum(1 for item in results if item.status == "ok")
        skipped_count = sum(1 for item in results if item.status == "skipped")
        unavailable_count = sum(1 for item in results if item.status == "unavailable")
        failed_count = sum(1 for item in results if item.status == "failed")
        active_task_names = {task.name for task in tasks}
        stale_result_count = sum(1 for name in manifest["results"] if name not in active_task_names)
        manifest["summary"] = {
            "downloaded": downloaded_count,
            "skipped": skipped_count,
            "ok_or_skipped": ok_count,
            "unavailable": unavailable_count,
            "failed": failed_count,
            "total": len(results),
            "stale_manifest_results": stale_result_count,
        }
        manifest["updated_at_utc"] = _utc_now()
        _write_manifest(manifest, manifest_path)
        return manifest

    def estimate_task_sizes(
        self,
        tasks: list[DownloadTask],
        retries: int = DEFAULT_RETRIES,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        progress_every: int = 25,
    ) -> dict[str, Any]:
        probes: list[SizeProbe] = []
        for index, task in enumerate(tasks, start=1):
            probe = self._probe_download_size(task, retries=retries, backoff_seconds=backoff_seconds)
            probes.append(probe)
            if progress_every and (index % progress_every == 0 or index == len(tasks)):
                known = sum(item.content_length or 0 for item in probes)
                print(
                    f"Estimated {index}/{len(tasks)} tasks; "
                    f"known total {_format_bytes(known)}; "
                    f"unknown {sum(1 for item in probes if item.content_length is None)}"
                )

        by_kind: dict[str, dict[str, Any]] = {}
        for probe in probes:
            item = by_kind.setdefault(
                probe.kind,
                {"task_count": 0, "known_bytes": 0, "unknown_size_count": 0, "error_count": 0},
            )
            item["task_count"] += 1
            if probe.content_length is None:
                item["unknown_size_count"] += 1
            else:
                item["known_bytes"] += probe.content_length
            if probe.error or (probe.status and probe.status >= 400):
                item["error_count"] += 1

        total_known = sum(item.content_length or 0 for item in probes)
        summary = {
            "task_count": len(tasks),
            "known_bytes": total_known,
            "known_human": _format_bytes(total_known),
            "unknown_size_count": sum(1 for item in probes if item.content_length is None),
            "error_count": sum(1 for item in probes if item.error or (item.status and item.status >= 400)),
            "by_kind": {
                key: {**value, "known_human": _format_bytes(value["known_bytes"])}
                for key, value in sorted(by_kind.items())
            },
            "note": (
                "Content-Length is reported by GET response headers. "
                "For gzip-encoded responses this is compressed transfer size; decoded storage can be larger."
            ),
        }
        return {
            "generated_at_utc": _utc_now(),
            "summary": summary,
            "files": [asdict(probe) | {"size_human": _format_bytes(probe.content_length)} for probe in probes],
        }

    def _probe_download_size(
        self,
        task: DownloadTask,
        retries: int,
        backoff_seconds: float,
    ) -> SizeProbe:
        last_error: str | None = None
        for attempt in range(1, retries + 1):
            probe = _probe_download_size_with_hard_timeout(task, timeout_seconds=self.timeout_seconds)
            if not probe.error or (probe.status and probe.status >= 400):
                return probe
            last_error = probe.error
            if attempt < retries:
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))

        return SizeProbe(
            name=task.name,
            kind=task.kind,
            url=task.url,
            relative_path=str(task.relative_path),
            status=None,
            error=last_error,
        )

    def _download_url_to_json_file(
        self,
        url: str,
        destination: Path,
        retries: int,
        backoff_seconds: float,
        chunk_size: int,
        validate_json: bool,
        pretty_json: bool,
    ) -> DownloadResult:
        started = time.monotonic()
        destination.parent.mkdir(parents=True, exist_ok=True)
        raw_part = destination.with_name(f"{destination.name}.raw.part")
        decoded_part = destination.with_name(f"{destination.name}.part")
        last_error: str | None = None

        for attempt in range(1, retries + 1):
            try:
                for path in (raw_part, decoded_part):
                    if path.exists():
                        path.unlink()

                request = urllib.request.Request(
                    url,
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                        "User-Agent": "core-stack-ceew-data-utils/1.0",
                    },
                )
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    with raw_part.open("wb") as file_obj:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            file_obj.write(chunk)

                _decode_file_if_needed(raw_part, decoded_part)
                if validate_json:
                    _validate_json_file(decoded_part, url)
                if pretty_json:
                    _pretty_rewrite_json_file(decoded_part)

                decoded_part.replace(destination)
                if raw_part.exists():
                    raw_part.unlink()
                elapsed = time.monotonic() - started
                return DownloadResult(
                    name=destination.stem,
                    kind="download",
                    url=url,
                    path=str(destination),
                    status="ok",
                    attempts=attempt,
                    bytes_written=destination.stat().st_size,
                    sha256=_sha256_file(destination),
                    duration_seconds=round(elapsed, 3),
                )
            except urllib.error.HTTPError as exc:
                last_error = _http_error_message(exc)
                for path in (raw_part, decoded_part):
                    if path.exists():
                        path.unlink()
                if exc.code in NON_RETRYABLE_HTTP_STATUS_CODES:
                    elapsed = time.monotonic() - started
                    return DownloadResult(
                        name=destination.stem,
                        kind="download",
                        url=url,
                        path=str(destination),
                        status="unavailable",
                        attempts=attempt,
                        duration_seconds=round(elapsed, 3),
                        error=last_error,
                    )
                if attempt < retries:
                    sleep_for = backoff_seconds * (2 ** (attempt - 1))
                    print(f"  attempt {attempt} failed: {last_error}; retrying in {sleep_for:.1f}s")
                    time.sleep(sleep_for)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                for path in (raw_part, decoded_part):
                    if path.exists():
                        path.unlink()
                if attempt < retries:
                    sleep_for = backoff_seconds * (2 ** (attempt - 1))
                    print(f"  attempt {attempt} failed: {last_error}; retrying in {sleep_for:.1f}s")
                    time.sleep(sleep_for)

        elapsed = time.monotonic() - started
        return DownloadResult(
            name=destination.stem,
            kind="download",
            url=url,
            path=str(destination),
            status="failed",
            attempts=retries,
            duration_seconds=round(elapsed, 3),
            error=last_error,
        )

    def _profile_payload(self, request: SampleRequest, url: str, payload: Any) -> dict[str, Any]:
        data = payload.get("data") if isinstance(payload, dict) else payload
        metadata = payload.get("metadata") if isinstance(payload, dict) else None

        records = data if isinstance(data, list) else []
        first_record = records[0] if records and isinstance(records[0], dict) else {}
        category_keys = []
        category_value_keys = []
        if isinstance(first_record.get("categories"), dict):
            category_keys = list(first_record["categories"].keys())
            first_category = first_record["categories"].get(category_keys[0]) if category_keys else None
            if isinstance(first_category, dict):
                category_value_keys = list(first_category.keys())

        yearly_value_groups = []
        if isinstance(first_record.get("yearly_values"), dict):
            yearly_value_groups = list(first_record["yearly_values"].keys())

        usage_notes = self._usage_notes_for_sample(request.kind, first_record)

        return {
            "name": request.name,
            "kind": request.kind,
            "url": url,
            "record_count": len(records) if isinstance(data, list) else None,
            "top_level_keys": list(payload.keys()) if isinstance(payload, dict) else [],
            "metadata_keys": list(metadata.keys()) if isinstance(metadata, dict) else [],
            "record_keys": list(first_record.keys()),
            "category_keys": category_keys,
            "category_value_keys": category_value_keys,
            "yearly_value_groups": yearly_value_groups,
            "first_record_preview": _truncate_json(first_record, max_chars=2200),
            "possible_core_stack_usage": usage_notes,
        }

    def _usage_notes_for_sample(self, kind: str, first_record: dict[str, Any]) -> list[str]:
        if kind == "climate_metric":
            return [
                "Join zonal records to CoRE Stack district/admin IDs using normalized state and district names from zonal_id.",
                "Use category timelines as district-level climate indicators in KYL, planning dashboards, and vulnerability overlays.",
                "Use yearly_values for trend charts or slope-derived alerts once storage policy is decided.",
                "For micro-planning, rerun the same metric as gridData with a focusState/focusDistrict filter and spatially relate grid_id to villages/MWS polygons.",
            ]
        if kind == "risk_index":
            return [
                "Use risk_score and hazard-specific fields as district hazard context in KYL and DPR prioritisation.",
                "Join by zonal_id/state_name/district_name, then map to LGD-backed hierarchy resolution before persisting.",
                "Risk payloads already include population and multiple hazard summaries, useful as compact planning features.",
            ]
        if "coordinates" in first_record:
            return [
                "Grid records can be spatially indexed by lat/long and intersected with village, tehsil, or watershed boundaries.",
            ]
        return ["Inspect schema and join keys before deciding permanent storage."]

    def _api_url(self, path: str, params: dict[str, str] | None = None) -> str:
        quoted_path = urllib.parse.quote(path, safe="/._-")
        url = urllib.parse.urljoin(self.api_base_url, quoted_path)
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        return url

    def _get_json(self, url: str) -> Any:
        text = self._get_text(url, headers={"Accept": "application/json"})
        return json.loads(text)

    def _get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        request = urllib.request.Request(url, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
                raw = _decode_response_body(raw, response.headers.get("content-encoding"))
                charset = response.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Could not fetch {url}: {exc}") from exc

    def _probe_url(self, url: str) -> HTTPProbe:
        request = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return HTTPProbe(
                    url=url,
                    status=response.status,
                    content_type=response.headers.get("content-type"),
                    content_length=_int_or_none(response.headers.get("content-length")),
                    content_encoding=response.headers.get("content-encoding"),
                    last_modified=response.headers.get("last-modified"),
                    etag=response.headers.get("etag"),
                )
        except urllib.error.HTTPError as exc:
            return HTTPProbe(url=url, status=exc.code, error=str(exc))
        except urllib.error.URLError as exc:
            return HTTPProbe(url=url, status=None, error=str(exc))


def _decode_response_body(raw: bytes, content_encoding: str | None) -> bytes:
    if content_encoding and "gzip" in content_encoding.lower():
        return gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
    if raw.startswith(b"\x1f\x8b"):
        return gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
    return raw


def _probe_download_size_once(task: DownloadTask, timeout_seconds: int) -> SizeProbe:
    try:
        request = urllib.request.Request(
            task.url,
            method="GET",
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": "core-stack-ceew-data-utils/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return SizeProbe(
                name=task.name,
                kind=task.kind,
                url=task.url,
                relative_path=str(task.relative_path),
                status=response.status,
                content_length=_int_or_none(response.headers.get("content-length")),
                content_type=response.headers.get("content-type"),
                content_encoding=response.headers.get("content-encoding"),
            )
    except urllib.error.HTTPError as exc:
        return SizeProbe(
            name=task.name,
            kind=task.kind,
            url=task.url,
            relative_path=str(task.relative_path),
            status=exc.code,
            content_length=_int_or_none(exc.headers.get("content-length")) if exc.headers else None,
            content_type=exc.headers.get("content-type") if exc.headers else None,
            content_encoding=exc.headers.get("content-encoding") if exc.headers else None,
            error=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        return SizeProbe(
            name=task.name,
            kind=task.kind,
            url=task.url,
            relative_path=str(task.relative_path),
            status=None,
            error=str(exc),
        )


def _probe_download_size_worker(task: DownloadTask, timeout_seconds: int, queue: multiprocessing.Queue) -> None:
    queue.put(asdict(_probe_download_size_once(task, timeout_seconds)))


def _probe_download_size_with_hard_timeout(task: DownloadTask, timeout_seconds: int) -> SizeProbe:
    queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=1)
    process = multiprocessing.Process(target=_probe_download_size_worker, args=(task, timeout_seconds, queue))
    process.start()
    hard_timeout = max(timeout_seconds + 3, 5)
    process.join(hard_timeout)
    if process.is_alive():
        process.terminate()
        process.join(2)
        return SizeProbe(
            name=task.name,
            kind=task.kind,
            url=task.url,
            relative_path=str(task.relative_path),
            status=None,
            error=f"probe timed out after {hard_timeout}s",
        )
    if queue.empty():
        return SizeProbe(
            name=task.name,
            kind=task.kind,
            url=task.url,
            relative_path=str(task.relative_path),
            status=None,
            error=f"probe process exited with code {process.exitcode}",
        )
    return SizeProbe(**queue.get())


def _decode_file_if_needed(source: Path, destination: Path) -> None:
    with source.open("rb") as file_obj:
        magic = file_obj.read(2)

    if magic == b"\x1f\x8b":
        with gzip.open(source, "rb") as compressed, destination.open("wb") as decoded:
            while True:
                chunk = compressed.read(DEFAULT_CHUNK_SIZE)
                if not chunk:
                    break
                decoded.write(chunk)
        source.unlink()
    else:
        source.replace(destination)


def _looks_like_json_file(path: Path) -> bool:
    first = _first_non_space_byte(path)
    last = _last_non_space_byte(path)
    return (first, last) in {(b"{", b"}"), (b"[", b"]")}


def _validate_json_file(path: Path, url: str) -> None:
    if not _looks_like_json_file(path):
        raise RuntimeError(f"Downloaded payload does not look like JSON: {url}")
    try:
        with path.open("r", encoding="utf-8") as file_obj:
            json.load(file_obj)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Downloaded payload is not parseable JSON: {url}: {exc}") from exc


def _existing_download_is_reusable(path: Path, validate_json: bool) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    if not validate_json:
        return True
    try:
        _validate_json_file(path, str(path))
    except RuntimeError:
        return False
    return True


def _http_error_message(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read(4096).decode("utf-8", "replace").strip()
    except Exception:  # noqa: BLE001
        body = ""
    if body:
        return f"HTTP {exc.code}: {body}"
    return f"HTTP {exc.code}: {exc.reason}"


def _pretty_rewrite_json_file(path: Path) -> None:
    pretty_path = path.with_name(f"{path.name}.pretty.part")
    with path.open("r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)
    with pretty_path.open("w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, indent=2, ensure_ascii=True)
        file_obj.write("\n")
    pretty_path.replace(path)


def _first_non_space_byte(path: Path) -> bytes:
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(8192)
            if not chunk:
                return b""
            stripped = chunk.lstrip()
            if stripped:
                return stripped[:1]


def _last_non_space_byte(path: Path) -> bytes:
    size = path.stat().st_size
    if size == 0:
        return b""
    with path.open("rb") as file_obj:
        position = size
        while position > 0:
            read_size = min(8192, position)
            position -= read_size
            file_obj.seek(position)
            chunk = file_obj.read(read_size).rstrip()
            if chunk:
                return chunk[-1:]
    return b""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(DEFAULT_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        backup_path = path.with_suffix(f"{path.suffix}.corrupt.{int(time.time())}")
        path.replace(backup_path)
        return {"warnings": [f"Existing manifest was corrupt and moved to {backup_path}"]}


def _write_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part_path = path.with_name(f"{path.name}.part")
    part_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    part_path.replace(path)


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned or "unnamed"


def _scope_path(focus_state: str, focus_district: str, focus_grid: str) -> Path:
    parts = []
    parts.append(_safe_filename(focus_state.lower()) if focus_state else "all_india")
    if focus_district:
        parts.append(_safe_filename(focus_district.lower()))
    if focus_grid:
        parts.append(f"grid_{_safe_filename(focus_grid.lower())}")
    return Path(*parts)


def _csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _expand_scenario_type_values(values: list[str]) -> list[str]:
    if not values or any(value.lower() == "all" for value in values):
        return list(SCENARIO_TYPE_VALUES)
    return values


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _truncate_json(value: Any, max_chars: int) -> str:
    rendered = json.dumps(value, ensure_ascii=True, sort_keys=True)
    if len(rendered) <= max_chars:
        return rendered
    return f"{rendered[:max_chars]}..."


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _format_bytes(value: int | None) -> str:
    if value is None:
        return ""
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")


def write_csv_rows(rows: Iterable[dict[str, Any]], output_path: Path, fieldnames: list[str] | None = None) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    materialized_rows = list(rows)
    if fieldnames is None:
        fieldnames = []
        for row in materialized_rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized_rows)


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown_report(report), encoding="utf-8")


def render_markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# CEEW CRAVIS Discovery Report",
        "",
        f"Generated at: `{report['generated_at_utc']}`",
        "",
        "## Inventory Summary",
        "",
        f"- Climate metrics listed by metadata API: {summary['climate_metric_count']}",
        f"- Sectoral resources listed by metadata API: {summary['sectoral_resource_count']}",
        f"- Risk indices listed by metadata API: {summary['risk_index_count']}",
        f"- App static file references probed: {summary['static_file_reference_count']}",
        f"- App static file references accessible: {summary['accessible_static_file_count']}",
        "",
        "## Sample Analysis",
        "",
    ]

    for sample in report.get("sample_analysis", []):
        lines.extend(
            [
                f"### {sample['name']}",
                "",
                f"- Kind: {sample['kind']}",
                f"- URL: `{sample['url']}`",
                f"- Record count: {sample['record_count']}",
                f"- Record keys: `{', '.join(sample['record_keys'])}`",
                f"- Category keys: `{', '.join(sample['category_keys'])}`",
                f"- Category value keys: `{', '.join(sample['category_value_keys'])}`",
                f"- Yearly value groups: `{', '.join(sample['yearly_value_groups'])}`",
                "- Possible CoRE Stack usage:",
            ]
        )
        lines.extend(f"  - {note}" for note in sample["possible_core_stack_usage"])
        lines.extend(["", "- First record preview:", "", "```json", sample["first_record_preview"], "```", ""])

    lines.extend(
        [
            "## Notes",
            "",
            *[f"- {note}" for note in report.get("notes", [])],
            "",
        ]
    )
    return "\n".join(lines)


def build_catalog_rows(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in inventory.get("climate_metrics", []):
        rows.append(
            {
                "resource_type": "climate_metric",
                "key": item.get("key"),
                "label": item.get("label"),
                "category": item.get("category"),
                "subcategory": item.get("subcategory"),
                "period": item.get("period"),
                "unit": item.get("unit"),
                "source_or_dataset": "CEEW CRAVIS climate metrics API",
                "spatial_unit": "district/zonal or 25km grid depending on representation",
                "representations": ",".join(item.get("candidate_representations", [])),
                "scenario_modes": ",".join(item.get("scenario_modes", [])),
                "rcps": ",".join(item.get("rcps", [])),
                "warming_levels": ",".join(item.get("warming_levels", [])),
                "file_indicator": item.get("file_indicator"),
                "threshold": item.get("threshold"),
                "endpoint_or_template": item.get("endpoint_template"),
                "core_stack_mapping": (
                    "Join zonal records through normalized state/district and LGD hierarchy; "
                    "intersect gridData coordinates with village/tehsil/MWS geometries."
                ),
            }
        )
    for item in inventory.get("risk_indices", []):
        rows.append(
            {
                "resource_type": "risk_index",
                "key": item.get("key"),
                "label": item.get("label"),
                "category": "Risk Index",
                "subcategory": item.get("source"),
                "period": "",
                "unit": (item.get("legend") or {}).get("unit") if isinstance(item.get("legend"), dict) else "",
                "source_or_dataset": item.get("source"),
                "spatial_unit": "district/zonal",
                "representations": "zonalData",
                "scenario_modes": "",
                "rcps": "",
                "warming_levels": "",
                "file_indicator": item.get("filename"),
                "threshold": "",
                "endpoint_or_template": item.get("endpoint_template"),
                "core_stack_mapping": "Join by zonal_id/state_name/district_name to KYL/DPR hazard context.",
            }
        )
    for item in inventory.get("sectoral_resources", []):
        rows.append(
            {
                "resource_type": "sectoral",
                "key": item.get("key"),
                "label": item.get("dataset"),
                "category": item.get("group"),
                "subcategory": item.get("category"),
                "period": "",
                "unit": "",
                "source_or_dataset": item.get("source") or item.get("dataset"),
                "spatial_unit": item.get("datatype"),
                "representations": item.get("datatype"),
                "scenario_modes": "",
                "rcps": "",
                "warming_levels": "",
                "file_indicator": "",
                "threshold": "",
                "endpoint_or_template": item.get("endpoint"),
                "core_stack_mapping": (
                    "Point/area data can be spatially joined; zonal data can join by state/district/zonal_id."
                ),
            }
        )
    return rows


def render_estimate_markdown(estimate: dict[str, Any], threshold_bytes: int) -> str:
    summary = estimate["summary"]
    under_threshold = summary["unknown_size_count"] == 0 and summary["known_bytes"] < threshold_bytes
    lines = [
        "# CEEW Download Size Estimate",
        "",
        f"Generated at: `{estimate['generated_at_utc']}`",
        "",
        f"- Task count: {summary['task_count']}",
        f"- Known transfer size: {summary['known_human']}",
        f"- Unknown-size files: {summary['unknown_size_count']}",
        f"- Probe errors: {summary['error_count']}",
        f"- Threshold: {_format_bytes(threshold_bytes)}",
        f"- Download all now: {'yes' if under_threshold else 'no'}",
        "",
        "## By Type",
        "",
        "| Type | Files | Known Size | Unknown | Errors |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for kind, item in summary["by_kind"].items():
        lines.append(
            f"| {kind} | {item['task_count']} | {item['known_human']} | "
            f"{item['unknown_size_count']} | {item['error_count']} |"
        )

    largest = sorted(
        estimate["files"],
        key=lambda row: row.get("content_length") or 0,
        reverse=True,
    )[:20]
    lines.extend(["", "## Largest Probed Files", "", "| Size | Type | Name |", "| ---: | --- | --- |"])
    for row in largest:
        lines.append(f"| {row['size_human']} | {row['kind']} | `{row['name']}` |")
    lines.extend(["", f"Note: {summary['note']}", ""])
    return "\n".join(lines)


def render_catalog_markdown(inventory: dict[str, Any], catalog_csv: Path) -> str:
    summary = inventory["summary"]
    categories: dict[str, int] = {}
    units: dict[str, int] = {}
    for metric in inventory.get("climate_metrics", []):
        categories[str(metric.get("category") or "Uncategorised")] = categories.get(str(metric.get("category") or "Uncategorised"), 0) + 1
        units[str(metric.get("unit") or "unknown")] = units.get(str(metric.get("unit") or "unknown"), 0) + 1

    lines = [
        "# CEEW CRAVIS Available Data Catalog",
        "",
        f"Generated at: `{inventory['generated_at_utc']}`",
        f"CSV catalog: `{catalog_csv}`",
        "",
        "## What Is Available",
        "",
        f"- Climate metrics: {summary['climate_metric_count']}",
        f"- Sectoral resources: {summary['sectoral_resource_count']}",
        f"- Risk indices: {summary['risk_index_count']}",
        f"- App static file references accessible: {summary['accessible_static_file_count']} of {summary['static_file_reference_count']}",
        "",
        "## Climate Metric Categories",
        "",
        "| Category | Metric Count |",
        "| --- | ---: |",
    ]
    for category, count in sorted(categories.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {category} | {count} |")

    lines.extend(["", "## Units", "", "| Unit | Metric Count |", "| --- | ---: |"])
    for unit, count in sorted(units.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {unit} | {count} |")

    lines.extend(
        [
            "",
            "## CoRE Stack Fit",
            "",
            "- Climate zonal data can enrich district-level KYL, DPR prioritisation, and climate risk summaries.",
            "- Climate grid data is suitable for spatial overlays with villages, tehsils, MWS polygons, watershed boundaries, and project sites.",
            "- Risk indices provide compact district hazard context for plan scoring and vulnerability narratives.",
            "- Sectoral point/area/zonal data can support exposure analysis for power, agriculture, public health, LULC, and extreme-event context.",
            "",
        ]
    )
    return "\n".join(lines)


def profile_downloaded_directory(download_dir: Path, output_dir: Path, max_plot_series: int = 30) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_rows: list[dict[str, Any]] = []
    stats_rows: list[dict[str, Any]] = []

    for path in sorted(download_dir.rglob("*.json")):
        if path.name.endswith(".part") or path.name == "ceew_download_manifest.json":
            continue
        relative = path.relative_to(download_dir)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            file_rows.append(
                {
                    "relative_path": str(relative),
                    "kind": _kind_from_path(relative),
                    "file_size_bytes": path.stat().st_size,
                    "file_size_human": _format_bytes(path.stat().st_size),
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue

        records = _payload_records(payload)
        record_count = len(records)
        first = records[0] if records and isinstance(records[0], dict) else {}
        numeric_series = collect_numeric_series(payload)
        file_rows.append(
            {
                "relative_path": str(relative),
                "kind": _kind_from_path(relative),
                "file_size_bytes": path.stat().st_size,
                "file_size_human": _format_bytes(path.stat().st_size),
                "status": "ok",
                "record_count": record_count,
                "top_level_keys": ",".join(payload.keys()) if isinstance(payload, dict) else "",
                "record_keys": ",".join(first.keys()) if isinstance(first, dict) else "",
                "has_coordinates": any("coordinates" in record for record in records if isinstance(record, dict)),
                "has_zonal_id": any("zonal_id" in record for record in records if isinstance(record, dict)),
                "numeric_series_count": len(numeric_series),
                "core_stack_mapping": _mapping_note_for_path(relative, first),
            }
        )

        for series_name, values in sorted(numeric_series.items()):
            stats = summarize_numbers(values)
            if not stats:
                continue
            stats_rows.append(
                {
                    "relative_path": str(relative),
                    "kind": _kind_from_path(relative),
                    "series": series_name,
                    **stats,
                }
            )

    write_csv_rows(file_rows, output_dir / "downloaded_file_profile.csv")
    write_csv_rows(stats_rows, output_dir / "numeric_series_stats.csv")

    plot_rows = sorted(stats_rows, key=lambda row: (row["kind"], row["relative_path"], row["series"]))[:max_plot_series]
    if plot_rows:
        (output_dir / "plots").mkdir(parents=True, exist_ok=True)
        (output_dir / "plots" / "boxplots.svg").write_text(render_boxplot_svg(plot_rows), encoding="utf-8")
        (output_dir / "plots" / "violin_histograms.svg").write_text(render_violin_histogram_svg(plot_rows), encoding="utf-8")

    report = {
        "generated_at_utc": _utc_now(),
        "download_dir": str(download_dir),
        "summary": {
            "file_count": len(file_rows),
            "profiled_ok": sum(1 for row in file_rows if row.get("status") == "ok"),
            "profile_errors": sum(1 for row in file_rows if row.get("status") == "error"),
            "total_size_bytes": sum(int(row.get("file_size_bytes") or 0) for row in file_rows),
            "total_size_human": _format_bytes(sum(int(row.get("file_size_bytes") or 0) for row in file_rows)),
            "numeric_series_count": len(stats_rows),
        },
        "outputs": {
            "file_profile_csv": str(output_dir / "downloaded_file_profile.csv"),
            "numeric_stats_csv": str(output_dir / "numeric_series_stats.csv"),
            "boxplots_svg": str(output_dir / "plots" / "boxplots.svg") if plot_rows else None,
            "violin_histograms_svg": str(output_dir / "plots" / "violin_histograms.svg") if plot_rows else None,
        },
    }
    write_json_report(report, output_dir / "downloaded_data_profile_summary.json")
    (output_dir / "downloaded_data_profile.md").write_text(render_profile_markdown(report, file_rows, stats_rows), encoding="utf-8")
    return report


def _payload_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [item for item in payload["data"] if isinstance(item, dict)]
    if isinstance(payload, dict) and payload.get("type") == "FeatureCollection" and isinstance(payload.get("features"), list):
        return [item for item in payload["features"] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def collect_numeric_series(payload: Any) -> dict[str, list[float]]:
    records = _payload_records(payload)
    series: dict[str, list[float]] = {}
    for record in records:
        for path, value in _walk_numeric_values(record):
            if path.endswith(".coordinates") or ".geometry.coordinates" in path:
                continue
            series.setdefault(path, []).append(value)
    return {key: values for key, values in series.items() if values}


def _walk_numeric_values(value: Any, prefix: str = "") -> Iterable[tuple[str, float]]:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        yield prefix or "value", float(value)
        return
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            yield from _walk_numeric_values(child, child_prefix)
        return
    if isinstance(value, list):
        if all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value):
            for item in value:
                yield prefix or "values", float(item)
        return


def summarize_numbers(values: list[float]) -> dict[str, Any] | None:
    clean = sorted(value for value in values if value == value)
    if not clean:
        return None
    count = len(clean)
    mean = sum(clean) / count
    variance = sum((value - mean) ** 2 for value in clean) / count
    return {
        "count": count,
        "mean": round(mean, 6),
        "variance": round(variance, 6),
        "stdev": round(variance ** 0.5, 6),
        "min": round(clean[0], 6),
        "q1": round(_percentile(clean, 0.25), 6),
        "median": round(_percentile(clean, 0.5), 6),
        "q3": round(_percentile(clean, 0.75), 6),
        "max": round(clean[-1], 6),
    }


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def render_boxplot_svg(rows: list[dict[str, Any]]) -> str:
    width = 1200
    row_height = 28
    left = 410
    plot_width = 720
    height = 80 + row_height * len(rows)
    values = [float(row["min"]) for row in rows] + [float(row["max"]) for row in rows]
    min_value, max_value = min(values), max(values)
    span = max(max_value - min_value, 1e-9)

    def x(value: Any) -> float:
        return left + ((float(value) - min_value) / span) * plot_width

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="20" y="32" font-family="Arial" font-size="20" font-weight="700">CEEW Numeric Series Box Plots</text>',
        f'<text x="{left}" y="58" font-family="Arial" font-size="12">min {min_value:.2f}</text>',
        f'<text x="{left + plot_width - 90}" y="58" font-family="Arial" font-size="12">max {max_value:.2f}</text>',
    ]
    for index, row in enumerate(rows):
        y = 82 + index * row_height
        label = _svg_escape(f"{Path(row['relative_path']).name}: {row['series']}"[:58])
        svg.append(f'<text x="20" y="{y + 5}" font-family="Arial" font-size="11">{label}</text>')
        svg.append(f'<line x1="{x(row["min"]):.1f}" y1="{y}" x2="{x(row["max"]):.1f}" y2="{y}" stroke="#444"/>')
        svg.append(
            f'<rect x="{x(row["q1"]):.1f}" y="{y - 7}" width="{max(x(row["q3"]) - x(row["q1"]), 1):.1f}" '
            'height="14" fill="#9ecae1" stroke="#225ea8"/>'
        )
        svg.append(f'<line x1="{x(row["median"]):.1f}" y1="{y - 9}" x2="{x(row["median"]):.1f}" y2="{y + 9}" stroke="#08306b" stroke-width="2"/>')
    svg.append("</svg>")
    return "\n".join(svg)


def render_violin_histogram_svg(rows: list[dict[str, Any]]) -> str:
    width = 1200
    row_height = 28
    left = 410
    plot_width = 720
    height = 80 + row_height * len(rows)
    values = [float(row["min"]) for row in rows] + [float(row["max"]) for row in rows]
    min_value, max_value = min(values), max(values)
    span = max(max_value - min_value, 1e-9)

    def x(value: Any) -> float:
        return left + ((float(value) - min_value) / span) * plot_width

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="20" y="32" font-family="Arial" font-size="20" font-weight="700">CEEW Distribution Shape Approximation</text>',
        '<text x="20" y="54" font-family="Arial" font-size="12">Mirrored density uses five-number summaries, not raw histograms.</text>',
    ]
    for index, row in enumerate(rows):
        y = 82 + index * row_height
        label = _svg_escape(f"{Path(row['relative_path']).name}: {row['series']}"[:58])
        points = [
            (x(row["min"]), y),
            (x(row["q1"]), y - 6),
            (x(row["median"]), y - 9),
            (x(row["q3"]), y - 6),
            (x(row["max"]), y),
            (x(row["q3"]), y + 6),
            (x(row["median"]), y + 9),
            (x(row["q1"]), y + 6),
        ]
        svg.append(f'<text x="20" y="{y + 5}" font-family="Arial" font-size="11">{label}</text>')
        svg.append(
            '<polygon points="'
            + " ".join(f"{px:.1f},{py:.1f}" for px, py in points)
            + '" fill="#c7e9c0" stroke="#238b45"/>'
        )
    svg.append("</svg>")
    return "\n".join(svg)


def render_profile_markdown(report: dict[str, Any], file_rows: list[dict[str, Any]], stats_rows: list[dict[str, Any]]) -> str:
    summary = report["summary"]
    by_kind: dict[str, dict[str, Any]] = {}
    for row in file_rows:
        item = by_kind.setdefault(row.get("kind", ""), {"files": 0, "bytes": 0})
        item["files"] += 1
        item["bytes"] += int(row.get("file_size_bytes") or 0)

    lines = [
        "# CEEW Downloaded Data Profile",
        "",
        f"Generated at: `{report['generated_at_utc']}`",
        f"Download directory: `{report['download_dir']}`",
        "",
        f"- Files profiled: {summary['file_count']}",
        f"- Total decoded storage: {summary['total_size_human']}",
        f"- Numeric series profiled: {summary['numeric_series_count']}",
        "",
        "## Files By Type",
        "",
        "| Type | Files | Size |",
        "| --- | ---: | ---: |",
    ]
    for kind, item in sorted(by_kind.items()):
        lines.append(f"| {kind} | {item['files']} | {_format_bytes(item['bytes'])} |")

    lines.extend(
        [
            "",
            "## CoRE Stack Mapping",
            "",
            "- Climate zonal files: join by `zonal_id`, then normalize state/district into LGD-backed hierarchy records.",
            "- Climate grid files: use `coordinates.lat`, `coordinates.long`, and `grid_id`; spatially intersect with village, tehsil, MWS, or watershed geometries.",
            "- Risk-index files: district-level hazard context for KYL, DPR prioritisation, plan scoring, and adaptation recommendations.",
            "- Sectoral point/area files: spatial overlays for exposure analysis; sectoral zonal files join by state/district/zonal identifiers.",
            "",
            "## Outputs",
            "",
            f"- File profile CSV: `{report['outputs']['file_profile_csv']}`",
            f"- Numeric stats CSV: `{report['outputs']['numeric_stats_csv']}`",
        ]
    )
    if report["outputs"].get("boxplots_svg"):
        lines.append(f"- Box plots SVG: `{report['outputs']['boxplots_svg']}`")
    if report["outputs"].get("violin_histograms_svg"):
        lines.append(f"- Violin-style distribution SVG: `{report['outputs']['violin_histograms_svg']}`")

    top_stats = sorted(stats_rows, key=lambda row: abs(float(row.get("variance") or 0)), reverse=True)[:20]
    lines.extend(["", "## Highest-Variance Numeric Series", "", "| Variance | Mean | Count | Series | File |", "| ---: | ---: | ---: | --- | --- |"])
    for row in top_stats:
        lines.append(
            f"| {row['variance']} | {row['mean']} | {row['count']} | `{row['series']}` | `{Path(row['relative_path']).name}` |"
        )
    lines.append("")
    return "\n".join(lines)


def _kind_from_path(relative: Path) -> str:
    return relative.parts[0] if relative.parts else "unknown"


def _mapping_note_for_path(relative: Path, first_record: dict[str, Any]) -> str:
    kind = _kind_from_path(relative)
    if kind == "climate" and "coordinates" in first_record:
        return "Spatial join grid coordinates/grid_id to CoRE Stack admin, village, tehsil, MWS, or watershed geometries."
    if kind == "climate":
        return "Join zonal_id to normalized state/district and LGD hierarchy."
    if kind == "risk_index":
        return "Use district hazard scores as KYL/DPR/plan prioritisation features."
    if kind == "sectoral":
        return "Use geometry or zonal fields for exposure overlays and sectoral context."
    if kind == "metadata":
        return "Use as data dictionary/catalog metadata."
    return "Inspect keys and join fields before production use."


def _svg_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


FULL_DOWNLOAD_HELP = """Traceable all-data download command:

  python3 utilities/ceew_data_utils.py --timeout 120 download \\
      --output-dir data/ceew_downloads_full_traceable \\
      --manifest data/ceew_downloads_full_traceable/ceew_download_manifest.json \\
      --all-metrics \\
      --representations zonalData,gridData \\
      --scenario-modes rcpssp,globalWarming \\
      --rcps all \\
      --warming-levels 1c,1_5c,2c,3c \\
      --state "" --district "" \\
      --include-metadata \\
      --include-risk-indices --risk-indices all \\
      --include-sectoral --sectoral-keys all \\
      --allow-unscoped-grid \\
      --retries 6 --backoff 5 --chunk-size 1048576 \\
      --pretty-json \\
      --progress-every 10 \\
      --tasks-output data/ceew_downloads_full_traceable/resolved_download_tasks.json

Notes:
  - The manifest records every task, final path, status, byte count, SHA-256, attempts, and errors.
  - Existing parseable JSON files are skipped on rerun unless --overwrite is passed.
  - --rcps all expands to imd,RCP45,RCP85, matching the API's scenario_type enum.
  - --warming-levels is accepted for compatibility; the current API schema exposes no warming-level selector.
  - --pretty-json validates and rewrites each decoded payload as readable JSON without dropping fields.
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app-base-url", default=DEFAULT_APP_BASE_URL)
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)

    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="List available resources without sampling data payloads")
    inventory.add_argument("--no-static-probe", action="store_true", help="Skip probing /data files referenced by the app")
    inventory.add_argument("--output", type=Path)

    catalog = subparsers.add_parser("catalog", help="Write human-readable CSV/Markdown catalog of available data")
    catalog.add_argument("--output-dir", type=Path, default=Path("data/ceew_catalog"))
    catalog.add_argument("--no-static-probe", action="store_true")

    sample = subparsers.add_parser("sample", help="Analyse two scoped sample payloads")
    sample.add_argument("--metric", default=DEFAULT_METRIC)
    sample.add_argument("--risk-index", default=DEFAULT_RISK_INDEX)
    sample.add_argument("--state", default=DEFAULT_STATE)
    sample.add_argument("--district", default=DEFAULT_DISTRICT)
    sample.add_argument("--output", type=Path)

    analyse = subparsers.add_parser("analyse", help="Build inventory plus two sample analyses")
    analyse.add_argument("--metric", default=DEFAULT_METRIC)
    analyse.add_argument("--risk-index", default=DEFAULT_RISK_INDEX)
    analyse.add_argument("--state", default=DEFAULT_STATE)
    analyse.add_argument("--district", default=DEFAULT_DISTRICT)
    analyse.add_argument("--no-static-probe", action="store_true")
    analyse.add_argument("--output", type=Path, default=Path("data/ceew_discovery/ceew_analysis_report.json"))
    analyse.add_argument("--markdown-output", type=Path)

    download = subparsers.add_parser(
        "download",
        help="Download selected datasets with retries and a manifest",
        epilog=FULL_DOWNLOAD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    download.add_argument("--output-dir", type=Path, default=Path("data/ceew_downloads"))
    download.add_argument("--manifest", type=Path, help="Manifest path. Defaults to <output-dir>/ceew_download_manifest.json")
    download.add_argument("--metrics", default=DEFAULT_METRIC, help="Comma-separated metric keys, or 'all'")
    download.add_argument("--all-metrics", action="store_true", help="Download all climate metrics from metadata")
    download.add_argument("--skip-climate", action="store_true", help="Do not download climate metric payloads")
    download.add_argument("--representations", default="zonalData", help="Comma-separated: zonalData,gridData")
    download.add_argument("--scenario-modes", default="rcpssp", help="Comma-separated: rcpssp,globalWarming")
    download.add_argument(
        "--rcps",
        default="RCP85",
        help="Comma-separated scenario_type values (imd,RCP45,RCP85), or 'all'",
    )
    download.add_argument(
        "--warming-levels",
        default="1_5c",
        help="Compatibility option; current API schema exposes no warming-level selector",
    )
    download.add_argument("--state", default=DEFAULT_STATE, help="Focus state. Empty string requests all India where supported")
    download.add_argument("--district", default=DEFAULT_DISTRICT, help="Focus district. Empty string requests state/all-India where supported")
    download.add_argument("--focus-grid", default="")
    download.add_argument("--slope", action="store_true")
    download.add_argument(
        "--include-metadata",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include metrics/sectoral/risk metadata files",
    )
    download.add_argument(
        "--include-risk-indices",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include risk-index payloads",
    )
    download.add_argument("--risk-indices", default=DEFAULT_RISK_INDEX, help="Comma-separated risk keys, or 'all'")
    download.add_argument("--include-sectoral", action="store_true", help="Include sectoral payloads")
    download.add_argument("--sectoral-keys", default="", help="Comma-separated sectoral keys, or 'all'")
    download.add_argument("--allow-unscoped-grid", action="store_true")
    download.add_argument("--overwrite", action="store_true")
    download.add_argument(
        "--retry-unavailable",
        action="store_true",
        help="Retry tasks previously recorded as non-retryable HTTP 4xx/unavailable",
    )
    download.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    download.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS)
    download.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    download.add_argument("--no-validate-json", action="store_true")
    download.add_argument(
        "--pretty-json",
        action="store_true",
        help="Parse and rewrite each decoded payload as indented JSON after download validation",
    )
    download.add_argument("--progress-every", type=int, default=5)
    download.add_argument("--dry-run", action="store_true", help="Resolve tasks but do not download")
    download.add_argument("--tasks-output", type=Path, help="Optional JSON file containing resolved tasks")

    estimate = subparsers.add_parser("estimate", help="Estimate file sizes for selected downloads before downloading")
    estimate.add_argument("--output-dir", type=Path, default=Path("data/ceew_estimate"))
    estimate.add_argument("--metrics", default="all", help="Comma-separated metric keys, or 'all'")
    estimate.add_argument("--all-metrics", action="store_true", help="Estimate all climate metrics from metadata")
    estimate.add_argument("--skip-climate", action="store_true")
    estimate.add_argument("--representations", default="zonalData,gridData")
    estimate.add_argument("--scenario-modes", default="rcpssp,globalWarming")
    estimate.add_argument("--rcps", default="all", help="Comma-separated scenario_type values (imd,RCP45,RCP85), or 'all'")
    estimate.add_argument(
        "--warming-levels",
        default="1c,1_5c,2c,3c",
        help="Compatibility option; current API schema exposes no warming-level selector",
    )
    estimate.add_argument("--state", default="", help="Empty default estimates all-India where supported")
    estimate.add_argument("--district", default="")
    estimate.add_argument("--focus-grid", default="")
    estimate.add_argument("--slope", action="store_true")
    estimate.add_argument("--include-metadata", action=argparse.BooleanOptionalAction, default=True)
    estimate.add_argument("--include-risk-indices", action=argparse.BooleanOptionalAction, default=True)
    estimate.add_argument("--risk-indices", default="all")
    estimate.add_argument("--include-sectoral", action=argparse.BooleanOptionalAction, default=True)
    estimate.add_argument("--sectoral-keys", default="all")
    estimate.add_argument("--allow-unscoped-grid", action="store_true", default=True)
    estimate.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    estimate.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS)
    estimate.add_argument("--progress-every", type=int, default=25)
    estimate.add_argument("--threshold-bytes", type=int, default=DEFAULT_DOWNLOAD_LIMIT_BYTES)
    estimate.add_argument("--download-if-under-limit", action="store_true")
    estimate.add_argument("--download-output-dir", type=Path, default=Path("data/ceew_downloads_full"))

    profile = subparsers.add_parser("profile", help="Profile downloaded JSON files and generate CSV stats plus SVG plots")
    profile.add_argument("--download-dir", type=Path, default=Path("data/ceew_downloads_full"))
    profile.add_argument("--output-dir", type=Path, default=Path("data/ceew_profile"))
    profile.add_argument("--max-plot-series", type=int, default=30)

    subparsers.add_parser("self-test", help="Run embedded offline tests for downloader helpers")

    return parser


def run_self_tests() -> int:
    """Embedded offline smoke tests for safe refactoring and easy validation."""

    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    try:
        parsed = build_parser().parse_args(["download", "--metrics", "all", "--dry-run"])
        check(parsed.command == "download", "parser should accept download command")
        check(parsed.metrics == "all", "parser should preserve metrics argument")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"parser test raised {exc}")

    try:
        client = CEEWDataExtractor(api_base_url="https://example.invalid")
        url = client._api_url("/api/v1/metrics/metadata", {"focusState": "BIHAR", "focusDistrict": "JAMUI"})
        check("focusState=BIHAR" in url and "focusDistrict=JAMUI" in url, "api URL should encode params")
        spaced = client._api_url("/api/v1/metrics/zonalData/rcpssp/RCP85/metric with space")
        check("metric%20with%20space" in spaced, "api URL should quote path segments")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"url builder test raised {exc}")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.json.gz"
            payload = {"metadata": {"test": True}, "data": [{"zonal_id": "jamui_bihar"}]}
            with gzip.open(source, "wb") as file_obj:
                file_obj.write(json.dumps(payload).encode("utf-8"))

            task = DownloadTask(
                name="local_gzip_json",
                kind="test",
                url=source.as_uri(),
                relative_path=Path("downloads") / "local_gzip_json.json",
            )
            manifest = client.download_tasks(
                [task],
                temp_path / "out",
                retries=1,
                backoff_seconds=0,
                chunk_size=128,
                validate_json=True,
                pretty_json=True,
                progress_every=1,
            )
            output_file = temp_path / "out" / task.relative_path
            check(output_file.exists(), "downloaded test file should exist")
            check(json.loads(output_file.read_text(encoding="utf-8")) == payload, "gzip JSON should decode")
            check("\n  " in output_file.read_text(encoding="utf-8"), "pretty JSON should be indented")
            check(manifest["summary"]["ok_or_skipped"] == 1, "manifest should record successful task")

            manifest_path = temp_path / "out" / "ceew_download_manifest.json"
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_payload["results"]["local_gzip_json"]["status"] = "failed"
            _write_manifest(manifest_payload, manifest_path)
            second_manifest = client.download_tasks(
                [task],
                temp_path / "out",
                retries=1,
                backoff_seconds=0,
                chunk_size=128,
                validate_json=True,
                pretty_json=True,
                progress_every=0,
            )
            result = second_manifest["results"]["local_gzip_json"]
            check(result["status"] == "skipped", "existing parseable JSON should be skipped despite stale manifest")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"download helper test raised {exc}")

    try:
        check(_safe_filename("Bihar / Jamui") == "Bihar_Jamui", "safe filename should normalize separators")
        check(str(_scope_path("BIHAR", "JAMUI", "")) == "bihar/jamui", "scope path should include state/district")
        check(_csv_list("a,b, c ,,") == ["a", "b", "c"], "csv list parser should trim blanks")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"utility helper test raised {exc}")

    if failures:
        print("Self-test failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Self-test passed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = CEEWDataExtractor(
        app_base_url=args.app_base_url,
        api_base_url=args.api_base_url,
        timeout_seconds=args.timeout,
    )

    if args.command == "inventory":
        report = client.build_inventory(probe_static_files=not args.no_static_probe)
        if args.output:
            write_json_report(report, args.output)
            print(f"Wrote inventory report to {args.output}")
        else:
            print(json.dumps(report["summary"], indent=2))
        return 0

    if args.command == "catalog":
        inventory_report = client.build_inventory(probe_static_files=not args.no_static_probe)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        inventory_path = args.output_dir / "available_data_inventory.json"
        catalog_csv = args.output_dir / "available_data_catalog.csv"
        catalog_md = args.output_dir / "available_data_catalog.md"
        write_json_report(inventory_report, inventory_path)
        write_csv_rows(build_catalog_rows(inventory_report), catalog_csv)
        catalog_md.write_text(render_catalog_markdown(inventory_report, catalog_csv), encoding="utf-8")
        print(f"Wrote catalog CSV to {catalog_csv}")
        print(f"Wrote catalog report to {catalog_md}")
        return 0

    if args.command == "sample":
        samples = client.analyse_samples(args.metric, args.risk_index, args.state, args.district)
        report = {"generated_at_utc": _utc_now(), "sample_analysis": samples}
        if args.output:
            write_json_report(report, args.output)
            print(f"Wrote sample report to {args.output}")
        else:
            print(json.dumps(samples, indent=2))
        return 0

    if args.command == "analyse":
        report = client.build_inventory(probe_static_files=not args.no_static_probe)
        report["sample_analysis"] = client.analyse_samples(
            args.metric,
            args.risk_index,
            args.state,
            args.district,
        )
        write_json_report(report, args.output)
        print(f"Wrote analysis report to {args.output}")
        if args.markdown_output:
            write_markdown_report(report, args.markdown_output)
            print(f"Wrote markdown report to {args.markdown_output}")
        return 0

    if args.command == "download":
        tasks = client.build_download_tasks(
            metrics=_csv_list(args.metrics),
            all_metrics=args.all_metrics,
            representations=_csv_list(args.representations),
            scenario_modes=_csv_list(args.scenario_modes),
            rcps=_csv_list(args.rcps),
            warming_levels=_csv_list(args.warming_levels),
            focus_state=args.state,
            focus_district=args.district,
            focus_grid=args.focus_grid,
            slope=args.slope,
            include_metadata=args.include_metadata,
            include_climate=not args.skip_climate,
            include_risk_indices=args.include_risk_indices,
            risk_indices=_csv_list(args.risk_indices),
            include_sectoral=args.include_sectoral,
            sectoral_keys=_csv_list(args.sectoral_keys),
            allow_unscoped_grid=args.allow_unscoped_grid,
        )
        task_payload = {
            "generated_at_utc": _utc_now(),
            "task_count": len(tasks),
            "tasks": [
                {
                    "name": task.name,
                    "kind": task.kind,
                    "url": task.url,
                    "relative_path": str(task.relative_path),
                }
                for task in tasks
            ],
        }
        if args.tasks_output:
            write_json_report(task_payload, args.tasks_output)
            print(f"Wrote resolved tasks to {args.tasks_output}")
        if args.dry_run:
            print(json.dumps({"task_count": len(tasks), "first_tasks": task_payload["tasks"][:10]}, indent=2))
            return 0

        manifest = client.download_tasks(
            tasks,
            output_dir=args.output_dir,
            manifest_path=args.manifest,
            overwrite=args.overwrite,
            retries=args.retries,
            backoff_seconds=args.backoff,
            chunk_size=args.chunk_size,
            validate_json=not args.no_validate_json,
            pretty_json=args.pretty_json,
            progress_every=args.progress_every,
            retry_unavailable=args.retry_unavailable,
        )
        print(json.dumps(manifest.get("summary", {}), indent=2))
        return 1 if manifest.get("summary", {}).get("failed") else 0

    if args.command == "estimate":
        tasks = client.build_download_tasks(
            metrics=_csv_list(args.metrics),
            all_metrics=args.all_metrics,
            representations=_csv_list(args.representations),
            scenario_modes=_csv_list(args.scenario_modes),
            rcps=_csv_list(args.rcps),
            warming_levels=_csv_list(args.warming_levels),
            focus_state=args.state,
            focus_district=args.district,
            focus_grid=args.focus_grid,
            slope=args.slope,
            include_metadata=args.include_metadata,
            include_climate=not args.skip_climate,
            include_risk_indices=args.include_risk_indices,
            risk_indices=_csv_list(args.risk_indices),
            include_sectoral=args.include_sectoral,
            sectoral_keys=_csv_list(args.sectoral_keys),
            allow_unscoped_grid=args.allow_unscoped_grid,
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        tasks_path = args.output_dir / "resolved_download_tasks.csv"
        write_csv_rows(
            (
                {
                    "name": task.name,
                    "kind": task.kind,
                    "url": task.url,
                    "relative_path": str(task.relative_path),
                }
                for task in tasks
            ),
            tasks_path,
        )
        estimate_report = client.estimate_task_sizes(
            tasks,
            retries=args.retries,
            backoff_seconds=args.backoff,
            progress_every=args.progress_every,
        )
        estimate_json = args.output_dir / "download_size_estimate.json"
        estimate_csv = args.output_dir / "download_size_estimate.csv"
        estimate_md = args.output_dir / "download_size_estimate.md"
        write_json_report(estimate_report, estimate_json)
        write_csv_rows(estimate_report["files"], estimate_csv)
        estimate_md.write_text(render_estimate_markdown(estimate_report, args.threshold_bytes), encoding="utf-8")
        print(f"Wrote task list to {tasks_path}")
        print(f"Wrote size estimate CSV to {estimate_csv}")
        print(f"Wrote size estimate report to {estimate_md}")

        summary = estimate_report["summary"]
        can_download = summary["unknown_size_count"] == 0 and summary["error_count"] == 0 and summary["known_bytes"] < args.threshold_bytes
        print(
            json.dumps(
                {
                    "task_count": summary["task_count"],
                    "known_size": summary["known_human"],
                    "unknown_size_count": summary["unknown_size_count"],
                    "error_count": summary["error_count"],
                    "threshold": _format_bytes(args.threshold_bytes),
                    "download_if_under_limit": args.download_if_under_limit,
                    "can_download": can_download,
                },
                indent=2,
            )
        )
        if args.download_if_under_limit:
            if not can_download:
                print("Not downloading because estimate is incomplete, has errors, or exceeds threshold.")
                return 2
            manifest = client.download_tasks(
                tasks,
                output_dir=args.download_output_dir,
                retries=args.retries,
                backoff_seconds=args.backoff,
                progress_every=args.progress_every,
            )
            print(json.dumps(manifest.get("summary", {}), indent=2))
            return 1 if manifest.get("summary", {}).get("failed") else 0
        return 0

    if args.command == "profile":
        profile_report = profile_downloaded_directory(args.download_dir, args.output_dir, args.max_plot_series)
        print(json.dumps(profile_report["summary"], indent=2))
        return 0

    if args.command == "self-test":
        return run_self_tests()

    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
