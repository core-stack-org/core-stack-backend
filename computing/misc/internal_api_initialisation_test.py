#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import logging
import os
import re
import sys
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from utilities.constants import FIRST_COMPUTING_API_PATH, ADMIN_BOUNDARY_INPUT_DIR

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

for env_name in (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
    "GOTO_NUM_THREADS",
):
    os.environ.setdefault(env_name, "1")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

ADMIN_BOUNDARY_INPUT_DIR = ROOT_DIR / "data" / "admin-boundary" / "input"

SUCCESS_STATUSES = {200, 201, 202, 204, 301, 302, 400, 401, 403, 404, 405}


@dataclass
class CheckResult:
    level: str
    name: str
    detail: str


@dataclass
class RouteResult:
    method: str
    path: str
    status: int
    detail: str


@dataclass
class SampleLocation:
    state: str
    district: str
    block: str
    source_file: Path


def print_result(result: CheckResult) -> None:
    print(f"[{result.level}] {result.name}: {result.detail}")


def format_sample_location(sample: SampleLocation) -> str:
    return f"state={sample.state}, district={sample.district}, " f"block={sample.block}"


def normalize_path(path: str) -> str:
    path = path.replace("\\.", ".").replace("\\", "")
    path = path.replace("^", "").replace("$", "")
    path = re.sub(r"\(\?P<format>[^)]+\)", "json", path)
    path = re.sub(
        r"\(\?P<(?P<name>[^>]+)>[^)]+\)",
        lambda match: sample_value(match.group("name")),
        path,
    )
    path = re.sub(
        r"<(?P<kind>[^:>]+):(?P<name>[^>]+)>",
        lambda match: sample_value(match.group("name"), match.group("kind")),
        path,
    )
    path = re.sub(
        r"<(?P<name>[^:>]+)>", lambda match: sample_value(match.group("name")), path
    )
    path = path.replace("?", "")
    path = re.sub(r"/+", "/", path)
    if not path.startswith("/"):
        path = f"/{path}"
    return path


def sample_value(name: str, kind: str | None = None) -> str:
    lowered = name.lower()
    if kind == "int" or lowered.endswith("_id") or lowered == "pk":
        return "1"
    if "uidb64" in lowered:
        return "sampleuid"
    if "token" in lowered:
        return "sample-token"
    if "uuid" in lowered:
        return "sample-uuid"
    return "sample"


def iter_routes(urlpatterns, prefix: str = ""):
    from django.urls.resolvers import URLPattern, URLResolver

    for entry in urlpatterns:
        fragment = str(entry.pattern)
        full_path = normalize_path(f"{prefix}{fragment}")

        if isinstance(entry, URLResolver):
            yield from iter_routes(entry.url_patterns, prefix=full_path)
            continue

        if isinstance(entry, URLPattern):
            yield full_path, entry.name or entry.lookup_str


def setup_django():
    import django

    django.setup()


def run_configuration_checks(require_gee: bool) -> list[CheckResult]:
    from django.conf import settings
    from django.db import connection
    from gee_computing.models import GEEAccount

    results: list[CheckResult] = []
    gee_missing_level = "FAIL" if require_gee else "WARN"

    try:
        connection.ensure_connection()
        results.append(
            CheckResult("PASS", "database", "Database connection succeeded.")
        )
    except Exception as exc:
        results.append(
            CheckResult("FAIL", "database", f"Database connection failed: {exc}")
        )
        return results

    for env_name in ("GEE_DEFAULT_ACCOUNT_ID", "GEE_HELPER_ACCOUNT_ID"):
        raw_value = getattr(settings, env_name, "")
        value = str(raw_value).strip()

        if not value:
            results.append(
                CheckResult(
                    gee_missing_level,
                    env_name,
                    "Value is blank in .env.",
                )
            )
            continue

        if not value.isdigit():
            results.append(
                CheckResult(
                    "FAIL" if require_gee else "WARN",
                    env_name,
                    f"Expected an integer id but found {raw_value!r}.",
                )
            )
            continue

        exists = GEEAccount.objects.filter(pk=int(value)).exists()
        level = "PASS" if exists else ("FAIL" if require_gee else "WARN")
        detail = (
            f"Configured id={value} exists in gee_computing_geeaccount."
            if exists
            else f"Configured id={value} was not found in gee_computing_geeaccount."
        )
        results.append(CheckResult(level, env_name, detail))

    for env_name in ("TMP_LOCATION", "DEPLOYMENT_DIR", "WHATSAPP_MEDIA_PATH"):
        raw_value = getattr(settings, env_name, "")
        value = str(raw_value).strip()
        if value:
            results.append(CheckResult("PASS", env_name, value))
        else:
            results.append(
                CheckResult(
                    "WARN",
                    env_name,
                    "Value is blank. This may break file-based workflows later.",
                )
            )

    geoserver_url = str(getattr(settings, "GEOSERVER_URL", "")).strip()
    if geoserver_url:
        results.append(CheckResult("PASS", "GEOSERVER_URL", geoserver_url))
    else:
        results.append(
            CheckResult(
                "WARN",
                "GEOSERVER_URL",
                "Value is blank. GeoServer-backed publish and download flows will fail.",
            )
        )

    return results


def run_gee_probe(require_gee: bool) -> CheckResult:
    from django.conf import settings
    from utilities.gee_utils import probe_gee_connection

    account_id = str(getattr(settings, "GEE_DEFAULT_ACCOUNT_ID", "")).strip()
    if not account_id:
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "gee-probe",
            "Skipped because GEE_DEFAULT_ACCOUNT_ID is blank.",
        )

    try:
        if probe_gee_connection(account_id=account_id):
            return CheckResult(
                "PASS",
                "gee-probe",
                f"Authenticated successfully with GEE account id={account_id}.",
            )
    except Exception as exc:
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "gee-probe",
            f"Earth Engine authentication failed: {exc}",
        )

    level = "FAIL" if require_gee else "WARN"
    return CheckResult(
        level,
        "gee-probe",
        "Earth Engine probe did not return a success signal.",
    )


def run_gcs_upload_probe(require_gee: bool) -> CheckResult:
    from django.conf import settings
    from utilities.gee_utils import probe_gcs_upload_access

    account_id = str(getattr(settings, "GEE_DEFAULT_ACCOUNT_ID", "")).strip()
    if not account_id:
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "gcs-upload-probe",
            "Skipped because GEE_DEFAULT_ACCOUNT_ID is blank. "
            "The first computing API uploads shapefile parts to Google Cloud Storage "
            "before importing them into Earth Engine.",
        )

    try:
        probe_result = probe_gcs_upload_access(gee_account_id=account_id)
    except Exception as exc:
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "gcs-upload-probe",
            f"{exc}. Grant Google Cloud Storage write access to the configured service "
            "account for the bucket used by this installation, then rerun the test.",
        )

    return CheckResult(
        "PASS",
        "gcs-upload-probe",
        "Uploaded a temporary object to "
        f"gs://{probe_result['bucket_name']}/{probe_result['blob_name']} using "
        f"{probe_result['service_account_email']}. {probe_result['detail']}",
    )


def run_geoserver_probe() -> CheckResult:
    import requests
    from django.conf import settings

    geoserver_url = str(getattr(settings, "GEOSERVER_URL", "")).strip()
    geoserver_username = str(getattr(settings, "GEOSERVER_USERNAME", "")).strip()
    geoserver_password = str(getattr(settings, "GEOSERVER_PASSWORD", "")).strip()

    if not geoserver_url:
        return CheckResult(
            "WARN",
            "geoserver-probe",
            "Skipped because GEOSERVER_URL is blank. The first computing API publishes "
            "artifacts to GeoServer after local shapefile generation.",
        )

    parsed = urlparse(geoserver_url)
    if not parsed.scheme or not parsed.netloc:
        return CheckResult(
            "FAIL",
            "geoserver-probe",
            f"GEOSERVER_URL is invalid: {geoserver_url!r}. Use a full URL such as "
            "'https://host/geoserver'.",
        )

    if not geoserver_username or not geoserver_password:
        return CheckResult(
            "WARN",
            "geoserver-probe",
            "GEOSERVER_USERNAME or GEOSERVER_PASSWORD is blank. GeoServer publish flows "
            "will fail until REST credentials are configured.",
        )

    probe_url = f"{geoserver_url.rstrip('/')}/rest/about/version.json"
    try:
        response = requests.get(
            probe_url,
            auth=(geoserver_username, geoserver_password),
            timeout=10,
        )
    except Exception as exc:
        return CheckResult(
            "WARN",
            "geoserver-probe",
            f"GeoServer REST probe failed for {probe_url}: {exc}",
        )

    if response.status_code == 200:
        return CheckResult(
            "PASS",
            "geoserver-probe",
            f"GeoServer REST is reachable at {probe_url}.",
        )

    return CheckResult(
        "WARN",
        "geoserver-probe",
        f"GeoServer REST returned HTTP {response.status_code} for {probe_url}.",
    )


def build_auth_headers():
    from django.contrib.auth import get_user_model
    from geoadmin.models import UserAPIKey

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if not user:
        return {}, {}, None

    api_key_obj, generated_key = UserAPIKey.objects.create_key(
        user=user,
        name=f"internal-api-initialisation-{uuid.uuid4().hex[:8]}",
    )
    jwt_headers = {}
    api_key_headers = {"HTTP_X_API_KEY": generated_key}

    try:
        from rest_framework_simplejwt.tokens import RefreshToken

        refresh = RefreshToken.for_user(user)
        jwt_headers = {"HTTP_AUTHORIZATION": f"Bearer {refresh.access_token}"}
    except Exception as exc:
        print_result(
            CheckResult(
                "WARN",
                "jwt-auth",
                f"JWT header generation skipped: {exc}",
            )
        )

    return jwt_headers, api_key_headers, api_key_obj


def run_auth_probe() -> CheckResult:
    from django.contrib.auth import get_user_model

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if not user:
        return CheckResult(
            "FAIL",
            "jwt-auth",
            "No active Django user was found. Create a superuser or activate a user "
            "so the test can generate a Bearer token automatically.",
        )

    jwt_headers, _api_key_headers, api_key_obj = build_auth_headers()
    try:
        if jwt_headers:
            return CheckResult(
                "PASS",
                "jwt-auth",
                "Generated a JWT Bearer token automatically for "
                f"user={user.username}. No manual token copy/paste is required for "
                "this installer validation.",
            )
    finally:
        if api_key_obj is not None:
            api_key_obj.delete()

    return CheckResult(
        "FAIL",
        "jwt-auth",
        f"Active user {user.username} exists, but JWT generation failed.",
    )


def probe_request(client, method: str, path: str, **headers) -> RouteResult:
    request_headers = {"HTTP_HOST": "localhost"}
    request_headers.update(headers)
    response = client.generic(method, path, **request_headers)
    status = response.status_code
    detail = response.reason_phrase
    if status >= 500:
        body = response.content.decode("utf-8", errors="replace")[:240].strip()
        if body:
            detail = body
    return RouteResult(method=method, path=path, status=status, detail=detail)


def run_endpoint_initialisation_checks():
    from django.test import Client
    from django.urls import get_resolver

    client = Client()
    client.raise_request_exception = False
    client.defaults["SERVER_NAME"] = "localhost"

    resolver = get_resolver()
    discovered_routes = []
    seen_paths = set()
    for path, name in iter_routes(resolver.url_patterns):
        if path not in seen_paths:
            discovered_routes.append((path, name))
            seen_paths.add(path)

    jwt_headers, api_key_headers, api_key_obj = build_auth_headers()

    route_results: list[RouteResult] = []
    try:
        for path, _name in discovered_routes:
            route_results.append(probe_request(client, "OPTIONS", path))

        curated_checks = [
            ("GET", "/", {}),
            ("GET", "/redoc/", {}),
            ("GET", "/swagger/", {}),
            ("GET", "/api/v1/get_states/", {}),
            ("GET", "/api/v1/proposed_blocks/", {}),
            ("GET", "/api/v1/auth/register/available_organizations/", {}),
            ("GET", "/api/v1/get_active_locations/", api_key_headers),
            ("GET", "/api/v1/get_user_api_keys/", jwt_headers),
        ]

        for method, path, headers in curated_checks:
            route_results.append(probe_request(client, method, path, **headers))
    finally:
        if api_key_obj is not None:
            api_key_obj.delete()

    return discovered_routes, route_results


def summarize_route_results(route_results: list[RouteResult]) -> list[CheckResult]:
    failures = [
        result for result in route_results if result.status not in SUCCESS_STATUSES
    ]

    summary = [
        CheckResult(
            "PASS" if not failures else "FAIL",
            "route-initialisation-summary",
            f"Executed {len(route_results)} endpoint probes with {len(failures)} unexpected statuses.",
        )
    ]

    for failure in failures[:20]:
        summary.append(
            CheckResult(
                "FAIL",
                f"{failure.method} {failure.path}",
                f"Returned {failure.status}: {failure.detail}",
            )
        )

    return summary


def discover_admin_boundary_sample() -> SampleLocation | None:
    import geopandas as gpd

    preferred_candidates = [
        ADMIN_BOUNDARY_INPUT_DIR / "assam" / "baksa.geojson",
    ]

    seen_paths: set[Path] = set()
    candidate_paths: list[Path] = []
    for path in preferred_candidates + sorted(
        ADMIN_BOUNDARY_INPUT_DIR.glob("*/*.geojson")
    ):
        if not path.is_file() or path.name == "soi_tehsil.geojson":
            continue
        resolved = path.resolve()
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)
        candidate_paths.append(path)

    for path in candidate_paths:
        try:
            gdf = gpd.read_file(path)
        except Exception:
            continue

        if gdf.empty or "TEHSIL" not in gdf.columns:
            continue

        tehsil_values = gdf["TEHSIL"].dropna().astype(str).str.strip()
        tehsil_values = tehsil_values[tehsil_values != ""]
        if tehsil_values.empty:
            continue

        return SampleLocation(
            state=path.parent.name.lower(),
            district=path.stem.lower(),
            block=tehsil_values.iloc[0].lower(),
            source_file=path,
        )

    return None


def run_admin_boundary_compute_check() -> tuple[CheckResult, SampleLocation | None]:
    import shutil

    from computing.misc.admin_boundary import (
        clip_block_from_admin_boundary,
        create_shp_files,
    )
    from utilities.gee_utils import valid_gee_text

    sample = discover_admin_boundary_sample()
    if sample is None:
        return (
            CheckResult(
                "FAIL",
                "admin-boundary-compute",
                "No usable admin-boundary sample was found under data/admin-boundary/input. "
                "Confirm that the installer finished the admin boundary step and that district "
                "GeoJSON files are present.",
            ),
            None,
        )

    try:
        with contextlib.redirect_stdout(io.StringIO()):
            collection, state_dir = clip_block_from_admin_boundary(
                sample.state,
                sample.district,
                sample.block,
            )
    except Exception as exc:
        return (
            CheckResult(
                "FAIL",
                "admin-boundary-compute",
                f"Local admin-boundary compute failed for {format_sample_location(sample)}: {exc}",
            ),
            sample,
        )

    features = collection.get("features", []) if hasattr(collection, "get") else []
    if not features:
        return (
            CheckResult(
                "FAIL",
                "admin-boundary-compute",
                "The admin-boundary compute path ran but returned 0 features for "
                f"{format_sample_location(sample)}. Check that {sample.source_file.relative_to(ROOT_DIR)} "
                "contains TEHSIL rows that match the block names used by the project.",
            ),
            sample,
        )

    state_dir_path = Path(state_dir)
    if not state_dir_path.is_absolute():
        state_dir_path = ROOT_DIR / state_dir_path

    output_prefix = (
        f"{valid_gee_text(sample.district.lower())}_"
        f"{valid_gee_text(sample.block.lower())}"
    )
    output_json = state_dir_path / f"{output_prefix}.json"
    output_shape_dir = state_dir_path / output_prefix
    expected_shape_parts = [
        output_shape_dir / f"{output_prefix}.shp",
        output_shape_dir / f"{output_prefix}.shx",
        output_shape_dir / f"{output_prefix}.dbf",
    ]

    if output_json.exists():
        output_json.unlink()
    if output_shape_dir.is_dir():
        shutil.rmtree(output_shape_dir)

    try:
        with contextlib.redirect_stdout(io.StringIO()):
            create_shp_files(collection, state_dir, sample.district, sample.block, None)
    except Exception as exc:
        return (
            CheckResult(
                "FAIL",
                "admin-boundary-compute",
                "Local admin-boundary compute built features, but artifact generation "
                f"failed for {format_sample_location(sample)}: {exc}",
            ),
            sample,
        )

    output_json_ready = output_json.exists()
    output_shape_dir_ready = output_shape_dir.is_dir()
    shape_parts_ready = all(path.exists() for path in expected_shape_parts)
    if not (output_json_ready and output_shape_dir_ready and shape_parts_ready):
        missing_artifacts = []
        if not output_json_ready:
            missing_artifacts.append(str(output_json.relative_to(ROOT_DIR)))
        if not output_shape_dir_ready:
            missing_artifacts.append(str(output_shape_dir.relative_to(ROOT_DIR)))
        if not shape_parts_ready:
            missing_artifacts.extend(
                str(path.relative_to(ROOT_DIR))
                for path in expected_shape_parts
                if not path.exists()
            )
        return (
            CheckResult(
                "FAIL",
                "admin-boundary-compute",
                "Local admin-boundary compute returned features, but did not leave the "
                "expected output artifacts. Missing: "
                + ", ".join(dict.fromkeys(missing_artifacts)),
            ),
            sample,
        )

    return (
        CheckResult(
            "PASS",
            "admin-boundary-compute",
            f"Built {len(features)} features for {format_sample_location(sample)} using "
            f"{sample.source_file.relative_to(ROOT_DIR)}. Artifacts verified under "
            f"{state_dir_path.relative_to(ROOT_DIR)}",
        ),
        sample,
    )


def run_first_computing_api_check(
    require_gee: bool,
    sample: SampleLocation | None,
    gcs_upload_result: CheckResult,
    geoserver_result: CheckResult,
) -> CheckResult:
    import shutil

    from django.conf import settings
    from django.test import Client
    from nrm_app.celery import app
    from utilities.gee_utils import valid_gee_text

    if sample is None:
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "first-computing-api",
            "Skipped because no admin-boundary sample location could be prepared.",
        )

    account_id = str(getattr(settings, "GEE_DEFAULT_ACCOUNT_ID", "")).strip()
    if not account_id or not account_id.isdigit():
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "first-computing-api-next-step",
            "Local admin-boundary compute is ready, but the first authenticated computing "
            "API initiation still needs GEE. Configure GEE in the installer or set "
            "`GEE_DEFAULT_ACCOUNT_ID` / `GEE_HELPER_ACCOUNT_ID`, then rerun "
            f"`python {Path(__file__).relative_to(ROOT_DIR)} --require-gee` for "
            f"{format_sample_location(sample)}.",
        )

    if gcs_upload_result.level != "PASS":
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "first-computing-api",
            "Skipped full POST /api/v1/generate_block_layer/ execution because the "
            "required Google Cloud Storage upload path is not ready yet. Fix "
            "`gcs-upload-probe` first, then rerun this initialisation test.",
        )

    if geoserver_result.level != "PASS":
        level = "FAIL" if require_gee else "WARN"
        return CheckResult(
            level,
            "first-computing-api",
            "Skipped full POST /api/v1/generate_block_layer/ execution because the "
            "GeoServer publish path is not ready yet. Fix `geoserver-probe` first, "
            "then rerun this initialisation test.",
        )

    jwt_headers, _api_key_headers, api_key_obj = build_auth_headers()
    if not jwt_headers:
        if api_key_obj is not None:
            api_key_obj.delete()
        return CheckResult(
            "FAIL",
            "first-computing-api",
            "JWT authentication could not be prepared. Create a Django user or superuser "
            "and rerun the installer so the authenticated computing API path can be verified.",
        )

    client = Client()
    client.raise_request_exception = False
    output_state_dir = ROOT_DIR / "data" / "admin-boundary" / "output" / sample.state
    output_prefix = (
        f"{valid_gee_text(sample.district.lower())}_"
        f"{valid_gee_text(sample.block.lower())}"
    )
    output_json = output_state_dir / f"{output_prefix}.json"
    output_shape_dir = output_state_dir / output_prefix
    expected_shape_parts = [
        output_shape_dir / f"{output_prefix}.shp",
        output_shape_dir / f"{output_prefix}.shx",
        output_shape_dir / f"{output_prefix}.dbf",
    ]

    if output_json.exists():
        output_json.unlink()
    if output_shape_dir.is_dir():
        shutil.rmtree(output_shape_dir)

    payload = {
        "state": sample.state,
        "district": sample.district,
        "block": sample.block,
        "gee_account_id": account_id,
    }

    previous_task_always_eager = app.conf.task_always_eager
    previous_task_eager_propagates = app.conf.task_eager_propagates
    execution_log = io.StringIO()
    try:
        app.conf.task_always_eager = True
        app.conf.task_eager_propagates = True
        with contextlib.redirect_stdout(execution_log):
            response = client.post(
                FIRST_COMPUTING_API_PATH,
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_HOST="localhost",
                **jwt_headers,
            )
    finally:
        app.conf.task_always_eager = previous_task_always_eager
        app.conf.task_eager_propagates = previous_task_eager_propagates
        if api_key_obj is not None:
            api_key_obj.delete()

    response_body = response.content.decode("utf-8", errors="replace")[:240].strip()
    execution_log_text = execution_log.getvalue().strip()
    output_json_ready = output_json.exists()
    output_shape_dir_ready = output_shape_dir.is_dir()
    shape_parts_ready = all(path.exists() for path in expected_shape_parts)

    if (
        response.status_code in {200, 201, 202}
        and output_json_ready
        and output_shape_dir_ready
        and shape_parts_ready
    ):
        return CheckResult(
            "PASS",
            "first-computing-api",
            f"POST {FIRST_COMPUTING_API_PATH} executed through Celery eager mode for "
            f"{format_sample_location(sample)} with gee_account_id={account_id}. "
            f"Artifacts verified: {output_json.relative_to(ROOT_DIR)} and "
            f"{output_shape_dir.relative_to(ROOT_DIR)}",
        )

    if response.status_code in {200, 201, 202}:
        missing_artifacts = []
        if not output_json_ready:
            missing_artifacts.append(str(output_json.relative_to(ROOT_DIR)))
        if not output_shape_dir_ready:
            missing_artifacts.append(str(output_shape_dir.relative_to(ROOT_DIR)))
        if not shape_parts_ready:
            missing_artifacts.extend(
                str(path.relative_to(ROOT_DIR))
                for path in expected_shape_parts
                if not path.exists()
            )
        return CheckResult(
            "FAIL",
            "first-computing-api",
            f"POST {FIRST_COMPUTING_API_PATH} returned {response.status_code}, but the "
            "admin-boundary workflow did not leave the expected local artifacts. Missing: "
            + ", ".join(dict.fromkeys(missing_artifacts)),
        )

    detail = response_body or response.reason_phrase
    if execution_log_text:
        detail = f"{detail} | task log: {execution_log_text[-240:]}"
    if output_json_ready or output_shape_dir_ready or shape_parts_ready:
        detail = (
            f"{detail} | local artifacts reached: "
            f"{output_json.relative_to(ROOT_DIR)}={output_json_ready}, "
            f"{output_shape_dir.relative_to(ROOT_DIR)}={output_shape_dir_ready}, "
            f"shapefile_parts={shape_parts_ready}"
        )
    return CheckResult(
        "FAIL",
        "first-computing-api",
        f"POST {FIRST_COMPUTING_API_PATH} returned {response.status_code}: {detail}",
    )


def build_next_step_guidance(
    require_gee: bool,
    sample: SampleLocation | None,
    auth_result: CheckResult,
    gee_probe_result: CheckResult,
    gcs_upload_result: CheckResult,
    geoserver_result: CheckResult,
    admin_boundary_result: CheckResult,
    first_api_result: CheckResult,
) -> CheckResult:
    if admin_boundary_result.level == "FAIL":
        return CheckResult(
            "FAIL",
            "setup-next-step",
            "Finish the admin boundary setup first. Confirm that "
            "`data/admin-boundary/input/soi_tehsil.geojson` and state-wise district files "
            "exist, then rerun the installer or this initialisation test.",
        )

    if auth_result.level != "PASS":
        return CheckResult(
            "FAIL",
            "setup-next-step",
            "Create a Django superuser or another active user first. The installer "
            "test generates the Bearer token automatically, so no manual auth token "
            "setup is needed once a valid user exists.",
        )

    if gee_probe_result.level == "PASS" and first_api_result.level == "PASS":
        sample_text = (
            format_sample_location(sample) if sample else "the verified sample block"
        )
        return CheckResult(
            "PASS",
            "setup-next-step",
            "Setup is ready for the first authenticated computing API call. "
            f"Verified sample: {sample_text}. Endpoint: POST {FIRST_COMPUTING_API_PATH}",
        )

    if gee_probe_result.level != "PASS":
        sample_text = format_sample_location(sample) if sample else "a sample block"
        return CheckResult(
            "WARN",
            "setup-next-step",
            "Local computing prerequisites are ready. Next, configure GEE in the installer "
            "or set `GEE_DEFAULT_ACCOUNT_ID` / `GEE_HELPER_ACCOUNT_ID`, then rerun "
            f"`python {Path(__file__).relative_to(ROOT_DIR)} --require-gee` to verify the first "
            f"authenticated computing API call for {sample_text}.",
        )

    if gcs_upload_result.level != "PASS":
        return CheckResult(
            "FAIL",
            "setup-next-step",
            "Earth Engine authentication is working, but the configured service account "
            "still cannot upload shapefile parts to the Google Cloud Storage bucket used "
            "by `/api/v1/generate_block_layer/`. Grant bucket write access "
            "(`storage.objects.create`, and ideally `storage.objects.delete` for probe cleanup) "
            "to the same service account, then rerun the initialisation test.",
        )

    if geoserver_result.level != "PASS":
        return CheckResult(
            "FAIL",
            "setup-next-step",
            "Earth Engine and GCS are ready, but GeoServer publish settings are not. "
            "Set `GEOSERVER_URL`, `GEOSERVER_USERNAME`, and `GEOSERVER_PASSWORD` to a "
            "reachable GeoServer REST endpoint, then rerun the initialisation test.",
        )

    if first_api_result.level != "PASS":
        return CheckResult(
            "FAIL",
            "setup-next-step",
            "The automated in-process test already handled Bearer token generation and "
            "Celery eager execution, so `runserver` and an external worker are only needed "
            "for manual API testing. The remaining blocker is inside the endpoint workflow "
            "itself. Review the `first-computing-api` result and rerun the initialisation test.",
        )
    return CheckResult(
        "WARN",
        "setup-next-step",
        "Core initialization checks passed. Rerun with `--require-gee` when you want the "
        "test to fail fast on any missing GEE dependency.",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run internal CoRE Stack API and initialization checks."
    )
    parser.add_argument(
        "--require-gee",
        action="store_true",
        help="Fail the run unless Earth Engine is configured and a real GEE probe succeeds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(f"Repo root: {ROOT_DIR}")

    try:
        setup_django()
        logging.getLogger("django.request").setLevel(logging.CRITICAL)
        logging.getLogger("django.security.DisallowedHost").setLevel(logging.ERROR)
        logging.getLogger("django.server").setLevel(logging.CRITICAL)
    except Exception as exc:
        print_result(
            CheckResult(
                "FAIL",
                "django-setup",
                f"Django setup failed: {exc}",
            )
        )
        traceback.print_exc()
        return 1

    config_results = run_configuration_checks(require_gee=args.require_gee)
    for result in config_results:
        print_result(result)

    gee_probe_result = run_gee_probe(require_gee=args.require_gee)
    print_result(gee_probe_result)

    gcs_upload_result = run_gcs_upload_probe(require_gee=args.require_gee)
    print_result(gcs_upload_result)

    geoserver_result = run_geoserver_probe()
    print_result(geoserver_result)

    auth_result = run_auth_probe()
    print_result(auth_result)

    admin_boundary_result, sample = run_admin_boundary_compute_check()
    print_result(admin_boundary_result)

    first_api_result = run_first_computing_api_check(
        require_gee=args.require_gee,
        sample=sample,
        gcs_upload_result=gcs_upload_result,
        geoserver_result=geoserver_result,
    )
    print_result(first_api_result)

    try:
        with contextlib.redirect_stdout(io.StringIO()):
            discovered_routes, route_results = run_endpoint_initialisation_checks()
        print_result(
            CheckResult(
                "PASS",
                "url-import",
                f"Resolved {len(discovered_routes)} routes from the project URLConf.",
            )
        )
    except Exception as exc:
        print_result(
            CheckResult(
                "FAIL",
                "url-import",
                f"URL resolution or endpoint probing failed: {exc}",
            )
        )
        traceback.print_exc()
        return 1

    for result in summarize_route_results(route_results):
        print_result(result)

    guidance_result = build_next_step_guidance(
        require_gee=args.require_gee,
        sample=sample,
        auth_result=auth_result,
        gee_probe_result=gee_probe_result,
        gcs_upload_result=gcs_upload_result,
        geoserver_result=geoserver_result,
        admin_boundary_result=admin_boundary_result,
        first_api_result=first_api_result,
    )
    print_result(guidance_result)

    failure_count = sum(1 for result in config_results if result.level == "FAIL")
    failure_count += 1 if auth_result.level == "FAIL" else 0
    failure_count += 1 if gee_probe_result.level == "FAIL" else 0
    failure_count += 1 if gcs_upload_result.level == "FAIL" else 0
    failure_count += 1 if admin_boundary_result.level == "FAIL" else 0
    failure_count += 1 if first_api_result.level == "FAIL" else 0
    failure_count += 1 if guidance_result.level == "FAIL" else 0
    failure_count += sum(
        1 for result in route_results if result.status not in SUCCESS_STATUSES
    )

    if failure_count:
        print("Internal API initialisation test finished with failures.")
        return 1

    print("Internal API initialisation test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
