#!/usr/bin/env python3
"""CoRE Stack public API helper.

This script has three main jobs:

1. smoke-test
   Run a minimal, installation-friendly probe against two public APIs.

2. resolve / locations
   Use the activated-location hierarchy to validate spellings, inspect what is
   active, and suggest close matches when a user types an inexact name.

3. download
   Download tehsil-level, village-level, and MWS-level public data streams in
   bulk, using either canonical administrative names, lat/lon, or a specific
   MWS id.

Runtime expectations:

- standard-library only: no third-party pip package is required
- no installation step or conda environment is required
- works directly on Linux, WSL, and Windows with Python 3.10+
- API credentials can come from `--api-key`, environment variables, or an
  optional `.env` file
- if `utilities/hierarchy_matching.py` is unavailable or broken, the client
  falls back to an internal standard-library matcher

Location scope rules:

- `--state --district --tehsil`: one tehsil package
- `--state --district`: expand across all activated tehsils in that district
- `--state`: expand across all activated tehsils in that state
- `--latitude --longitude`: resolve the containing tehsil automatically

Dataset bundles:

- `metadata`: active locations + tehsil data + generated layer catalog
- `layers`: layer catalog + downloadable layers + village/MWS geometries
- `watersheds`: MWS geometries + MWS analytics + KYL + reports
- `full`: the default end-to-end tehsil package

Examples:

  # Minimal smoke test on the default verified sample.
  python installation/public_api_client.py smoke-test

  # Smoke test a point by lat/lon.
  python installation/public_api_client.py smoke-test \
      --latitude 24.7387057899787 \
      --longitude 86.30411868979151

  # Inspect active districts and tehsils for one state.
  python installation/public_api_client.py locations --state assam
  python installation/public_api_client.py locations --state assam --district cachar

  # Resolve misspelled inputs and show closest hierarchy matches.
  python installation/public_api_client.py resolve \
      --state bihar \
      --district jamu \
      --tehsil jami

  # Download everything available for a tehsil.
  python installation/public_api_client.py download \
      --state assam \
      --district cachar \
      --tehsil lakhipur

  # Download only raster layers and tehsil metadata.
  python installation/public_api_client.py download \
      --state assam \
      --district cachar \
      --tehsil lakhipur \
      --datasets tehsil_data,layer_catalog,layers \
      --layer-types raster

  # Download village geometries for one tehsil and filter to one village name.
  python installation/public_api_client.py download \
      --state assam \
      --district cachar \
      --tehsil lakhipur \
      --streams village_geometries \
      --village-name "Lakhipur"

  # Download only one MWS payload by point lookup.
  python installation/public_api_client.py download \
      --latitude 24.7387057899787 \
      --longitude 86.30411868979151 \
      --datasets point_lookup,mws_data,mws_kyl,mws_report

  # Download all MWS payloads for the containing tehsil instead of just the
  # point MWS.
  python installation/public_api_client.py download \
      --latitude 24.7387057899787 \
      --longitude 86.30411868979151 \
      --bundle watersheds \
      --all-mws-in-tehsil
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import importlib
import json
import os
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _load_hierarchy_matching_backend() -> tuple[dict[str, Any], str, Exception | None]:
    try:
        matcher_module = importlib.import_module("utilities.hierarchy_matching")
        return (
            {
                "CandidateResolution": matcher_module.CandidateResolution,
                "HierarchyCandidate": matcher_module.HierarchyCandidate,
                "HierarchyResolution": matcher_module.HierarchyResolution,
                "normalize_match_text": matcher_module.normalize_match_text,
                "rank_candidates": matcher_module.rank_candidates,
                "resolve_best_candidate": matcher_module.resolve_best_candidate,
                "resolve_best_hierarchy_candidate": matcher_module.resolve_best_hierarchy_candidate,
            },
            "utilities.hierarchy_matching",
            None,
        )
    except Exception as exc:  # noqa: BLE001
        import re
        import unicodedata
        from difflib import SequenceMatcher

        @dataclass(frozen=True)
        class FallbackCandidateScore:
            candidate: str
            normalized_candidate: str
            score: float
            sequence_score: float
            edit_score: float
            damerau_edit_score: float
            jaro_winkler_score: float
            token_score: float
            prefix_score: float
            substring_score: float
            phonetic_score: float
            broad_phonetic_score: float
            consonant_score: float
            ngram_score: float
            soundex_score: float
            token_alignment_score: float

        @dataclass(frozen=True)
        class FallbackCandidateResolution:
            best_match: FallbackCandidateScore | None
            alternatives: list[FallbackCandidateScore]
            accepted: bool
            margin: float
            reason: str

        @dataclass(frozen=True)
        class FallbackHierarchyCandidate:
            state: str
            district: str | None = None
            tehsil: str | None = None
            payload: object | None = None

        @dataclass(frozen=True)
        class FallbackHierarchyScore:
            candidate: FallbackHierarchyCandidate
            score: float
            state_score: float
            district_score: float
            tehsil_score: float

        @dataclass(frozen=True)
        class FallbackHierarchyResolution:
            best_match: FallbackHierarchyScore | None
            alternatives: list[FallbackHierarchyScore]
            accepted: bool
            margin: float
            reason: str

        def fallback_normalize_match_text(value: str) -> str:
            normalized = unicodedata.normalize("NFKD", value or "")
            normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
            normalized = normalized.lower()
            normalized = re.sub(r"[&/,_()\\-]+", " ", normalized)
            normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
            normalized = re.sub(r"\s+", " ", normalized).strip()
            return normalized

        def fallback_ratio(left: str, right: str) -> float:
            normalized_left = fallback_normalize_match_text(left)
            normalized_right = fallback_normalize_match_text(right)
            if not normalized_left or not normalized_right:
                return 0.0
            if normalized_left == normalized_right:
                return 1.0

            sequence_score = SequenceMatcher(None, normalized_left, normalized_right).ratio()
            compact_score = SequenceMatcher(
                None,
                normalized_left.replace(" ", ""),
                normalized_right.replace(" ", ""),
            ).ratio()
            prefix_score = (
                1.0
                if normalized_right.startswith(normalized_left) or normalized_left.startswith(normalized_right)
                else 0.0
            )
            substring_score = (
                1.0 if normalized_left in normalized_right or normalized_right in normalized_left else 0.0
            )

            left_tokens = set(normalized_left.split())
            right_tokens = set(normalized_right.split())
            token_score = 0.0
            if left_tokens and right_tokens:
                token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)

            return max(
                sequence_score,
                compact_score,
                0.55 * sequence_score
                + 0.20 * compact_score
                + 0.15 * token_score
                + 0.05 * prefix_score
                + 0.05 * substring_score,
            )

        def fallback_rank_candidates(
            query: str,
            candidates: list[str],
            *,
            limit: int = 5,
            min_score: float = 0.0,
        ) -> list[FallbackCandidateScore]:
            ranked: list[FallbackCandidateScore] = []
            for candidate in candidates:
                score = fallback_ratio(query, candidate)
                if score < min_score:
                    continue
                normalized_candidate = fallback_normalize_match_text(candidate)
                ranked.append(
                    FallbackCandidateScore(
                        candidate=candidate,
                        normalized_candidate=normalized_candidate,
                        score=score,
                        sequence_score=score,
                        edit_score=score,
                        damerau_edit_score=score,
                        jaro_winkler_score=score,
                        token_score=score,
                        prefix_score=score,
                        substring_score=score,
                        phonetic_score=score,
                        broad_phonetic_score=score,
                        consonant_score=score,
                        ngram_score=score,
                        soundex_score=score,
                        token_alignment_score=score,
                    )
                )
            ranked.sort(key=lambda item: (-item.score, item.normalized_candidate, item.candidate))
            return ranked[:limit]

        def fallback_resolve_best_candidate(
            query: str,
            candidates: list[str],
            *,
            limit: int = 3,
            auto_accept_score: float = 0.84,
            min_margin: float = 0.06,
            min_score: float = 0.0,
        ) -> FallbackCandidateResolution:
            ranked = fallback_rank_candidates(query, candidates, limit=limit, min_score=min_score)
            if not ranked:
                return FallbackCandidateResolution(
                    best_match=None,
                    alternatives=[],
                    accepted=False,
                    margin=0.0,
                    reason="no_candidates",
                )

            best = ranked[0]
            runner_up = ranked[1].score if len(ranked) > 1 else 0.0
            margin = best.score - runner_up
            accepted = best.score >= auto_accept_score and margin >= min_margin
            return FallbackCandidateResolution(
                best_match=best,
                alternatives=ranked,
                accepted=accepted,
                margin=margin,
                reason="accepted" if accepted else "low_confidence",
            )

        def fallback_resolve_best_hierarchy_candidate(
            *,
            candidates: list[FallbackHierarchyCandidate],
            state_query: str,
            district_query: str | None = None,
            tehsil_query: str | None = None,
            limit: int = 3,
            auto_accept_score: float = 0.84,
            min_margin: float = 0.06,
        ) -> FallbackHierarchyResolution:
            ranked: list[FallbackHierarchyScore] = []
            for candidate in candidates:
                state_score = fallback_ratio(state_query, candidate.state)
                district_score = fallback_ratio(district_query or "", candidate.district or "") if district_query else 0.0
                tehsil_score = fallback_ratio(tehsil_query or "", candidate.tehsil or "") if tehsil_query else 0.0

                weighted_scores: list[tuple[float, float]] = [(state_score, 0.25)]
                if district_query:
                    weighted_scores.append((district_score, 0.35))
                if tehsil_query:
                    weighted_scores.append((tehsil_score, 0.40))

                total_weight = sum(weight for _, weight in weighted_scores)
                combined_score = (
                    sum(score * weight for score, weight in weighted_scores) / total_weight
                    if total_weight
                    else 0.0
                )
                ranked.append(
                    FallbackHierarchyScore(
                        candidate=candidate,
                        score=combined_score,
                        state_score=state_score,
                        district_score=district_score,
                        tehsil_score=tehsil_score,
                    )
                )

            ranked.sort(
                key=lambda item: (
                    -item.score,
                    -item.tehsil_score,
                    -item.district_score,
                    -item.state_score,
                    fallback_normalize_match_text(item.candidate.state),
                    fallback_normalize_match_text(item.candidate.district or ""),
                    fallback_normalize_match_text(item.candidate.tehsil or ""),
                )
            )
            ranked = ranked[:limit]
            if not ranked:
                return FallbackHierarchyResolution(
                    best_match=None,
                    alternatives=[],
                    accepted=False,
                    margin=0.0,
                    reason="no_candidates",
                )

            best = ranked[0]
            runner_up = ranked[1].score if len(ranked) > 1 else 0.0
            margin = best.score - runner_up
            accepted = best.score >= auto_accept_score and margin >= min_margin
            return FallbackHierarchyResolution(
                best_match=best,
                alternatives=ranked,
                accepted=accepted,
                margin=margin,
                reason="accepted" if accepted else "low_confidence",
            )

        return (
            {
                "CandidateResolution": FallbackCandidateResolution,
                "HierarchyCandidate": FallbackHierarchyCandidate,
                "HierarchyResolution": FallbackHierarchyResolution,
                "normalize_match_text": fallback_normalize_match_text,
                "rank_candidates": fallback_rank_candidates,
                "resolve_best_candidate": fallback_resolve_best_candidate,
                "resolve_best_hierarchy_candidate": fallback_resolve_best_hierarchy_candidate,
            },
            "internal_fallback",
            exc,
        )


_MATCHER_EXPORTS, MATCHER_BACKEND, MATCHER_BACKEND_ERROR = _load_hierarchy_matching_backend()
CandidateResolution = _MATCHER_EXPORTS["CandidateResolution"]
HierarchyCandidate = _MATCHER_EXPORTS["HierarchyCandidate"]
HierarchyResolution = _MATCHER_EXPORTS["HierarchyResolution"]
normalize_match_text = _MATCHER_EXPORTS["normalize_match_text"]
rank_candidates = _MATCHER_EXPORTS["rank_candidates"]
resolve_best_candidate = _MATCHER_EXPORTS["resolve_best_candidate"]
resolve_best_hierarchy_candidate = _MATCHER_EXPORTS["resolve_best_hierarchy_candidate"]


DEFAULT_PUBLIC_API_BASE_URL = "https://geoserver.core-stack.org/api/v1"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_SAMPLE_STATE = "assam"
DEFAULT_SAMPLE_DISTRICT = "cachar"
DEFAULT_SAMPLE_TEHSIL = "lakhipur"
DEFAULT_ACTIVE_LOCATIONS_FILE = (
    ROOT_DIR
    / "data"
    / "activated_locations"
    / "active_locations.json"
)
DEFAULT_FUZZY_AUTO_ACCEPT_SCORE = 0.84
DEFAULT_FUZZY_MIN_MARGIN = 0.06
FILTER_FUZZY_AUTO_ACCEPT_SCORE = 0.70
FILTER_FUZZY_MIN_MARGIN = 0.08

ALL_DOWNLOAD_STREAMS = {
    "active_locations",
    "point_lookup",
    "tehsil_data",
    "layer_catalog",
    "layers",
    "village_geometries",
    "mws_geometries",
    "mws_data",
    "mws_kyl",
    "mws_report",
}
DEFAULT_DOWNLOAD_STREAMS = {
    "tehsil_data",
    "layer_catalog",
    "layers",
    "village_geometries",
    "mws_geometries",
    "mws_data",
    "mws_kyl",
    "mws_report",
}
DATASET_BUNDLES = {
    "full": set(DEFAULT_DOWNLOAD_STREAMS),
    "metadata": {"active_locations", "tehsil_data", "layer_catalog"},
    "layers": {"layer_catalog", "layers", "village_geometries", "mws_geometries"},
    "watersheds": {"mws_geometries", "mws_data", "mws_kyl", "mws_report"},
}


class PublicAPIError(RuntimeError):
    pass


class PublicAPIHTTPError(PublicAPIError):
    def __init__(self, endpoint: str, url: str, status_code: int, body: str) -> None:
        self.endpoint = endpoint
        self.url = url
        self.status_code = status_code
        self.body = body
        snippet = " ".join(body.split())[:280]
        super().__init__(f"{endpoint} failed with HTTP {status_code}: {snippet}")


@dataclass(frozen=True)
class ActiveLocationPath:
    state: str
    district: str
    tehsil: str
    state_id: str | None = None
    district_id: str | None = None
    tehsil_id: str | None = None


@dataclass(frozen=True)
class ActiveLocationResolution:
    path: ActiveLocationPath
    matched_via: str
    score: float
    margin: float
    alternatives: list[HierarchyCandidate]


@dataclass(frozen=True)
class DownloadPlan:
    scope: str
    root_location: dict[str, Any] | None
    tehsil_targets: list[dict[str, Any]]
    notes: list[str]


def strip_wrapping_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def parse_env_file(env_file: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_file.is_file():
        return values

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = strip_wrapping_quotes(value)
    return values


def normalize_base_url(value: str) -> str:
    normalized = strip_wrapping_quotes(value).strip().rstrip("/")
    if not normalized:
        raise PublicAPIError("A public API base URL is required.")
    if not normalized.endswith("/api/v1"):
        normalized = f"{normalized}/api/v1"
    return normalized


def sanitize_slug(value: str) -> str:
    normalized = normalize_match_text(value).replace(" ", "_")
    return normalized or "item"


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, data: Any) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8")


def build_url(base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
    url = f"{normalize_base_url(base_url)}/{endpoint.strip('/')}/"
    if params:
        query_items = [(key, value) for key, value in params.items() if value is not None]
        url = f"{url}?{urllib.parse.urlencode(query_items, doseq=True)}"
    return url


def request_json(
    *,
    base_url: str,
    endpoint: str,
    api_key: str,
    params: dict[str, Any] | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    url = build_url(base_url, endpoint, params)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-API-Key": api_key,
            "User-Agent": "corestack-public-api-client/2.0",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise PublicAPIHTTPError(endpoint=endpoint, url=url, status_code=exc.code, body=body) from exc
    except urllib.error.URLError as exc:
        raise PublicAPIError(f"{endpoint} failed while connecting to {url}: {exc.reason}") from exc

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise PublicAPIError(f"{endpoint} returned invalid JSON from {url}") from exc


def request_optional_json(
    *,
    base_url: str,
    endpoint: str,
    api_key: str,
    params: dict[str, Any],
    timeout: int,
) -> Any | None:
    try:
        return request_json(
            base_url=base_url,
            endpoint=endpoint,
            api_key=api_key,
            params=params,
            timeout=timeout,
        )
    except PublicAPIHTTPError as exc:
        if exc.status_code == 404:
            return None
        raise


def download_to_file(url: str, destination: Path, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> None:
    ensure_directory(destination.parent)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "corestack-public-api-client/2.0"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response, destination.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 128)
            if not chunk:
                break
            handle.write(chunk)


def resolve_runtime_config(
    *,
    env_file: Path,
    api_key: str | None,
    base_url: str | None,
) -> tuple[str, str]:
    env_values = parse_env_file(env_file)

    resolved_api_key = api_key or os.environ.get("PUBLIC_API_X_API_KEY") or env_values.get("PUBLIC_API_X_API_KEY")
    resolved_base_url = (
        base_url
        or os.environ.get("PUBLIC_API_BASE_URL")
        or env_values.get("PUBLIC_API_BASE_URL")
        or DEFAULT_PUBLIC_API_BASE_URL
    )

    if not resolved_api_key:
        raise PublicAPIError(
            "PUBLIC_API_X_API_KEY is not configured. Pass --api-key or add it to the environment/.env file."
        )

    return resolved_api_key, normalize_base_url(resolved_base_url)


def format_candidate_suggestions(title: str, entries: list[Any]) -> str:
    if not entries:
        return f"{title}: none"

    rendered = []
    for entry in entries:
        label = entry.candidate if hasattr(entry, "candidate") else str(entry)
        score = getattr(entry, "score", None)
        if score is None:
            rendered.append(label)
        else:
            rendered.append(f"{label} ({score:.2f})")
    return f"{title}: " + ", ".join(rendered)


def format_path_suggestions(entries: list[Any]) -> str:
    if not entries:
        return "Top full-path matches: none"
    rendered = []
    for entry in entries:
        candidate = entry.candidate
        rendered.append(
            f"{candidate.state} > {candidate.district} > {candidate.tehsil} ({entry.score:.2f})"
        )
    return "Top full-path matches: " + ", ".join(rendered)


def iter_active_location_paths(active_locations: list[dict[str, Any]]) -> list[ActiveLocationPath]:
    paths: list[ActiveLocationPath] = []
    for state in active_locations:
        state_label = str(state.get("label", "")).strip()
        state_id = str(state.get("state_id", "")).strip() or None
        for district in state.get("district", []):
            district_label = str(district.get("label", "")).strip()
            district_id = str(district.get("district_id", "")).strip() or None
            for block in district.get("blocks", []):
                tehsil_label = str(block.get("label", "")).strip()
                tehsil_id = (
                    str(block.get("tehsil_id", "")).strip()
                    or str(block.get("block_id", "")).strip()
                    or None
                )
                paths.append(
                    ActiveLocationPath(
                        state=state_label,
                        district=district_label,
                        tehsil=tehsil_label,
                        state_id=state_id,
                        district_id=district_id,
                        tehsil_id=tehsil_id,
                    )
                )
    return paths


def hierarchy_candidates_from_paths(paths: list[ActiveLocationPath]) -> list[HierarchyCandidate]:
    return [
        HierarchyCandidate(
            state=path.state,
            district=path.district,
            tehsil=path.tehsil,
            payload=path,
        )
        for path in paths
    ]


def load_active_locations_catalog(
    *,
    base_url: str,
    api_key: str,
    active_locations_file: Path,
    refresh: bool,
    timeout: int,
) -> list[dict[str, Any]]:
    if active_locations_file.is_file() and not refresh:
        return json.loads(active_locations_file.read_text(encoding="utf-8"))

    active_locations = request_json(
        base_url=base_url,
        endpoint="get_active_locations",
        api_key=api_key,
        timeout=timeout,
    )
    ensure_directory(active_locations_file.parent)
    active_locations_file.write_text(
        json.dumps(active_locations, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return active_locations


def exact_match_path(
    *,
    paths: list[ActiveLocationPath],
    state: str,
    district: str,
    tehsil: str,
) -> ActiveLocationPath | None:
    wanted_state = normalize_match_text(state)
    wanted_district = normalize_match_text(district)
    wanted_tehsil = normalize_match_text(tehsil)
    for path in paths:
        if (
            normalize_match_text(path.state) == wanted_state
            and normalize_match_text(path.district) == wanted_district
            and normalize_match_text(path.tehsil) == wanted_tehsil
        ):
            return path
    return None


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        key = normalize_match_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def path_resolution_alternatives(
    resolution: HierarchyResolution,
) -> list[HierarchyCandidate]:
    return [entry.candidate for entry in resolution.alternatives]


def build_active_location_error(
    *,
    paths: list[ActiveLocationPath],
    state: str,
    district: str,
    tehsil: str,
    path_resolution: HierarchyResolution,
    strict_location_match: bool,
) -> str:
    states = sorted({path.state for path in paths})
    state_resolution = resolve_best_candidate(
        state,
        states,
        limit=3,
        auto_accept_score=2.0,
        min_margin=2.0,
    )

    shortlisted_state_names = unique_preserve_order(
        [entry.candidate for entry in state_resolution.alternatives]
    )
    state_shortlist = [
        path
        for path in paths
        if normalize_match_text(path.state) in {normalize_match_text(name) for name in shortlisted_state_names}
    ]
    district_pool = unique_preserve_order([path.district for path in state_shortlist])
    district_resolution = resolve_best_candidate(
        district,
        district_pool,
        limit=3,
        auto_accept_score=2.0,
        min_margin=2.0,
    ) if district_pool else CandidateResolution(None, [], False, 0.0, "no_candidates")

    shortlisted_district_names = unique_preserve_order(
        [entry.candidate for entry in district_resolution.alternatives]
    )
    district_shortlist = [
        path
        for path in state_shortlist
        if normalize_match_text(path.district) in {normalize_match_text(name) for name in shortlisted_district_names}
    ]
    tehsil_pool = unique_preserve_order([path.tehsil for path in district_shortlist])
    tehsil_resolution = resolve_best_candidate(
        tehsil,
        tehsil_pool,
        limit=3,
        auto_accept_score=2.0,
        min_margin=2.0,
    ) if tehsil_pool else CandidateResolution(None, [], False, 0.0, "no_candidates")

    lines = [
        "The provided state/district/tehsil combination was not found in activated locations.",
    ]
    if strict_location_match:
        lines.append("Strict matching is enabled, so only exact canonical names are accepted.")
    if path_resolution.best_match is not None:
        best = path_resolution.best_match
        lines.append(
            "Best full-path match: "
            f"{best.candidate.state} > {best.candidate.district} > {best.candidate.tehsil} "
            f"(score {best.score:.3f}, margin {path_resolution.margin:.3f})"
        )
    lines.extend(
        [
            format_candidate_suggestions("Closest states", state_resolution.alternatives),
            format_candidate_suggestions(
                "Closest districts within the shortlisted states",
                district_resolution.alternatives,
            ),
            format_candidate_suggestions(
                "Closest tehsils within the shortlisted districts",
                tehsil_resolution.alternatives,
            ),
            format_path_suggestions(path_resolution.alternatives),
            "Use `locations` or `resolve` to inspect the active hierarchy, or rerun with `--allow-unlisted-location`.",
        ]
    )
    return "\n".join(lines)


def resolve_active_location(
    *,
    active_locations: list[dict[str, Any]],
    state: str,
    district: str,
    tehsil: str,
    strict_location_match: bool = False,
) -> ActiveLocationResolution:
    paths = iter_active_location_paths(active_locations)
    exact = exact_match_path(paths=paths, state=state, district=district, tehsil=tehsil)
    if exact is not None:
        return ActiveLocationResolution(
            path=exact,
            matched_via="active_locations_exact",
            score=1.0,
            margin=1.0,
            alternatives=[],
        )

    path_resolution = resolve_best_hierarchy_candidate(
        candidates=hierarchy_candidates_from_paths(paths),
        state_query=state,
        district_query=district,
        tehsil_query=tehsil,
        limit=3,
        auto_accept_score=DEFAULT_FUZZY_AUTO_ACCEPT_SCORE,
        min_margin=DEFAULT_FUZZY_MIN_MARGIN,
    )

    if (
        not strict_location_match
        and path_resolution.accepted
        and path_resolution.best_match is not None
        and isinstance(path_resolution.best_match.candidate.payload, ActiveLocationPath)
    ):
        return ActiveLocationResolution(
            path=path_resolution.best_match.candidate.payload,
            matched_via="active_locations_fuzzy",
            score=path_resolution.best_match.score,
            margin=path_resolution.margin,
            alternatives=path_resolution_alternatives(path_resolution),
        )

    raise PublicAPIError(
        build_active_location_error(
            paths=paths,
            state=state,
            district=district,
            tehsil=tehsil,
            path_resolution=path_resolution,
            strict_location_match=strict_location_match,
        )
    )


def validate_active_location(
    *,
    active_locations: list[dict[str, Any]],
    state: str,
    district: str,
    tehsil: str,
    strict_location_match: bool = False,
) -> ActiveLocationPath:
    return resolve_active_location(
        active_locations=active_locations,
        state=state,
        district=district,
        tehsil=tehsil,
        strict_location_match=strict_location_match,
    ).path


def resolve_location(args: argparse.Namespace, *, api_key: str, base_url: str) -> dict[str, Any]:
    if args.state and args.district and args.tehsil:
        return {
            "state": args.state.strip(),
            "district": args.district.strip(),
            "tehsil": args.tehsil.strip(),
            "resolved_via": "admin_args",
        }

    if (args.latitude is None) ^ (args.longitude is None):
        raise PublicAPIError("Provide both --latitude and --longitude together.")

    if args.latitude is None or args.longitude is None:
        raise PublicAPIError("Provide either --state/--district/--tehsil or --latitude/--longitude.")

    location = request_json(
        base_url=base_url,
        endpoint="get_admin_details_by_latlon",
        api_key=api_key,
        params={"latitude": args.latitude, "longitude": args.longitude},
        timeout=args.timeout,
    )
    return {
        "state": location["State"],
        "district": location["District"],
        "tehsil": location["Tehsil"],
        "latitude": args.latitude,
        "longitude": args.longitude,
        "resolved_via": "latlon",
    }


def resolve_download_location(
    args: argparse.Namespace,
    *,
    api_key: str,
    base_url: str,
    active_locations: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    location = resolve_location(args, api_key=api_key, base_url=base_url)

    if location.get("resolved_via") == "latlon":
        return location

    if active_locations is None:
        return location

    try:
        resolution = resolve_active_location(
            active_locations=active_locations,
            state=location["state"],
            district=location["district"],
            tehsil=location["tehsil"],
            strict_location_match=args.strict_location_match,
        )
    except PublicAPIError:
        if args.allow_unlisted_location:
            return {
                **location,
                "resolved_via": "admin_args_unlisted",
            }
        raise

    resolved = {
        "state": resolution.path.state,
        "district": resolution.path.district,
        "tehsil": resolution.path.tehsil,
        "state_id": resolution.path.state_id,
        "district_id": resolution.path.district_id,
        "tehsil_id": resolution.path.tehsil_id,
        "resolved_via": resolution.matched_via,
    }
    if resolution.matched_via == "active_locations_fuzzy":
        resolved.update(
            {
                "input_state": location["state"],
                "input_district": location["district"],
                "input_tehsil": location["tehsil"],
                "match_score": round(resolution.score, 6),
                "match_margin": round(resolution.margin, 6),
            }
        )
    return resolved


def location_query(location: dict[str, Any]) -> dict[str, Any]:
    return {
        "state": location["state"],
        "district": location["district"],
        "tehsil": location["tehsil"],
    }


def extract_mws_ids(geojson: Any) -> list[str]:
    if not isinstance(geojson, dict):
        return []

    seen: set[str] = set()
    ordered: list[str] = []
    for feature in geojson.get("features", []):
        properties = feature.get("properties", {})
        uid = str(properties.get("uid", "")).strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        ordered.append(uid)
    return ordered


def maybe_limit(items: list[Any], limit: int | None) -> list[Any]:
    if not limit or limit < 0:
        return items
    return items[:limit]


def infer_layer_extension(layer: dict[str, Any]) -> str:
    layer_type = str(layer.get("layer_type", "")).lower()
    if layer_type == "raster":
        return "tif"
    return "geojson"


def parse_csv_argument(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def expand_streams(raw_streams: str | None) -> set[str]:
    if not raw_streams:
        return set(DEFAULT_DOWNLOAD_STREAMS)

    requested = {value.lower() for value in parse_csv_argument(raw_streams)}
    if "all" in requested:
        requested.remove("all")
        requested |= set(DEFAULT_DOWNLOAD_STREAMS)

    unknown = requested - ALL_DOWNLOAD_STREAMS
    if unknown:
        raise PublicAPIError(f"Unknown stream(s): {', '.join(sorted(unknown))}")
    return requested


def expand_requested_datasets(
    *,
    raw_datasets: str | None,
    raw_streams: str | None,
    bundle: str | None,
) -> set[str]:
    if raw_datasets and raw_streams:
        raise PublicAPIError("Use either --datasets or --streams, not both.")
    if raw_datasets:
        return expand_streams(raw_datasets)
    if raw_streams:
        return expand_streams(raw_streams)
    if bundle:
        selected = DATASET_BUNDLES.get(bundle.lower())
        if selected is None:
            raise PublicAPIError(
                f"Unknown bundle {bundle!r}. Choose from: {', '.join(sorted(DATASET_BUNDLES))}"
            )
        return set(selected)
    return set(DEFAULT_DOWNLOAD_STREAMS)


def filter_layers_by_type(layers: list[dict[str, Any]], raw_layer_types: str | None) -> list[dict[str, Any]]:
    if not raw_layer_types:
        return layers

    requested = {value.lower() for value in parse_csv_argument(raw_layer_types)}
    if "all" in requested or not requested:
        return layers
    return [
        layer for layer in layers if str(layer.get("layer_type", "")).lower() in requested
    ]


def filter_village_geometries(
    geojson: dict[str, Any],
    *,
    village_id: str | None,
    village_name: str | None,
    strict_name_match: bool = False,
) -> dict[str, Any]:
    if not village_id and not village_name:
        return geojson

    features = geojson.get("features", [])
    if village_id:
        filtered = [
            feature
            for feature in features
            if str(feature.get("properties", {}).get("vill_ID", "")).strip() == village_id.strip()
        ]
        if not filtered:
            raise PublicAPIError(f"No village geometry matched vill_ID={village_id}.")
        return {**geojson, "features": filtered}

    exact = [
        feature
        for feature in features
        if normalize_match_text(str(feature.get("properties", {}).get("vill_name", "")))
        == normalize_match_text(village_name or "")
    ]
    if exact:
        return {**geojson, "features": exact}

    names = [
        str(feature.get("properties", {}).get("vill_name", "")).strip()
        for feature in features
        if str(feature.get("properties", {}).get("vill_name", "")).strip()
    ]
    resolution = resolve_best_candidate(
        village_name or "",
        names,
        limit=3,
        auto_accept_score=DEFAULT_FUZZY_AUTO_ACCEPT_SCORE,
        min_margin=DEFAULT_FUZZY_MIN_MARGIN,
    )
    if not strict_name_match and resolution.accepted and resolution.best_match is not None:
        matched_name = resolution.best_match.candidate
        selected = [
            feature
            for feature in features
            if normalize_match_text(str(feature.get("properties", {}).get("vill_name", "")))
            == normalize_match_text(matched_name)
        ]
        if selected:
            return {**geojson, "features": selected}
    raise PublicAPIError(
        "\n".join(
            [
                f"No village geometry matched village name {village_name!r}.",
                "Strict matching is enabled." if strict_name_match else "The best fuzzy village match was not confident enough to auto-select.",
                format_candidate_suggestions("Closest villages", resolution.alternatives),
            ]
        )
    )


def extract_point_mws_id(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    for key in ("mws_id", "uid"):
        value = str(payload.get(key, "")).strip()
        if value:
            return value
    return None


def output_directory_for(
    *,
    explicit_output_dir: str | None,
    location: dict[str, Any] | None,
    streams: set[str],
) -> Path:
    if explicit_output_dir:
        return Path(explicit_output_dir).expanduser().resolve()

    if location is None:
        return Path.cwd() / "public-api-downloads" / "active_locations"

    path = Path.cwd() / "public-api-downloads"
    if location.get("state"):
        path /= sanitize_slug(location["state"])
    if location.get("district"):
        path /= sanitize_slug(location["district"])
    if location.get("tehsil"):
        path /= sanitize_slug(location["tehsil"])
    return path


def summary_for_active_locations(active_locations: list[dict[str, Any]]) -> dict[str, Any]:
    states = len(active_locations)
    districts = sum(len(state.get("district", [])) for state in active_locations)
    tehsils = sum(
        len(district.get("blocks", []))
        for state in active_locations
        for district in state.get("district", [])
    )
    return {
        "states": states,
        "districts": districts,
        "tehsils": tehsils,
    }


def read_json_if_exists(path: Path) -> Any | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def location_display_name(location: dict[str, Any]) -> str:
    return " / ".join(
        value
        for value in [
            str(location.get("state", "")).strip(),
            str(location.get("district", "")).strip(),
            str(location.get("tehsil", "")).strip(),
        ]
        if value
    )


def hierarchy_from_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state_index: dict[str, dict[str, Any]] = {}
    ordered_states: list[dict[str, Any]] = []

    for location in locations:
        state_label = str(location.get("state", "")).strip()
        district_label = str(location.get("district", "")).strip()
        tehsil_label = str(location.get("tehsil", "")).strip()
        if not state_label or not district_label or not tehsil_label:
            continue

        state_key = normalize_match_text(state_label)
        state_entry = state_index.get(state_key)
        if state_entry is None:
            state_entry = {
                "label": state_label,
                "state_id": location.get("state_id"),
                "district": [],
                "_district_index": {},
            }
            state_index[state_key] = state_entry
            ordered_states.append(state_entry)

        district_key = normalize_match_text(district_label)
        district_entry = state_entry["_district_index"].get(district_key)
        if district_entry is None:
            district_entry = {
                "label": district_label,
                "district_id": location.get("district_id"),
                "blocks": [],
                "_block_index": set(),
            }
            state_entry["_district_index"][district_key] = district_entry
            state_entry["district"].append(district_entry)

        tehsil_key = normalize_match_text(tehsil_label)
        if tehsil_key in district_entry["_block_index"]:
            continue
        district_entry["_block_index"].add(tehsil_key)
        district_entry["blocks"].append(
            {
                "label": tehsil_label,
                "block_id": location.get("tehsil_id"),
                "tehsil_id": location.get("tehsil_id"),
            }
        )

    for state_entry in ordered_states:
        state_entry.pop("_district_index", None)
        for district_entry in state_entry["district"]:
            district_entry.pop("_block_index", None)
    return ordered_states


def feature_identity(feature: dict[str, Any], *, preferred_keys: tuple[str, ...]) -> str:
    properties = feature.get("properties", {})
    for key in preferred_keys:
        value = str(properties.get(key, "")).strip()
        if value:
            return f"{key}:{value}"

    name_fields = ["vill_name", "village_name", "name", "Name"]
    normalized_names = [
        normalize_match_text(str(properties.get(key, "")).strip())
        for key in name_fields
        if str(properties.get(key, "")).strip()
    ]
    geometry = feature.get("geometry")
    if normalized_names or geometry:
        return json.dumps(
            {
                "names": normalized_names,
                "geometry": geometry,
            },
            sort_keys=True,
        )
    return json.dumps(feature, sort_keys=True)


def merge_feature_collections(
    collections: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    preferred_keys: tuple[str, ...],
) -> tuple[dict[str, Any], dict[str, list[str]], int]:
    merged_features: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    source_index: dict[str, list[str]] = {}
    duplicate_count = 0

    for location, payload in collections:
        if not isinstance(payload, dict):
            continue
        source_label = location_display_name(location)
        for feature in payload.get("features", []):
            if not isinstance(feature, dict):
                continue
            identity = feature_identity(feature, preferred_keys=preferred_keys)
            sources = source_index.setdefault(identity, [])
            if source_label and source_label not in sources:
                sources.append(source_label)
            if identity in seen_keys:
                duplicate_count += 1
                continue
            seen_keys.add(identity)
            merged_features.append(feature)

    merged_payload = {
        "type": "FeatureCollection",
        "features": merged_features,
    }
    return merged_payload, source_index, duplicate_count


def merge_layer_catalogs(
    catalogs: list[tuple[dict[str, Any], list[dict[str, Any]]]],
) -> tuple[list[dict[str, Any]], dict[str, list[str]], int]:
    merged_layers: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    source_index: dict[str, list[str]] = {}
    duplicate_count = 0

    for location, layers in catalogs:
        source_label = location_display_name(location)
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            key = json.dumps(
                {
                    "layer_name": layer.get("layer_name"),
                    "layer_url": layer.get("layer_url"),
                    "dataset_name": layer.get("dataset_name"),
                },
                sort_keys=True,
            )
            sources = source_index.setdefault(key, [])
            if source_label and source_label not in sources:
                sources.append(source_label)
            if key in seen_keys:
                duplicate_count += 1
                continue
            seen_keys.add(key)
            merged_layers.append(layer)

    return merged_layers, source_index, duplicate_count


def collect_mws_payload_index(tehsil_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    index: dict[str, dict[str, Any]] = {}

    for summary in tehsil_summaries:
        location = summary["location"]
        source_label = location_display_name(location)
        mws_dir = Path(summary["output_dir"]) / "mws"
        if not mws_dir.is_dir():
            continue

        for entry in sorted(mws_dir.iterdir()):
            if not entry.is_dir():
                continue
            item = index.setdefault(
                entry.name,
                {
                    "mws_id": entry.name,
                    "source_tehsils": [],
                    "files": {},
                },
            )
            if source_label and source_label not in item["source_tehsils"]:
                item["source_tehsils"].append(source_label)
            for payload_file in sorted(entry.iterdir()):
                if not payload_file.is_file():
                    continue
                item["files"].setdefault(payload_file.name, []).append(str(payload_file))

    return {
        "mws_ids": sorted(index),
        "items": [index[mws_id] for mws_id in sorted(index)],
    }


def aggregate_bulk_download_outputs(
    *,
    root_output_dir: Path,
    root_location: dict[str, Any],
    tehsil_summaries: list[dict[str, Any]],
    streams: set[str],
) -> dict[str, Any]:
    root_metadata_dir = ensure_directory(root_output_dir / "metadata")
    locations = [summary["location"] for summary in tehsil_summaries]

    selected_tehsils = [
        {
            "state": summary["location"]["state"],
            "district": summary["location"]["district"],
            "tehsil": summary["location"]["tehsil"],
            "state_id": summary["location"].get("state_id"),
            "district_id": summary["location"].get("district_id"),
            "tehsil_id": summary["location"].get("tehsil_id"),
            "output_dir": summary["output_dir"],
        }
        for summary in tehsil_summaries
    ]
    write_json(root_metadata_dir / "selected_tehsils.json", selected_tehsils)
    write_json(root_metadata_dir / "selected_hierarchy.json", hierarchy_from_locations(locations))
    write_json(root_metadata_dir / "requested_scope_location.json", root_location)

    aggregates: dict[str, Any] = {
        "selected_tehsil_count": len(tehsil_summaries),
    }

    if "active_locations" in streams:
        write_json(root_metadata_dir / "active_locations_aggregated.json", hierarchy_from_locations(locations))

    if {"layer_catalog", "layers"} & streams:
        layer_catalogs: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
        for summary in tehsil_summaries:
            metadata_dir = Path(summary["output_dir"]) / "metadata"
            payload = read_json_if_exists(metadata_dir / "selected_layer_urls.json")
            if payload is None:
                payload = read_json_if_exists(metadata_dir / "generated_layer_urls.json")
            if isinstance(payload, list):
                layer_catalogs.append((summary["location"], payload))
        if layer_catalogs:
            merged_layers, layer_sources, duplicate_layers = merge_layer_catalogs(layer_catalogs)
            write_json(root_metadata_dir / "generated_layer_urls_aggregated.json", merged_layers)
            write_json(root_metadata_dir / "generated_layer_url_sources.json", layer_sources)
            aggregates["unique_layer_count"] = len(merged_layers)
            aggregates["duplicate_layer_count"] = duplicate_layers

    if "tehsil_data" in streams:
        tehsil_data_entries: list[dict[str, Any]] = []
        for summary in tehsil_summaries:
            payload = read_json_if_exists(Path(summary["output_dir"]) / "metadata" / "tehsil_data.json")
            if payload is None:
                continue
            tehsil_data_entries.append(
                {
                    "location": summary["location"],
                    "data": payload,
                }
            )
        if tehsil_data_entries:
            write_json(root_metadata_dir / "tehsil_data_aggregated.json", tehsil_data_entries)
            aggregates["tehsil_data_count"] = len(tehsil_data_entries)

    if "village_geometries" in streams:
        village_collections: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for summary in tehsil_summaries:
            payload = read_json_if_exists(Path(summary["output_dir"]) / "metadata" / "village_geometries.json")
            if isinstance(payload, dict):
                village_collections.append((summary["location"], payload))
        if village_collections:
            merged_villages, village_sources, duplicate_villages = merge_feature_collections(
                village_collections,
                preferred_keys=("vill_ID", "village_id", "id", "ID"),
            )
            write_json(root_metadata_dir / "village_geometries_aggregated.geojson", merged_villages)
            write_json(root_metadata_dir / "village_geometry_sources.json", village_sources)
            aggregates["unique_village_feature_count"] = len(merged_villages["features"])
            aggregates["duplicate_village_feature_count"] = duplicate_villages

    if {"mws_geometries", "mws_data", "mws_kyl", "mws_report"} & streams:
        mws_collections: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for summary in tehsil_summaries:
            payload = read_json_if_exists(Path(summary["output_dir"]) / "metadata" / "mws_geometries.json")
            if isinstance(payload, dict):
                mws_collections.append((summary["location"], payload))
        if mws_collections:
            merged_mws, mws_sources, duplicate_mws = merge_feature_collections(
                mws_collections,
                preferred_keys=("uid", "mws_id", "id", "ID"),
            )
            write_json(root_metadata_dir / "mws_geometries_aggregated.geojson", merged_mws)
            write_json(root_metadata_dir / "mws_geometry_sources.json", mws_sources)
            aggregates["unique_mws_feature_count"] = len(merged_mws["features"])
            aggregates["duplicate_mws_feature_count"] = duplicate_mws

        payload_index = collect_mws_payload_index(tehsil_summaries)
        if payload_index["items"]:
            write_json(root_metadata_dir / "mws_payload_index.json", payload_index)
            aggregates["unique_mws_payload_count"] = len(payload_index["items"])

    write_json(root_metadata_dir / "aggregation_summary.json", aggregates)
    return aggregates


def resolve_filter_candidate(
    *,
    query: str,
    candidates: list[str],
    label: str,
    strict_location_match: bool,
) -> tuple[str, str | None]:
    exact = next(
        (
            candidate
            for candidate in candidates
            if normalize_match_text(candidate) == normalize_match_text(query)
        ),
        None,
    )
    if exact is not None:
        return exact, None

    resolution = resolve_best_candidate(
        query,
        candidates,
        limit=3,
        auto_accept_score=FILTER_FUZZY_AUTO_ACCEPT_SCORE,
        min_margin=FILTER_FUZZY_MIN_MARGIN,
    )
    if not strict_location_match and resolution.accepted and resolution.best_match is not None:
        matched = resolution.best_match.candidate
        note = (
            f"Auto-resolved {label} {query!r} -> {matched!r} "
            f"(score {resolution.best_match.score:.3f}, margin {resolution.margin:.3f})"
        )
        return matched, note

    raise PublicAPIError(
        "\n".join(
            [
                f"No active {label} matched {query!r}.",
                "Strict matching is enabled." if strict_location_match else "The best fuzzy match was not confident enough to auto-select.",
                format_candidate_suggestions(f"Closest {label}s", resolution.alternatives),
            ]
        )
    )


def subset_active_locations(
    active_locations: list[dict[str, Any]],
    *,
    state: str | None,
    district: str | None,
    strict_location_match: bool = False,
) -> tuple[list[dict[str, Any]], list[str]]:
    notes: list[str] = []
    if district and not state:
        raise PublicAPIError(
            "District filtering needs --state too so the hierarchy can be narrowed safely."
        )
    if not state:
        return active_locations, notes

    state_names = [str(item.get("label", "")).strip() for item in active_locations if str(item.get("label", "")).strip()]
    matched_state, state_note = resolve_filter_candidate(
        query=state,
        candidates=state_names,
        label="state",
        strict_location_match=strict_location_match,
    )
    if state_note:
        notes.append(state_note)

    filtered_states = [
        item
        for item in active_locations
        if normalize_match_text(item.get("label", "")) == normalize_match_text(matched_state)
    ]

    if not district:
        return filtered_states, notes

    state_item = filtered_states[0]
    districts = state_item.get("district", [])
    district_names = [str(item.get("label", "")).strip() for item in districts if str(item.get("label", "")).strip()]
    matched_district, district_note = resolve_filter_candidate(
        query=district,
        candidates=district_names,
        label="district",
        strict_location_match=strict_location_match,
    )
    if district_note:
        notes.append(
            f"{district_note} inside state {state_item.get('label')!r}"
        )

    filtered_districts = [
        item
        for item in districts
        if normalize_match_text(item.get("label", "")) == normalize_match_text(matched_district)
    ]

    return [{**state_item, "district": filtered_districts}], notes


def iter_tehsil_targets_from_subset(subset: list[dict[str, Any]]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for state in subset:
        state_label = str(state.get("label", "")).strip()
        state_id = str(state.get("state_id", "")).strip() or None
        for district in state.get("district", []):
            district_label = str(district.get("label", "")).strip()
            district_id = str(district.get("district_id", "")).strip() or None
            for block in district.get("blocks", []):
                tehsil_label = str(block.get("label", "")).strip()
                tehsil_id = (
                    str(block.get("tehsil_id", "")).strip()
                    or str(block.get("block_id", "")).strip()
                    or None
                )
                targets.append(
                    {
                        "state": state_label,
                        "district": district_label,
                        "tehsil": tehsil_label,
                        "state_id": state_id,
                        "district_id": district_id,
                        "tehsil_id": tehsil_id,
                        "resolved_via": "active_locations_scope",
                    }
                )
    return targets


def bulk_child_output_dir(root_output_dir: Path, scope: str, location: dict[str, Any]) -> Path:
    if scope == "district":
        return root_output_dir / sanitize_slug(location["tehsil"])
    if scope == "state":
        return (
            root_output_dir
            / sanitize_slug(location["district"])
            / sanitize_slug(location["tehsil"])
        )
    return root_output_dir


def resolve_download_plan(
    args: argparse.Namespace,
    *,
    api_key: str,
    base_url: str,
    active_locations: list[dict[str, Any]] | None,
) -> DownloadPlan:
    if (
        getattr(args, "mws_id", None)
        and not args.tehsil
        and args.latitude is None
        and args.longitude is None
    ):
        raise PublicAPIError(
            "--mws-id needs a single-tehsil scope. Use --state/--district/--tehsil or "
            "--latitude/--longitude, not a whole district or state."
        )

    if args.latitude is not None or args.longitude is not None:
        location = resolve_download_location(
            args,
            api_key=api_key,
            base_url=base_url,
            active_locations=active_locations,
        )
        return DownloadPlan(
            scope="tehsil",
            root_location=location,
            tehsil_targets=[location],
            notes=[],
        )

    if args.tehsil:
        if not args.state or not args.district:
            raise PublicAPIError(
                "Tehsil downloads need --state and --district too. "
                "Use --state/--district/--tehsil together, or use --latitude/--longitude."
            )
        location = resolve_download_location(
            args,
            api_key=api_key,
            base_url=base_url,
            active_locations=active_locations,
        )
        return DownloadPlan(
            scope="tehsil",
            root_location=location,
            tehsil_targets=[location],
            notes=[],
        )

    if args.district and not args.state:
        raise PublicAPIError(
            "District-level downloads need --state too so the hierarchy can be expanded safely."
        )

    if args.state and args.district:
        if active_locations is None:
            raise PublicAPIError("Activated locations are required for district-level expansion.")
        subset, notes = subset_active_locations(
            active_locations,
            state=args.state,
            district=args.district,
            strict_location_match=args.strict_location_match,
        )
        targets = maybe_limit(iter_tehsil_targets_from_subset(subset), args.tehsil_limit)
        if not targets:
            raise PublicAPIError("No activated tehsils were found inside the selected district.")
        state_item = subset[0]
        district_item = state_item.get("district", [])[0]
        root_location = {
            "state": state_item.get("label"),
            "district": district_item.get("label"),
            "resolved_via": "active_locations_scope",
            "scope": "district",
            "tehsil_count": len(targets),
            "resolution_notes": notes,
        }
        return DownloadPlan(
            scope="district",
            root_location=root_location,
            tehsil_targets=targets,
            notes=notes,
        )

    if args.state:
        if active_locations is None:
            raise PublicAPIError("Activated locations are required for state-level expansion.")
        subset, notes = subset_active_locations(
            active_locations,
            state=args.state,
            district=None,
            strict_location_match=args.strict_location_match,
        )
        targets = maybe_limit(iter_tehsil_targets_from_subset(subset), args.tehsil_limit)
        if not targets:
            raise PublicAPIError("No activated tehsils were found inside the selected state.")
        state_item = subset[0]
        root_location = {
            "state": state_item.get("label"),
            "resolved_via": "active_locations_scope",
            "scope": "state",
            "district_count": len(state_item.get("district", [])),
            "tehsil_count": len(targets),
            "resolution_notes": notes,
        }
        return DownloadPlan(
            scope="state",
            root_location=root_location,
            tehsil_targets=targets,
            notes=notes,
        )

    raise PublicAPIError(
        "Provide --state, --state/--district, --state/--district/--tehsil, or --latitude/--longitude."
    )


def resolve_smoke_test_target(
    args: argparse.Namespace,
    *,
    api_key: str,
    base_url: str,
    active_locations: list[dict[str, Any]],
) -> tuple[dict[str, Any], str, list[str]]:
    if (
        not args.state
        and not args.district
        and not args.tehsil
        and args.latitude is None
        and args.longitude is None
    ):
        return (
            {
                "state": DEFAULT_SAMPLE_STATE,
                "district": DEFAULT_SAMPLE_DISTRICT,
                "tehsil": DEFAULT_SAMPLE_TEHSIL,
                "resolved_via": "default_sample",
            },
            "tehsil",
            [],
        )

    if args.latitude is not None or args.longitude is not None:
        return resolve_location(args, api_key=api_key, base_url=base_url), "point", []

    if args.state and args.district and args.tehsil:
        return (
            resolve_download_location(
                args,
                api_key=api_key,
                base_url=base_url,
                active_locations=active_locations,
            ),
            "tehsil",
            [],
        )

    scope_args = argparse.Namespace(**vars(args))
    scope_args.tehsil_limit = None
    plan = resolve_download_plan(
        scope_args,
        api_key=api_key,
        base_url=base_url,
        active_locations=active_locations,
    )
    selected = plan.tehsil_targets[0]
    notes = list(plan.notes)
    notes.append(
        "Smoke-test selected the first activated tehsil in this "
        f"{plan.scope}: {selected['state']} / {selected['district']} / {selected['tehsil']} "
        f"(from {len(plan.tehsil_targets)} activated tehsil(s))."
    )
    return selected, plan.scope, notes


def print_active_locations_text(active_locations: list[dict[str, Any]]) -> None:
    for state in active_locations:
        districts = state.get("district", [])
        print(f"{state.get('label')} ({len(districts)} districts)")
        for district in districts:
            tehsils = district.get("blocks", [])
            print(f"  - {district.get('label')} ({len(tehsils)} tehsils)")
            for tehsil in tehsils:
                print(f"    - {tehsil.get('label')}")


def run_locations(args: argparse.Namespace) -> int:
    api_key, base_url = resolve_runtime_config(
        env_file=args.env_file,
        api_key=args.api_key,
        base_url=args.base_url,
    )
    active_locations = load_active_locations_catalog(
        base_url=base_url,
        api_key=api_key,
        active_locations_file=args.active_locations_file,
        refresh=args.refresh_active_locations,
        timeout=args.timeout,
    )
    subset, notes = subset_active_locations(
        active_locations,
        state=args.state,
        district=args.district,
        strict_location_match=args.strict_location_match,
    )

    payload = {
        "summary": summary_for_active_locations(subset),
        "items": subset,
        "notes": notes,
    }

    if args.output:
        write_json(Path(args.output).resolve(), payload)

    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        for note in notes:
            print(note)
        print_active_locations_text(subset)
    return 0


def run_resolve(args: argparse.Namespace) -> int:
    api_key, base_url = resolve_runtime_config(
        env_file=args.env_file,
        api_key=args.api_key,
        base_url=args.base_url,
    )

    if args.latitude is not None and args.longitude is not None:
        resolved = resolve_location(args, api_key=api_key, base_url=base_url)
        if args.include_mws_id:
            point_payload = request_optional_json(
                base_url=base_url,
                endpoint="get_mwsid_by_latlon",
                api_key=api_key,
                params={"latitude": args.latitude, "longitude": args.longitude},
                timeout=args.timeout,
            )
            if point_payload is not None:
                resolved["mws_id"] = extract_point_mws_id(point_payload)
        print(json.dumps(resolved, indent=2))
        return 0

    if not (args.state and args.district and args.tehsil):
        raise PublicAPIError(
            "resolve requires either --latitude/--longitude or all of --state, --district, and --tehsil."
        )

    active_locations = load_active_locations_catalog(
        base_url=base_url,
        api_key=api_key,
        active_locations_file=args.active_locations_file,
        refresh=args.refresh_active_locations,
        timeout=args.timeout,
    )
    resolution = resolve_active_location(
        active_locations=active_locations,
        state=args.state,
        district=args.district,
        tehsil=args.tehsil,
        strict_location_match=args.strict_location_match,
    )
    payload = {
        **asdict(resolution.path),
        "resolved_via": resolution.matched_via,
        "match_score": round(resolution.score, 6),
        "match_margin": round(resolution.margin, 6),
    }
    if resolution.matched_via == "active_locations_fuzzy":
        payload["original_input"] = {
            "state": args.state,
            "district": args.district,
            "tehsil": args.tehsil,
        }
    print(json.dumps(payload, indent=2))
    return 0


def run_smoke_test(args: argparse.Namespace) -> int:
    api_key, base_url = resolve_runtime_config(
        env_file=args.env_file,
        api_key=args.api_key,
        base_url=args.base_url,
    )
    active_locations = load_active_locations_catalog(
        base_url=base_url,
        api_key=api_key,
        active_locations_file=args.active_locations_file,
        refresh=args.refresh_active_locations,
        timeout=args.timeout,
    )
    location, requested_scope, notes = resolve_smoke_test_target(
        args,
        api_key=api_key,
        base_url=base_url,
        active_locations=active_locations,
    )
    layers = request_json(
        base_url=base_url,
        endpoint="get_generated_layer_urls",
        api_key=api_key,
        params=location_query(location),
        timeout=args.timeout,
    )
    mws_geometries = request_json(
        base_url=base_url,
        endpoint="get_mws_geometries",
        api_key=api_key,
        params=location_query(location),
        timeout=args.timeout,
    )

    summary = {
        "base_url": base_url,
        "requested_scope": requested_scope,
        "location": location,
        "layer_count": len(layers) if isinstance(layers, list) else 0,
        "sample_layers": [layer.get("layer_name") for layer in layers[:5]],
        "mws_feature_count": len(mws_geometries.get("features", [])),
        "sample_mws_ids": extract_mws_ids(mws_geometries)[:5],
        "notes": notes,
    }

    if args.output:
        write_json(Path(args.output).resolve(), summary)
    print(json.dumps(summary, indent=2))
    return 0


def download_for_tehsil_target(
    args: argparse.Namespace,
    *,
    api_key: str,
    base_url: str,
    active_locations: list[dict[str, Any]],
    location: dict[str, Any],
    streams: set[str],
    output_dir: Path | None = None,
) -> dict[str, Any]:
    output_dir = output_dir or output_directory_for(
        explicit_output_dir=args.output_dir,
        location=location,
        streams=streams,
    )
    metadata_dir = ensure_directory(output_dir / "metadata")
    layers_dir = ensure_directory(output_dir / "layers")
    mws_dir = ensure_directory(output_dir / "mws")

    write_json(metadata_dir / "active_locations_catalog_summary.json", summary_for_active_locations(active_locations))
    write_json(metadata_dir / "resolved_location.json", {"base_url": base_url, **location})
    print(f"Resolved location: {location['state']} / {location['district']} / {location['tehsil']}")
    if location.get("resolved_via") == "active_locations_fuzzy":
        print(
            "Auto-resolved input hierarchy "
            f"{location['input_state']} / {location['input_district']} / {location['input_tehsil']} "
            f"-> {location['state']} / {location['district']} / {location['tehsil']} "
            f"(score {location['match_score']:.3f}, margin {location['match_margin']:.3f})"
        )
    print(f"Using public API base URL: {base_url}")
    print(f"Writing outputs under: {output_dir}")

    query = location_query(location)
    point_lookup_payload: dict[str, Any] | None = None
    point_mws_id: str | None = None
    if location.get("resolved_via") == "latlon" and (
        "point_lookup" in streams or {"mws_data", "mws_kyl", "mws_report"} & streams
    ):
        point_lookup_payload = request_optional_json(
            base_url=base_url,
            endpoint="get_mwsid_by_latlon",
            api_key=api_key,
            params={"latitude": location["latitude"], "longitude": location["longitude"]},
            timeout=args.timeout,
        )
        if point_lookup_payload is not None:
            point_mws_id = extract_point_mws_id(point_lookup_payload)
            write_json(metadata_dir / "point_mws_lookup.json", point_lookup_payload)

    if "active_locations" in streams:
        subset, notes = subset_active_locations(
            active_locations,
            state=location["state"],
            district=location["district"],
            strict_location_match=args.strict_location_match,
        )
        write_json(metadata_dir / "active_locations.json", subset)
        if notes:
            write_json(metadata_dir / "active_locations_filter_notes.json", notes)

    generated_layers: list[dict[str, Any]] = []
    if {"layer_catalog", "layers"} & streams:
        generated_layers = request_json(
            base_url=base_url,
            endpoint="get_generated_layer_urls",
            api_key=api_key,
            params=query,
            timeout=args.timeout,
        )
        write_json(metadata_dir / "generated_layer_urls.json", generated_layers)
        selected_layers = filter_layers_by_type(generated_layers, args.layer_types)
        if args.layer_types:
            write_json(metadata_dir / "selected_layer_urls.json", selected_layers)
        generated_layers = selected_layers
        print(f"Discovered {len(generated_layers)} selected generated layers.")

        if not args.metadata_only and "layers" in streams:
            layer_failures: list[dict[str, str]] = []
            for index, layer in enumerate(maybe_limit(generated_layers, args.layer_limit), start=1):
                layer_name = str(layer.get("layer_name", f"layer_{index}"))
                extension = infer_layer_extension(layer)
                destination = layers_dir / f"{index:03d}_{sanitize_slug(layer_name)}.{extension}"
                try:
                    download_to_file(str(layer["layer_url"]), destination, timeout=args.timeout)
                    print(f"Downloaded layer {index}: {layer_name}")
                except Exception as exc:  # noqa: BLE001
                    layer_failures.append({"layer_name": layer_name, "error": str(exc)})
                    print(f"Layer download failed for {layer_name}: {exc}", file=sys.stderr)

            if layer_failures:
                write_json(metadata_dir / "layer_download_failures.json", layer_failures)

    if "tehsil_data" in streams:
        tehsil_data = request_optional_json(
            base_url=base_url,
            endpoint="get_tehsil_data",
            api_key=api_key,
            params=query,
            timeout=args.timeout,
        )
        if tehsil_data is not None:
            write_json(metadata_dir / "tehsil_data.json", tehsil_data)

    village_geometries: dict[str, Any] | None = None
    if "village_geometries" in streams:
        village_geometries = request_optional_json(
            base_url=base_url,
            endpoint="get_village_geometries",
            api_key=api_key,
            params=query,
            timeout=args.timeout,
        )
        if village_geometries is not None:
            write_json(metadata_dir / "village_geometries.json", village_geometries)
            if args.village_id or args.village_name:
                selected_villages = filter_village_geometries(
                    village_geometries,
                    village_id=args.village_id,
                    village_name=args.village_name,
                    strict_name_match=args.strict_location_match,
                )
                write_json(metadata_dir / "selected_village_geometries.json", selected_villages)

    mws_geometries: dict[str, Any] | None = None
    if {"mws_geometries", "mws_data", "mws_kyl", "mws_report"} & streams:
        mws_geometries = request_optional_json(
            base_url=base_url,
            endpoint="get_mws_geometries",
            api_key=api_key,
            params=query,
            timeout=args.timeout,
        )
        if mws_geometries is not None and "mws_geometries" in streams:
            write_json(metadata_dir / "mws_geometries.json", mws_geometries)

    mws_ids: list[str] = []
    if args.mws_id:
        mws_ids = [args.mws_id]
    elif point_mws_id and not args.all_mws_in_tehsil and {"mws_data", "mws_kyl", "mws_report"} & streams:
        mws_ids = [point_mws_id]
    elif mws_geometries is not None:
        mws_ids = maybe_limit(extract_mws_ids(mws_geometries), args.mws_limit)

    if {"mws_data", "mws_kyl", "mws_report"} & streams:
        print(f"Preparing MWS payloads for {len(mws_ids)} MWS ids.")
        mws_failures: list[dict[str, str]] = []
        for uid in mws_ids:
            record_dir = ensure_directory(mws_dir / sanitize_slug(uid))
            params = {**query, "mws_id": uid}

            endpoint_plan = []
            if "mws_data" in streams:
                endpoint_plan.append(("get_mws_data", "mws_data.json"))
            if "mws_kyl" in streams:
                endpoint_plan.append(("get_mws_kyl_indicators", "kyl_indicators.json"))
            if "mws_report" in streams:
                endpoint_plan.append(("get_mws_report", "report.json"))

            for endpoint, filename in endpoint_plan:
                payload = request_optional_json(
                    base_url=base_url,
                    endpoint=endpoint,
                    api_key=api_key,
                    params=params,
                    timeout=args.timeout,
                )
                if payload is None:
                    continue
                write_json(record_dir / filename, payload)

            if not any(record_dir.iterdir()):
                mws_failures.append({"mws_id": uid, "error": "No MWS payloads were returned."})

        if mws_failures:
            write_json(metadata_dir / "mws_download_failures.json", mws_failures)

    summary = {
        "base_url": base_url,
        "output_dir": str(output_dir),
        "location": location,
        "datasets": sorted(streams),
        "streams": sorted(streams),
        "layer_count": len(generated_layers),
        "mws_count": len(mws_ids),
        "metadata_only": args.metadata_only,
    }
    write_json(metadata_dir / "download_summary.json", summary)
    return summary


def run_download(args: argparse.Namespace) -> int:
    api_key, base_url = resolve_runtime_config(
        env_file=args.env_file,
        api_key=args.api_key,
        base_url=args.base_url,
    )
    streams = expand_requested_datasets(
        raw_datasets=args.datasets,
        raw_streams=args.streams,
        bundle=args.bundle,
    )
    active_locations = load_active_locations_catalog(
        base_url=base_url,
        api_key=api_key,
        active_locations_file=args.active_locations_file,
        refresh=args.refresh_active_locations,
        timeout=args.timeout,
    )

    if streams == {"active_locations"} and not (
        args.state or args.district or args.tehsil or args.latitude is not None or args.longitude is not None
    ):
        output_dir = output_directory_for(explicit_output_dir=args.output_dir, location=None, streams=streams)
        write_json(output_dir / "active_locations.json", active_locations)
        write_json(output_dir / "summary.json", {"summary": summary_for_active_locations(active_locations), "notes": []})
        print(f"Wrote active locations under {output_dir}")
        return 0

    if streams == {"active_locations"} and (args.state or args.district):
        subset, notes = subset_active_locations(
            active_locations,
            state=args.state,
            district=args.district,
            strict_location_match=args.strict_location_match,
        )
        output_dir = output_directory_for(
            explicit_output_dir=args.output_dir,
            location={
                "state": subset[0].get("label") if subset else args.state,
                "district": subset[0].get("district", [{}])[0].get("label") if args.district and subset else None,
            },
            streams=streams,
        )
        write_json(output_dir / "active_locations.json", subset)
        write_json(output_dir / "summary.json", {"summary": summary_for_active_locations(subset), "notes": notes})
        for note in notes:
            print(note)
        print(f"Wrote active locations under {output_dir}")
        return 0

    plan = resolve_download_plan(
        args,
        api_key=api_key,
        base_url=base_url,
        active_locations=active_locations,
    )

    if plan.scope == "tehsil":
        summary = download_for_tehsil_target(
            args,
            api_key=api_key,
            base_url=base_url,
            active_locations=active_locations,
            location=plan.tehsil_targets[0],
            streams=streams,
        )
        print(json.dumps(summary, indent=2))
        return 0

    root_output_dir = output_directory_for(
        explicit_output_dir=args.output_dir,
        location=plan.root_location,
        streams=streams,
    )
    root_metadata_dir = ensure_directory(root_output_dir / "metadata")

    print(
        f"Resolved {plan.scope}: "
        + " / ".join(
            part
            for part in [
                str(plan.root_location.get("state", "")) if plan.root_location else "",
                str(plan.root_location.get("district", "")) if plan.root_location else "",
            ]
            if part
        )
    )
    for note in plan.notes:
        print(note)
    print(
        f"Expanding {plan.scope}-level request into {len(plan.tehsil_targets)} activated tehsil downloads."
    )
    if len(plan.tehsil_targets) <= 10:
        print(
            "Activated tehsils selected: "
            + ", ".join(target["tehsil"] for target in plan.tehsil_targets)
        )
    print(f"Writing bulk outputs under: {root_output_dir}")

    tehsil_summaries: list[dict[str, Any]] = []
    for index, target in enumerate(plan.tehsil_targets, start=1):
        print(
            f"[{index}/{len(plan.tehsil_targets)}] "
            f"{target['state']} / {target['district']} / {target['tehsil']}"
        )
        child_output_dir = bulk_child_output_dir(root_output_dir, plan.scope, target)
        summary = download_for_tehsil_target(
            args,
            api_key=api_key,
            base_url=base_url,
            active_locations=active_locations,
            location=target,
            streams=streams,
            output_dir=child_output_dir,
        )
        tehsil_summaries.append(summary)

    aggregates = aggregate_bulk_download_outputs(
        root_output_dir=root_output_dir,
        root_location=plan.root_location or {},
        tehsil_summaries=tehsil_summaries,
        streams=streams,
    )
    bulk_summary = {
        "base_url": base_url,
        "scope": plan.scope,
        "output_dir": str(root_output_dir),
        "location": plan.root_location,
        "datasets": sorted(streams),
        "streams": sorted(streams),
        "tehsil_count": len(plan.tehsil_targets),
        "notes": plan.notes,
        "aggregates": aggregates,
        "tehsils": [
            {
                "state": summary["location"]["state"],
                "district": summary["location"]["district"],
                "tehsil": summary["location"]["tehsil"],
                "output_dir": summary["output_dir"],
                "layer_count": summary["layer_count"],
                "mws_count": summary["mws_count"],
            }
            for summary in tehsil_summaries
        ],
    }
    write_json(root_metadata_dir / "bulk_download_summary.json", bulk_summary)
    print(json.dumps(bulk_summary, indent=2))
    return 0


def add_location_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state", help="State name")
    parser.add_argument("--district", help="District name")
    parser.add_argument("--tehsil", help="Tehsil/block name")
    parser.add_argument("--latitude", type=float, help="Latitude for admin lookup")
    parser.add_argument("--longitude", type=float, help="Longitude for admin lookup")


def add_matching_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strict-location-match",
        action="store_true",
        help="Require exact canonical names instead of auto-accepting high-confidence fuzzy matches",
    )


def common_help_epilog() -> str:
    return textwrap.dedent(
        """
        Tips:
          - This client is standard-library only. No conda environment or pip install is required.
          - If the optional hierarchy matcher module is unavailable, the client uses an internal fallback matcher.
          - Register or sign in at https://dashboard.core-stack.org/ if you don't have an API key yet.
          - `locations` shows the activated hierarchy and helps validate spellings.
          - `resolve` returns canonical names or closest matches before you download.
          - `smoke-test` is intentionally simple and is the command used by the installer.
          - `download` supports tehsil-, district-, and state-level bulk expansion by activated tehsils.
          - Prefer `--datasets ...` for explicit dataset names or `--bundle watersheds` for common presets.
          - Scope guide: `--state` = all tehsils in a state, `--state --district` = all tehsils in a district,
            `--state --district --tehsil` = one tehsil, and `--latitude --longitude` = resolve by point.
          - Bundle guide: `metadata` = catalogs and tables, `layers` = downloadable layer files and geometry,
            `watersheds` = MWS-focused payloads, `full` = the default package.
          - Use `--layer-types raster` or `--layer-types vector,point` to filter layer downloads.
          - Use `--allow-unlisted-location` only when you want to bypass active-location validation.
          - Bash line continuations need `\\` as the last character on the line with no trailing spaces.
        """
    ).strip()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke-test, inspect, resolve, and bulk-download CoRE Stack public API data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=common_help_epilog(),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "nrm_app" / ".env",
        # help="Path to the .env file containing PUBLIC_API_X_API_KEY and PUBLIC_API_BASE_URL",
        help="""Path to the .env file containing PUBLIC_API_X_API_KEY and PUBLIC_API_BASE_URL.
        Register or sign in at https://dashboard.core-stack.org/ if you don't have an API key yet."""        
    )
    parser.add_argument("--api-key", help="Override PUBLIC_API_X_API_KEY")
    parser.add_argument("--base-url", help="Override PUBLIC_API_BASE_URL")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout in seconds")
    parser.add_argument(
        "--active-locations-file",
        type=Path,
        default=DEFAULT_ACTIVE_LOCATIONS_FILE,
        help="Path to the activated-locations cache JSON",
    )
    parser.add_argument(
        "--refresh-active-locations",
        action="store_true",
        help="Refresh data/activated_locations/active_locations.json from the public API before running",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    smoke_parser = subparsers.add_parser(
        "smoke-test",
        help="Run a minimal smoke test against two public APIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              python installation/public_api_client.py smoke-test
              python installation/public_api_client.py smoke-test --state assam --district cachar --tehsil lakhipur
              python installation/public_api_client.py smoke-test --state assam --district cachar
              python installation/public_api_client.py smoke-test --state assam
              python installation/public_api_client.py smoke-test --latitude 24.7387 --longitude 86.3041
            """
        ).strip(),
    )
    add_location_arguments(smoke_parser)
    add_matching_arguments(smoke_parser)
    smoke_parser.add_argument("--output", help="Optional path to write the smoke-test summary JSON")
    smoke_parser.set_defaults(allow_unlisted_location=False, tehsil_limit=None, mws_id=None)
    smoke_parser.set_defaults(handler=run_smoke_test)

    locations_parser = subparsers.add_parser(
        "locations",
        help="Inspect activated public locations by state, district, and tehsil",
    )
    locations_parser.add_argument("--state", help="Optional state filter")
    locations_parser.add_argument("--district", help="Optional district filter within the matched state")
    add_matching_arguments(locations_parser)
    locations_parser.add_argument("--format", choices=("text", "json"), default="text")
    locations_parser.add_argument("--output", help="Optional JSON output file")
    locations_parser.set_defaults(handler=run_locations)

    resolve_parser = subparsers.add_parser(
        "resolve",
        help="Resolve canonical state/district/tehsil names or a point lookup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              python installation/public_api_client.py resolve --state bihar --district jamui --tehsil jamui
              python installation/public_api_client.py resolve --state bihar --district jamu --tehsil jamu
              python installation/public_api_client.py resolve --latitude 24.7387 --longitude 86.3041 --include-mws-id
            """
        ).strip(),
    )
    add_location_arguments(resolve_parser)
    add_matching_arguments(resolve_parser)
    resolve_parser.add_argument(
        "--include-mws-id",
        action="store_true",
        help="When resolving from lat/lon, also call get_mwsid_by_latlon",
    )
    resolve_parser.set_defaults(handler=run_resolve)

    download_parser = subparsers.add_parser(
        "download",
        help="Download public datasets for tehsil, village, and MWS workflows",
        description=textwrap.dedent(
            """
            Download data at one of four scopes:

              - tehsil: `--state --district --tehsil`
              - district: `--state --district`
              - state: `--state`
              - point lookup: `--latitude --longitude`

            Use `--datasets` for precise dataset selection or `--bundle` for a simpler preset:

              - metadata: active locations, tehsil analytics, generated layer catalog
              - layers: layer catalog, downloadable layers, village geometries, MWS geometries
              - watersheds: MWS geometries, MWS data, KYL indicators, reports
              - full: the default tehsil-oriented package
            """
        ).strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              python installation/public_api_client.py download --state assam --district cachar --tehsil lakhipur
              python installation/public_api_client.py download --state assam --district cachar
              python installation/public_api_client.py download --state assam
              python installation/public_api_client.py download --state assam --district cachar --tehsil lakhipur --datasets layer_catalog,layers --layer-types raster
              python installation/public_api_client.py download --latitude 24.7387 --longitude 86.3041 --bundle watersheds
              python installation/public_api_client.py download --state assam --district cachar --tehsil lakhipur --datasets village_geometries --village-name Lakhipur
              python installation/public_api_client.py download --datasets active_locations --state assam
            """
        ).strip(),
    )
    add_location_arguments(download_parser)
    add_matching_arguments(download_parser)
    download_parser.add_argument("--output-dir", help="Directory to write downloaded files into")
    download_parser.add_argument(
        "--datasets",
        help=(
            "Comma-separated dataset names. Supported values: all, active_locations, point_lookup, "
            "tehsil_data, layer_catalog, layers, village_geometries, mws_geometries, "
            "mws_data, mws_kyl, mws_report"
        ),
    )
    download_parser.add_argument(
        "--bundle",
        choices=tuple(sorted(DATASET_BUNDLES)),
        help="Named dataset preset: metadata, layers, watersheds, or full",
    )
    download_parser.add_argument(
        "--streams",
        help="Legacy alias for --datasets",
    )
    download_parser.add_argument(
        "--layer-types",
        help="Filter layer downloads/catalog entries by type: raster, vector, point, or all",
    )
    download_parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Save JSON metadata but skip binary layer downloads",
    )
    download_parser.add_argument(
        "--layer-limit",
        type=int,
        help="Only download the first N selected layers",
    )
    download_parser.add_argument(
        "--mws-limit",
        type=int,
        help="Only fetch the first N MWS ids when expanding from tehsil geometries",
    )
    download_parser.add_argument(
        "--tehsil-limit",
        type=int,
        help="When downloading a district or state in bulk, only process the first N activated tehsils",
    )
    download_parser.add_argument("--mws-id", help="Fetch only one specific MWS id")
    download_parser.add_argument(
        "--all-mws-in-tehsil",
        action="store_true",
        help="When lat/lon is provided, fetch all tehsil MWS ids instead of only the point MWS id",
    )
    download_parser.add_argument("--village-id", help="Filter village geometries to one vill_ID")
    download_parser.add_argument("--village-name", help="Filter village geometries to one village name")
    download_parser.add_argument(
        "--allow-unlisted-location",
        action="store_true",
        help="Bypass activated-location validation when the hierarchy cache does not contain the requested location",
    )
    download_parser.set_defaults(handler=run_download)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except PublicAPIError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
