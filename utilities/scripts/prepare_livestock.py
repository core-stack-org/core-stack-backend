"""Prepare 20th Livestock Census rows with LGD village/ward identifiers.

The preferred source is the ART Park/IITM all-India CSV, which already carries
LGD-like state and district IDs. This script uses those district IDs as the
primary rural anchor, resolves subdistrict and village IDs from the GP mapping,
and resolves urban ward IDs from the urban local body ward mapping.

Resolution is intentionally staged:

1. exact unique keys inside the district/town anchor;
2. relaxed exact keys for census suffix noise such as ``(CT)``, ``(RV)``, roman
   numerals, and ward-number formatting;
3. explicitly-labelled state-level unique fallbacks for likely boundary-version
   changes;
4. fuzzy scoring only after signature-based candidate narrowing.

The expensive similarity function is reused from ``admin_resolve.py`` but is
only applied to tiny candidate sets, not all possible village/ward pairs.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from difflib import SequenceMatcher
import json
from pathlib import Path
import re
import sys
import time
from typing import Iterable, Iterator, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utilities.scripts.admin_resolve import (  # noqa: E402
    clean_text,
    compact_match_text,
    consonant_signature,
    normalize_match_text,
    score_candidate,
    soundex_code,
)


DEFAULT_LIVESTOCK_DIR = REPO_ROOT / "data" / "livestock"
DEFAULT_SOURCE = DEFAULT_LIVESTOCK_DIR / "all-india-20th-livestock-census-artpark-iitm.csv"
DEFAULT_GP_MAPPING = DEFAULT_LIVESTOCK_DIR / "gp_mapping.01Apr2026.csv"
DEFAULT_URBAN_WARDS = DEFAULT_LIVESTOCK_DIR / "urban_local_body_wards.25May2026.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_LIVESTOCK_DIR / "processed"

SPECIES = ("cattle", "buffalo", "sheep", "goat", "pig")
LOCATION_TYPES = ("rural", "urban")


@dataclass(frozen=True)
class RuralReference:
    ref_id: int
    state_code: int
    state_name: str
    district_code: int
    district_name: str
    district_census2011_code: str
    subdistrict_code: int
    subdistrict_name: str
    subdistrict_census2011_code: str
    village_code: int
    village_name: str
    village_census2011_code: str
    local_body_code: int | None
    local_body_name: str
    village_norm: str
    village_relaxed_norm: str
    subdistrict_norm: str
    signatures: tuple[str, ...]


@dataclass(frozen=True)
class UrbanReference:
    ref_id: int
    state_code: int
    state_name: str
    local_body_code: int
    local_body_name: str
    ward_code: int
    ward_number: str
    ward_name: str
    local_body_norm: str
    ward_norm: str
    ward_relaxed_norm: str
    signatures: tuple[str, ...]


@dataclass(frozen=True)
class Match:
    location_scope: str
    method: str
    score: float
    margin: float
    candidate_count: int
    rural: RuralReference | None = None
    urban: UrbanReference | None = None


UniqueValue = RuralReference | UrbanReference | None


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def as_repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def parse_int(value: object) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    if text.endswith(".0") and re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]
    match = re.search(r"(-?\d+)$", text)
    if not match:
        return None
    return int(match.group(1))


def parse_count(value: object) -> int:
    return parse_int(value) or 0


def normalize_text(value: object) -> str:
    return normalize_match_text(clean_text(value) or "")


def roman_to_int_token(token: str) -> str:
    roman = {
        "i": "1",
        "ii": "2",
        "iii": "3",
        "iv": "4",
        "v": "5",
        "vi": "6",
        "vii": "7",
        "viii": "8",
        "ix": "9",
        "x": "10",
        "xi": "11",
        "xii": "12",
        "xiii": "13",
        "xiv": "14",
        "xv": "15",
        "xvi": "16",
        "xvii": "17",
        "xviii": "18",
        "xix": "19",
        "xx": "20",
        "xxi": "21",
        "xxii": "22",
        "xxiii": "23",
        "xxiv": "24",
        "xxv": "25",
        "xxvi": "26",
        "xxvii": "27",
        "xxviii": "28",
        "xxix": "29",
        "xxx": "30",
    }
    return roman.get(token, token)


def normalize_relaxed_place(value: object) -> str:
    text = clean_text(value) or ""
    text = re.sub(r"\bRural\s+MDDS\s+Code\s*:?\s*\d+\b", " ", text, flags=re.I)
    text = re.sub(r"\((?:rv|ct|og|part|rural|revenue village)\)", " ", text, flags=re.I)
    text = re.sub(r"\b(?:rv|ct|og|rural|revenue village)\b", " ", text, flags=re.I)
    normalized = normalize_text(text)
    tokens = [roman_to_int_token(token) for token in normalized.split()]
    normalized = " ".join(tokens)
    normalized = re.sub(r"\bpart\s+([0-9]+)\b", r"\1", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def extract_ward_number(*values: object) -> str:
    for value in values:
        text = clean_text(value) or ""
        patterns = (
            r"\bward\s*(?:no\.?|number)?\s*[-.:]*\s*0*(\d+)\b",
            r"\bno\.?\s*[-.:]*\s*0*(\d+)\b",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if match:
                return str(int(match.group(1)))
        parsed = parse_int(text)
        if parsed is not None:
            return str(parsed)
    return ""


def signatures(value: object) -> tuple[str, ...]:
    text = clean_text(value) or ""
    compact = compact_match_text(text)
    values = {
        compact[:3],
        compact[:4],
        soundex_code(text),
        consonant_signature(text),
    }
    values.discard("")
    return tuple(sorted(values))


def simple_similarity(left: object, right: object) -> float:
    left_norm = normalize_text(left)
    right_norm = normalize_text(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    left_compact = left_norm.replace(" ", "")
    right_compact = right_norm.replace(" ", "")
    sequence = SequenceMatcher(None, left_norm, right_norm).ratio()
    compact_sequence = SequenceMatcher(None, left_compact, right_compact).ratio()
    substring = 1.0 if left_compact in right_compact or right_compact in left_compact else 0.0
    prefix = 1.0 if left_compact[:4] and left_compact[:4] == right_compact[:4] else 0.0
    return min(
        1.0,
        (0.60 * max(sequence, compact_sequence)) + (0.22 * substring) + (0.18 * prefix),
    )


def register_unique(mapping: dict[tuple[object, ...], UniqueValue], key: tuple[object, ...], value: UniqueValue) -> None:
    if any(part in ("", None) for part in key):
        return
    existing = mapping.get(key)
    if existing is None and key in mapping:
        return
    if existing is None:
        mapping[key] = value
        return
    existing_code = unique_value_code(existing)
    new_code = unique_value_code(value)
    if existing_code != new_code:
        mapping[key] = None


def unique_value_code(value: UniqueValue) -> int | None:
    if isinstance(value, RuralReference):
        return value.village_code
    if isinstance(value, UrbanReference):
        return value.ward_code
    return None


def add_index(index: dict[tuple[object, ...], list[object]], key: tuple[object, ...], value: object) -> None:
    if any(part in ("", None) for part in key):
        return
    index.setdefault(key, []).append(value)


def iter_csv(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        yield from csv.DictReader(handle)


def load_rural_references(path: Path) -> tuple[list[RuralReference], dict[str, dict[tuple[object, ...], UniqueValue]], dict[str, dict[tuple[object, ...], list[object]]]]:
    references: list[RuralReference] = []
    unique_maps: dict[str, dict[tuple[object, ...], UniqueValue]] = {
        "rural_exact_district_subdistrict_village": {},
        "rural_exact_relaxed_district_subdistrict_village": {},
        "rural_exact_district_village_unique": {},
        "rural_exact_relaxed_district_village_unique": {},
        "rural_boundary_fallback_state_subdistrict_village_unique": {},
        "rural_boundary_fallback_state_subdistrict_relaxed_village_unique": {},
        "rural_boundary_fallback_state_village_unique": {},
        "rural_boundary_fallback_state_relaxed_village_unique": {},
    }
    indexes: dict[str, dict[tuple[object, ...], list[object]]] = {
        "parent": {},
        "district": {},
        "state_parent": {},
        "state": {},
    }

    for ref_id, row in enumerate(iter_csv(path), start=1):
        state_code = parse_int(row.get("State Code"))
        district_code = parse_int(row.get("District Code"))
        subdistrict_code = parse_int(row.get("Subdistrict Code"))
        village_code = parse_int(row.get("Village Code"))
        if state_code is None or district_code is None or subdistrict_code is None or village_code is None:
            continue
        village_name = clean_text(row.get("Village Name (In English)")) or ""
        subdistrict_name = clean_text(row.get("Subdistrict Name (In English)")) or ""
        reference = RuralReference(
            ref_id=ref_id,
            state_code=state_code,
            state_name=clean_text(row.get("State Name")) or "",
            district_code=district_code,
            district_name=clean_text(row.get("District Name (In English)")) or "",
            district_census2011_code=clean_text(row.get("District Census 2011 Code")) or "",
            subdistrict_code=subdistrict_code,
            subdistrict_name=subdistrict_name,
            subdistrict_census2011_code=clean_text(row.get("Subdistrict Census 2011 Code")) or "",
            village_code=village_code,
            village_name=village_name,
            village_census2011_code=clean_text(row.get("Village Census 2011 Code")) or "",
            local_body_code=parse_int(row.get("Local Body Code")),
            local_body_name=clean_text(row.get("Local Body Name (In English)")) or "",
            village_norm=normalize_text(village_name),
            village_relaxed_norm=normalize_relaxed_place(village_name),
            subdistrict_norm=normalize_text(subdistrict_name),
            signatures=signatures(village_name),
        )
        references.append(reference)
        register_unique(
            unique_maps["rural_exact_district_subdistrict_village"],
            (reference.district_code, reference.subdistrict_norm, reference.village_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_exact_relaxed_district_subdistrict_village"],
            (reference.district_code, reference.subdistrict_norm, reference.village_relaxed_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_exact_district_village_unique"],
            (reference.district_code, reference.village_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_exact_relaxed_district_village_unique"],
            (reference.district_code, reference.village_relaxed_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_boundary_fallback_state_subdistrict_village_unique"],
            (reference.state_code, reference.subdistrict_norm, reference.village_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_boundary_fallback_state_subdistrict_relaxed_village_unique"],
            (reference.state_code, reference.subdistrict_norm, reference.village_relaxed_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_boundary_fallback_state_village_unique"],
            (reference.state_code, reference.village_norm),
            reference,
        )
        register_unique(
            unique_maps["rural_boundary_fallback_state_relaxed_village_unique"],
            (reference.state_code, reference.village_relaxed_norm),
            reference,
        )
        for signature in reference.signatures:
            add_index(indexes["parent"], (reference.district_code, reference.subdistrict_norm, signature), reference)
            add_index(indexes["district"], (reference.district_code, signature), reference)
            add_index(indexes["state_parent"], (reference.state_code, reference.subdistrict_norm, signature), reference)
            add_index(indexes["state"], (reference.state_code, signature), reference)
    return references, unique_maps, indexes


def load_urban_references(path: Path) -> tuple[list[UrbanReference], dict[str, dict[tuple[object, ...], UniqueValue]], dict[str, dict[tuple[object, ...], list[object]]]]:
    references: list[UrbanReference] = []
    unique_maps: dict[str, dict[tuple[object, ...], UniqueValue]] = {
        "urban_exact_state_town_ward": {},
        "urban_exact_relaxed_state_town_ward": {},
        "urban_exact_state_town_ward_number_unique": {},
        "urban_fallback_state_ward_number_unique": {},
    }
    indexes: dict[str, dict[tuple[object, ...], list[object]]] = {
        "town": {},
        "state_town_signature_number": {},
        "state_ward_number": {},
        "state_signature": {},
    }

    for ref_id, row in enumerate(iter_csv(path), start=1):
        state_code = parse_int(row.get("State Code"))
        local_body_code = parse_int(row.get("Local Body Code"))
        ward_code = parse_int(row.get("Ward Code"))
        if state_code is None or local_body_code is None or ward_code is None:
            continue
        local_body_name = clean_text(row.get("Local Body Name")) or ""
        ward_name = clean_text(row.get("Ward Name")) or ""
        ward_number = extract_ward_number(row.get("Ward Number"), ward_name)
        reference = UrbanReference(
            ref_id=ref_id,
            state_code=state_code,
            state_name=clean_text(row.get("State Name")) or "",
            local_body_code=local_body_code,
            local_body_name=local_body_name,
            ward_code=ward_code,
            ward_number=ward_number,
            ward_name=ward_name,
            local_body_norm=normalize_text(local_body_name),
            ward_norm=normalize_text(ward_name),
            ward_relaxed_norm=normalize_relaxed_place(ward_name),
            signatures=signatures(ward_name),
        )
        references.append(reference)
        register_unique(
            unique_maps["urban_exact_state_town_ward"],
            (reference.state_code, reference.local_body_norm, reference.ward_norm),
            reference,
        )
        register_unique(
            unique_maps["urban_exact_relaxed_state_town_ward"],
            (reference.state_code, reference.local_body_norm, reference.ward_relaxed_norm),
            reference,
        )
        register_unique(
            unique_maps["urban_exact_state_town_ward_number_unique"],
            (reference.state_code, reference.local_body_norm, reference.ward_number),
            reference,
        )
        register_unique(
            unique_maps["urban_fallback_state_ward_number_unique"],
            (reference.state_code, reference.ward_number),
            reference,
        )
        add_index(indexes["town"], (reference.state_code, reference.local_body_norm, reference.ward_number), reference)
        for local_body_signature in signatures(local_body_name):
            add_index(
                indexes["state_town_signature_number"],
                (reference.state_code, reference.ward_number, local_body_signature),
                reference,
            )
        add_index(indexes["state_ward_number"], (reference.state_code, reference.ward_number), reference)
        for signature in reference.signatures:
            add_index(indexes["state_signature"], (reference.state_code, signature), reference)
    return references, unique_maps, indexes


def dedupe_candidates(candidates: Iterable[object], *, limit: int) -> list[object]:
    seen: set[int] = set()
    result: list[object] = []
    for candidate in candidates:
        code = unique_value_code(candidate)  # type: ignore[arg-type]
        if code is None or code in seen:
            continue
        seen.add(code)
        result.append(candidate)
        if len(result) > limit:
            return result
    return result


def lookup_unique(mapping: dict[tuple[object, ...], UniqueValue], key: tuple[object, ...]) -> UniqueValue:
    return mapping.get(key)


def rural_exact_match(row: dict[str, str], unique_maps: dict[str, dict[tuple[object, ...], UniqueValue]]) -> Match | None:
    state_code = parse_int(row.get("state.ID"))
    district_code = parse_int(row.get("district.ID"))
    block_norm = normalize_text(row.get("block.name"))
    village_norm = normalize_text(row.get("village.name"))
    village_relaxed = normalize_relaxed_place(row.get("village.name"))
    stages = (
        ("rural_exact_district_subdistrict_village", 1.0, (district_code, block_norm, village_norm), "district_anchor"),
        ("rural_exact_relaxed_district_subdistrict_village", 0.995, (district_code, block_norm, village_relaxed), "district_anchor"),
        ("rural_exact_district_village_unique", 0.985, (district_code, village_norm), "district_anchor"),
        ("rural_exact_relaxed_district_village_unique", 0.975, (district_code, village_relaxed), "district_anchor"),
        ("rural_boundary_fallback_state_subdistrict_village_unique", 0.965, (state_code, block_norm, village_norm), "state_block_boundary_fallback"),
        ("rural_boundary_fallback_state_subdistrict_relaxed_village_unique", 0.96, (state_code, block_norm, village_relaxed), "state_block_boundary_fallback"),
        ("rural_boundary_fallback_state_village_unique", 0.955, (state_code, village_norm), "state_boundary_fallback"),
        ("rural_boundary_fallback_state_relaxed_village_unique", 0.945, (state_code, village_relaxed), "state_boundary_fallback"),
    )
    for method, score, key, scope in stages:
        reference = lookup_unique(unique_maps[method], key)
        if isinstance(reference, RuralReference):
            return Match(location_scope=scope, method=method, score=score, margin=score, candidate_count=1, rural=reference)
    return None


def candidate_scores_rural(row: dict[str, str], candidates: Sequence[object], *, scope: str) -> list[tuple[RuralReference, float, float]]:
    village_name = clean_text(row.get("village.name")) or ""
    village_relaxed = normalize_relaxed_place(village_name)
    block_name = clean_text(row.get("block.name")) or ""
    district_name = clean_text(row.get("district.name")) or ""
    scored: list[tuple[RuralReference, float, float]] = []
    for candidate in candidates:
        if not isinstance(candidate, RuralReference):
            continue
        cheap_village = simple_similarity(village_name, candidate.village_name)
        if village_relaxed and village_relaxed == candidate.village_relaxed_norm:
            cheap_village = max(cheap_village, 0.98)
        if cheap_village < 0.70:
            continue
        village_score = score_candidate(village_name, candidate.village_name).score
        if village_relaxed and village_relaxed == candidate.village_relaxed_norm:
            village_score = max(village_score, 0.98)
        if scope == "parent":
            total = village_score
        elif scope == "district":
            subdistrict_score = score_candidate(block_name, candidate.subdistrict_name).score
            total = (0.84 * village_score) + (0.16 * subdistrict_score)
        elif scope == "state_parent":
            district_score = score_candidate(district_name, candidate.district_name).score
            total = (0.86 * village_score) + (0.14 * district_score)
        else:
            district_score = score_candidate(district_name, candidate.district_name).score
            subdistrict_score = score_candidate(block_name, candidate.subdistrict_name).score
            total = (0.74 * village_score) + (0.16 * district_score) + (0.10 * subdistrict_score)
        scored.append((candidate, min(1.0, total), village_score))
    scored.sort(key=lambda item: (-item[1], -item[2], item[0].village_code))
    return scored


def rural_fuzzy_match(
    row: dict[str, str],
    indexes: dict[str, dict[tuple[object, ...], list[object]]],
    *,
    max_candidates: int,
    auto_accept_score: float,
    min_margin: float,
    min_village_score: float,
    enable_state_fuzzy: bool,
) -> Match | None:
    state_code = parse_int(row.get("state.ID"))
    district_code = parse_int(row.get("district.ID"))
    block_norm = normalize_text(row.get("block.name"))
    sigs = signatures(row.get("village.name"))
    scopes = (
        ("parent", "rural_fuzzy_district_subdistrict_village", "district_anchor", (district_code, block_norm)),
        ("district", "rural_fuzzy_district_village", "district_anchor", (district_code,)),
        ("state_parent", "rural_fuzzy_state_subdistrict_village", "state_block_boundary_fallback", (state_code, block_norm)),
    )
    if enable_state_fuzzy:
        scopes = (*scopes, ("state", "rural_fuzzy_state_village", "state_boundary_fallback", (state_code,)))
    for scope, method, location_scope, prefix in scopes:
        raw_candidates: list[object] = []
        for signature in sigs:
            raw_candidates.extend(indexes[scope].get((*prefix, signature), ()))
        candidates = dedupe_candidates(raw_candidates, limit=max_candidates)
        if not candidates or len(candidates) > max_candidates:
            continue
        scored = candidate_scores_rural(row, candidates, scope=scope)
        if not scored:
            continue
        best = scored[0]
        margin = best[1] - scored[1][1] if len(scored) > 1 else best[1]
        if best[1] >= auto_accept_score and best[2] >= min_village_score and margin >= min_margin:
            return Match(
                location_scope=location_scope,
                method=method,
                score=best[1],
                margin=margin,
                candidate_count=len(candidates),
                rural=best[0],
            )
    return None


def urban_exact_match(row: dict[str, str], unique_maps: dict[str, dict[tuple[object, ...], UniqueValue]]) -> Match | None:
    state_code = parse_int(row.get("state.ID"))
    town_norm = normalize_text(row.get("town.name"))
    ward_norm = normalize_text(row.get("ward.name"))
    ward_relaxed = normalize_relaxed_place(row.get("ward.name"))
    ward_number = extract_ward_number(row.get("ward.name"))
    stages = (
        ("urban_exact_state_town_ward", 1.0, (state_code, town_norm, ward_norm), "town_anchor"),
        ("urban_exact_relaxed_state_town_ward", 0.995, (state_code, town_norm, ward_relaxed), "town_anchor"),
        ("urban_exact_state_town_ward_number_unique", 0.985, (state_code, town_norm, ward_number), "town_anchor"),
        ("urban_fallback_state_ward_number_unique", 0.90, (state_code, ward_number), "state_ward_number_fallback"),
    )
    for method, score, key, scope in stages:
        reference = lookup_unique(unique_maps[method], key)
        if isinstance(reference, UrbanReference):
            return Match(location_scope=scope, method=method, score=score, margin=score, candidate_count=1, urban=reference)
    return None


def candidate_scores_urban(row: dict[str, str], candidates: Sequence[object], *, scope: str) -> list[tuple[UrbanReference, float, float]]:
    town_name = clean_text(row.get("town.name")) or ""
    ward_name = clean_text(row.get("ward.name")) or ""
    ward_relaxed = normalize_relaxed_place(ward_name)
    ward_number = extract_ward_number(ward_name)
    scored: list[tuple[UrbanReference, float, float]] = []
    for candidate in candidates:
        if not isinstance(candidate, UrbanReference):
            continue
        town_score = score_candidate(town_name, candidate.local_body_name).score
        ward_score = score_candidate(ward_name, candidate.ward_name).score
        if ward_relaxed and ward_relaxed == candidate.ward_relaxed_norm:
            ward_score = max(ward_score, 0.98)
        number_bonus = 1.0 if ward_number and ward_number == candidate.ward_number else 0.0
        if scope == "town":
            total = (0.58 * ward_score) + (0.34 * number_bonus) + (0.08 * town_score)
        else:
            total = (0.50 * ward_score) + (0.30 * number_bonus) + (0.20 * town_score)
        scored.append((candidate, min(1.0, total), ward_score))
    scored.sort(key=lambda item: (-item[1], -item[2], item[0].ward_code))
    return scored


def urban_fuzzy_match(
    row: dict[str, str],
    indexes: dict[str, dict[tuple[object, ...], list[object]]],
    *,
    max_candidates: int,
    auto_accept_score: float,
    min_margin: float,
) -> Match | None:
    state_code = parse_int(row.get("state.ID"))
    town_norm = normalize_text(row.get("town.name"))
    ward_number = extract_ward_number(row.get("ward.name"))
    sigs = signatures(row.get("ward.name"))
    stages = [
        ("town", "urban_fuzzy_state_town_ward", "town_anchor", (state_code, town_norm, ward_number)),
        ("state_town_signature_number", "urban_fuzzy_state_town_alias_ward_number", "town_alias_fallback", ()),
        ("state_ward_number", "urban_fuzzy_state_ward_number", "state_ward_number_fallback", (state_code, ward_number)),
    ]
    raw_signature_candidates: list[object] = []
    for signature in sigs:
        raw_signature_candidates.extend(indexes["state_signature"].get((state_code, signature), ()))
    if raw_signature_candidates:
        stages.append(("state_signature", "urban_fuzzy_state_ward", "state_signature_fallback", ()))

    for scope, method, location_scope, key in stages:
        if scope == "state_signature":
            candidates = dedupe_candidates(raw_signature_candidates, limit=max_candidates)
        elif scope == "state_town_signature_number":
            raw_town_candidates: list[object] = []
            for town_signature in signatures(row.get("town.name")):
                raw_town_candidates.extend(
                    indexes[scope].get((state_code, ward_number, town_signature), ())
                )
            candidates = dedupe_candidates(raw_town_candidates, limit=max_candidates)
        else:
            candidates = dedupe_candidates(indexes[scope].get(key, ()), limit=max_candidates)
        if not candidates or len(candidates) > max_candidates:
            continue
        scored = candidate_scores_urban(row, candidates, scope="town" if scope == "town" else "state")
        if not scored:
            continue
        best = scored[0]
        margin = best[1] - scored[1][1] if len(scored) > 1 else best[1]
        if best[1] >= auto_accept_score and margin >= min_margin:
            return Match(
                location_scope=location_scope,
                method=method,
                score=best[1],
                margin=margin,
                candidate_count=len(candidates),
                urban=best[0],
            )
    return None


def resolve_row(
    row: dict[str, str],
    rural_unique: dict[str, dict[tuple[object, ...], UniqueValue]],
    rural_indexes: dict[str, dict[tuple[object, ...], list[object]]],
    urban_unique: dict[str, dict[tuple[object, ...], UniqueValue]],
    urban_indexes: dict[str, dict[tuple[object, ...], list[object]]],
    *,
    max_candidates: int,
    rural_auto_accept_score: float,
    urban_auto_accept_score: float,
    min_margin: float,
    min_village_score: float,
    enable_state_fuzzy: bool,
) -> Match | None:
    location_type = normalize_text(row.get("location.type"))
    if location_type == "rural":
        return rural_exact_match(row, rural_unique) or rural_fuzzy_match(
            row,
            rural_indexes,
            max_candidates=max_candidates,
            auto_accept_score=rural_auto_accept_score,
            min_margin=min_margin,
            min_village_score=min_village_score,
            enable_state_fuzzy=enable_state_fuzzy,
        )
    if location_type == "urban":
        return urban_exact_match(row, urban_unique) or urban_fuzzy_match(
            row,
            urban_indexes,
            max_candidates=max_candidates,
            auto_accept_score=urban_auto_accept_score,
            min_margin=min_margin,
        )
    return None


def output_header() -> list[str]:
    count_columns: list[str] = []
    for species in SPECIES:
        count_columns.extend(
            [
                f"{species}_male",
                f"{species}_female",
                f"{species}_total",
            ]
        )
    return [
        "source_row",
        "location_type",
        "source_state_name",
        "source_state_code",
        "source_district_name",
        "source_district_code",
        "source_block_name",
        "source_village_name",
        "source_town_name",
        "source_ward_name",
        "lgd_state_code",
        "lgd_state_name",
        "lgd_district_code",
        "lgd_district_name",
        "lgd_district_census2011_code",
        "lgd_subdistrict_code",
        "lgd_subdistrict_name",
        "lgd_subdistrict_census2011_code",
        "lgd_village_code",
        "village_census2011_code",
        "lgd_village_name",
        "local_body_code",
        "local_body_name",
        "ward_code",
        "ward_number",
        "ward_name",
        *count_columns,
        "match_status",
        "match_scope",
        "match_method",
        "match_score",
        "match_margin",
        "match_candidate_count",
    ]


def output_row(source_row: int, row: dict[str, str], match: Match | None) -> dict[str, object]:
    location_type = normalize_text(row.get("location.type"))
    rural = match.rural if match else None
    urban = match.urban if match else None
    resolved_state_code = rural.state_code if rural else urban.state_code if urban else parse_int(row.get("state.ID"))
    resolved_state_name = rural.state_name if rural else urban.state_name if urban else clean_text(row.get("state.name")) or ""

    counts: dict[str, int] = {}
    for species in SPECIES:
        male = parse_count(row.get(f"population.{species}.male"))
        female = parse_count(row.get(f"population.{species}.female"))
        counts[f"{species}_male"] = male
        counts[f"{species}_female"] = female
        counts[f"{species}_total"] = male + female

    return {
        "source_row": source_row,
        "location_type": location_type,
        "source_state_name": clean_text(row.get("state.name")) or "",
        "source_state_code": parse_int(row.get("state.ID")) or "",
        "source_district_name": clean_text(row.get("district.name")) or "",
        "source_district_code": parse_int(row.get("district.ID")) or "",
        "source_block_name": clean_text(row.get("block.name")) or "",
        "source_village_name": clean_text(row.get("village.name")) or "",
        "source_town_name": clean_text(row.get("town.name")) or "",
        "source_ward_name": clean_text(row.get("ward.name")) or "",
        "lgd_state_code": resolved_state_code or "",
        "lgd_state_name": resolved_state_name,
        "lgd_district_code": rural.district_code if rural else parse_int(row.get("district.ID")) or "",
        "lgd_district_name": rural.district_name if rural else clean_text(row.get("district.name")) or "",
        "lgd_district_census2011_code": rural.district_census2011_code if rural else "",
        "lgd_subdistrict_code": rural.subdistrict_code if rural else "",
        "lgd_subdistrict_name": rural.subdistrict_name if rural else "",
        "lgd_subdistrict_census2011_code": rural.subdistrict_census2011_code if rural else "",
        "lgd_village_code": rural.village_code if rural else "",
        "village_census2011_code": rural.village_census2011_code if rural else "",
        "lgd_village_name": rural.village_name if rural else "",
        "local_body_code": urban.local_body_code if urban else rural.local_body_code if rural and rural.local_body_code else "",
        "local_body_name": urban.local_body_name if urban else rural.local_body_name if rural else "",
        "ward_code": urban.ward_code if urban else "",
        "ward_number": urban.ward_number if urban else "",
        "ward_name": urban.ward_name if urban else "",
        **counts,
        "match_status": "matched" if match else "unmatched",
        "match_scope": match.location_scope if match else "unmatched",
        "match_method": match.method if match else "unmatched",
        "match_score": round(match.score, 6) if match else 0.0,
        "match_margin": round(match.margin, 6) if match else 0.0,
        "match_candidate_count": match.candidate_count if match else 0,
    }


def write_outputs(
    source: Path,
    output_dir: Path,
    rural_unique: dict[str, dict[tuple[object, ...], UniqueValue]],
    rural_indexes: dict[str, dict[tuple[object, ...], list[object]]],
    urban_unique: dict[str, dict[tuple[object, ...], UniqueValue]],
    urban_indexes: dict[str, dict[tuple[object, ...], list[object]]],
    args: argparse.Namespace,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_path = output_dir / "livestock_lgd_alignment_all.csv"
    matched_path = output_dir / "livestock_lgd_aligned.csv"
    unmatched_path = output_dir / "livestock_lgd_unmatched.csv"
    header = output_header()

    counts = {
        "total_rows": 0,
        "matched_rows": 0,
        "unmatched_rows": 0,
        "rural_rows": 0,
        "urban_rows": 0,
        "rural_matched_rows": 0,
        "urban_matched_rows": 0,
        "rural_unmatched_rows": 0,
        "urban_unmatched_rows": 0,
    }
    method_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}

    with (
        all_path.open("w", encoding="utf-8", newline="") as all_handle,
        matched_path.open("w", encoding="utf-8", newline="") as matched_handle,
        unmatched_path.open("w", encoding="utf-8", newline="") as unmatched_handle,
    ):
        all_writer = csv.DictWriter(all_handle, fieldnames=header)
        matched_writer = csv.DictWriter(matched_handle, fieldnames=header)
        unmatched_writer = csv.DictWriter(unmatched_handle, fieldnames=header)
        all_writer.writeheader()
        matched_writer.writeheader()
        unmatched_writer.writeheader()

        for source_row, row in enumerate(iter_csv(source), start=2):
            location_type = normalize_text(row.get("location.type"))
            if location_type not in LOCATION_TYPES:
                location_type = "unknown"
            match = resolve_row(
                row,
                rural_unique,
                rural_indexes,
                urban_unique,
                urban_indexes,
                max_candidates=args.max_candidates,
                rural_auto_accept_score=args.rural_auto_accept_score,
                urban_auto_accept_score=args.urban_auto_accept_score,
                min_margin=args.min_margin,
                min_village_score=args.min_village_score,
                enable_state_fuzzy=args.enable_state_fuzzy,
            )
            prepared = output_row(source_row, row, match)
            all_writer.writerow(prepared)
            counts["total_rows"] += 1
            if location_type == "rural":
                counts["rural_rows"] += 1
            elif location_type == "urban":
                counts["urban_rows"] += 1

            if match:
                matched_writer.writerow(prepared)
                counts["matched_rows"] += 1
                method_counts[match.method] = method_counts.get(match.method, 0) + 1
                scope_counts[match.location_scope] = scope_counts.get(match.location_scope, 0) + 1
                if location_type == "rural":
                    counts["rural_matched_rows"] += 1
                elif location_type == "urban":
                    counts["urban_matched_rows"] += 1
            else:
                unmatched_writer.writerow(prepared)
                counts["unmatched_rows"] += 1
                if location_type == "rural":
                    counts["rural_unmatched_rows"] += 1
                elif location_type == "urban":
                    counts["urban_unmatched_rows"] += 1

    return {
        "paths": {
            "all_alignment_csv": as_repo_path(all_path),
            "matched_csv": as_repo_path(matched_path),
            "unmatched_csv": as_repo_path(unmatched_path),
        },
        "counts": counts,
        "method_counts": dict(sorted(method_counts.items())),
        "scope_counts": dict(sorted(scope_counts.items())),
    }


def build_summary(
    *,
    started_at: str,
    elapsed_seconds: float,
    rural_reference_rows: int,
    urban_reference_rows: int,
    outputs: dict[str, object],
    args: argparse.Namespace,
) -> dict[str, object]:
    counts = outputs["counts"]
    total_rows = int(counts["total_rows"]) or 1
    rural_rows = int(counts["rural_rows"]) or 1
    urban_rows = int(counts["urban_rows"]) or 1
    return {
        "started_at": started_at,
        "finished_at": utc_now(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "inputs": {
            "source": as_repo_path(args.source),
            "gp_mapping": as_repo_path(args.gp_mapping),
            "urban_wards": as_repo_path(args.urban_wards),
        },
        "parameters": {
            "max_candidates": args.max_candidates,
            "rural_auto_accept_score": args.rural_auto_accept_score,
            "urban_auto_accept_score": args.urban_auto_accept_score,
            "min_margin": args.min_margin,
            "min_village_score": args.min_village_score,
            "enable_state_fuzzy": args.enable_state_fuzzy,
        },
        "reference_rows": {
            "rural_gp_mapping": rural_reference_rows,
            "urban_wards": urban_reference_rows,
        },
        "rows": {
            **counts,
            "match_rate": round(int(counts["matched_rows"]) / total_rows, 6),
            "rural_match_rate": round(int(counts["rural_matched_rows"]) / rural_rows, 6),
            "urban_match_rate": round(int(counts["urban_matched_rows"]) / urban_rows, 6),
        },
        "method_counts": outputs["method_counts"],
        "scope_counts": outputs["scope_counts"],
        "outputs": outputs["paths"],
        "notes": [
            "Rural rows resolve to LGD subdistrict and village IDs from GP mapping.",
            "Urban rows resolve to local body and ward IDs from urban ward mapping.",
            "State+block and state-level rural fallbacks are labelled as boundary-version fallbacks.",
            "Urban town-alias fallbacks handle renamed combined local bodies when ward number and score agree.",
            "Fuzzy scoring is applied only after indexed signature narrowing.",
        ],
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare ART Park/IITM livestock CSV with LGD IDs.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--gp-mapping", type=Path, default=DEFAULT_GP_MAPPING)
    parser.add_argument("--urban-wards", type=Path, default=DEFAULT_URBAN_WARDS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-candidates", type=int, default=100)
    parser.add_argument("--rural-auto-accept-score", type=float, default=0.88)
    parser.add_argument("--urban-auto-accept-score", type=float, default=0.88)
    parser.add_argument("--min-margin", type=float, default=0.035)
    parser.add_argument("--min-village-score", type=float, default=0.82)
    parser.add_argument(
        "--enable-state-fuzzy",
        action="store_true",
        dest="enable_state_fuzzy",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--disable-state-fuzzy",
        action="store_false",
        dest="enable_state_fuzzy",
        help="Skip broad state-level fuzzy matching for rural rows after safer scopes.",
    )
    parser.set_defaults(enable_state_fuzzy=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = utc_now()
    start = time.perf_counter()

    rural_references, rural_unique, rural_indexes = load_rural_references(args.gp_mapping)
    urban_references, urban_unique, urban_indexes = load_urban_references(args.urban_wards)
    outputs = write_outputs(
        args.source,
        args.output_dir,
        rural_unique,
        rural_indexes,
        urban_unique,
        urban_indexes,
        args,
    )
    summary = build_summary(
        started_at=started_at,
        elapsed_seconds=time.perf_counter() - start,
        rural_reference_rows=len(rural_references),
        urban_reference_rows=len(urban_references),
        outputs=outputs,
        args=args,
    )
    summary_path = args.output_dir / "livestock_lgd_alignment_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    summary["outputs"]["summary_json"] = as_repo_path(summary_path)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
