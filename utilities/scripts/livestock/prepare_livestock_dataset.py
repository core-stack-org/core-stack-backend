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

The active rural pipeline is standalone: it uses local normalization, phonetic
signatures, child-village overlap, and one-to-one assignment to produce a final
CSV with unique LGD village IDs.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass
from difflib import SequenceMatcher
import json
from pathlib import Path
import re
import sys
import time
import unicodedata
from typing import Iterable, Iterator, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


@dataclass(frozen=True)
class RuralSourceRow:
    source_row: int
    state_code: int | None
    state_name: str
    district_code: int | None
    district_name: str
    block_name: str
    block_norm: str
    block_relaxed_norm: str
    village_name: str
    village_match_name: str
    village_norm: str
    village_relaxed_norm: str
    village_signatures: tuple[str, ...]
    row: dict[str, str]


@dataclass(frozen=True)
class RuralSourceVillage:
    source_id: int
    rows: tuple[RuralSourceRow, ...]
    state_code: int
    state_name: str
    district_code: int
    district_name: str
    block_name: str
    block_norm: str
    village_name: str
    village_norm: str
    village_relaxed_norm: str
    village_signatures: tuple[str, ...]
    counts: dict[str, int]


@dataclass(frozen=True)
class SourceBlock:
    key: tuple[int, int, str]
    state_code: int
    state_name: str
    district_code: int
    district_name: str
    block_name: str
    block_norm: str
    block_relaxed_norm: str
    source_order: int
    row_count: int
    village_norms: tuple[str, ...]
    village_relaxed_norms: tuple[str, ...]


@dataclass(frozen=True)
class SubdistrictReference:
    state_code: int
    state_name: str
    district_code: int
    district_name: str
    district_census2011_code: str
    subdistrict_code: int
    subdistrict_name: str
    subdistrict_census2011_code: str
    subdistrict_norm: str
    subdistrict_relaxed_norm: str
    signatures: tuple[str, ...]


@dataclass(frozen=True)
class HierarchyAssignment:
    target_code: int
    method: str
    score: float
    margin: float
    candidate_count: int


@dataclass(frozen=True)
class RuralVillageAssignment:
    reference: RuralReference
    method: str
    score: float
    margin: float
    candidate_count: int


@dataclass(frozen=True)
class CandidateScore:
    score: float


UniqueValue = RuralReference | UrbanReference | None


PHONETIC_REPLACEMENTS = (
    ("tch", "ch"),
    ("dge", "j"),
    ("ph", "f"),
    ("bh", "b"),
    ("dh", "d"),
    ("gh", "g"),
    ("kh", "k"),
    ("sh", "s"),
    ("ch", "c"),
    ("ck", "k"),
    ("qu", "k"),
    ("q", "k"),
    ("x", "ks"),
    ("v", "w"),
    ("z", "j"),
    ("oo", "u"),
    ("ou", "u"),
    ("ee", "i"),
    ("aa", "a"),
    ("ai", "e"),
)

SOUNDEX_GROUPS = {
    **{ch: "1" for ch in "bfpvw"},
    **{ch: "2" for ch in "cgjkqsxz"},
    **{ch: "3" for ch in "dt"},
    "l": "4",
    **{ch: "5" for ch in "mn"},
    "r": "6",
}


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return None
    return text


def normalize_match_text(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = normalized.replace("ôçô", "-").replace("â€“", "-").replace("â€”", "-")
    normalized = re.sub(r"[&/,_()\-.:]+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def compact_match_text(value: str) -> str:
    return normalize_match_text(value).replace(" ", "")


def phonetic_form(value: str) -> str:
    transformed = compact_match_text(value)
    for source, target in PHONETIC_REPLACEMENTS:
        transformed = transformed.replace(source, target)
    transformed = re.sub(r"(.)\1+", r"\1", transformed)
    return transformed


def consonant_signature(value: str) -> str:
    phonetic = phonetic_form(value)
    consonants = re.sub(r"[aeiouy]", "", phonetic)
    return consonants or phonetic[:1]


def soundex_code(value: str) -> str:
    phonetic = phonetic_form(value)
    if not phonetic:
        return ""
    first = phonetic[0]
    digits: list[str] = []
    previous = ""
    for char in phonetic[1:]:
        digit = SOUNDEX_GROUPS.get(char, "")
        if digit and digit != previous:
            digits.append(digit)
        previous = digit
    return (first.upper() + "".join(digits) + "000")[:4]


def token_similarity(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    containment = intersection / max(min(len(left_tokens), len(right_tokens)), 1)
    jaccard = intersection / max(union, 1)
    return (0.65 * containment) + (0.35 * jaccard)


def score_candidate(query: str, candidate: str) -> CandidateScore:
    query_norm = normalize_match_text(query)
    candidate_norm = normalize_match_text(candidate)
    if not query_norm or not candidate_norm:
        return CandidateScore(0.0)
    if query_norm == candidate_norm:
        return CandidateScore(1.0)
    query_compact = query_norm.replace(" ", "")
    candidate_compact = candidate_norm.replace(" ", "")
    sequence = SequenceMatcher(None, query_norm, candidate_norm).ratio()
    compact_sequence = SequenceMatcher(None, query_compact, candidate_compact).ratio()
    token_score = token_similarity(query_norm, candidate_norm)
    substring = 1.0 if query_compact in candidate_compact or candidate_compact in query_compact else 0.0
    prefix = 1.0 if query_compact[:4] and query_compact[:4] == candidate_compact[:4] else 0.0
    phonetic = 1.0 if phonetic_form(query) == phonetic_form(candidate) else 0.0
    score = (
        0.48 * max(sequence, compact_sequence)
        + 0.22 * token_score
        + 0.12 * substring
        + 0.10 * prefix
        + 0.08 * phonetic
    )
    return CandidateScore(max(0.0, min(1.0, score)))


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
    text = re.sub(r"\(\s*\d+\s*\)", " ", text)
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


def rural_count_values(row: dict[str, str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for species in SPECIES:
        male = parse_count(row.get(f"population.{species}.male"))
        female = parse_count(row.get(f"population.{species}.female"))
        counts[f"{species}_male"] = male
        counts[f"{species}_female"] = female
        counts[f"{species}_total"] = male + female
    return counts


def normalize_source_village_label(value: object) -> str:
    text = clean_text(value) or ""
    text = (
        text.replace("ÔÇô", "-")
        .replace("â€“", "-")
        .replace("â€”", "-")
        .replace("–", "-")
        .replace("—", "-")
    )
    text = re.sub(r"\s+", " ", text).strip()
    gp_match = re.match(r"(.+?)\s*\(\s*gp\s*\)\s*-?\s*ward\s*no\.?\s*\d+\s*$", text, flags=re.I)
    if gp_match:
        base = gp_match.group(1).strip()
        return re.sub(r"(?<=[a-z])(?:North|South|East|West|Central)$", "", base).strip()
    ward_match = re.match(r"(.+?)\s+-?\s*ward\s*no\.?\s*\d+\s*$", text, flags=re.I)
    if ward_match:
        return ward_match.group(1).strip()
    compact_ward_match = re.match(r"(.+?)\s*\(\s*gp\s*\)\s*wardno\.?\s*\d+\s*$", text, flags=re.I)
    if compact_ward_match:
        base = compact_ward_match.group(1).strip()
        return re.sub(r"(?<=[a-z])(?:North|South|East|West|Central)$", "", base).strip()
    return text


def load_rural_source_rows(path: Path) -> tuple[list[RuralSourceRow], Counter[str]]:
    rows: list[RuralSourceRow] = []
    counts: Counter[str] = Counter()
    for source_row, row in enumerate(iter_csv(path), start=2):
        counts["total_source_rows"] += 1
        location_type = normalize_text(row.get("location.type"))
        if location_type == "rural":
            counts["rural_source_rows"] += 1
        elif location_type == "urban":
            counts["urban_source_rows_skipped"] += 1
            continue
        else:
            counts["unknown_location_type_rows_skipped"] += 1
            continue

        block_name = clean_text(row.get("block.name")) or ""
        village_name = clean_text(row.get("village.name")) or ""
        village_match_name = normalize_source_village_label(village_name)
        rows.append(
            RuralSourceRow(
                source_row=source_row,
                state_code=parse_int(row.get("state.ID")),
                state_name=clean_text(row.get("state.name")) or "",
                district_code=parse_int(row.get("district.ID")),
                district_name=clean_text(row.get("district.name")) or "",
                block_name=block_name,
                block_norm=normalize_text(block_name),
                block_relaxed_norm=normalize_relaxed_place(block_name),
                village_name=village_name,
                village_match_name=village_match_name,
                village_norm=normalize_text(village_match_name),
                village_relaxed_norm=normalize_relaxed_place(village_match_name),
                village_signatures=signatures(village_match_name),
                row=row,
            )
        )
    return rows, counts


def build_source_blocks(rows: Sequence[RuralSourceRow]) -> dict[tuple[int, int, str], SourceBlock]:
    grouped: dict[tuple[int, int, str], dict[str, object]] = {}
    for row in rows:
        if row.state_code is None or row.district_code is None or not row.block_norm:
            continue
        key = (row.state_code, row.district_code, row.block_norm)
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = {
                "state_code": row.state_code,
                "state_name": row.state_name,
                "district_code": row.district_code,
                "district_name": row.district_name,
                "block_name": row.block_name,
                "block_norm": row.block_norm,
                "block_relaxed_norm": row.block_relaxed_norm,
                "source_order": row.source_row,
                "row_count": 1,
                "village_norms": {row.village_norm} if row.village_norm else set(),
                "village_relaxed_norms": {row.village_relaxed_norm} if row.village_relaxed_norm else set(),
            }
        else:
            existing["row_count"] = int(existing["row_count"]) + 1
            if row.village_norm:
                existing["village_norms"].add(row.village_norm)  # type: ignore[union-attr]
            if row.village_relaxed_norm:
                existing["village_relaxed_norms"].add(row.village_relaxed_norm)  # type: ignore[union-attr]

    return {
        key: SourceBlock(
            key=key,
            state_code=int(payload["state_code"]),
            state_name=str(payload["state_name"]),
            district_code=int(payload["district_code"]),
            district_name=str(payload["district_name"]),
            block_name=str(payload["block_name"]),
            block_norm=str(payload["block_norm"]),
            block_relaxed_norm=str(payload["block_relaxed_norm"]),
            source_order=int(payload["source_order"]),
            row_count=int(payload["row_count"]),
            village_norms=tuple(sorted(payload["village_norms"])),  # type: ignore[arg-type]
            village_relaxed_norms=tuple(sorted(payload["village_relaxed_norms"])),  # type: ignore[arg-type]
        )
        for key, payload in grouped.items()
    }


def add_counts(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    result = dict(left)
    for key, value in right.items():
        result[key] = result.get(key, 0) + value
    return result


def build_source_villages(rows: Sequence[RuralSourceRow]) -> list[RuralSourceVillage]:
    grouped: dict[tuple[int, int, str, str], list[RuralSourceRow]] = defaultdict(list)
    for row in rows:
        if row.state_code is None or row.district_code is None or not row.block_norm or not row.village_relaxed_norm:
            continue
        key = (row.state_code, row.district_code, row.block_norm, row.village_relaxed_norm)
        grouped[key].append(row)

    source_villages: list[RuralSourceVillage] = []
    for group_rows in grouped.values():
        group_rows = sorted(group_rows, key=lambda item: item.source_row)
        first = group_rows[0]
        counts: dict[str, int] = {}
        for row in group_rows:
            counts = add_counts(counts, rural_count_values(row.row))
        source_villages.append(
            RuralSourceVillage(
                source_id=first.source_row,
                rows=tuple(group_rows),
                state_code=first.state_code or 0,
                state_name=first.state_name,
                district_code=first.district_code or 0,
                district_name=first.district_name,
                block_name=first.block_name,
                block_norm=first.block_norm,
                village_name=first.village_match_name,
                village_norm=first.village_norm,
                village_relaxed_norm=first.village_relaxed_norm,
                village_signatures=first.village_signatures,
                counts=counts,
            )
        )
    source_villages.sort(key=lambda item: item.source_id)
    return source_villages


def build_bulk_district_targets(
    source_villages: Sequence[RuralSourceVillage],
    references: Sequence[RuralReference],
    *,
    min_overlap: int,
    min_source_ratio: float,
    min_target_ratio: float,
) -> dict[tuple[int, int], list[int]]:
    source_names_by_district: dict[tuple[int, int], set[str]] = defaultdict(set)
    for source in source_villages:
        if source.village_relaxed_norm:
            source_names_by_district[(source.state_code, source.district_code)].add(source.village_relaxed_norm)

    reference_names_by_district: dict[int, set[str]] = defaultdict(set)
    reference_district_by_name: dict[str, set[int]] = defaultdict(set)
    for reference in references:
        if not reference.village_relaxed_norm:
            continue
        reference_names_by_district[reference.district_code].add(reference.village_relaxed_norm)
        reference_district_by_name[reference.village_relaxed_norm].add(reference.district_code)

    targets: dict[tuple[int, int], list[int]] = {}
    for source_key, source_names in source_names_by_district.items():
        if not source_names:
            continue
        votes: Counter[int] = Counter()
        for name in source_names:
            votes.update(reference_district_by_name.get(name, ()))
        accepted: list[tuple[float, float, int, int]] = []
        for district_code, overlap_count in votes.items():
            if district_code == source_key[1] or overlap_count < min_overlap:
                continue
            reference_names = reference_names_by_district.get(district_code, set())
            source_ratio = overlap_count / len(source_names)
            target_ratio = overlap_count / max(min(len(source_names), len(reference_names)), 1)
            if source_ratio >= min_source_ratio or target_ratio >= min_target_ratio:
                accepted.append((source_ratio, target_ratio, overlap_count, district_code))
        if accepted:
            accepted.sort(reverse=True)
            targets[source_key] = [district_code for _, _, _, district_code in accepted[:3]]
    return targets


def build_rural_hierarchy_indexes(
    references: Sequence[RuralReference],
) -> tuple[
    dict[int, dict[int, SubdistrictReference]],
    dict[int, SubdistrictReference],
    dict[int, list[RuralReference]],
    dict[int, dict[str, list[RuralReference]]],
    dict[int, dict[str, list[RuralReference]]],
    dict[int, dict[str, list[RuralReference]]],
    dict[int, dict[str, list[RuralReference]]],
    dict[int, dict[str, list[RuralReference]]],
    dict[int, RuralReference],
    dict[int, set[str]],
    dict[int, dict[str, set[int]]],
]:
    subdistricts_by_district: dict[int, dict[int, SubdistrictReference]] = defaultdict(dict)
    subdistrict_by_code: dict[int, SubdistrictReference] = {}
    villages_by_subdistrict: dict[int, list[RuralReference]] = defaultdict(list)
    village_norm_index: dict[int, dict[str, list[RuralReference]]] = defaultdict(lambda: defaultdict(list))
    village_relaxed_index: dict[int, dict[str, list[RuralReference]]] = defaultdict(lambda: defaultdict(list))
    village_signature_index: dict[int, dict[str, list[RuralReference]]] = defaultdict(lambda: defaultdict(list))
    district_village_relaxed_index: dict[int, dict[str, list[RuralReference]]] = defaultdict(lambda: defaultdict(list))
    state_village_relaxed_index: dict[int, dict[str, list[RuralReference]]] = defaultdict(lambda: defaultdict(list))
    village_by_code: dict[int, RuralReference] = {}
    village_relaxed_names_by_subdistrict: dict[int, set[str]] = defaultdict(set)
    state_village_relaxed_subdistricts: dict[int, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))

    for reference in references:
        if reference.subdistrict_code not in subdistricts_by_district[reference.district_code]:
            subdistrict = SubdistrictReference(
                state_code=reference.state_code,
                state_name=reference.state_name,
                district_code=reference.district_code,
                district_name=reference.district_name,
                district_census2011_code=reference.district_census2011_code,
                subdistrict_code=reference.subdistrict_code,
                subdistrict_name=reference.subdistrict_name,
                subdistrict_census2011_code=reference.subdistrict_census2011_code,
                subdistrict_norm=reference.subdistrict_norm,
                subdistrict_relaxed_norm=normalize_relaxed_place(reference.subdistrict_name),
                signatures=signatures(reference.subdistrict_name),
            )
            subdistricts_by_district[reference.district_code][reference.subdistrict_code] = subdistrict
            subdistrict_by_code[reference.subdistrict_code] = subdistrict
        villages_by_subdistrict[reference.subdistrict_code].append(reference)
        village_by_code.setdefault(reference.village_code, reference)
        if reference.village_relaxed_norm:
            village_relaxed_names_by_subdistrict[reference.subdistrict_code].add(reference.village_relaxed_norm)
            state_village_relaxed_subdistricts[reference.state_code][reference.village_relaxed_norm].add(
                reference.subdistrict_code
            )
            district_village_relaxed_index[reference.district_code][reference.village_relaxed_norm].append(reference)
            state_village_relaxed_index[reference.state_code][reference.village_relaxed_norm].append(reference)
        if reference.village_norm:
            village_norm_index[reference.subdistrict_code][reference.village_norm].append(reference)
        if reference.village_relaxed_norm:
            village_relaxed_index[reference.subdistrict_code][reference.village_relaxed_norm].append(reference)
        for signature in reference.signatures:
            village_signature_index[reference.subdistrict_code][signature].append(reference)

    for villages in villages_by_subdistrict.values():
        villages.sort(key=lambda item: item.village_code)
    return (
        {district_code: dict(subdistricts) for district_code, subdistricts in subdistricts_by_district.items()},
        subdistrict_by_code,
        dict(villages_by_subdistrict),
        {code: dict(index) for code, index in village_norm_index.items()},
        {code: dict(index) for code, index in village_relaxed_index.items()},
        {code: dict(index) for code, index in village_signature_index.items()},
        {code: dict(index) for code, index in district_village_relaxed_index.items()},
        {code: dict(index) for code, index in state_village_relaxed_index.items()},
        village_by_code,
        dict(village_relaxed_names_by_subdistrict),
        {state_code: dict(index) for state_code, index in state_village_relaxed_subdistricts.items()},
    )


def best_unique_assignments(
    candidates_by_source: dict[object, list[tuple[int, str, float]]],
    *,
    source_order: dict[object, int],
    min_score: float,
    min_margin: float,
    exact_score: float = 0.995,
) -> tuple[dict[object, HierarchyAssignment], dict[object, HierarchyAssignment]]:
    source_best: dict[object, HierarchyAssignment] = {}
    for source_key, candidates in candidates_by_source.items():
        ranked = sorted(candidates, key=lambda item: (-item[2], item[0], item[1]))
        if not ranked:
            continue
        best_target, best_method, best_score = ranked[0]
        second_score = ranked[1][2] if len(ranked) > 1 else 0.0
        margin = best_score - second_score if len(ranked) > 1 else best_score
        tied_best = len(ranked) > 1 and abs(best_score - second_score) < 1e-12
        exact_and_unique = best_score >= exact_score and not tied_best
        fuzzy_and_clear = best_score >= min_score and margin >= min_margin
        if not exact_and_unique and not fuzzy_and_clear:
            continue
        source_best[source_key] = HierarchyAssignment(
            target_code=best_target,
            method=best_method,
            score=best_score,
            margin=margin,
            candidate_count=len(ranked),
        )

    by_target: dict[int, list[tuple[object, HierarchyAssignment]]] = defaultdict(list)
    for source_key, assignment in source_best.items():
        by_target[assignment.target_code].append((source_key, assignment))

    final: dict[object, HierarchyAssignment] = {}
    for target_code, options in by_target.items():
        winner_source, winner_assignment = sorted(
            options,
            key=lambda item: (
                -item[1].score,
                -item[1].margin,
                source_order.get(item[0], 0),
                str(item[0]),
            ),
        )[0]
        final[winner_source] = winner_assignment
    return source_best, final


def village_assignment_stage(method: str) -> int:
    if method == "village_exact_subdistrict":
        return 0
    if method == "village_exact_relaxed_subdistrict":
        return 1
    if method in {"village_exact_child_supported", "village_exact_relaxed_child_supported"}:
        return 2
    if method == "village_fuzzy_subdistrict":
        return 3
    if method in {"village_exact_district", "village_exact_relaxed_district"}:
        return 4
    return 5


def staged_unique_assignments(
    candidates_by_source: dict[object, list[tuple[int, str, float]]],
    *,
    source_order: dict[object, int],
    min_score: float,
    min_margin: float,
) -> tuple[dict[object, HierarchyAssignment], dict[object, HierarchyAssignment]]:
    source_best: dict[object, HierarchyAssignment] = {}
    final: dict[object, HierarchyAssignment] = {}
    used_targets: set[int] = set()

    stages = sorted({village_assignment_stage(method) for candidates in candidates_by_source.values() for _, method, _ in candidates})
    for stage in stages:
        stage_candidates: dict[object, list[tuple[int, str, float]]] = {}
        for source_key, candidates in candidates_by_source.items():
            if source_key in final:
                continue
            filtered = [
                (target_code, method, score)
                for target_code, method, score in candidates
                if target_code not in used_targets and village_assignment_stage(method) == stage
            ]
            if filtered:
                stage_candidates[source_key] = filtered
        if not stage_candidates:
            continue
        stage_best, stage_final = best_unique_assignments(
            stage_candidates,
            source_order=source_order,
            min_score=min_score,
            min_margin=min_margin,
        )
        source_best.update({key: value for key, value in stage_best.items() if key not in source_best})
        for source_key, assignment in stage_final.items():
            if source_key in final or assignment.target_code in used_targets:
                continue
            final[source_key] = assignment
            used_targets.add(assignment.target_code)
    return source_best, final


def subdistrict_child_overlap(
    block: SourceBlock,
    reference: SubdistrictReference,
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
) -> tuple[int, float, float]:
    source_names = set(block.village_relaxed_norms or block.village_norms)
    reference_names = village_relaxed_names_by_subdistrict.get(reference.subdistrict_code, set())
    if not source_names or not reference_names:
        return 0, 0.0, 0.0
    overlap_count = len(source_names & reference_names)
    if overlap_count == 0:
        return 0, 0.0, 0.0
    source_ratio = overlap_count / len(source_names)
    min_ratio = overlap_count / max(min(len(source_names), len(reference_names)), 1)
    return overlap_count, source_ratio, min_ratio


def child_overlap_confirmed(
    overlap_count: int,
    source_ratio: float,
    min_ratio: float,
    *,
    min_overlap: int,
    min_ratio_required: float,
) -> bool:
    if overlap_count < min_overlap:
        return False
    return min_ratio >= min_ratio_required or source_ratio >= 0.35


def subdistrict_candidate_score(
    block: SourceBlock,
    reference: SubdistrictReference,
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
    *,
    min_child_overlap: int,
    min_child_overlap_ratio: float,
) -> tuple[str, float] | None:
    overlap_count, source_ratio, min_ratio = subdistrict_child_overlap(
        block,
        reference,
        village_relaxed_names_by_subdistrict,
    )
    has_child_overlap = child_overlap_confirmed(
        overlap_count,
        source_ratio,
        min_ratio,
        min_overlap=min_child_overlap,
        min_ratio_required=min_child_overlap_ratio,
    )
    child_score = (
        min(0.985, 0.82 + (0.10 * min(min_ratio, 1.0)) + (0.15 * min(source_ratio, 1.0)))
        if has_child_overlap
        else 0.0
    )
    boundary_scope = "district" if block.district_code == reference.district_code else "state_boundary"

    if block.block_norm and block.block_norm == reference.subdistrict_norm:
        score = 1.0 if boundary_scope == "district" else max(0.985, child_score)
        return f"subdistrict_exact_{boundary_scope}", score
    if block.block_relaxed_norm and block.block_relaxed_norm == reference.subdistrict_relaxed_norm:
        score = 0.995 if boundary_scope == "district" else max(0.98, child_score)
        return f"subdistrict_exact_relaxed_{boundary_scope}", score
    cheap = simple_similarity(block.block_name, reference.subdistrict_name)
    name_score = score_candidate(block.block_name, reference.subdistrict_name).score if cheap >= 0.45 else 0.0
    if has_child_overlap:
        score = max(child_score, min(0.985, name_score))
        return f"subdistrict_child_overlap_{boundary_scope}", score
    if name_score < 0.70:
        return None
    return f"subdistrict_fuzzy_{boundary_scope}", name_score


def match_source_blocks_to_subdistricts(
    source_blocks: dict[tuple[int, int, str], SourceBlock],
    subdistricts_by_district: dict[int, dict[int, SubdistrictReference]],
    subdistrict_by_code: dict[int, SubdistrictReference],
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
    state_village_relaxed_subdistricts: dict[int, dict[str, set[int]]],
    *,
    min_score: float,
    min_margin: float,
    min_child_overlap: int,
    min_child_overlap_ratio: float,
) -> tuple[
    dict[tuple[int, int, str], HierarchyAssignment],
    dict[tuple[int, int, str], HierarchyAssignment],
    dict[tuple[int, int, str], list[HierarchyAssignment]],
]:
    candidates_by_source: dict[object, list[tuple[int, str, float]]] = {}
    source_order: dict[object, int] = {}
    for key, block in source_blocks.items():
        source_order[key] = block.source_order
        references: dict[int, SubdistrictReference] = dict(subdistricts_by_district.get(block.district_code, {}))
        overlap_counts: Counter[int] = Counter()
        state_index = state_village_relaxed_subdistricts.get(block.state_code, {})
        for village_name in block.village_relaxed_norms:
            overlap_counts.update(state_index.get(village_name, ()))
        for subdistrict_code, overlap_count in overlap_counts.items():
            if overlap_count < min_child_overlap:
                continue
            reference = subdistrict_by_code.get(subdistrict_code)
            if reference is not None and reference.state_code == block.state_code:
                references.setdefault(subdistrict_code, reference)
        candidates: list[tuple[int, str, float]] = []
        for reference in references.values():
            scored = subdistrict_candidate_score(
                block,
                reference,
                village_relaxed_names_by_subdistrict,
                min_child_overlap=min_child_overlap,
                min_child_overlap_ratio=min_child_overlap_ratio,
            )
            if scored is None:
                continue
            method, score = scored
            candidates.append((reference.subdistrict_code, method, score))
        if candidates:
            candidates_by_source[key] = candidates

    source_best, final = staged_unique_assignments(
        candidates_by_source,
        source_order=source_order,
        min_score=min_score,
        min_margin=min_margin,
    )
    source_options: dict[tuple[int, int, str], list[HierarchyAssignment]] = {}
    for key, candidates in candidates_by_source.items():
        if not isinstance(key, tuple):
            continue
        ranked = sorted(candidates, key=lambda item: (-item[2], item[0], item[1]))
        if not ranked:
            continue
        best_score = ranked[0][2]
        options: list[HierarchyAssignment] = []
        for target_code, method, score in ranked:
            if score < min_score and score < 0.98:
                continue
            if best_score - score > 0.08:
                continue
            options.append(
                HierarchyAssignment(
                    target_code=target_code,
                    method=method,
                    score=score,
                    margin=best_score - score,
                    candidate_count=len(ranked),
                )
            )
        if options:
            source_options[key] = options
    return (
        {key: value for key, value in source_best.items() if isinstance(key, tuple)},
        {key: value for key, value in final.items() if isinstance(key, tuple)},
        source_options,
    )


def dedupe_rural_references(candidates: Iterable[RuralReference]) -> list[RuralReference]:
    seen: set[int] = set()
    result: list[RuralReference] = []
    for candidate in candidates:
        if candidate.village_code in seen:
            continue
        seen.add(candidate.village_code)
        result.append(candidate)
    return result


def village_candidate_score(
    source: RuralSourceVillage,
    reference: RuralReference,
    *,
    assigned_subdistrict_code: int,
    source_block: SourceBlock | None,
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
    allow_unconfirmed_cross_subdistrict: bool,
) -> tuple[str, float] | None:
    same_subdistrict = reference.subdistrict_code == assigned_subdistrict_code
    child_supported = False
    if source_block is not None and not same_subdistrict:
        overlap_count, source_ratio, min_ratio = subdistrict_child_overlap(
            source_block,
            SubdistrictReference(
                state_code=reference.state_code,
                state_name=reference.state_name,
                district_code=reference.district_code,
                district_name=reference.district_name,
                district_census2011_code=reference.district_census2011_code,
                subdistrict_code=reference.subdistrict_code,
                subdistrict_name=reference.subdistrict_name,
                subdistrict_census2011_code=reference.subdistrict_census2011_code,
                subdistrict_norm=reference.subdistrict_norm,
                subdistrict_relaxed_norm=normalize_relaxed_place(reference.subdistrict_name),
                signatures=signatures(reference.subdistrict_name),
            ),
            village_relaxed_names_by_subdistrict,
        )
        child_supported = child_overlap_confirmed(
            overlap_count,
            source_ratio,
            min_ratio,
            min_overlap=3,
            min_ratio_required=0.08,
        )
    scope = "subdistrict" if same_subdistrict else "child_supported" if child_supported else "district"
    if not same_subdistrict and not child_supported and not allow_unconfirmed_cross_subdistrict:
        return None
    if source.village_norm and source.village_norm == reference.village_norm:
        if same_subdistrict:
            return "village_exact_subdistrict", 1.0
        return f"village_exact_{scope}", 0.99 if child_supported else 0.965
    if source.village_relaxed_norm and source.village_relaxed_norm == reference.village_relaxed_norm:
        if same_subdistrict:
            return "village_exact_relaxed_subdistrict", 0.995
        return f"village_exact_relaxed_{scope}", 0.985 if child_supported else 0.96
    if not same_subdistrict:
        return None
    cheap = simple_similarity(source.village_name, reference.village_name)
    if cheap < 0.70:
        return None
    score = score_candidate(source.village_name, reference.village_name).score
    if source.village_relaxed_norm and source.village_relaxed_norm == reference.village_relaxed_norm:
        score = max(score, 0.98)
    if score < 0.70:
        return None
    return "village_fuzzy_subdistrict", score


def village_candidates_for_source(
    source: RuralSourceVillage,
    subdistrict_code: int,
    source_block: SourceBlock | None,
    villages_by_subdistrict: dict[int, list[RuralReference]],
    village_norm_index: dict[int, dict[str, list[RuralReference]]],
    village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    village_signature_index: dict[int, dict[str, list[RuralReference]]],
    district_village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    state_village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    bulk_district_targets: dict[tuple[int, int], list[int]],
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
    *,
    fallback_all_limit: int,
) -> list[tuple[int, str, float]]:
    candidate_refs: dict[int, tuple[RuralReference, bool]] = {}
    def add_candidate(reference: RuralReference, *, allow_unconfirmed_cross_subdistrict: bool) -> None:
        existing = candidate_refs.get(reference.village_code)
        if existing is None or (allow_unconfirmed_cross_subdistrict and not existing[1]):
            candidate_refs[reference.village_code] = (reference, allow_unconfirmed_cross_subdistrict)

    if source.village_norm:
        for reference in village_norm_index.get(subdistrict_code, {}).get(source.village_norm, ()):
            add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)
    if source.village_relaxed_norm:
        for reference in village_relaxed_index.get(subdistrict_code, {}).get(source.village_relaxed_norm, ()):
            add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)
    if not candidate_refs:
        for signature in source.village_signatures:
            for reference in village_signature_index.get(subdistrict_code, {}).get(signature, ()):
                add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)
    if source.village_relaxed_norm:
        for reference in district_village_relaxed_index.get(source.district_code, {}).get(source.village_relaxed_norm, ()):
            add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)
        for target_district_code in bulk_district_targets.get((source.state_code, source.district_code), ()):
            for reference in district_village_relaxed_index.get(target_district_code, {}).get(source.village_relaxed_norm, ()):
                add_candidate(reference, allow_unconfirmed_cross_subdistrict=False)
        state_candidates = state_village_relaxed_index.get(source.state_code, {}).get(source.village_relaxed_norm, ())
        if len(state_candidates) <= 3:
            for reference in state_candidates:
                add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)
    references = villages_by_subdistrict.get(subdistrict_code, ())
    if not candidate_refs and len(references) <= fallback_all_limit:
        for reference in references:
            add_candidate(reference, allow_unconfirmed_cross_subdistrict=True)

    scored: list[tuple[int, str, float]] = []
    for reference, allow_unconfirmed_cross_subdistrict in candidate_refs.values():
        candidate = village_candidate_score(
            source,
            reference,
            assigned_subdistrict_code=subdistrict_code,
            source_block=source_block,
            village_relaxed_names_by_subdistrict=village_relaxed_names_by_subdistrict,
            allow_unconfirmed_cross_subdistrict=allow_unconfirmed_cross_subdistrict,
        )
        if candidate is None:
            continue
        method, score = candidate
        scored.append((reference.village_code, method, score))
    return scored


def match_source_rows_to_villages(
    source_villages: Sequence[RuralSourceVillage],
    source_blocks: dict[tuple[int, int, str], SourceBlock],
    block_matches: dict[tuple[int, int, str], HierarchyAssignment],
    block_options: dict[tuple[int, int, str], list[HierarchyAssignment]],
    villages_by_subdistrict: dict[int, list[RuralReference]],
    village_norm_index: dict[int, dict[str, list[RuralReference]]],
    village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    village_signature_index: dict[int, dict[str, list[RuralReference]]],
    district_village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    state_village_relaxed_index: dict[int, dict[str, list[RuralReference]]],
    bulk_district_targets: dict[tuple[int, int], list[int]],
    village_relaxed_names_by_subdistrict: dict[int, set[str]],
    village_by_code: dict[int, RuralReference],
    *,
    min_score: float,
    min_margin: float,
    fallback_all_limit: int,
) -> tuple[dict[int, RuralVillageAssignment], dict[int, HierarchyAssignment]]:
    candidates_by_source: dict[object, list[tuple[int, str, float]]] = {}
    source_order: dict[object, int] = {}
    for source in source_villages:
        block_key = (source.state_code, source.district_code, source.block_norm)
        candidate_block_assignments = []
        block_assignment = block_matches.get(block_key)
        if block_assignment is not None:
            candidate_block_assignments = [block_assignment]
        else:
            candidate_block_assignments = block_options.get(block_key, [])
        if not candidate_block_assignments:
            continue
        source_order[source.source_id] = source.source_id
        source_block = source_blocks.get(block_key)
        candidates: list[tuple[int, str, float]] = []
        for candidate_block_assignment in candidate_block_assignments:
            candidates.extend(
                village_candidates_for_source(
                    source,
                    candidate_block_assignment.target_code,
                    source_block,
                    villages_by_subdistrict,
                    village_norm_index,
                    village_relaxed_index,
                    village_signature_index,
                    district_village_relaxed_index,
                    state_village_relaxed_index,
                    bulk_district_targets,
                    village_relaxed_names_by_subdistrict,
                    fallback_all_limit=fallback_all_limit,
                )
            )
        if candidates:
            best_by_target: dict[int, tuple[int, str, float]] = {}
            for target_code, method, score in candidates:
                existing = best_by_target.get(target_code)
                if existing is None or score > existing[2]:
                    best_by_target[target_code] = (target_code, method, score)
            candidates_by_source[source.source_id] = list(best_by_target.values())

    source_best, final = best_unique_assignments(
        candidates_by_source,
        source_order=source_order,
        min_score=min_score,
        min_margin=min_margin,
    )

    final_assignments: dict[int, RuralVillageAssignment] = {}
    for source_key, assignment in final.items():
        if not isinstance(source_key, int):
            continue
        reference = village_by_code.get(assignment.target_code)
        if reference is None:
            continue
        final_assignments[source_key] = RuralVillageAssignment(
            reference=reference,
            method=assignment.method,
            score=assignment.score,
            margin=assignment.margin,
            candidate_count=assignment.candidate_count,
        )
    return final_assignments, {key: value for key, value in source_best.items() if isinstance(key, int)}


def pan_india_header() -> list[str]:
    count_columns: list[str] = []
    for species in SPECIES:
        count_columns.extend([f"{species}_male", f"{species}_female", f"{species}_total"])
    return [
        "village_code",
        "row_index",
        "location_type",
        "state_code",
        "state_name",
        "district_code",
        "district_name",
        "subdistrict_code",
        "subdistrict_name",
        "local_body_code",
        "local_body_name",
        "village_name",
        "town_name",
        "ward_code",
        "ward_number",
        "ward_name",
        *count_columns,
    ]


def audit_header() -> list[str]:
    return [
        "source_row",
        "match_status",
        "unmatched_reason",
        "winning_source_row",
        "source_state_code",
        "source_state_name",
        "source_district_code",
        "source_district_name",
        "source_block_name",
        "source_village_name",
        "lgd_state_code",
        "lgd_state_name",
        "lgd_district_code",
        "lgd_district_name",
        "lgd_subdistrict_code",
        "lgd_subdistrict_name",
        "lgd_village_code",
        "lgd_village_name",
        "match_method",
        "match_score",
        "match_margin",
        "match_candidate_count",
    ]


def pan_india_row(source: RuralSourceVillage, assignment: RuralVillageAssignment) -> dict[str, object]:
    reference = assignment.reference
    return {
        "village_code": reference.village_code,
        "row_index": source.source_id,
        "location_type": "rural",
        "state_code": reference.state_code,
        "state_name": reference.state_name,
        "district_code": reference.district_code,
        "district_name": reference.district_name,
        "subdistrict_code": reference.subdistrict_code,
        "subdistrict_name": reference.subdistrict_name,
        "local_body_code": reference.local_body_code or "",
        "local_body_name": reference.local_body_name,
        "village_name": reference.village_name,
        "town_name": "",
        "ward_code": "",
        "ward_number": "",
        "ward_name": "",
        **source.counts,
    }


def audit_row(
    source: RuralSourceRow,
    *,
    status: str,
    reason: str,
    assignment: RuralVillageAssignment | None = None,
    best_assignment: HierarchyAssignment | None = None,
    block_assignment: HierarchyAssignment | None = None,
    block_reference: SubdistrictReference | None = None,
    village_by_code: dict[int, RuralReference] | None = None,
    winning_source_row: int | None = None,
) -> dict[str, object]:
    reference = assignment.reference if assignment else None
    if reference is None and best_assignment is not None and village_by_code is not None:
        reference = village_by_code.get(best_assignment.target_code)
    method = assignment.method if assignment else best_assignment.method if best_assignment else block_assignment.method if block_assignment else ""
    score = assignment.score if assignment else best_assignment.score if best_assignment else block_assignment.score if block_assignment else 0.0
    margin = assignment.margin if assignment else best_assignment.margin if best_assignment else block_assignment.margin if block_assignment else 0.0
    candidate_count = (
        assignment.candidate_count
        if assignment
        else best_assignment.candidate_count
        if best_assignment
        else block_assignment.candidate_count
        if block_assignment
        else 0
    )
    return {
        "source_row": source.source_row,
        "match_status": status,
        "unmatched_reason": reason,
        "winning_source_row": winning_source_row or "",
        "source_state_code": source.state_code or "",
        "source_state_name": source.state_name,
        "source_district_code": source.district_code or "",
        "source_district_name": source.district_name,
        "source_block_name": source.block_name,
        "source_village_name": source.village_name,
        "lgd_state_code": reference.state_code if reference else block_reference.state_code if block_reference else "",
        "lgd_state_name": reference.state_name if reference else block_reference.state_name if block_reference else "",
        "lgd_district_code": reference.district_code if reference else block_reference.district_code if block_reference else "",
        "lgd_district_name": reference.district_name if reference else block_reference.district_name if block_reference else "",
        "lgd_subdistrict_code": reference.subdistrict_code if reference else block_reference.subdistrict_code if block_reference else "",
        "lgd_subdistrict_name": reference.subdistrict_name if reference else block_reference.subdistrict_name if block_reference else "",
        "lgd_village_code": reference.village_code if reference else "",
        "lgd_village_name": reference.village_name if reference else "",
        "match_method": method,
        "match_score": round(score, 6),
        "match_margin": round(margin, 6),
        "match_candidate_count": candidate_count,
    }


def write_hierarchical_rural_outputs(
    source: Path,
    output_dir: Path,
    rural_references: Sequence[RuralReference],
    args: argparse.Namespace,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_rows, source_counts = load_rural_source_rows(source)
    source_blocks = build_source_blocks(source_rows)
    (
        subdistricts_by_district,
        subdistrict_by_code,
        villages_by_subdistrict,
        village_norm_index,
        village_relaxed_index,
        village_signature_index,
        district_village_relaxed_index,
        state_village_relaxed_index,
        village_by_code,
        village_relaxed_names_by_subdistrict,
        state_village_relaxed_subdistricts,
    ) = build_rural_hierarchy_indexes(rural_references)
    source_villages = build_source_villages(source_rows)
    bulk_district_targets = build_bulk_district_targets(
        source_villages,
        rural_references,
        min_overlap=args.min_bulk_district_overlap,
        min_source_ratio=args.min_bulk_district_source_ratio,
        min_target_ratio=args.min_bulk_district_target_ratio,
    )
    source_village_by_row: dict[int, RuralSourceVillage] = {}
    for source_village in source_villages:
        for row in source_village.rows:
            source_village_by_row[row.source_row] = source_village
    block_best, block_matches, block_options = match_source_blocks_to_subdistricts(
        source_blocks,
        subdistricts_by_district,
        subdistrict_by_code,
        village_relaxed_names_by_subdistrict,
        state_village_relaxed_subdistricts,
        min_score=args.subdistrict_auto_accept_score,
        min_margin=args.min_margin,
        min_child_overlap=args.min_subdistrict_child_overlap,
        min_child_overlap_ratio=args.min_subdistrict_child_overlap_ratio,
    )
    village_matches, village_best = match_source_rows_to_villages(
        source_villages,
        source_blocks,
        block_matches,
        block_options,
        villages_by_subdistrict,
        village_norm_index,
        village_relaxed_index,
        village_signature_index,
        district_village_relaxed_index,
        state_village_relaxed_index,
        bulk_district_targets,
        village_relaxed_names_by_subdistrict,
        village_by_code,
        min_score=args.rural_auto_accept_score,
        min_margin=args.min_margin,
        fallback_all_limit=args.fallback_all_candidate_limit,
    )

    village_winners: dict[int, int] = {
        assignment.reference.village_code: source_row
        for source_row, assignment in village_matches.items()
    }
    pan_india_path = output_dir / "livestock_pan_india.csv"
    aligned_path = output_dir / "livestock_lgd_aligned.csv"
    all_path = output_dir / "livestock_lgd_alignment_all.csv"
    unmatched_path = output_dir / "livestock_lgd_unmatched.csv"
    reason_counts: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()
    block_method_counts: Counter[str] = Counter(assignment.method for assignment in block_matches.values())
    examples_by_reason: dict[str, list[dict[str, object]]] = defaultdict(list)

    compact_header = pan_india_header()
    detailed_header = audit_header()
    with (
        pan_india_path.open("w", encoding="utf-8", newline="") as pan_handle,
        aligned_path.open("w", encoding="utf-8", newline="") as aligned_handle,
        all_path.open("w", encoding="utf-8", newline="") as all_handle,
        unmatched_path.open("w", encoding="utf-8", newline="") as unmatched_handle,
    ):
        pan_writer = csv.DictWriter(pan_handle, fieldnames=compact_header)
        aligned_writer = csv.DictWriter(aligned_handle, fieldnames=compact_header)
        all_writer = csv.DictWriter(all_handle, fieldnames=detailed_header)
        unmatched_writer = csv.DictWriter(unmatched_handle, fieldnames=detailed_header)
        pan_writer.writeheader()
        aligned_writer.writeheader()
        all_writer.writeheader()
        unmatched_writer.writeheader()

        for source_village in source_villages:
            assignment = village_matches.get(source_village.source_id)
            if assignment is None:
                continue
            compact = pan_india_row(source_village, assignment)
            pan_writer.writerow(compact)
            aligned_writer.writerow(compact)
            method_counts[assignment.method] += 1

        for source_row in source_rows:
            source_village = source_village_by_row.get(source_row.source_row)
            block_key = (
                source_row.state_code,
                source_row.district_code,
                source_row.block_norm,
            )
            block_assignment = block_matches.get(block_key) if None not in block_key else None
            block_option = None
            if block_assignment is None and None not in block_key:
                options = block_options.get(block_key, ())
                block_option = options[0] if options else None
            block_reference = None
            if block_assignment is not None and source_row.district_code is not None:
                block_reference = subdistrict_by_code.get(block_assignment.target_code)
            elif block_option is not None:
                block_reference = subdistrict_by_code.get(block_option.target_code)
            assignment = village_matches.get(source_village.source_id) if source_village else None
            if assignment is not None:
                all_writer.writerow(
                    audit_row(
                        source_row,
                        status="matched",
                        reason="",
                        assignment=assignment,
                        block_assignment=block_assignment,
                        block_reference=block_reference,
                    )
                )
                continue

            best_assignment = village_best.get(source_village.source_id) if source_village else None
            if source_village is None or source_row.state_code is None or source_row.district_code is None or not source_row.block_norm:
                reason = "invalid_or_missing_district_block"
            elif block_assignment is None:
                reason = "subdistrict_unmatched" if block_option is None else "village_unmatched"
                block_assignment = block_best.get(block_key)
                if block_assignment is None:
                    block_assignment = block_option
                if block_assignment is not None:
                    block_reference = subdistrict_by_code.get(block_assignment.target_code)
            elif best_assignment is None:
                reason = "village_unmatched"
            else:
                reason = "duplicate_village_dropped"
            winning_source_row = (
                village_winners.get(best_assignment.target_code)
                if best_assignment is not None
                else None
            )
            reason_counts[reason] += 1
            detailed = audit_row(
                source_row,
                status="unmatched",
                reason=reason,
                best_assignment=best_assignment,
                block_assignment=block_assignment,
                block_reference=block_reference,
                village_by_code=village_by_code,
                winning_source_row=winning_source_row,
            )
            all_writer.writerow(detailed)
            unmatched_writer.writerow(detailed)
            if len(examples_by_reason[reason]) < 25:
                examples_by_reason[reason].append(
                    {
                        "source_row": source_row.source_row,
                        "source_state_name": source_row.state_name,
                        "source_district_name": source_row.district_name,
                        "source_block_name": source_row.block_name,
                        "source_village_name": source_row.village_name,
                        "lgd_subdistrict_name": detailed["lgd_subdistrict_name"],
                        "lgd_village_name": detailed["lgd_village_name"],
                        "winning_source_row": detailed["winning_source_row"],
                    }
                )

    duplicate_check: Counter[int] = Counter()
    for assignment in village_matches.values():
        duplicate_check[assignment.reference.village_code] += 1
    duplicate_village_codes = [code for code, count in duplicate_check.items() if count > 1]
    matched_source_rows = sum(
        len(source_village.rows)
        for source_village in source_villages
        if source_village.source_id in village_matches
    )
    counts = {
        **dict(source_counts),
        "source_village_groups": len(source_villages),
        "rural_output_rows": len(village_matches),
        "rural_matched_rows": matched_source_rows,
        "rural_unmatched_rows": len(source_rows) - matched_source_rows,
        "source_blocks": len(source_blocks),
        "matched_source_blocks": len(block_matches),
        "source_blocks_with_candidate_subdistricts": len(block_options),
        "bulk_district_target_sources": len(bulk_district_targets),
        "unmatched_source_blocks": len(source_blocks) - len(block_matches),
        "duplicate_village_codes_in_output": len(duplicate_village_codes),
    }
    analysis_path = output_dir / "livestock_lgd_unmatched_analysis.json"
    analysis = {
        "generated_at": utc_now(),
        "remaining_unmatched_category_counts": dict(sorted(reason_counts.items())),
        "examples_by_category": dict(examples_by_reason),
        "duplicate_guard": {
            "duplicate_village_codes_in_output": len(duplicate_village_codes),
            "extra_duplicate_rows_in_output": sum(duplicate_check.values()) - len(duplicate_check),
        },
    }
    with analysis_path.open("w", encoding="utf-8") as handle:
        json.dump(analysis, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    return {
        "paths": {
            "pan_india_csv": as_repo_path(pan_india_path),
            "matched_csv": as_repo_path(aligned_path),
            "all_alignment_csv": as_repo_path(all_path),
            "unmatched_csv": as_repo_path(unmatched_path),
            "unmatched_analysis_json": as_repo_path(analysis_path),
        },
        "counts": counts,
        "method_counts": dict(sorted(method_counts.items())),
        "block_method_counts": dict(sorted(block_method_counts.items())),
        "unmatched_reason_counts": dict(sorted(reason_counts.items())),
    }


def build_hierarchical_summary(
    *,
    started_at: str,
    elapsed_seconds: float,
    rural_reference_rows: int,
    outputs: dict[str, object],
    args: argparse.Namespace,
) -> dict[str, object]:
    counts = outputs["counts"]
    rural_rows = int(counts["rural_source_rows"]) or 1
    matched_rows = int(counts["rural_matched_rows"])
    return {
        "started_at": started_at,
        "finished_at": utc_now(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "inputs": {
            "source": as_repo_path(args.source),
            "gp_mapping": as_repo_path(args.gp_mapping),
        },
        "parameters": {
            "subdistrict_auto_accept_score": args.subdistrict_auto_accept_score,
            "rural_auto_accept_score": args.rural_auto_accept_score,
            "min_margin": args.min_margin,
            "min_subdistrict_child_overlap": args.min_subdistrict_child_overlap,
            "min_subdistrict_child_overlap_ratio": args.min_subdistrict_child_overlap_ratio,
            "min_bulk_district_overlap": args.min_bulk_district_overlap,
            "min_bulk_district_source_ratio": args.min_bulk_district_source_ratio,
            "min_bulk_district_target_ratio": args.min_bulk_district_target_ratio,
            "fallback_all_candidate_limit": args.fallback_all_candidate_limit,
        },
        "reference_rows": {
            "rural_gp_mapping": rural_reference_rows,
        },
        "rows": {
            **counts,
            "rural_match_rate": round(matched_rows / rural_rows, 6),
        },
        "method_counts": outputs["method_counts"],
        "block_method_counts": outputs["block_method_counts"],
        "unmatched_reason_counts": outputs["unmatched_reason_counts"],
        "outputs": outputs["paths"],
        "notes": [
            "Only source rows with location.type = rural are processed.",
            "District IDs from the source are used first, with same-state boundary-split fallbacks only when child village overlap supports them.",
            "Source blocks keep a short list of candidate LGD subdistricts when child villages indicate that one source block spans multiple LGD subdistricts.",
            "Source districts can use bulk district-split fallbacks only when a large majority of child village names points to another LGD district.",
            "Village rows are grouped by source district/block/base-village label before matching, so ward fragments can be summed into one output village.",
            "Village assignment is staged: weaker fallback stages can only fill still-unmatched source groups and unused LGD village IDs.",
            "Villages are assigned one-to-one by LGD village_code; ambiguous duplicate LGD village names remain unmatched.",
            "The final pan-India CSV is unique by village_code.",
        ],
    }


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
    parser = argparse.ArgumentParser(description="Prepare a unique rural LGD village livestock layer.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--gp-mapping", type=Path, default=DEFAULT_GP_MAPPING)
    parser.add_argument("--urban-wards", type=Path, default=DEFAULT_URBAN_WARDS, help=argparse.SUPPRESS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-candidates", type=int, default=100, help=argparse.SUPPRESS)
    parser.add_argument(
        "--subdistrict-auto-accept-score",
        type=float,
        default=0.86,
        help="Minimum fuzzy score for block-to-subdistrict assignment inside a district.",
    )
    parser.add_argument(
        "--min-subdistrict-child-overlap",
        type=int,
        default=3,
        help="Minimum overlapping child village names needed to use child-overlap subdistrict matching.",
    )
    parser.add_argument(
        "--min-subdistrict-child-overlap-ratio",
        type=float,
        default=0.08,
        help="Minimum child-overlap ratio against the smaller source/reference village-name set.",
    )
    parser.add_argument("--rural-auto-accept-score", type=float, default=0.88)
    parser.add_argument("--urban-auto-accept-score", type=float, default=0.88, help=argparse.SUPPRESS)
    parser.add_argument("--min-margin", type=float, default=0.035)
    parser.add_argument("--min-village-score", type=float, default=0.82, help=argparse.SUPPRESS)
    parser.add_argument(
        "--fallback-all-candidate-limit",
        type=int,
        default=125,
        help="Score all villages in a matched subdistrict when signature indexes produce no candidates and the subdistrict is small enough.",
    )
    parser.add_argument(
        "--min-bulk-district-overlap",
        type=int,
        default=100,
        help="Minimum exact child-village-name overlap needed to allow a source district to use another LGD district as a bulk fallback.",
    )
    parser.add_argument(
        "--min-bulk-district-source-ratio",
        type=float,
        default=0.50,
        help="Minimum share of source district village names that must point to the fallback district.",
    )
    parser.add_argument(
        "--min-bulk-district-target-ratio",
        type=float,
        default=0.75,
        help="Minimum overlap ratio against the smaller source/fallback district village-name set.",
    )
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
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(enable_state_fuzzy=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = utc_now()
    start = time.perf_counter()

    rural_references, _, _ = load_rural_references(args.gp_mapping)
    outputs = write_hierarchical_rural_outputs(
        args.source,
        args.output_dir,
        rural_references,
        args,
    )
    summary = build_hierarchical_summary(
        started_at=started_at,
        elapsed_seconds=time.perf_counter() - start,
        rural_reference_rows=len(rural_references),
        outputs=outputs,
        args=args,
    )
    summary_path = args.output_dir / "livestock_lgd_alignment_summary.json"
    summary["outputs"]["summary_json"] = as_repo_path(summary_path)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
