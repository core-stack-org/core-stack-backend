"""Hierarchy-aware approximate matching helpers.

This module is designed for user-entered administrative names where the input
is often:

- partially misspelled
- typed from sound rather than from official spelling
- romanized from Indic pronunciations in multiple valid ways
- entered at a hierarchy level where parent context matters

The scoring pipeline blends several widely used ideas from search, record
linkage, and entity resolution:

- Unicode normalization and token cleanup
- Levenshtein and Damerau-Levenshtein edit distance
- Jaro-Winkler similarity
- character n-gram overlap
- token overlap and token-alignment scoring
- Soundex-style phonetic bucketing
- additional heuristic phonetic normalization tuned for romanized South Asian
  place names
- weighted hierarchical ranking so state/district/tehsil context can jointly
  influence the final decision

Typical usage:

    from utilities.hierarchy_matching import (
        HierarchyCandidate,
        rank_candidates,
        resolve_best_hierarchy_candidate,
    )

    ranked = rank_candidates("kachar", ["Cachar", "Kamrup", "Dima Hasao"])
    resolution = resolve_best_hierarchy_candidate(
        candidates=[
            HierarchyCandidate(state="Assam", district="Cachar", tehsil="Lakhipur"),
            HierarchyCandidate(state="Assam", district="Kamrup", tehsil="Rangia"),
        ],
        state_query="assam",
        district_query="kachar",
        tehsil_query="lakhipur",
    )

See `utilities/README_hierarchy_matching.md` for a fuller explanation of the
algorithms, thresholds, and workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
import unicodedata
from typing import Iterable, Sequence


PHONETIC_REPLACEMENTS = (
    ("tch", "ch"),
    ("dge", "j"),
    ("dzh", "j"),
    ("zh", "j"),
    ("ph", "f"),
    ("bh", "b"),
    ("dh", "d"),
    ("gh", "g"),
    ("kh", "k"),
    ("qh", "k"),
    ("jh", "j"),
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
    ("uu", "u"),
    ("ee", "i"),
    ("ii", "i"),
    ("ie", "i"),
    ("aa", "a"),
    ("ah", "a"),
    ("ae", "a"),
    ("aw", "o"),
    ("au", "o"),
    ("oa", "o"),
    ("ow", "o"),
    ("ai", "e"),
    ("ay", "e"),
)

SOUNDEX_GROUPS = {
    **{ch: "1" for ch in "bfpvw"},
    **{ch: "2" for ch in "cgjkqsxz"},
    **{ch: "3" for ch in "dt"},
    "l": "4",
    **{ch: "5" for ch in "mn"},
    "r": "6",
}

BROAD_PHONETIC_GROUPS = {
    **{ch: "a" for ch in "aeiouy"},
    **{ch: "b" for ch in "bfpvw"},
    **{ch: "k" for ch in "cgjkqx"},
    **{ch: "s" for ch in "sz"},
    **{ch: "d" for ch in "dt"},
    "l": "l",
    **{ch: "n" for ch in "mn"},
    "r": "r",
    "h": "",
}


@dataclass(frozen=True)
class CandidateScore:
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
class CandidateResolution:
    best_match: CandidateScore | None
    alternatives: list[CandidateScore]
    accepted: bool
    margin: float
    reason: str


@dataclass(frozen=True)
class HierarchyCandidate:
    state: str
    district: str | None = None
    tehsil: str | None = None
    payload: object | None = None


@dataclass(frozen=True)
class HierarchyScore:
    candidate: HierarchyCandidate
    score: float
    state_score: float
    district_score: float
    tehsil_score: float


@dataclass(frozen=True)
class HierarchyResolution:
    best_match: HierarchyScore | None
    alternatives: list[HierarchyScore]
    accepted: bool
    margin: float
    reason: str


def normalize_match_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = re.sub(r"[&/,_()\-]+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def compact_match_text(value: str) -> str:
    return normalize_match_text(value).replace(" ", "")


def tokenize_match_text(value: str) -> list[str]:
    normalized = normalize_match_text(value)
    return [token for token in normalized.split(" ") if token]


def collapse_repeated_characters(value: str) -> str:
    return re.sub(r"(.)\1+", r"\1", value)


def apply_phonetic_replacements(value: str) -> str:
    transformed = value
    for source, target in PHONETIC_REPLACEMENTS:
        transformed = transformed.replace(source, target)
    transformed = re.sub(r"([bcdfgjklmnpqrstvwxyz])h", r"\1", transformed)
    transformed = collapse_repeated_characters(transformed)
    return transformed


def phonetic_form(value: str) -> str:
    compact = compact_match_text(value)
    if not compact:
        return ""
    return apply_phonetic_replacements(compact)


def broad_phonetic_form(value: str) -> str:
    phonetic = phonetic_form(value)
    if not phonetic:
        return ""

    broad = "".join(BROAD_PHONETIC_GROUPS.get(ch, ch) for ch in phonetic)
    broad = re.sub(r"a+", "a", broad)
    return collapse_repeated_characters(broad)


def consonant_signature(value: str) -> str:
    broad = broad_phonetic_form(value)
    if not broad:
        return ""
    consonants = re.sub(r"[aeiou]", "", broad)
    if consonants:
        return consonants
    return broad[:1]


def soundex_code(value: str) -> str:
    broad = broad_phonetic_form(value)
    if not broad:
        return ""

    first_letter = broad[0]
    digits: list[str] = []
    previous_digit = ""
    for char in broad[1:]:
        digit = SOUNDEX_GROUPS.get(char, "")
        if digit and digit != previous_digit:
            digits.append(digit)
        previous_digit = digit

    return (first_letter.upper() + "".join(digits) + "000")[:4]


def levenshtein_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            insertion = current[right_index - 1] + 1
            deletion = previous[right_index] + 1
            substitution = previous[right_index - 1] + (left_char != right_char)
            current.append(min(insertion, deletion, substitution))
        previous = current
    return previous[-1]


def damerau_levenshtein_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    distances: dict[tuple[int, int], int] = {}
    max_distance = len(left) + len(right)
    distances[(-1, -1)] = max_distance

    for index in range(len(left) + 1):
        distances[(index, -1)] = max_distance
        distances[(index, 0)] = index
    for index in range(len(right) + 1):
        distances[(-1, index)] = max_distance
        distances[(0, index)] = index

    last_seen: dict[str, int] = {}
    for left_index in range(1, len(left) + 1):
        match_column = 0
        for right_index in range(1, len(right) + 1):
            previous_match_row = last_seen.get(right[right_index - 1], 0)
            previous_match_column = match_column
            cost = 0 if left[left_index - 1] == right[right_index - 1] else 1
            if cost == 0:
                match_column = right_index

            distances[(left_index, right_index)] = min(
                distances[(left_index - 1, right_index - 1)] + cost,
                distances[(left_index, right_index - 1)] + 1,
                distances[(left_index - 1, right_index)] + 1,
                distances[(previous_match_row - 1, previous_match_column - 1)]
                + (left_index - previous_match_row - 1)
                + 1
                + (right_index - previous_match_column - 1),
            )
        last_seen[left[left_index - 1]] = left_index

    return distances[(len(left), len(right))]


def jaro_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0

    match_distance = (max(len(left), len(right)) // 2) - 1
    left_matches = [False] * len(left)
    right_matches = [False] * len(right)

    matches = 0
    transpositions = 0

    for left_index, left_char in enumerate(left):
        start = max(0, left_index - match_distance)
        end = min(left_index + match_distance + 1, len(right))
        for right_index in range(start, end):
            if right_matches[right_index] or right[right_index] != left_char:
                continue
            left_matches[left_index] = True
            right_matches[right_index] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    right_index = 0
    for left_index, matched in enumerate(left_matches):
        if not matched:
            continue
        while not right_matches[right_index]:
            right_index += 1
        if left[left_index] != right[right_index]:
            transpositions += 1
        right_index += 1

    return (
        (matches / len(left))
        + (matches / len(right))
        + ((matches - (transpositions / 2.0)) / matches)
    ) / 3.0


def jaro_winkler_similarity(left: str, right: str, prefix_scale: float = 0.1) -> float:
    jaro = jaro_similarity(left, right)
    prefix_length = 0
    for left_char, right_char in zip(left[:4], right[:4]):
        if left_char != right_char:
            break
        prefix_length += 1
    return jaro + (prefix_length * prefix_scale * (1.0 - jaro))


def similarity_ratio(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0

    sequence_score = SequenceMatcher(None, left, right).ratio()
    edit_score = 1.0 - (
        levenshtein_distance(left, right) / max(len(left), len(right), 1)
    )
    return max(0.0, min(1.0, (0.55 * sequence_score) + (0.45 * max(edit_score, 0.0))))


def character_ngram_set(value: str, n: int) -> set[str]:
    if not value:
        return set()
    if len(value) <= n:
        return {value}
    return {value[index : index + n] for index in range(len(value) - n + 1)}


def ngram_jaccard_score(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0

    scores = []
    for n in (2, 3):
        left_set = character_ngram_set(left, n)
        right_set = character_ngram_set(right, n)
        union = left_set | right_set
        if not union:
            scores.append(0.0)
            continue
        scores.append(len(left_set & right_set) / len(union))
    return sum(scores) / len(scores)


def token_jaccard_score(query_tokens: Sequence[str], candidate_tokens: Sequence[str]) -> float:
    if not query_tokens and not candidate_tokens:
        return 1.0
    if not query_tokens or not candidate_tokens:
        return 0.0
    query_set = set(query_tokens)
    candidate_set = set(candidate_tokens)
    return len(query_set & candidate_set) / len(query_set | candidate_set)


def token_alignment_score(query_tokens: Sequence[str], candidate_tokens: Sequence[str]) -> float:
    if not query_tokens and not candidate_tokens:
        return 1.0
    if not query_tokens or not candidate_tokens:
        return 0.0

    def average_best_match(left_tokens: Sequence[str], right_tokens: Sequence[str]) -> float:
        totals = []
        for left_token in left_tokens:
            totals.append(max(similarity_ratio(left_token, right_token) for right_token in right_tokens))
        return sum(totals) / len(totals)

    left_to_right = average_best_match(query_tokens, candidate_tokens)
    right_to_left = average_best_match(candidate_tokens, query_tokens)
    return (left_to_right + right_to_left) / 2.0


def score_candidate(query: str, candidate: str) -> CandidateScore:
    normalized_query = normalize_match_text(query)
    normalized_candidate = normalize_match_text(candidate)

    if not normalized_query or not normalized_candidate:
        return CandidateScore(
            candidate=candidate,
            normalized_candidate=normalized_candidate,
            score=0.0,
            sequence_score=0.0,
            edit_score=0.0,
            damerau_edit_score=0.0,
            jaro_winkler_score=0.0,
            token_score=0.0,
            prefix_score=0.0,
            substring_score=0.0,
            phonetic_score=0.0,
            broad_phonetic_score=0.0,
            consonant_score=0.0,
            ngram_score=0.0,
            soundex_score=0.0,
            token_alignment_score=0.0,
        )

    if normalized_query == normalized_candidate:
        return CandidateScore(
            candidate=candidate,
            normalized_candidate=normalized_candidate,
            score=1.0,
            sequence_score=1.0,
            edit_score=1.0,
            damerau_edit_score=1.0,
            jaro_winkler_score=1.0,
            token_score=1.0,
            prefix_score=1.0,
            substring_score=1.0,
            phonetic_score=1.0,
            broad_phonetic_score=1.0,
            consonant_score=1.0,
            ngram_score=1.0,
            soundex_score=1.0,
            token_alignment_score=1.0,
        )

    compact_query = compact_match_text(query)
    compact_candidate = compact_match_text(candidate)
    query_tokens = tokenize_match_text(query)
    candidate_tokens = tokenize_match_text(candidate)
    query_phonetic = phonetic_form(query)
    candidate_phonetic = phonetic_form(candidate)
    query_broad = broad_phonetic_form(query)
    candidate_broad = broad_phonetic_form(candidate)
    query_consonants = consonant_signature(query)
    candidate_consonants = consonant_signature(candidate)
    query_soundex = soundex_code(query)
    candidate_soundex = soundex_code(candidate)

    sequence_score = SequenceMatcher(None, normalized_query, normalized_candidate).ratio()
    edit_distance = levenshtein_distance(normalized_query, normalized_candidate)
    edit_score = 1.0 - (
        edit_distance / max(len(normalized_query), len(normalized_candidate), 1)
    )
    damerau_distance = damerau_levenshtein_distance(normalized_query, normalized_candidate)
    damerau_edit_score = 1.0 - (
        damerau_distance / max(len(normalized_query), len(normalized_candidate), 1)
    )
    jaro_winkler_score = jaro_winkler_similarity(normalized_query, normalized_candidate)
    token_score = token_jaccard_score(query_tokens, candidate_tokens)
    prefix_score = 1.0 if (
        normalized_candidate.startswith(normalized_query)
        or normalized_query.startswith(normalized_candidate)
        or candidate_phonetic.startswith(query_phonetic)
        or query_phonetic.startswith(candidate_phonetic)
    ) else 0.0
    substring_score = 1.0 if (
        normalized_query in normalized_candidate
        or normalized_candidate in normalized_query
        or query_phonetic in candidate_phonetic
        or candidate_phonetic in query_phonetic
    ) else 0.0
    phonetic_score = similarity_ratio(query_phonetic, candidate_phonetic)
    broad_phonetic_score = similarity_ratio(query_broad, candidate_broad)
    consonant_score = similarity_ratio(query_consonants, candidate_consonants)
    ngram_score = max(
        ngram_jaccard_score(compact_query, compact_candidate),
        ngram_jaccard_score(query_phonetic, candidate_phonetic),
    )
    soundex_score = similarity_ratio(query_soundex, candidate_soundex)
    aligned_token_score = token_alignment_score(query_tokens, candidate_tokens)

    combined = min(
        1.0,
        (0.10 * sequence_score)
        + (0.08 * max(edit_score, 0.0))
        + (0.08 * max(damerau_edit_score, 0.0))
        + (0.10 * jaro_winkler_score)
        + (0.08 * token_score)
        + (0.04 * prefix_score)
        + (0.02 * substring_score)
        + (0.12 * phonetic_score)
        + (0.08 * broad_phonetic_score)
        + (0.09 * consonant_score)
        + (0.07 * ngram_score)
        + (0.06 * soundex_score)
        + (0.08 * aligned_token_score),
    )

    return CandidateScore(
        candidate=candidate,
        normalized_candidate=normalized_candidate,
        score=combined,
        sequence_score=sequence_score,
        edit_score=max(edit_score, 0.0),
        damerau_edit_score=max(damerau_edit_score, 0.0),
        jaro_winkler_score=jaro_winkler_score,
        token_score=token_score,
        prefix_score=prefix_score,
        substring_score=substring_score,
        phonetic_score=phonetic_score,
        broad_phonetic_score=broad_phonetic_score,
        consonant_score=consonant_score,
        ngram_score=ngram_score,
        soundex_score=soundex_score,
        token_alignment_score=aligned_token_score,
    )


def rank_candidates(
    query: str,
    candidates: Sequence[str],
    *,
    limit: int = 3,
    min_score: float = 0.0,
) -> list[CandidateScore]:
    ranked = [score_candidate(query, candidate) for candidate in candidates]
    ranked = [entry for entry in ranked if entry.score >= min_score]
    ranked.sort(
        key=lambda entry: (
            -entry.score,
            -entry.phonetic_score,
            -entry.jaro_winkler_score,
            -entry.broad_phonetic_score,
            -entry.consonant_score,
            entry.normalized_candidate,
            entry.candidate,
        )
    )
    if limit >= 0:
        return ranked[:limit]
    return ranked


def match_margin(scores: Sequence[CandidateScore | HierarchyScore]) -> float:
    if not scores:
        return 0.0
    if len(scores) == 1:
        return scores[0].score
    return scores[0].score - scores[1].score


def is_confident_match(
    *,
    best_score: float,
    margin: float,
    auto_accept_score: float = 0.75,
    min_margin: float = 0.06,
    exact_score: float = 0.999,
) -> tuple[bool, str]:
    if best_score >= exact_score:
        return True, "exact"
    if best_score >= auto_accept_score and margin >= min_margin:
        return True, "high_confidence"
    return False, "ambiguous"


def resolve_best_candidate(
    query: str,
    candidates: Sequence[str],
    *,
    limit: int = 3,
    min_score: float = 0.0,
    auto_accept_score: float = 0.75,
    min_margin: float = 0.06,
) -> CandidateResolution:
    ranked = rank_candidates(query, candidates, limit=limit, min_score=min_score)
    if not ranked:
        return CandidateResolution(
            best_match=None,
            alternatives=[],
            accepted=False,
            margin=0.0,
            reason="no_candidates",
        )

    margin = match_margin(ranked)
    accepted, reason = is_confident_match(
        best_score=ranked[0].score,
        margin=margin,
        auto_accept_score=auto_accept_score,
        min_margin=min_margin,
    )
    return CandidateResolution(
        best_match=ranked[0],
        alternatives=ranked,
        accepted=accepted,
        margin=margin,
        reason=reason,
    )


def rank_hierarchy_candidates(
    *,
    candidates: Iterable[HierarchyCandidate],
    state_query: str | None = None,
    district_query: str | None = None,
    tehsil_query: str | None = None,
    weights: tuple[float, float, float] = (0.5, 0.3, 0.2),
    limit: int = 3,
) -> list[HierarchyScore]:
    provided_parts = [
        bool(state_query),
        bool(district_query),
        bool(tehsil_query),
    ]
    active_weights = [weight for weight, provided in zip(weights, provided_parts) if provided]
    weight_total = sum(active_weights) or 1.0

    ranked: list[HierarchyScore] = []
    for candidate in candidates:
        state_score = score_candidate(state_query or "", candidate.state).score if state_query else 0.0
        district_score = (
            score_candidate(district_query or "", candidate.district or "").score
            if district_query
            else 0.0
        )
        tehsil_score = (
            score_candidate(tehsil_query or "", candidate.tehsil or "").score
            if tehsil_query
            else 0.0
        )

        total = 0.0
        if state_query:
            total += weights[0] * state_score
        if district_query:
            total += weights[1] * district_score
        if tehsil_query:
            total += weights[2] * tehsil_score

        ranked.append(
            HierarchyScore(
                candidate=candidate,
                score=min(1.0, total / weight_total),
                state_score=state_score,
                district_score=district_score,
                tehsil_score=tehsil_score,
            )
        )

    ranked.sort(
        key=lambda entry: (
            -entry.score,
            -entry.state_score,
            -entry.district_score,
            -entry.tehsil_score,
            normalize_match_text(entry.candidate.state),
            normalize_match_text(entry.candidate.district or ""),
            normalize_match_text(entry.candidate.tehsil or ""),
        )
    )
    if limit >= 0:
        return ranked[:limit]
    return ranked


def resolve_best_hierarchy_candidate(
    *,
    candidates: Iterable[HierarchyCandidate],
    state_query: str | None = None,
    district_query: str | None = None,
    tehsil_query: str | None = None,
    weights: tuple[float, float, float] = (0.5, 0.3, 0.2),
    limit: int = 3,
    auto_accept_score: float = 0.84,
    min_margin: float = 0.06,
) -> HierarchyResolution:
    ranked = rank_hierarchy_candidates(
        candidates=candidates,
        state_query=state_query,
        district_query=district_query,
        tehsil_query=tehsil_query,
        weights=weights,
        limit=limit,
    )
    if not ranked:
        return HierarchyResolution(
            best_match=None,
            alternatives=[],
            accepted=False,
            margin=0.0,
            reason="no_candidates",
        )

    margin = match_margin(ranked)
    accepted, reason = is_confident_match(
        best_score=ranked[0].score,
        margin=margin,
        auto_accept_score=auto_accept_score,
        min_margin=min_margin,
    )
    return HierarchyResolution(
        best_match=ranked[0],
        alternatives=ranked,
        accepted=accepted,
        margin=margin,
        reason=reason,
    )
