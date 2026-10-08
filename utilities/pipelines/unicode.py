"""Small UTF-8/Unicode normalization helpers for final pipeline outputs."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any

import pandas as pd


def normalize_unicode_text(value: str) -> str:
    """Return safe NFC text while preserving Indic and other scripts.

    Invalid surrogate code points become the Unicode replacement character.
    NUL and non-whitespace C0 control characters are removed because they are
    invalid or unreliable in GeoPackage and JSON text fields.
    """

    safe = "".join(
        "\ufffd" if 0xD800 <= ord(character) <= 0xDFFF else character
        for character in value
    )
    normalized = unicodedata.normalize("NFC", safe)
    return "".join(
        character
        for character in normalized
        if character in {"\t", "\n", "\r"}
        or not unicodedata.category(character).startswith("C")
    )


def normalize_unicode_frame(frame: pd.DataFrame):
    """Normalize all textual cells in a DataFrame/GeoDataFrame in one call."""

    normalized = frame.copy()
    for position in range(len(normalized.columns)):
        series = normalized.iloc[:, position]
        if str(series.dtype) not in {"object", "string"}:
            continue
        normalized.iloc[:, position] = series.map(
            lambda value: normalize_unicode_text(value) if isinstance(value, str) else value
        )
    return normalized


def normalize_unicode_data(value: Any) -> Any:
    """Recursively normalize strings in JSON-compatible metadata structures."""

    if isinstance(value, str):
        return normalize_unicode_text(value)
    if isinstance(value, Mapping):
        return {
            normalize_unicode_text(str(key)): normalize_unicode_data(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [normalize_unicode_data(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_unicode_data(item) for item in value]
    if isinstance(value, set):
        return [normalize_unicode_data(item) for item in sorted(value, key=str)]
    return value
