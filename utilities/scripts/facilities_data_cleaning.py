#!/usr/bin/env python3
"""
Facilities Data Cleaning Script
================================
Modular script for cleaning raw facility CSV files from 'data/facilities/raw/'
and saving standardised outputs to 'data/facilities/cleaned/'.

INSTALLATION
------------
    pip install pandas numpy pyarrow spacy
    python -m spacy download en_core_web_sm

QUICK START
-----------
    # Clean a specific registered file
    python facilities_data_cleaning.py --file agri_industry

    # Clean all 7 registered files (including the new agri_industry reclassifier)
    python facilities_data_cleaning.py --file all

    # List available registered files
    python facilities_data_cleaning.py --list-files

    # Points-only extraction (any CSV with a uid + lat/lon columns)
    python facilities_data_cleaning.py --points-only path/to/file.csv \\
        --uid-col gid --lat-col latitude --lon-col longitude --prefix my_facility

    # Generic column-select + rename pipeline (any CSV)
    python facilities_data_cleaning.py --generic path/to/file.csv \\
        --keep-cols "gid,name,latitude,longitude" \\
        --rename "gid:facility_uid,name:facility_name" \\
        --output path/to/output.csv

    # Override input / output for a registered file
    python facilities_data_cleaning.py --file college \\
        --input data/other/college_v2.csv \\
        --output data/cleaned/college_v2_clean.csv

ARCHITECTURE
------------
┌───────────────────────────────────────────────────────────────┐
│  Section 1  │ Imports & Feature Detection                     │
│  Section 2  │ Constants & Logging                             │
│  Section 3  │ General I/O Functions (fast read/write)         │
│  Section 4  │ Pincode Centroid Lookup                         │
│  Section 5  │ General Cleaning Utilities                      │
│  Section 6  │ File Configurations (declarative registry)      │
│  Section 7  │ File-Specific Cleaning Pipelines (7 files)      │
│  Section 8  │ Generic / Points-Only Pipelines                 │
│  Section 9  │ CLI (argparse) & Entry Point                    │
└───────────────────────────────────────────────────────────────┘

Each registered file has:
  • columns_to_keep   – which raw columns to select
  • rename_map        – old_col → new_col
  • dtype_map         – new_col → target dtype
  • cleaner           – a function(df, **kw) → cleaned DataFrame

General functions handle:
  • Fast CSV I/O with pyarrow engine
  • Data-type enforcement (int, float, str)
  • 6-digit pincode validation
  • 4-digit year validation
  • Coordinate gap-filling from pincode centroids
  • NER-based institution name cleaning (spacy)
  • Stopword-aware proper casing
  • Geometry coordinate string parsing

AGRI-INDUSTRY RECLASSIFICATION
------------------------------
The `agri_industry` cleaner aggregates data from two major PMGSY 2023 source files:
  1. Industries (Layer 11) - 69k rows
  2. Agri Resources (Layer 10) - 48k rows

Total: ~118k facilities reclassified into categories defined in the external mapping file.

Each category is saved as a separate CSV file in `data/facilities/cleaned/`, prefixed with `agri_industry_`.
Detailed mapping stats are saved to `data/facilities/cleaned/AGRI_INDUSTRY_MAPPING_STATS.csv` upon running.

------------------------------
SCHOOLS (Reclassified Split)
------------------------------
Schools are now split into multiple files based on the `school_classification` 
column derived from the external `schools_reclassification.csv` mapping.

Outputs: `data/facilities/cleaned/school_{classification}.csv`
"""

from __future__ import annotations

# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 1 — Imports & Feature Detection                          ║
# ╚══════════════════════════════════════════════════════════════════════╝
import argparse
import ast
import json
import logging
import os
import re
import sys
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# --- Optional: pyarrow for fast CSV reads ---
try:
    import pyarrow  # noqa: F401
    CSV_ENGINE = "pyarrow"
except ImportError:
    CSV_ENGINE = "c"  # pandas default fast C parser

# --- Optional: spacy for NER cleaning ---
_NLP_CACHE: Any = None
HAS_SPACY = False
try:
    import spacy
    HAS_SPACY = True
except ImportError:
    pass


def _get_nlp():
    """Lazy-load and cache the spacy model (en_core_web_sm)."""
    global _NLP_CACHE
    if _NLP_CACHE is None:
        if not HAS_SPACY:
            warnings.warn(
                "spacy not installed — NER cleaning disabled. "
                "Install with: pip install spacy && python -m spacy download en_core_web_sm"
            )
            return None
        try:
            _NLP_CACHE = spacy.load("en_core_web_sm")
        except OSError:
            warnings.warn("spacy model 'en_core_web_sm' not found. Run: python -m spacy download en_core_web_sm")
            return None
    return _NLP_CACHE


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 2 — Constants & Logging                                   ║
# ╚══════════════════════════════════════════════════════════════════════╝
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "facilities" / "raw"
OUT_DIR = BASE_DIR / "data" / "facilities" / "cleaned"
PINCODE_FILE = BASE_DIR / "data" / "india_pincodes_centroid.csv"

# Stopwords kept lowercase in title-casing
TITLE_STOPWORDS = frozenset({
    "of", "the", "and", "in", "for", "at", "to", "a", "an",
    "by", "on", "or", "is", "as", "its", "with", "from",
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-7s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("facility_cleaner")


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 3 — General I/O Functions                                 ║
# ╚══════════════════════════════════════════════════════════════════════╝

def read_csv_fast(
    filepath: str | Path,
    usecols: Optional[List[str]] = None,
    dtype: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read a CSV with the fastest available engine.

    Parameters
    ----------
    filepath   : Path to CSV file.
    usecols    : Subset of columns to read (saves memory on wide files).
    dtype      : Column dtype overrides.
    chunksize  : If set, returns a TextFileReader for chunked iteration.
    nrows      : Read only the first N rows (useful for previews).

    Returns
    -------
    pd.DataFrame  (or TextFileReader if chunksize is set)

    Notes
    -----
    pyarrow engine is fastest for full reads but does NOT support usecols,
    nrows, or chunksize.  We fall back to the C engine for those cases.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV not found: {filepath}")

    # pyarrow doesn't support usecols / nrows / chunksize — use C engine for those
    needs_c = chunksize or usecols or nrows
    engine = "c" if needs_c else CSV_ENGINE

    kwargs: Dict[str, Any] = dict(
        filepath_or_buffer=filepath,
        dtype=dtype,
        low_memory=False,
        engine=engine,
    )

    if usecols:
        kwargs["usecols"] = usecols
    if nrows:
        kwargs["nrows"] = nrows
    if chunksize:
        kwargs["chunksize"] = chunksize

    log.info("Reading %s  (engine=%s, cols=%s)", filepath.name, engine,
             len(usecols) if usecols else "all")
    result = pd.read_csv(**kwargs)

    # For pyarrow full-reads, post-select columns if usecols was intended
    # (not needed here since we use C engine for usecols, but as safety)
    return result


def save_csv(df: pd.DataFrame, filepath: str | Path, index: bool = False) -> Path:
    """Save DataFrame to CSV, creating parent dirs if needed."""
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=index)
    log.info("Saved %d rows × %d cols → %s", len(df), len(df.columns), filepath)
    return filepath


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 4 — Pincode Centroid Lookup                               ║
# ╚══════════════════════════════════════════════════════════════════════╝

_PINCODE_CENTROIDS: Optional[Dict[int, Tuple[float, float]]] = None


def load_pincode_centroids(filepath: str | Path = PINCODE_FILE) -> Dict[int, Tuple[float, float]]:
    """
    Load pincode → (lat, lon) lookup.
    Cached after first call.  Returns {pincode_int: (pin_lat, pin_long)}.
    """
    global _PINCODE_CENTROIDS
    if _PINCODE_CENTROIDS is not None:
        return _PINCODE_CENTROIDS

    filepath = Path(filepath)
    if not filepath.exists():
        log.warning("Pincode centroid file not found: %s — coordinate gap-fill disabled.", filepath)
        _PINCODE_CENTROIDS = {}
        return _PINCODE_CENTROIDS

    pdf = read_csv_fast(filepath, usecols=["pin_code", "pin_lat", "pin_long"])
    pdf = pdf.dropna(subset=["pin_code", "pin_lat", "pin_long"])
    pdf["pin_code"] = pdf["pin_code"].astype(int)
    _PINCODE_CENTROIDS = {
        row.pin_code: (row.pin_lat, row.pin_long)
        for row in pdf.itertuples(index=False)
    }
    log.info("Loaded %d pincode centroids from %s", len(_PINCODE_CENTROIDS), filepath.name)
    return _PINCODE_CENTROIDS


def fill_coords_from_pincode(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    pin_col: str,
) -> pd.DataFrame:
    """
    Fill missing / invalid lat-lon values using pincode centroid lookup.
    A coordinate is considered *invalid* if it is NaN, zero, or a bare integer
    (e.g. 28 instead of 28.612345).

    Parameters
    ----------
    df      : DataFrame (modified in-place).
    lat_col : Name of the latitude column.
    lon_col : Name of the longitude column.
    pin_col : Name of the pincode column.
    """
    centroids = load_pincode_centroids()
    if not centroids:
        return df

    def _is_bad(val):
        if pd.isna(val) or val == 0:
            return True
        if isinstance(val, (int, np.integer)):
            return True
        if isinstance(val, float) and val == int(val):
            return True
        return False

    bad_mask = df[lat_col].apply(_is_bad) | df[lon_col].apply(_is_bad)
    n_bad = bad_mask.sum()
    if n_bad == 0:
        return df

    filled = 0
    for idx in df.index[bad_mask]:
        pin = df.at[idx, pin_col]
        if pd.notna(pin):
            try:
                pin_int = int(float(pin))
            except (ValueError, TypeError):
                continue
            if pin_int in centroids:
                clat, clon = centroids[pin_int]
                df.at[idx, lat_col] = clat
                df.at[idx, lon_col] = clon
                filled += 1

    log.info("Coordinate gap-fill: %d/%d bad coords, %d filled from pincode.", n_bad, len(df), filled)
    return df


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 5 — General Cleaning Utilities                            ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ---------- dtype enforcement ----------

def enforce_integer(series: pd.Series) -> pd.Series:
    """Coerce to nullable integer. Non-numeric → pd.NA."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def enforce_float(series: pd.Series) -> pd.Series:
    """Coerce to float64. Non-numeric → NaN."""
    return pd.to_numeric(series, errors="coerce").astype("float64")


def enforce_string(series: pd.Series) -> pd.Series:
    """Coerce to stripped string. NaN stays NaN."""
    return series.astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})


def enforce_dtypes(df: pd.DataFrame, dtype_map: Dict[str, str]) -> pd.DataFrame:
    """
    Apply a {column: type_spec} map to a DataFrame.

    Supported type_specs
    --------------------
    'int'    → nullable Int64
    'float'  → float64
    'str'    → stripped string
    """
    handlers = {
        "int": enforce_integer,
        "float": enforce_float,
        "str": enforce_string,
    }
    for col, spec in dtype_map.items():
        if col not in df.columns:
            continue
        fn = handlers.get(spec)
        if fn:
            df[col] = fn(df[col])
        else:
            log.warning("Unknown dtype spec '%s' for column '%s' — skipped.", spec, col)
    return df


# ---------- validation ----------

def validate_pincode(series: pd.Series) -> pd.Series:
    """Keep only 6-digit pincodes; everything else → pd.NA."""
    s = pd.to_numeric(series, errors="coerce")
    mask = (s >= 100000) & (s <= 999999)
    return s.where(mask, other=pd.NA).astype("Int64")


def validate_year(series: pd.Series, min_year: int = 1800, max_year: int = 2030) -> pd.Series:
    """Keep only 4-digit years within [min_year, max_year]; else → pd.NA."""
    s = pd.to_numeric(series, errors="coerce")
    mask = (s >= min_year) & (s <= max_year) & (s == s.astype("Int64", errors="ignore"))
    return s.where(mask, other=pd.NA).astype("Int64")


# ---------- text cleaning ----------

# Common institutional abbreviations (keep as-is)
KNOWN_ABBREVIATIONS = frozenset({
    # Institutions (2-4 chars)
    "IIT", "IIM", "IIIT", "NIT", "BIT", "MIT", "VIT", "SRM", "BITS",
    "IES", "IAS", "IPS", "KV", "JNV", "DAV", "DU", "JU", "BHU",
    # Institutions (5+ chars - must be explicitly listed)
    "IIITM", "AIIMS", "NIPER", "NITIE", "IIITDM", "IISER",
    # Degrees & titles
    "MBBS", "MD", "MS", "PhD", "BSc", "MSc", "BA", "MA",
    "BBA", "MBA", "BCA", "MCA", "LLB", "LLM", "BTech", "MTech",
    # Education boards
    "CBSE", "ICSE", "SSC", "HSC",
    # Titles
    "Dr", "Mr", "Mrs", "Ms", "St", "Jr", "Sr",
})


def _analyze_raw_text(text: str) -> Tuple[set, bool]:
    """
    First pass: analyze the original raw text to identify abbreviations.
    
    KEY INSIGHT: If a word is ALL-CAPS in the source, it's likely intentional
    (an abbreviation/acronym) and should be preserved.
    
    HOWEVER: If the ENTIRE text is all-caps, this signal is useless.
    
    Returns (set of abbreviations, is_all_caps_heavy).
    """
    if not text or not isinstance(text, str):
        return set(), False
    
    # Normalize whitespace but preserve original casing
    text = re.sub(r'\s+', ' ', text.strip())
    words = text.split()
    
    if not words:
        return set(), False

    # Check if input is "all-caps heavy"
    upper_words = [w for w in words if any(c.isupper() for c in w) and w.strip(".,;:!?'\"").isupper()]
    is_all_caps_heavy = (len(upper_words) / len(words)) > 0.7 if words else False
    
    abbreviations = set()
    
    for i, word in enumerate(words):
        clean = word.strip(".,;:!?'\"")
        
        # Skip empty
        if not clean:
            continue
        
        # CRITICAL: Skip stopwords even if all-caps (OF, AND, FOR, THE, etc.)
        if clean.lower() in TITLE_STOPWORDS:
            continue
        
        # 1. Known abbreviation (case-insensitive check)
        if clean.upper() in KNOWN_ABBREVIATIONS:
            abbreviations.add(clean)
            continue
        
        # 2. Contains dots → abbreviation (V.V., Ph.D., etc.)
        if "." in word:
            abbreviations.add(word)
            continue
        
        # 3. ALL-CAPS **SHORT** words → likely acronyms (IES, IIITM, MIT)
        #    BUT: If the whole string is caps, we only trust this if it's very short (≤3)
        #    Otherwise names like "FINE ART" or "NEAR UNIT" get caught.
        threshold = 3 if is_all_caps_heavy else 4
        if clean.isupper() and 2 <= len(clean) <= threshold and clean.isalpha():
            abbreviations.add(clean)
            continue
        
        # 4. Single letter → likely abbreviation (V in "V Ramakrishnan")
        if len(clean) == 1 and clean.isalpha():
            abbreviations.add(clean)
            continue
    
    return abbreviations, is_all_caps_heavy


def _strip_leading_ids(text: str) -> str:
    """
    Remove leading numeric IDs or pincodes often found at the start of names.
    Expl: '130021-rsspm's' -> 'rsspm's'
          '2. Shree Vashista' -> 'Shree Vashista'
    """
    # 1. Strip '1. ', '2. ' etc.
    text = re.sub(r'^\d+[\.\)]\s*', '', text)
    # 2. Strip '123456-' or '123456 ' at the start
    text = re.sub(r'^\d+[-\s]+', '', text)
    return text.strip()


def _normalize_single_letters(words: list, abbreviation_set: set) -> list:
    """
    Handle consecutive single letters: B R AMBEDKAR → B. R. Ambedkar
    """
    result = []
    i = 0
    
    while i < len(words):
        word = words[i]
        clean = word.strip(".,;:!?'\"")
        
        # Check if this and next word(s) are single letters
        if clean in abbreviation_set and len(clean) == 1 and clean.isalpha():
            # Collect consecutive single letters
            letters = [clean.upper()]
            j = i + 1
            
            while j < len(words):
                next_clean = words[j].strip(".,;:!?'\"")
                if next_clean in abbreviation_set and len(next_clean) == 1 and next_clean.isalpha():
                    letters.append(next_clean.upper())
                    j += 1
                else:
                    break
            
            # Add dots: ["B", "R"] → "B. R."
            # Note: Using dots like "B. R." as requested. 
            # If we want just uppercase "B R", we'd omit dots.
            # But "B. R." is standard for initials.
            result.extend([f"{letter}." for letter in letters])
            i = j
        else:
            result.append(word)
            i += 1
    
    return result


def smart_title_case(text: str) -> str:
    """
    Smart title-casing that intelligently detects and preserves abbreviations.
    
    Algorithm (two-pass):
    ----------------------
    1. ANALYZE: Scan raw text to identify abbreviations (all-caps words, dots, etc.)
    2. NORMALIZE: Apply title-casing while preserving detected abbreviations
    
    Examples:
        'V.V.COLLEGE OF ENGINEERING' → 'V. V. College of Engineering'
        '130021-IES COLLEGE' → 'IES College'
        'National Institute of FINE ART' → 'National Institute of Fine Art'
    """
    if not text or not isinstance(text, str):
        return text
    
    # Pre-clean: strip leading IDs
    text = _strip_leading_ids(text)
    
    # Normalize whitespace
    original = re.sub(r'\s+', ' ', text.strip())
    
    # PASS 1: Analyze raw text to identify abbreviations
    abbreviation_set, is_all_caps_heavy = _analyze_raw_text(original)
    
    # PASS 2: Apply smart casing
    words = original.split()
    
    # First, handle consecutive single letters (B R → B. R.)
    words = _normalize_single_letters(words, abbreviation_set)
    
    result = []
    for i, word in enumerate(words):
        clean = word.strip(".,;:!?'\"")
        punctuation = word[len(word.rstrip(".,;:!?'\"")):]  # trailing punctuation
        
        # Check if this word (or its clean version) is an abbreviation
        is_abbrev = (
            clean in abbreviation_set or
            word in abbreviation_set or
            clean.upper() in KNOWN_ABBREVIATIONS
        )
        
        if is_abbrev:
            # PRESERVE abbreviations - but normalize dot spacing
            normalized = re.sub(r'([A-Za-z])\.([A-Za-z])', r'\1. \2', word)
            
            # Keep all-caps abbreviations in all-caps
            if clean.isupper() and clean.isalpha():
                result.append(clean.upper() + punctuation)
            else:
                result.append(normalized)
        else:
            # Apply standard title-case rules
            if i > 0 and clean.lower() in TITLE_STOPWORDS:
                result.append(word.lower())
            else:
                # Title case: capitalize first letter only
                if clean:
                    # Special case for 's: "rsspm's" -> "Rsspm's"
                    parts = clean.split("'")
                    title_parts = [p[0].upper() + p[1:].lower() if p else "" for p in parts]
                    
                    # Fix: If part after ' is just 'S', make it 's'
                    if len(title_parts) > 1 and title_parts[-1].upper() == 'S':
                        title_parts[-1] = 's'
                        
                    final_clean = "'".join(title_parts)
                    result.append(final_clean + punctuation)
                else:
                    result.append(word)
    
    return " ".join(result)


def clean_text_series(series: pd.Series) -> pd.Series:
    """Strip, collapse whitespace, apply smart title case to a text series."""
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\s+", " ", regex=True)
    s = s.apply(lambda x: smart_title_case(x) if pd.notna(x) and x not in ("nan", "<NA>", "None") else pd.NA)
    return s


def remove_after_first_comma(series: pd.Series) -> pd.Series:
    """Remove everything after the first comma in each value."""
    return series.astype(str).str.split(",").str[0].str.strip()


def _remove_trailing_gpe(text: str, nlp) -> str:
    """Use spacy NER to strip GPE (location) entities from the tail of a string."""
    if not text or not isinstance(text, str) or nlp is None:
        return text
    doc = nlp(text)
    gpe_ents = [ent for ent in doc.ents if ent.label_ == "GPE"]
    if not gpe_ents:
        return text

    result = text
    # Work backward: remove GPEs that sit at the end of the string
    for ent in reversed(gpe_ents):
        tail = result[ent.start_char:].strip().rstrip(" ,.-")
        # GPE must be at (or very near) the end
        if ent.end_char >= len(result.rstrip()) - 2:
            result = result[:ent.start_char].rstrip(" ,.-")
    return result.strip()


def clean_institution_name(series: pd.Series) -> pd.Series:
    """
    Full institution-name cleaning pipeline:
      1. Remove everything after first comma
      2. Remove trailing GPE entities via spacy NER
      3. Apply stopword-aware proper casing
    """
    nlp = _get_nlp()
    s = remove_after_first_comma(series)
    if nlp is not None:
        log.info("  → NER cleaning %d names (this may take a moment)…", len(s))
        s = s.apply(lambda x: _remove_trailing_gpe(x, nlp) if pd.notna(x) and x not in ("nan",) else x)
    else:
        log.info("  → Skipping NER (spacy unavailable); applying basic cleaning only.")
    s = clean_text_series(s)
    return s


# ---------- coordinate helpers ----------

def parse_coordinate_string(coord_str: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse a coordinate value that may be a string like '[77.53, 8.08]'
    or an already-parsed list.  Returns (longitude, latitude).
    """
    if coord_str is None or (isinstance(coord_str, float) and np.isnan(coord_str)):
        return (None, None)
    if isinstance(coord_str, str):
        try:
            coord_str = ast.literal_eval(coord_str)
        except (ValueError, SyntaxError):
            return (None, None)
    if isinstance(coord_str, (list, tuple)) and len(coord_str) >= 2:
        return (float(coord_str[0]), float(coord_str[1]))
    return (None, None)


def extract_coordinates(
    df: pd.DataFrame,
    coord_col: str = "coordinates",
    lon_col: str = "lon",
    lat_col: str = "lat",
) -> pd.DataFrame:
    """
    Parse a coordinates column ('[lon, lat]' strings) into separate lon/lat columns.
    """
    parsed = df[coord_col].apply(parse_coordinate_string)
    df[lon_col] = parsed.apply(lambda x: x[0])
    df[lat_col] = parsed.apply(lambda x: x[1])
    return df


def add_suffix(series: pd.Series, suffix: str) -> pd.Series:
    """Append a text suffix to non-null string values."""
    return series.astype(str).apply(
        lambda x: f"{x.strip()} {suffix}" if pd.notna(x) and x.strip() not in ("nan", "", "<NA>") else pd.NA
    )


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 6 — File Configurations (Declarative Registry)            ║
# ╚══════════════════════════════════════════════════════════════════════╝

FILE_REGISTRY: Dict[str, Dict[str, Any]] = {}


def _register(name: str, **kwargs):
    """Register a file configuration."""
    FILE_REGISTRY[name] = kwargs


# ── 1. agri_industries ──────────────────────────────────────────────────
_register(
    "agri_industries",
    filename="agri_industries.csv",
    columns_to_keep=["gid", "lattitude", "longitude"],
    rename_map={
        "gid": "agri_industries_uid",
        "lattitude": "agri_industries_lat",
        "longitude": "agri_industries_long",
    },
    dtype_map={
        "agri_industries_uid": "int",
        "agri_industries_lat": "float",
        "agri_industries_long": "float",
    },
    cleaner="clean_agri_industries",
)

# ── 2. apmc ──────────────────────────────────────────────────────────────
_register(
    "apmc",
    filename="apmc.csv",
    columns_to_keep=[
        "gid", "sl_no", "st_code", "dist_code", "district_n",
        "mandi_code", "fatehabad", "market_cat", "long", "lat",
    ],
    rename_map={
        "dist_code": "state_dist_code",
        "district_n": "district",
        "fatehabad": "apmc_name",
        "market_cat": "apmc_category",
        "long": "apmc_long",
        "lat": "apmc_lat",
    },
    dtype_map={
        "gid": "int",
        "sl_no": "int",
        "st_code": "str",
        "state_dist_code": "int",
        "district": "str",
        "mandi_code": "int",
        "apmc_name": "str",
        "apmc_category": "str",
        "apmc_lat": "float",
        "apmc_long": "float",
    },
    cleaner="clean_apmc",
)

# ── 3. college ────────────────────────────────────────────────────────────
_register(
    "college",
    filename="college.csv",
    columns_to_keep=[
        "aishecode", "colleget_1", "collegetyp", "collegeins",
        "locationid", "university", "affiliatin", "pincode",
        "yearofesta", "latitude", "longitude", "management",
        "ownerships", "districtlg",
    ],
    rename_map={
        "aishecode": "college_aishe_uid",
        "colleget_1": "college_type_code",
        "collegetyp": "college_type",
        "collegeins": "college_name",
        "locationid": "urban_rural",
        "university": "affiliating_uni_code",
        "affiliatin": "university",
        "pincode": "pincode",
        "yearofesta": "establishment_year",
        "latitude": "college_lat",
        "longitude": "college_long",
        "management": "management",
        "ownerships": "ownership",
        "districtlg": "district_lgd",
    },
    dtype_map={
        "college_aishe_uid": "str",
        "college_type_code": "int",
        "college_type": "str",
        "college_name": "str",
        "urban_rural": "str",
        "university": "str",
        "affiliating_uni_code": "str",
        "pincode": "int",
        "establishment_year": "int",
        "college_lat": "float",
        "college_long": "float",
        "management": "str",
        "ownership": "str",
        "district_lgd": "int",
    },
    cleaner="clean_college",
)

# ── 4. universities ──────────────────────────────────────────────────────
_register(
    "universities",
    filename="universities.csv",
    columns_to_keep=[
        "aishecode", "typeofin_1", "typeofinst", "name",
        "locationid", "pincode", "yearofesta", "latitude",
        "longitude", "management", "ownerships", "districtlg",
    ],
    rename_map={
        "aishecode": "uni_aishe_code",
        "typeofin_1": "uni_type_code",
        "typeofinst": "uni_type",
        "name": "uni_name",
        "locationid": "urban_rural",
        "pincode": "pincode",
        "yearofesta": "establishment_year",
        "latitude": "college_lat",
        "longitude": "college_long",
        "management": "management",
        "ownerships": "ownership",
        "districtlg": "district_lgd",
    },
    dtype_map={
        "uni_aishe_code": "str",
        "uni_type_code": "int",
        "uni_type": "str",
        "uni_name": "str",
        "urban_rural": "str",
        "pincode": "int",
        "establishment_year": "int",
        "college_lat": "float",
        "college_long": "float",
        "management": "str",
        "ownership": "str",
        "district_lgd": "int",
    },
    cleaner="clean_universities",
)

# ── 5. school ─────────────────────────────────────────────────────────────
_register(
    "school",
    filename="School__Source___MHRD_2023__4.csv",
    columns_to_keep=[
        "lgd_distri", "vilcode11", "vilname", "schcd", "schname",
        "school_cat", "schcat", "management", "schmgt",
        "latitude", "longitude",
    ],
    rename_map={
        "lgd_distri": "district_lgd",
        "vilcode11": "village_census11",
        "vilname": "village_name",
        "schcd": "school_code",
        "schname": "school_name",
        "school_cat": "school_category",
        "schcat": "school_category_code",
        "management": "school_management",
        "schmgt": "school_management_code",
        "latitude": "school_lat",
        "longitude": "school_long",
    },
    dtype_map={
        "district_lgd": "int",
        "village_census11": "int",
        "village_name": "str",
        "school_code": "int",
        "school_name": "str",
        "school_category": "str",
        "school_category_code": "int",
        "school_management": "str",
        "school_management_code": "int",
        "school_lat": "float",
        "school_long": "float",
    },
    cleaner="clean_school",
)

# ── 6. health_center ─────────────────────────────────────────────────────
_register(
    "health_center",
    filename="Health_Center_Source___Data_gov_2022__6.csv",
    columns_to_keep=["FID", "facility_t", "coordinates"],
    rename_map={},   # dynamic — handled in cleaner
    dtype_map={},    # dynamic — handled in cleaner
    cleaner="clean_health_center",
)

# ── 7. agri_industry (reclassified) ──────────────────────────────────────
"""
# Reclassification mapping: raw `subtype` values from **two** PMGSY 2023
# source files (Industries layer 11, Agri Resources layer 10) are grouped
# into 11 high-level categories.  Each category becomes a separate output
# CSV in data/facilities/cleaned/ with the prefix "agri_industry_".
#
# Source files:
#   • Industries__Sources___PMGSY_2023__11.csv   (69 k rows)
#   • Agri_Resources__Sources___PMGSY_2023__10.csv (48 k rows)
"""
# Note: Reclassification mapping is now loaded from data/facilities/agri_industry_reclassification.csv
# inside the clean_agri_industry() function.

_register(
    "agri_industry",
    filename=[
        "Industries__Sources___PMGSY_2023__11.csv",
        "Agri_Resources__Sources___PMGSY_2023__10.csv",
    ],
    columns_to_keep=["facility_i", "fac_desc", "dt_lgd", "lattitude", "longitude", "subtype"],
    rename_map={
        "facility_i": "agri_industry_id",
        "fac_desc":   "facility_name",
        "dt_lgd":     "district_lgd",
        "lattitude":  "agri_industry_lat",
        "longitude":  "agri_industry_long",
    },
    dtype_map={
        "agri_industry_id":   "int",
        "facility_name":      "str",
        "district_lgd":       "int",
        "agri_industry_lat":  "float",
        "agri_industry_long": "float",
        "subtype":            "str",
        "source_file":        "str",
    },
    cleaner="clean_agri_industry",
)

# ── 8. csc ────────────────────────────────────────────────────────────────
_register(
    "csc",
    filename="CSC-MeitY-2022.csv",
    columns_to_keep=["csc_id", "longitude", "latitude"],
    rename_map={
        "csc_id": "csc_uid",
        "longitude": "csc_long",
        "latitude": "csc_lat",
    },
    dtype_map={
        "csc_uid": "str",
        "csc_lat": "float",
        "csc_long": "float",
    },
    cleaner="clean_csc",
)

# ── 9. pds ────────────────────────────────────────────────────────────────
_register(
    "pds",
    filename="PDS__Sources___DoFD_2023__7.csv",
    columns_to_keep=["fps_code", "fpsname", "latitude", "longitude"],
    rename_map={
        "fps_code": "fps_uid",
        "fpsname": "fps_name",
        "latitude": "fps_lat",
        "longitude": "fps_long",
    },
    dtype_map={
        "fps_uid": "str",
        "fps_name": "str",
        "fps_lat": "float",
        "fps_long": "float",
    },
    cleaner="clean_pds",
)

# ── 10. bank_branch ──────────────────────────────────────────────────────
_register(
    "bank_branch",
    filename="bank_branch.csv",
    columns_to_keep=["objectid", "bank_name", "br_ifsc_cd", "br_lat", "br_long"],
    rename_map={
        "objectid": "bank_uid",
        "br_ifsc_cd": "bank_ifsc_cd",
        "br_lat": "bank_lat",
        "br_long": "bank_long",
    },
    dtype_map={
        "bank_uid": "int",
        "bank_name": "str",
        "bank_ifsc_cd": "str",
        "bank_lat": "float",
        "bank_long": "float",
    },
    cleaner="clean_bank_branch",
)

# ── 11. bank_atm ─────────────────────────────────────────────────────────
_register(
    "bank_atm",
    filename="bank_atm.csv",
    columns_to_keep=["objectid", "atm_cd", "bank_name", "atm_lat", "atm_long"],
    rename_map={
        "objectid": "bank_atm_uid",
        "atm_cd": "bank_atm_code",
        "atm_lat": "bank_atm_lat",
        "atm_long": "bank_atm_long",
    },
    dtype_map={
        "bank_atm_uid": "int",
        "bank_atm_code": "str",
        "bank_name": "str",
        "bank_atm_lat": "float",
        "bank_atm_long": "float",
    },
    cleaner="clean_bank_atm",
)

# ── 12. bank_mitra ───────────────────────────────────────────────────────
_register(
    "bank_mitra",
    filename="bank_mitra.csv",
    columns_to_keep=["objectid", "bk_mitr_cd", "bank_name", "bk_m_lat", "bk_m_long", "coordinates"],
    rename_map={
        "objectid": "bank_mitra_uid",
        "bk_mitr_cd": "bank_mitra_code",
        "bk_m_lat": "bank_mitra_lat",
        "bk_m_long": "bank_mitra_long",
    },
    dtype_map={
        "bank_mitra_uid": "int",
        "bank_mitra_code": "str",
        "bank_name": "str",
        "bank_mitra_lat": "float",
        "bank_mitra_long": "float",
    },
    cleaner="clean_bank_mitra",
)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 7 — File-Specific Cleaning Pipelines                      ║
# ╚══════════════════════════════════════════════════════════════════════╝

def _apply_common(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """Apply column selection → rename → dtype enforcement (common to most files)."""
    cols = cfg.get("columns_to_keep")
    if cols:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            log.warning("Columns not found in data (skipped): %s", missing)
            cols = [c for c in cols if c in df.columns]
        df = df[cols].copy()

    rename = cfg.get("rename_map", {})
    if rename:
        df = df.rename(columns=rename)

    dtypes = cfg.get("dtype_map", {})
    if dtypes:
        df = enforce_dtypes(df, dtypes)

    return df


# ── 1. agri_industries ──────────────────────────────────────────────────
def clean_agri_industries(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    agri_industries — simple 3-column point extraction.
    Output: agri_industries_uid, agri_industries_lat, agri_industries_long
    """
    cfg = FILE_REGISTRY["agri_industries"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "agri_industries.csv")

    log.info("═══ Cleaning: agri_industries ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)
    save_csv(df, dst)
    return df


# ── 2. apmc ──────────────────────────────────────────────────────────────

# APMC market category normalisation
_APMC_CAT_NORMALIZE = {
    "Farmers Market/Village Market": "FCM",
    "rural market": "FCM",
    "Others": "Other",
    "": "Other",
}

_APMC_CAT_MAP = {
    "PMY": "Primary Market Yard",
    "SMY": "Sub Market Yard",
    "RPM": "Regulated Primary Market",
    "RSM": "Regulated Sub Market",
    "NRM": "Non-Regulated Market",
    "FCM": "Farmers Consumer Market",
    "Other": "Other",
}


def clean_apmc(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    apmc — mandi/market data.
    Steps: column select → rename → normalise market_cat → map to full name
         → add "APMC" suffix to apmc_name → enforce dtypes
    """
    cfg = FILE_REGISTRY["apmc"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "apmc.csv")

    log.info("═══ Cleaning: apmc ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Normalise rare category labels then map to full names
    df["apmc_category"] = (
        df["apmc_category"]
        .fillna("Other")
        .replace(_APMC_CAT_NORMALIZE)
        .map(_APMC_CAT_MAP)
        .fillna("Other")
    )

    # Add " APMC" suffix to mandi name
    df["apmc_name"] = add_suffix(df["apmc_name"], "APMC")

    save_csv(df, dst)
    return df


# ── 3. college ────────────────────────────────────────────────────────────
def clean_college(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    college — AISHE college data (111 raw cols → 14 cleaned cols).
    Heavy cleaning: institution name NER, pincode validation, year validation,
    coordinate gap-filling from pincode centroid.
    """
    cfg = FILE_REGISTRY["college"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "college.csv")

    log.info("═══ Cleaning: college ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Clean institution names (college + university)
    log.info("  Cleaning college_name …")
    df["college_name"] = clean_institution_name(df["college_name"])
    log.info("  Cleaning university …")
    df["university"] = clean_institution_name(df["university"])

    # Validate pincode & year
    df["pincode"] = validate_pincode(df["pincode"])
    df["establishment_year"] = validate_year(df["establishment_year"])

    # Enforce district LGD as integer
    df["district_lgd"] = enforce_integer(df["district_lgd"])

    # Fill missing / integer-only coords from pincode centroid
    df = fill_coords_from_pincode(df, "college_lat", "college_long", "pincode")

    save_csv(df, dst)
    return df


# ── 4. universities ──────────────────────────────────────────────────────
def clean_universities(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    universities — AISHE university data (111 raw cols → 12 cleaned cols).
    Same cleaning logic as college: NER names, pincode, year, coord gap-fill.
    """
    cfg = FILE_REGISTRY["universities"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "universities.csv")

    log.info("═══ Cleaning: universities ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Clean institution name
    log.info("  Cleaning uni_name …")
    df["uni_name"] = clean_institution_name(df["uni_name"])

    # Validate pincode & year
    df["pincode"] = validate_pincode(df["pincode"])
    df["establishment_year"] = validate_year(df["establishment_year"])

    # Enforce district LGD as integer
    df["district_lgd"] = enforce_integer(df["district_lgd"])

    # Fill missing / integer-only coords from pincode centroid
    df = fill_coords_from_pincode(df, "college_lat", "college_long", "pincode")

    save_csv(df, dst)
    return df


# ── 5. school ─────────────────────────────────────────────────────────────

# School category code → available education level codes
SCHOOL_CATEGORY_LEVELS = {
    1: [1],
    2: [1, 2],
    4: [2],
    6: [1, 2, 3],
    3: [1, 2, 3, 4],
    7: [2, 3],
    5: [2, 3, 4],
    8: [3],
    10: [3, 4],
    11: [4],
    12: [1],
}

# Level code → human-readable name (used for output filenames)
SCHOOL_LEVEL_NAMES = {
    1: "Primary",
    2: "Upper Primary",
    3: "Secondary",
    4: "Higher Secondary",
}


def clean_school(
    input_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    chunksize: int = 500_000,
) -> Dict[str, pd.DataFrame]:
    """
    school — MHRD 2023 school data (~415 MB, millions of rows).

    Strategy
    --------
    1. Build schcat → school_cat dictionary for gap-filling missing school_cat
    2. Apply column renaming, dtype enforcement, text cleaning
    3. Classify each school by available education levels using
       SCHOOL_CATEGORY_LEVELS mapping
    4. Split into 4 level-based files — a school can appear in multiple files
    """
    cfg = FILE_REGISTRY["school"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dest_dir = output_dir or OUT_DIR

    log.info("═══ Cleaning: school (level-based split, %d rows/chunk) ═══", chunksize)

    # ── 0. Build schcat → school_cat dictionary (quick first pass) ────
    log.info("  Pass 1: Building schcat → school_cat dictionary …")
    schcat_to_schoolcat: Dict[str, str] = {}
    reader0 = read_csv_fast(src, usecols=["schcat", "school_cat"], chunksize=chunksize)
    for chunk0 in reader0:
        valid = chunk0.dropna(subset=["schcat", "school_cat"])
        sc = valid["schcat"].astype(str).str.strip()
        scat = valid["school_cat"].astype(str).str.strip()
        both_ok = (sc != "") & (sc != "nan") & (scat != "") & (scat != "nan")
        for k, v in zip(sc[both_ok], scat[both_ok]):
            schcat_to_schoolcat[k] = v
    log.info("  Built %d schcat → school_cat mappings.", len(schcat_to_schoolcat))

    # ── 1. Cleanup old school files ─────────────────────────────────
    old_files = list(dest_dir.glob("school_*.csv"))
    if old_files:
        log.info("  Removing %d old school_*.csv files …", len(old_files))
        for f in old_files:
            f.unlink()

    # Precompute which category codes belong to each level
    level_to_cats: Dict[int, List[int]] = {}
    for level_code in SCHOOL_LEVEL_NAMES:
        level_to_cats[level_code] = [
            cat for cat, levels in SCHOOL_CATEGORY_LEVELS.items()
            if level_code in levels
        ]

    # ── 2. Process chunks ───────────────────────────────────────────
    reader = read_csv_fast(src, usecols=cfg["columns_to_keep"], chunksize=chunksize)
    written_files: Set[str] = set()
    total_rows = 0

    for i, chunk in enumerate(reader):
        log.info("  Processing chunk %d  (%d rows) …", i + 1, len(chunk))

        # Fill missing school_cat from schcat dictionary
        missing_cat = (
            chunk["school_cat"].isna()
            | chunk["school_cat"].astype(str).str.strip().isin(["", "nan", "<NA>", "None"])
        )
        if missing_cat.any():
            chunk.loc[missing_cat, "school_cat"] = (
                chunk.loc[missing_cat, "schcat"]
                .astype(str).str.strip()
                .map(schcat_to_schoolcat)
            )

        # Apply renaming and dtypes
        chunk = chunk.rename(columns=cfg.get("rename_map", {}))
        chunk = enforce_dtypes(chunk, cfg.get("dtype_map", {}))

        # Clean school names (stopword-aware proper casing)
        chunk["school_name"] = clean_text_series(chunk["school_name"])

        # Split by level and save
        for level_code, level_name in SCHOOL_LEVEL_NAMES.items():
            valid_cats = level_to_cats[level_code]
            sub = chunk[chunk["school_category_code"].isin(valid_cats)].copy()
            if sub.empty:
                continue

            safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", level_name.strip().lower())
            safe_name = re.sub(r"_+", "_", safe_name).strip("_")
            fname = f"school_{safe_name}.csv"
            fpath = dest_dir / fname

            header = fname not in written_files
            sub.to_csv(fpath, mode="a", index=False, header=header)
            written_files.add(fname)

        total_rows += len(chunk)

    log.info("  Total rows processed: %d", total_rows)
    log.info("  School files created: %s", ", ".join(sorted(written_files)))
    log.info("  School cleaning completed.")

    return {}


# ── 6. health_center ─────────────────────────────────────────────────────
def clean_health_center(
    input_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
) -> Dict[str, pd.DataFrame]:
    """
    health_center — split by facility_t, extract coordinates.
    Produces one CSV per facility type:  {type}_uid, {type}_lat, {type}_long
    """
    cfg = FILE_REGISTRY["health_center"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dest_dir = output_dir or OUT_DIR

    log.info("═══ Cleaning: health_center (split by facility type) ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])

    # Extract lon/lat from coordinates column
    df = extract_coordinates(df, coord_col="coordinates", lon_col="_lon", lat_col="_lat")

    results: Dict[str, pd.DataFrame] = {}
    for ftype in df["facility_t"].dropna().unique():
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(ftype).strip().lower())
        sub = df[df["facility_t"] == ftype].copy()

        out = pd.DataFrame({
            f"{safe_name}_uid": sub["FID"].values,
            f"{safe_name}_lat": sub["_lat"].values,
            f"{safe_name}_long": sub["_lon"].values,
        })

        out[f"{safe_name}_uid"] = enforce_integer(out[f"{safe_name}_uid"])
        out[f"{safe_name}_lat"] = enforce_float(out[f"{safe_name}_lat"])
        out[f"{safe_name}_long"] = enforce_float(out[f"{safe_name}_long"])

        fname = f"health_{safe_name}.csv"
        save_csv(out, dest_dir / fname)
        results[safe_name] = out
        log.info("  → %s : %d rows", fname, len(out))

    return results


# ── 7. agri_industry (reclassified) ──────────────────────────────────────
def clean_agri_industry(
    input_path: Optional[List[Path]] = None,
    output_dir: Optional[Path] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Agri-Industry — reclassified split by subtype category.

    Reads **two** raw PMGSY 2023 files (Industries layer 11, Agri Resources
    layer 10), concatenates them, applies column selection, renaming, dtype
    enforcement, text cleaning, and then reclassifies each row's ``subtype``
    into one of 8 high-level categories using
    :data:`AGRI_INDUSTRY_RECLASSIFICATION`.

    Produces one CSV per category in the output directory, named::

        agri_industry_{safe_category_name}.csv

    Output columns per file:
        agri_industry_id, facility_name, district_lgd,
        agri_industry_lat, agri_industry_long, subtype

    **Stats (2023 Data)**:
      ~118k rows total.
      Top categories: Markets & Trading (~64k), Agri-Processing (~17k),
      Dairy & Animal Husbandry (~14k), Storage (~13k).


    Parameters
    ----------
    input_path : list of Path, optional
        Override source CSV paths.  Defaults to the two files registered
        in ``FILE_REGISTRY["agri_industry"]["filename"]``.
    output_dir : Path, optional
        Override destination directory.  Defaults to ``OUT_DIR``.

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of ``{safe_category_name: cleaned_dataframe}``.
    """
    cfg = FILE_REGISTRY["agri_industry"]
    filenames = cfg["filename"]  # list of two filenames
    dest_dir = output_dir or OUT_DIR

    # ── 0. Cleanup old files (One-shot guarantee) ────────────────────
    # Remove any existing agri_industry_*.csv files so that obsolete categories 
    # (e.g. "Specialized Farming", "Fisheries") don't linger.
    old_files = list(dest_dir.glob("agri_industry_*.csv"))
    if old_files:
        log.info("  Removing %d old agri_industry_*.csv files to ensure clean state…", len(old_files))
        for f in old_files:
            try:
                f.unlink()
            except OSError as e:
                log.warning("  Could not delete %s: %s", f.name, e)

    # Also clean the legacy text-based stats if it exists
    legacy_stats = dest_dir / "AGRI_INDUSTRY_MAPPING_STATS.txt"
    if legacy_stats.exists():
        legacy_stats.unlink()

    # ── 1. Read and concatenate both source files ────────────────────
    log.info("═══ Cleaning: agri_industry (reclassified, %d source files) ═══", len(filenames))
    frames: List[pd.DataFrame] = []
    if input_path:
        sources = input_path if isinstance(input_path, list) else [input_path]
    else:
        sources = [RAW_DIR / f for f in filenames]

    for src in sources:
        log.info("  Reading %s …", Path(src).name)
        part = read_csv_fast(src, usecols=cfg["columns_to_keep"])
        part["source_file"] = Path(src).name
        frames.append(part)

    # Concat and apply renaming/dtypes manually to keep "source_file" 
    # (since _apply_common filters to cfg["columns_to_keep"])
    df = pd.concat(frames, ignore_index=True)
    log.info("  Combined rows: %d", len(df))

    df = df.rename(columns=cfg.get("rename_map", {}))
    df = enforce_dtypes(df, cfg.get("dtype_map", {}))

    # ── 3. Clean text fields ─────────────────────────────────────────
    log.info("  Cleaning facility_name (basic proper case) …")
    df["facility_name"] = clean_text_series(df["facility_name"])

    # ── 4. Reclassify subtypes ───────────────────────────────────────
    # Load reclassification map from external CSV
    map_path = BASE_DIR / "data" / "facilities" / "agri_industry_reclassification.csv"
    if not map_path.exists():
        log.error("Reclassification map not found: %s", map_path)
        raise FileNotFoundError(f"Missing mapping file: {map_path}")

    log.info("  Loading reclassification mapping from %s …", map_path.name)
    mapping_df = pd.read_csv(map_path)
    # Build dict: subtype -> reclassified_category
    # (Note: same subtype might appear multiple times if it existed in both sources; drop_duplicates)
    reclass_map = mapping_df.drop_duplicates("subtype").set_index("subtype")["reclassified_category"].to_dict()

    df["reclassified_category"] = df["subtype"].map(reclass_map)

    unmapped = df["reclassified_category"].isna()
    if unmapped.any():
        # Map unmapped to 'Industrial Manufacturing' as a catch-all if not in file
        df.loc[unmapped, "reclassified_category"] = "Industrial Manufacturing"
        unmapped_vals = df.loc[unmapped, ["subtype"]].drop_duplicates()
        log.warning(
            "  ⚠ %d rows with subtypes not in reclassification file — assigned to Industrial Manufacturing:\n%s",
            unmapped.sum(),
            unmapped_vals.to_string(index=False),
        )

    # ── 5. Split by reclassified category and save ───────────────────
    results: Dict[str, pd.DataFrame] = {}
    for category in sorted(df["reclassified_category"].dropna().unique()):
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", category.strip().lower())
        safe_name = re.sub(r"_+", "_", safe_name).strip("_")  # collapse repeats

        sub = df[df["reclassified_category"] == category].copy()
        # Drop helper column and source_file column as requested
        out = sub.drop(columns=["reclassified_category", "source_file"])

        fname = f"agri_industry_{safe_name}.csv"
        save_csv(out, dest_dir / fname)
        results[safe_name] = out
        log.info("  → %s : %d rows", fname, len(out))

    log.info("  Total categories written: %d", len(results))
    log.info("  Agri-industry cleaning completed.")
    
    return results


# ── 8. csc ────────────────────────────────────────────────────────────────
def clean_csc(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    CSC — Common Service Centre (MeitY 2022).
    Output: csc_uid, csc_lat, csc_long
    """
    cfg = FILE_REGISTRY["csc"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "csc.csv")

    log.info("═══ Cleaning: csc ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)
    save_csv(df, dst)
    return df


# ── 9. pds ────────────────────────────────────────────────────────────────
def clean_pds(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    PDS — Public Distribution System (DoFD 2023).
    Output: fps_uid, fps_name, fps_lat, fps_long
    """
    cfg = FILE_REGISTRY["pds"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "pds.csv")

    log.info("═══ Cleaning: pds ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)
    save_csv(df, dst)
    return df


# ── 10. bank_branch ──────────────────────────────────────────────────────
def clean_bank_branch(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Bank Branch — branch location data.
    Output: bank_uid, bank_name, bank_ifsc_cd, bank_lat, bank_long
    """
    cfg = FILE_REGISTRY["bank_branch"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "bank_branch.csv")

    log.info("═══ Cleaning: bank_branch ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Basic cleaning on bank_name
    df["bank_name"] = clean_text_series(df["bank_name"])

    # Strip whitespace from IFSC code
    df["bank_ifsc_cd"] = (
        df["bank_ifsc_cd"].astype(str).str.strip()
        .replace({"nan": pd.NA, "": pd.NA, "None": pd.NA})
    )

    save_csv(df, dst)
    return df


# ── 11. bank_atm ─────────────────────────────────────────────────────────
def clean_bank_atm(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Bank ATM — ATM location data.
    Output: bank_atm_uid, bank_atm_code, bank_name, bank_atm_lat, bank_atm_long
    """
    cfg = FILE_REGISTRY["bank_atm"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "bank_atm.csv")

    log.info("═══ Cleaning: bank_atm ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Basic cleaning on bank_name
    df["bank_name"] = clean_text_series(df["bank_name"])

    # Clean ATM code: strip whitespace, replace bare '0' with NA
    df["bank_atm_code"] = (
        df["bank_atm_code"].astype(str).str.strip()
        .replace({"nan": pd.NA, "": pd.NA, "0": pd.NA, "None": pd.NA})
    )

    save_csv(df, dst)
    return df


# ── 12. bank_mitra ───────────────────────────────────────────────────────
def clean_bank_mitra(
    input_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Bank Mitra — banking correspondent agent data.
    Output: bank_mitra_uid, bank_mitra_code, bank_name, bank_mitra_lat, bank_mitra_long

    Lat/long fallback: if bk_m_lat / bk_m_long are missing, derive from
    the 'coordinates' column (format: '[lon, lat]').
    """
    cfg = FILE_REGISTRY["bank_mitra"]
    src = input_path or (RAW_DIR / cfg["filename"])
    dst = output_path or (OUT_DIR / "bank_mitra.csv")

    log.info("═══ Cleaning: bank_mitra ═══")
    df = read_csv_fast(src, usecols=cfg["columns_to_keep"])
    df = _apply_common(df, cfg)

    # Fill missing lat/long from coordinates column
    if "coordinates" in df.columns:
        bad_lat = df["bank_mitra_lat"].isna() | (df["bank_mitra_lat"] == 0)
        bad_long = df["bank_mitra_long"].isna() | (df["bank_mitra_long"] == 0)
        need_fill = bad_lat | bad_long

        if need_fill.any():
            log.info("  Filling %d missing lat/long from coordinates column …", need_fill.sum())
            parsed = df.loc[need_fill, "coordinates"].apply(parse_coordinate_string)
            # parse_coordinate_string returns (longitude, latitude)
            fill_long = parsed.apply(lambda x: x[0])
            fill_lat = parsed.apply(lambda x: x[1])

            df.loc[need_fill & bad_lat, "bank_mitra_lat"] = fill_lat[need_fill & bad_lat]
            df.loc[need_fill & bad_long, "bank_mitra_long"] = fill_long[need_fill & bad_long]

            filled = (~df.loc[need_fill, "bank_mitra_lat"].isna()).sum()
            log.info("  Filled %d coordinates from coordinates column.", filled)

        # Drop the helper column
        df = df.drop(columns=["coordinates"])

    save_csv(df, dst)
    return df


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 8 — Generic / Points-Only Pipelines                       ║
# ╚══════════════════════════════════════════════════════════════════════╝

def clean_points_only(
    input_path: str | Path,
    uid_col: str,
    lat_col: str,
    lon_col: str,
    prefix: str,
    output_path: Optional[str | Path] = None,
) -> pd.DataFrame:
    """
    Generic 3-column point extraction from any CSV.

    Reads uid_col, lat_col, lon_col → renames to {prefix}_uid, {prefix}_lat, {prefix}_long.
    """
    src = Path(input_path)
    dst = Path(output_path) if output_path else (OUT_DIR / f"{prefix}.csv")

    log.info("═══ Points-only extraction: %s ═══", src.name)
    df = read_csv_fast(src, usecols=[uid_col, lat_col, lon_col])
    df = df.rename(columns={
        uid_col: f"{prefix}_uid",
        lat_col: f"{prefix}_lat",
        lon_col: f"{prefix}_long",
    })
    df[f"{prefix}_uid"] = enforce_integer(df[f"{prefix}_uid"])
    df[f"{prefix}_lat"] = enforce_float(df[f"{prefix}_lat"])
    df[f"{prefix}_long"] = enforce_float(df[f"{prefix}_long"])

    save_csv(df, dst)
    return df


def clean_generic(
    input_path: str | Path,
    keep_cols: Optional[List[str]] = None,
    rename_map: Optional[Dict[str, str]] = None,
    dtype_map: Optional[Dict[str, str]] = None,
    output_path: Optional[str | Path] = None,
    clean_text_cols: Optional[List[str]] = None,
    validate_pincode_col: Optional[str] = None,
    validate_year_col: Optional[str] = None,
    coord_fill: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Generic cleaning pipeline for any CSV.

    Parameters
    ----------
    input_path         : Path to CSV file.
    keep_cols          : Columns to keep (None = all).
    rename_map         : {old: new} column rename mapping.
    dtype_map          : {col: spec} dtype enforcement after renaming.
    output_path        : Where to save (default: cleaned/<input_stem>.csv).
    clean_text_cols    : Columns to apply proper-case text cleaning.
    validate_pincode_col : Column to validate as 6-digit pincode.
    validate_year_col  : Column to validate as 4-digit year.
    coord_fill         : {'lat': col, 'lon': col, 'pin': col} for pincode coord fill.
    """
    src = Path(input_path)
    dst = Path(output_path) if output_path else (OUT_DIR / f"{src.stem}_cleaned.csv")

    log.info("═══ Generic cleaning: %s ═══", src.name)
    df = read_csv_fast(src, usecols=keep_cols)

    if rename_map:
        df = df.rename(columns=rename_map)

    if dtype_map:
        df = enforce_dtypes(df, dtype_map)

    if clean_text_cols:
        for col in clean_text_cols:
            if col in df.columns:
                df[col] = clean_text_series(df[col])

    if validate_pincode_col and validate_pincode_col in df.columns:
        df[validate_pincode_col] = validate_pincode(df[validate_pincode_col])

    if validate_year_col and validate_year_col in df.columns:
        df[validate_year_col] = validate_year(df[validate_year_col])

    if coord_fill:
        lat_c = coord_fill.get("lat")
        lon_c = coord_fill.get("lon")
        pin_c = coord_fill.get("pin")
        if lat_c and lon_c and pin_c:
            df = fill_coords_from_pincode(df, lat_c, lon_c, pin_c)

    save_csv(df, dst)
    return df


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  SECTION 9 — CLI (argparse) & Entry Point                          ║
# ╚══════════════════════════════════════════════════════════════════════╝

def _dispatch(name: str, args) -> None:
    """Route a registered file name to its cleaner function."""
    cleaner_name = FILE_REGISTRY[name]["cleaner"]
    cleaner_fn = globals()[cleaner_name]

    kwargs: Dict[str, Any] = {}
    if args.input:
        kwargs["input_path"] = Path(args.input)
    if args.output:
        if name in ("health_center", "agri_industry", "school"):
            kwargs["output_dir"] = Path(args.output)
        else:
            kwargs["output_path"] = Path(args.output)

    cleaner_fn(**kwargs)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="facilities_data_cleaning",
        description="Modular facility CSV cleaner — fast I/O, NER name cleaning, "
                    "pincode coord-fill, configurable per-file pipelines.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES
--------
  # Clean one file
  %(prog)s --file agri_industries

  # Clean agri-industry (reclassified into 11 categories)
  %(prog)s --file agri_industry


  # Clean all registered files
  %(prog)s --file all

  # See registered files
  %(prog)s --list-files

  # Points-only extraction (any CSV)
  %(prog)s --points-only data/my.csv --uid-col gid --lat-col lat --lon-col lon --prefix my

  # Generic cleaning (any CSV)
  %(prog)s --generic data/my.csv --keep-cols "col1,col2,col3" --rename "old:new,old2:new2"
""",
    )

    # --- Mutually exclusive primary modes ---
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--file", type=str, metavar="NAME",
        help="Registered file to clean (or 'all'). Use --list-files to see options.",
    )
    mode.add_argument(
        "--points-only", type=str, metavar="CSV_PATH",
        help="Generic 3-column (uid, lat, long) extraction from any CSV.",
    )
    mode.add_argument(
        "--generic", type=str, metavar="CSV_PATH",
        help="Generic column-select + rename pipeline for any CSV.",
    )
    mode.add_argument(
        "--list-files", action="store_true",
        help="List all registered file configs and exit.",
    )

    # --- Shared options ---
    p.add_argument("--input", type=str, help="Override input file path for --file mode.")
    p.add_argument("--output", type=str, help="Override output file/dir path.")

    # --- Points-only options ---
    pts = p.add_argument_group("points-only options")
    pts.add_argument("--uid-col", type=str, help="UID column name.")
    pts.add_argument("--lat-col", type=str, help="Latitude column name.")
    pts.add_argument("--lon-col", type=str, help="Longitude column name.")
    pts.add_argument("--prefix", type=str, help="Output column prefix (e.g. 'my_facility').")

    # --- Generic options ---
    gen = p.add_argument_group("generic options")
    gen.add_argument(
        "--keep-cols", type=str,
        help="Comma-separated columns to keep.  E.g. 'gid,name,lat,lon'",
    )
    gen.add_argument(
        "--rename", type=str,
        help="Comma-separated old:new rename pairs.  E.g. 'gid:uid,name:facility_name'",
    )
    gen.add_argument(
        "--dtypes", type=str,
        help="Comma-separated col:type pairs.  E.g. 'uid:int,lat:float,name:str'",
    )
    gen.add_argument(
        "--clean-text", type=str,
        help="Comma-separated columns to apply proper-case text cleaning.",
    )

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # --- List files ---
    if args.list_files:
        print("\nRegistered file configs")
        print("=" * 60)
        for name, cfg in FILE_REGISTRY.items():
            print(f"  {name:<20s}  →  {cfg['filename']}")
            print(f"    keep  : {cfg['columns_to_keep']}")
            rn = cfg.get("rename_map", {})
            if rn:
                print(f"    rename: { {k: v for k, v in list(rn.items())[:5]} }{'…' if len(rn) > 5 else ''}")
            print()
        return

    # --- File mode ---
    if args.file:
        if args.file == "all":
            for name in FILE_REGISTRY:
                _dispatch(name, args)
        elif args.file in FILE_REGISTRY:
            _dispatch(args.file, args)
        else:
            parser.error(
                f"Unknown file '{args.file}'. Available: {', '.join(FILE_REGISTRY)} or 'all'"
            )
        return

    # --- Points-only mode ---
    if args.points_only:
        for required in ("uid_col", "lat_col", "lon_col", "prefix"):
            if not getattr(args, required):
                parser.error(f"--{required.replace('_', '-')} is required with --points-only")
        clean_points_only(
            input_path=args.points_only,
            uid_col=args.uid_col,
            lat_col=args.lat_col,
            lon_col=args.lon_col,
            prefix=args.prefix,
            output_path=args.output,
        )
        return

    # --- Generic mode ---
    if args.generic:
        keep = [c.strip() for c in args.keep_cols.split(",")] if args.keep_cols else None
        rename = (
            dict(pair.split(":") for pair in args.rename.split(","))
            if args.rename else None
        )
        dtypes = (
            dict(pair.split(":") for pair in args.dtypes.split(","))
            if args.dtypes else None
        )
        text_cols = (
            [c.strip() for c in args.clean_text.split(",")]
            if args.clean_text else None
        )
        clean_generic(
            input_path=args.generic,
            keep_cols=keep,
            rename_map=rename,
            dtype_map=dtypes,
            output_path=args.output,
            clean_text_cols=text_cols,
        )
        return

    # Nothing specified — show help
    parser.print_help()


if __name__ == "__main__":
    main()
