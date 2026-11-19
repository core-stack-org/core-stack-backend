"""Drought decision rules (Manual for Drought Management, updated to December 2020).

All functions take numpy arrays (or scalars) and work element by element, so the
same code classifies one pixel or a whole block. NaN marks a missing value.
"""

import numpy as np

from computing.drought.local.config import WINDOW_WEEKS

# Window and week classes, as stored in the outputs.
NONE, MILD, MODERATE, SEVERE = 0, 1, 2, 3

# Impact indicator classes.
NORMAL, IMPACT_MODERATE, IMPACT_SEVERE = 0, 1, 2

DRY_WEEK_SHARE = 50  # a dry week has less than 50% of normal rainfall (manual 3.2.1 B)
DRY_SPELL_WEEKS = 4
SCANTY_DEVIATION = -60  # rainfall deviation of -60% or worse (manual table 3.1)
SPI_TRIGGER = -1.5


def _classify(value, severe, moderate):
    """severe / moderate are boolean arrays; a missing value counts as normal (decided rule A)."""
    value = np.asarray(value, dtype=float)
    out = np.full(value.shape, NORMAL, dtype=np.uint8)
    present = ~np.isnan(value)
    out[present & moderate] = IMPACT_MODERATE
    out[present & severe] = IMPACT_SEVERE
    return out


def vci_class(vci):
    """VCI in %: below 40 severe, 40 to below 60 moderate, 60 and above normal (manual table 3.4)."""
    v = np.asarray(vci, dtype=float)
    with np.errstate(invalid="ignore"):
        return _classify(v, v < 40, v < 60)


def mai_class(mai):
    """MAI in %: 50 or less severe, above 50 to 75 moderate, above 75 normal (manual table 3.7)."""
    v = np.asarray(mai, dtype=float)
    with np.errstate(invalid="ignore"):
        return _classify(v, v <= 50, v <= 75)


def sown_class(sown):
    """Area sown, % of normal: 75 or less severe, below 85 moderate, 85 and above normal (manual 3.2.3.1)."""
    v = np.asarray(sown, dtype=float)
    with np.errstate(invalid="ignore"):
        return _classify(v, v <= 75, v < 85)


def rain_trigger(dry_spell, deviation, spi):
    """Any one of: a dry spell in the window, scanty rainfall, SPI below -1.5 (manual table 3.11)."""
    deviation = np.asarray(deviation, dtype=float)
    spi = np.asarray(spi, dtype=float)
    with np.errstate(invalid="ignore"):
        return np.asarray(dry_spell, dtype=bool) | (deviation <= SCANTY_DEVIATION) | (spi < SPI_TRIGGER)


def window_class(trigger, vci_cls, mai_cls, sown_cls):
    """Severe: at least 2 indicators severe and the third at least moderate.
    Moderate: at least 2 moderate or severe. Mild: triggered but neither. None: no trigger."""
    classes = np.stack(np.broadcast_arrays(vci_cls, mai_cls, sown_cls))
    n_severe = (classes == IMPACT_SEVERE).sum(axis=0)
    n_moderate_or_worse = (classes >= IMPACT_MODERATE).sum(axis=0)
    severe = (n_severe >= 2) & (n_moderate_or_worse == 3)
    moderate = n_moderate_or_worse >= 2
    out = np.where(severe, SEVERE, np.where(moderate, MODERATE, MILD)).astype(np.uint8)
    return np.where(np.asarray(trigger, dtype=bool), out, NONE).astype(np.uint8)


def weekly_labels(window_classes):
    """window_classes[i] is the class of the window starting at the season's week i.
    Week w takes the worst class of the windows that contain it (those starting w-3 to w)."""
    window_classes = np.asarray(window_classes)
    n = window_classes.shape[0]
    weeks = n + WINDOW_WEEKS - 1
    labels = np.zeros((weeks,) + window_classes.shape[1:], dtype=np.uint8)
    for start in range(n):
        span = slice(start, start + WINDOW_WEEKS)
        labels[span] = np.maximum(labels[span], window_classes[start])
    return labels


def dry_weeks(week_rain, week_normal):
    """A week is dry when its rainfall is below 50% of the normal for that week."""
    week_rain = np.asarray(week_rain, dtype=float)
    week_normal = np.asarray(week_normal, dtype=float)
    with np.errstate(invalid="ignore"):
        return week_rain < week_normal * DRY_WEEK_SHARE / 100


def dry_spell_in_window(dry):
    """dry[0:4] are the window's 4 weeks; a dry spell needs all 4 dry."""
    return np.asarray(dry, dtype=bool)[:DRY_SPELL_WEEKS].all(axis=0)


def dry_spell_weeks(dry):
    """Weeks (after onset) that lie inside a run of 4 or more dry weeks."""
    dry = np.asarray(dry, dtype=bool)
    covered = np.zeros(dry.shape, dtype=bool)
    for start in range(dry.shape[0] - DRY_SPELL_WEEKS + 1):
        span = slice(start, start + DRY_SPELL_WEEKS)
        covered[span] |= dry[span].all(axis=0)
    return covered.sum(axis=0).astype(np.uint8)


def longest_dry_spell(dry):
    """Length in weeks of the longest run of dry weeks, if it is a dry spell (4 or more), else 0.
    Same meaning as GEE's dryspell_length (longest run of dry-spell windows + 3)."""
    dry = np.asarray(dry, dtype=bool)
    run = np.zeros(dry.shape[1:], dtype=np.int16)
    best = np.zeros(dry.shape[1:], dtype=np.int16)
    for week in dry:
        run = np.where(week, run + 1, 0)
        best = np.maximum(best, run)
    return np.where(best >= DRY_SPELL_WEEKS, best, 0).astype(np.uint8)


def season_counts(labels):
    """Weeks per class over the season: arrays for none, mild, moderate, severe."""
    labels = np.asarray(labels)
    return [(labels == c).sum(axis=0).astype(np.uint8) for c in (NONE, MILD, MODERATE, SEVERE)]
