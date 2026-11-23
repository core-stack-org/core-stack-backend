"""The temporal-correction rules for annual LULC v4 layers: pure numpy, no file access.

Copied unchanged from 4_temporal_correction.py (Scrubland-Field-Delineation, branch
`latest`, 2026-11-16), the script that made the pan-India temporally corrected LULC v4
layers. Keep this file identical to that script so the APIs reproduce its output pixel
for pixel; test_temporal_correction.py checks it against the reference outputs.

Every function takes lists of uint8 arrays, one per year:

  correct_block_6yr(fine, cluster)
      six years -> six corrected fine-label arrays (the six-year API)
  correct_block_incremental(prev_corrected, cur_raw, cur_cluster, next_raw)
      -> (corrected year, flipped mask) (the incremental API)
"""

import numpy as np


# ---------------------------------------------------------------------------
# Class space
# ---------------------------------------------------------------------------
# Fine -> grouped. Water {2,3,4} and crop {8,9,10,11} are merged so that the
# temporal reasoning is not driven by subclass churn. Class 5 is legacy and
# never appears in the data, hence its absence here.
ORIG = [0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13]
GROUP = [0, 1, 2, 2, 2, 6, 7, 8, 8, 8, 8, 12, 13]

WATER_SET = (2, 3, 4)
CROP_SET = (8, 9, 10, 11)

BAND_LABEL = 1      # predicted_label
BAND_CLUSTER = 2    # predicted_cluster (== cropping frequency)

_GROUP_LUT = np.zeros(256, np.uint8)
for _o, _g in zip(ORIG, GROUP):
    _GROUP_LUT[_o] = _g

_IS_WATER = np.zeros(256, bool)
_IS_WATER[list(WATER_SET)] = True
_IS_CROP = np.zeros(256, bool)
_IS_CROP[list(CROP_SET)] = True

# Semantic admissibility matrix (doc Table 1). Rows = target class A, columns =
# current class B; a correction is applied only where B -> A is permitted.
# 2026-11: three cells changed from doc Table 1 (0 -> 1), so these one-year
# blips are now corrected like C-T-C / T-C-T and Bu-C-Bu:
#   A = Crop,       B = Built-up    (C-Bu-C)
#   A = Crop,       B = Plantation  (C-P-C)
#   A = Plantation, B = Crop        (P-C-P)
MAT_CLASSES = [1, 2, 6, 7, 8, 12, 13]
ALLOW_TABLE = [
    #  Bu  Wa  Tr  Ba  Cr  Sc  Pl
    [0, 0, 0, 1, 1, 1, 1],   # A = Built-up
    [0, 0, 1, 0, 0, 0, 0],   # A = Water
    [1, 1, 0, 1, 1, 1, 1],   # A = Tree / Forest
    [1, 0, 1, 0, 1, 1, 1],   # A = Barren
    [1, 0, 1, 1, 0, 1, 1],   # A = Crop
    [0, 0, 1, 1, 1, 0, 1],   # A = Scrub
    [1, 0, 1, 1, 1, 1, 0],   # A = Plantation
]

_ALLOW = np.zeros((256, 256), bool)
for _i, _A in enumerate(MAT_CLASSES):
    for _j, _B in enumerate(MAT_CLASSES):
        _ALLOW[_A, _B] = bool(ALLOW_TABLE[_i][_j])


def allow_flip(A, B):
    """True where rewriting B to A is permitted. Mirrors allow_flip() in GEE."""
    return _ALLOW[A, B]


def group_classes(fine):
    return _GROUP_LUT[fine]


# ---------------------------------------------------------------------------
# Singlet detection - numpy port of singlet_category_6yr()
# ---------------------------------------------------------------------------
def singlet_centres(seq):
    """
    Admissible ABA centres for years 2..5 of a six-year stack.

    seq is a list of six uint8 arrays (grouped classes). Returns (s2, s3, s4, s5)
    boolean arrays: centre t is set where seq[t-1] == seq[t+1], seq[t] differs,
    and flipping seq[t] -> seq[t-1] is admissible.
    """
    out = []
    for t in range(1, 5):
        left, mid, right = seq[t - 1], seq[t], seq[t + 1]
        raw = (left == right) & (mid != left)
        out.append(raw & allow_flip(left, mid))
    return out


def singlet_stats(seq):
    """
    Returns (cat, s2, s3, s4, s5, i2, i3, i4, i5).

    cat follows the notebook's categories: 0 none, 1 disjoint singles,
    2 one run of two adjacent centres, 3 run of three, 4 run of four,
    6 composite (a run of two plus a separate isolated single).
    """
    s2, s3, s4, s5 = singlet_centres(seq)

    any_s = s2 | s3 | s4 | s5
    adj = (s2 & s3) | (s3 & s4) | (s4 & s5)

    run4 = s2 & s3 & s4 & s5
    run3 = (s2 & s3 & s4) | (s3 & s4 & s5)
    run2 = adj

    max_run = np.zeros(s2.shape, np.uint8)
    max_run[any_s] = 1
    max_run[run2] = 2
    max_run[run3] = 3
    max_run[run4] = 4

    # a set centre with no set neighbour
    i2 = s2 & ~s3
    i3 = s3 & ~s2 & ~s4
    i4 = s4 & ~s3 & ~s5
    i5 = s5 & ~s4
    has_isolated = i2 | i3 | i4 | i5

    is_max2 = max_run == 2
    composite = any_s & is_max2 & adj & has_isolated
    disjoint = any_s & ~adj

    # assignment order matches the chained .where() in the notebook
    cat = np.zeros(s2.shape, np.uint8)
    cat[disjoint] = 1
    cat[is_max2 & ~disjoint & ~composite] = 2
    cat[max_run == 3] = 3
    cat[max_run == 4] = 4
    cat[composite] = 6
    cat[~any_s] = 0

    return cat, s2, s3, s4, s5, i2, i3, i4, i5


# ---------------------------------------------------------------------------
# Singlet correction - numpy port of correct_singlets_6yr()
# ---------------------------------------------------------------------------
def _flip_B_to_A(seq, cond, A, B, years):
    """In the given year indices, rewrite B to A where cond holds."""
    for t in years:
        m = cond & (seq[t] == B)
        seq[t][m] = A[m]


def _resolve_run2(seq, cond, ai, bi):
    """
    Two adjacent centres (ABAB). Decide between A and B by majority over the
    full six years, ties to A, then rewrite the loser to the winner across all
    six bands.

    Note the count over six years is algebraically identical to voting with the
    two years outside the ABAB segment: the segment contributes exactly 2 to
    each side and cancels.
    """
    A = seq[ai].copy()
    B = seq[bi].copy()

    cA = np.zeros(A.shape, np.uint8)
    cB = np.zeros(A.shape, np.uint8)
    for t in range(6):
        cA += (seq[t] == A)
        cB += (seq[t] == B)

    a_wins = cA > cB
    b_wins = cB > cA
    tie = cA == cB

    _flip_B_to_A(seq, cond & (a_wins | tie), A, B, range(6))
    _flip_B_to_A(seq, cond & b_wins, B, A, range(6))


def correct_singlets(seq):
    """seq is modified in place. Mirrors correct_singlets_6yr()."""
    cat, s2, s3, s4, s5, i2, i3, i4, i5 = singlet_stats(seq)

    # --- CASE 1: disjoint singles -----------------------------------------
    # Each isolated centre takes its surrounding class. Applied left to right,
    # exactly as the notebook does; safe because no two centres are adjacent
    # in this category.
    case1 = cat == 1
    for t, s in zip((1, 2, 3, 4), (s2, s3, s4, s5)):
        m = case1 & s
        seq[t][m] = seq[t - 1][m]

    # --- CASE 3: run of three (ABABA) -------------------------------------
    case3 = cat == 3
    run3_123 = case3 & s2 & s3 & s4          # centres 2,3,4 -> years 1..5
    A, B = seq[0].copy(), seq[1].copy()
    _flip_B_to_A(seq, run3_123, A, B, [0, 1, 2, 3, 4])

    run3_234 = case3 & s3 & s4 & s5          # centres 3,4,5 -> years 2..6
    A, B = seq[1].copy(), seq[2].copy()
    _flip_B_to_A(seq, run3_234, A, B, [1, 2, 3, 4, 5])

    # --- CASE 4: run of four (ABABAB) -------------------------------------
    case4 = cat == 4
    run4 = case4 & s2 & s3 & s4 & s5
    A, B = seq[0].copy(), seq[1].copy()
    _flip_B_to_A(seq, run4, A, B, range(6))

    # --- CASE 2: run of two (ABAB) ----------------------------------------
    case2 = cat == 2
    _resolve_run2(seq, case2 & s2 & s3 & ~s4 & ~s5, 0, 1)
    _resolve_run2(seq, case2 & s3 & s4 & ~s2 & ~s5, 1, 2)
    _resolve_run2(seq, case2 & s4 & s5 & ~s2 & ~s3, 2, 3)

    # --- CASE 6: composite -------------------------------------------------
    # Isolated centres first, then recompute the singlet structure and apply
    # the run-of-two rule to what remains.
    case6 = cat == 6
    if case6.any():
        tmp = [a.copy() for a in seq]
        for t, i in zip((1, 2, 3, 4), (i2, i3, i4, i5)):
            m = case6 & i
            tmp[t][m] = tmp[t - 1][m]

        cat2, t2, t3, t4, t5, _, _, _, _ = singlet_stats(tmp)
        c6r2 = case6 & (cat2 == 2)
        _resolve_run2(tmp, c6r2 & t2 & t3 & ~t4 & ~t5, 0, 1)
        _resolve_run2(tmp, c6r2 & t3 & t4 & ~t2 & ~t5, 1, 2)
        _resolve_run2(tmp, c6r2 & t4 & t5 & ~t2 & ~t3, 2, 3)

        for t in range(6):
            seq[t][case6] = tmp[t][case6]


# ---------------------------------------------------------------------------
# Doublet detection and correction - ports of detect_doublet_6yr() /
# correct_doublets_6yr()
# ---------------------------------------------------------------------------
def detect_doublets(seq):
    """
    Two-year excursions X Y bounded by A on both sides, with at least two
    consecutive A on one side (doc 1.2.5). Detected on the *uncorrected*
    grouped stack. (A singlet correction can create a new doublet, which this
    single-pass design does not catch - an under-correction, never a wrong one.)

    The middle template accepts A A X Y A * and * A X Y A A (it used to require
    A A X Y A A, as do GEE assets exported before the 2026-11 notebook fix). It
    can coincide with an ABA centre (X A X Y A A, A A X Y A Y); singlets take
    precedence there, which keeps the singlet and doublet stages disjoint per
    pixel.
    """
    y1, y2, y3, y4, y5, y6 = seq

    def xy(A, X, Y):
        return (X != Y) & (X != A) & (Y != A)

    p1 = (y4 == y1) & (y5 == y1) & xy(y1, y2, y3) \
        & allow_flip(y1, y2) & allow_flip(y1, y3)
    p2 = (y5 == y2) & ((y1 == y2) | (y6 == y2)) & xy(y2, y3, y4) \
        & allow_flip(y2, y3) & allow_flip(y2, y4)
    p3 = (y3 == y2) & (y6 == y2) & xy(y2, y4, y5) \
        & allow_flip(y2, y4) & allow_flip(y2, y5)

    s2, s3, s4, s5 = singlet_centres(seq)
    p2 &= ~(s2 | s3 | s4 | s5)

    pat = np.zeros(y1.shape, np.uint8)
    pat[p1] = 1
    pat[p2] = 2
    pat[p3] = 3          # later assignment wins, as in the chained .where()
    return pat


def correct_doublets(seq, pat):
    """seq is modified in place."""
    c1 = pat == 1        # A X Y A A *  -> y2,y3 = A
    A = seq[0].copy()
    seq[1][c1] = A[c1]
    seq[2][c1] = A[c1]

    c2 = pat == 2        # A A X Y A *, * A X Y A A  -> y3,y4 = A
    A = seq[1].copy()
    seq[2][c2] = A[c2]
    seq[3][c2] = A[c2]

    c3 = pat == 3        # * A A X Y A  -> y4,y5 = A
    A = seq[1].copy()
    seq[3][c3] = A[c3]
    seq[4][c3] = A[c3]


# ---------------------------------------------------------------------------
# Re-introduction of the merged water / crop subclasses
# ---------------------------------------------------------------------------
def _safe_water(fine):
    """Force a valid water subclass, falling back to generic water (2)."""
    out = fine.copy()
    out[~_IS_WATER[fine]] = 2
    return out


def reinstate_6yr(fine, cluster, corrected_grouped, pat):
    """
    Expand grouped corrected labels back to the fine legend for all six years.
    Port of reinstate_merged_classes_6yr().

      fine    : list of 6 uint8 arrays, original fine labels
      cluster : list of 6 uint8 arrays, predicted_cluster (crop intensity)
      corrected_grouped : list of 6 uint8 arrays after correction
      pat     : doublet pattern, to pick the water source year for X vs Y
    """
    false = np.zeros(fine[0].shape, bool)
    p1, p2, p3 = pat == 1, pat == 2, pat == 3
    is_x = {1: false, 2: p1, 3: p2, 4: p3, 5: false, 6: false}
    is_y = {1: false, 2: false, 3: p1, 4: p2, 5: p3, 6: false}

    out = []
    for t in range(1, 7):
        orig = fine[t - 1]
        orig_grp = group_classes(orig)
        corr_grp = corrected_grouped[t - 1]

        final = orig.copy()

        is_water = corr_grp == 2
        is_crop = corr_grp == 8
        non_merged = ~(is_water | is_crop)

        # (A) classes that were never merged can be written straight back
        m = non_merged & (corr_grp != orig_grp)
        final[m] = corr_grp[m]

        # (B) crop: keep an existing crop subclass, else take the year's
        #     cropping-frequency class from predicted_cluster
        crop_fill = orig.copy()
        not_crop = ~_IS_CROP[orig]
        crop_fill[not_crop] = cluster[t - 1][not_crop]
        final[is_crop] = crop_fill[is_crop]

        # (C) water: keep an existing water subclass, else borrow from the
        #     previous year - two years back when this year is the doublet's Y
        prev1 = fine[t - 2] if t >= 2 else orig
        prev2 = fine[t - 3] if t >= 3 else prev1
        choose_prev2 = is_y[t]
        choose_prev1 = is_x[t] | ~choose_prev2

        water_source = np.full(orig.shape, 2, np.uint8)
        w1, w2 = _safe_water(prev1), _safe_water(prev2)
        water_source[choose_prev1] = w1[choose_prev1]
        water_source[choose_prev2] = w2[choose_prev2]

        water_fill = orig.copy()
        not_water = ~_IS_WATER[orig]
        water_fill[not_water] = water_source[not_water]
        final[is_water] = water_fill[is_water]

        out.append(final)
    return out


def reinstate_one_year(orig, cluster, corr_grp, prev_fine):
    """
    Single-year version used by the incremental step. Port of
    reinstate_merged_classes_year6(): the water source is always the previous
    (already corrected) year, since an ABA centre has no doublet X/Y role.
    """
    orig_grp = group_classes(orig)
    final = orig.copy()

    is_water = corr_grp == 2
    is_crop = corr_grp == 8
    non_merged = ~(is_water | is_crop)

    m = non_merged & (corr_grp != orig_grp)
    final[m] = corr_grp[m]

    crop_fill = orig.copy()
    not_crop = ~_IS_CROP[orig]
    crop_fill[not_crop] = cluster[not_crop]
    final[is_crop] = crop_fill[is_crop]

    water_source = _safe_water(prev_fine)
    water_fill = orig.copy()
    not_water = ~_IS_WATER[orig]
    water_fill[not_water] = water_source[not_water]
    final[is_water] = water_fill[is_water]

    return final


# ---------------------------------------------------------------------------
# Block drivers
# ---------------------------------------------------------------------------
def correct_block_6yr(fine, cluster):
    """Full six-year correction for one block. Returns 6 fine-label arrays."""
    grouped = [group_classes(f) for f in fine]
    pat = detect_doublets(grouped)          # detected before any correction

    seq = [g.copy() for g in grouped]
    correct_singlets(seq)
    correct_doublets(seq, pat)

    return reinstate_6yr(fine, cluster, seq, pat)


def correct_block_incremental(prev_fine, cur_fine, cur_cluster, next_fine):
    """
    Finalise the middle year of (corrected N-1, raw N, raw N+1) by testing it as
    an ABA centre. Port of detect_ABA_flips_year6() + reinstate.
    """
    g_prev = group_classes(prev_fine)
    g_cur = group_classes(cur_fine)
    g_next = group_classes(next_fine)

    aba = (g_prev == g_next) & (g_cur != g_prev) & allow_flip(g_prev, g_cur)

    corr_grp = g_cur.copy()
    corr_grp[aba] = g_prev[aba]

    return reinstate_one_year(cur_fine, cur_cluster, corr_grp, prev_fine), aba

