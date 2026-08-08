"""
Time-series tally plots — corrected version.
Selects 4 representative farms that Sir can visually verify.

Plot 1 (2x2): Four cases showing our vs Shuvam side-by-side
Plot 2: Gap-fill before/after demonstration + pipeline comparison

Saves to: data/tally_plots/
"""
import dotenv; dotenv.load_dotenv('.env')

import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
import rasterio.windows
import matplotlib.pyplot as plt
import os, sys
sys.path.insert(0, '.')
from computing.farm_boundaries.et_intersection import _gap_fill_monthly_farms

TEHSIL   = "sanganer"
BASE     = f"data/farm_boundaries/rajasthan/jaipur/{TEHSIL}"
SHUVAM   = f"data/et_rasters/shuvam_mai_{TEHSIL}_2018.tif"
OUT_DIR  = "data/tally_plots"
os.makedirs(OUT_DIR, exist_ok=True)

MONTH_LABELS = ['Jul-18','Aug-18','Sep-18','Oct-18','Nov-18','Dec-18',
                'Jan-19','Feb-19','Mar-19','Apr-19','May-19','Jun-19']
CROP_DATES = ['2018-07-01','2018-08-01','2018-09-01','2018-10-01',
              '2018-11-01','2018-12-01','2019-01-01','2019-02-01',
              '2019-03-01','2019-04-01','2019-05-01','2019-06-01']

# ── load ─────────────────────────────────────────────────────────────────────
print("Loading...")
static  = gpd.read_parquet(f"{BASE}/farm_static.parquet")
monthly = pd.read_parquet(f"{BASE}/farm_monthly.parquet")
tally   = pd.read_parquet(f"{BASE}/mai_tally_2018.parquet")

monthly['ds'] = monthly['date'].astype(str).str[:10]
monthly_cy    = monthly[monthly['ds'].isin(CROP_DATES)].copy()
monthly_cy['sk'] = monthly_cy['ds'].map({d:i for i,d in enumerate(CROP_DATES)})

# Load Shuvam raster into memory
with rasterio.open(SHUVAM) as src:
    rcrs    = src.crs
    rdata   = src.read().astype("float32")
    rtf     = src.transform
    nd      = float(src.nodata) if src.nodata else -9999.0
rdata[rdata == nd] = np.nan
rdata[rdata <= -9999] = np.nan
farms_rp = static.to_crs(rcrs)


def shuvam_series(fid):
    """12-month MAI from Shuvam's raster for one farm."""
    geom = farms_rp.loc[farms_rp['farm_id']==fid, 'geometry'].values[0]
    b = geom.bounds
    w = rasterio.windows.from_bounds(*b, rtf)
    r0, c0 = max(0, int(w.row_off)), max(0, int(w.col_off))
    r1 = min(rdata.shape[1], int(w.row_off + w.height)+1)
    c1 = min(rdata.shape[2], int(w.col_off + w.width)+1)
    if r1 <= r0 or c1 <= c0:
        return np.full(12, np.nan)
    clip = rdata[:12, r0:r1, c0:c1]
    return np.array([np.nanmean(clip[i]) if np.isfinite(clip[i]).any()
                     else np.nan for i in range(12)])


def our_series(fid):
    """12-month MAI from our parquet, crop-year order."""
    rows = monthly_cy[monthly_cy['farm_id']==fid].sort_values('sk')
    if len(rows) != 12:
        return np.full(12, np.nan)
    return rows['mai'].values.astype(float)


# ── select 4 farms wisely ────────────────────────────────────────────────────
# CRITICAL: only from farms with BOTH annual values valid
both_ok = tally.dropna(subset=['mai_annual','shuvam_annual']).copy()
both_ok['diff'] = (both_ok['mai_annual'] - both_ok['shuvam_annual']).abs()

# 1. Best match (smallest diff, > 0 to avoid trivial)
best = both_ok[both_ok['diff'] > 0].nsmallest(10, 'diff')['farm_id'].iloc[0]

# 2. Typical farm (diff near the median)
med_diff = both_ok['diff'].median()
typical = both_ok.iloc[(both_ok['diff'] - med_diff).abs().argsort()[:1]]['farm_id'].iloc[0]

# 3. Largest difference
worst = both_ok.nlargest(1, 'diff')['farm_id'].iloc[0]

# 4. A "rajasthan_jaipur_sanganer_000000" — the well-known reference farm
ref_farm = "rajasthan_jaipur_sanganer_000000"

FARMS = [
    (best,      "Farm A: best match"),
    (ref_farm,  "Farm B: reference farm"),
    (typical,   "Farm C: median-diff farm"),
    (worst,     "Farm D: largest difference"),
]

print("Selected farms:")
for fid, label in FARMS:
    row = both_ok[both_ok['farm_id']==fid]
    d = row['diff'].values[0] if len(row) else float('nan')
    print(f"  {label}: {fid}  diff={d:.4f}")

# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 1: 2×2 time-series comparison
# ═══════════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle(
    "MAI Time-Series Tally — Sanganer 2018\n"
    "Our Pipeline vs Shuvam's GEE Raster (crop-year Jul 2018 – Jun 2019)",
    fontsize=14, fontweight='bold', y=1.01
)

x = np.arange(12)

for ax, (fid, label) in zip(axes.flat, FARMS):
    ours = our_series(fid)
    shuv = shuvam_series(fid)

    ax.plot(x, shuv, 'o-', color='#E85D04', linewidth=2, markersize=6, label="Shuvam (GEE)")
    ax.plot(x, ours, 's--', color='#0077B6', linewidth=2, markersize=6, label="Ours (local)")

    # Difference shading
    valid = np.isfinite(ours) & np.isfinite(shuv)
    if valid.any():
        ax.fill_between(x, ours, shuv, where=valid, alpha=0.12, color='purple')

    # Kharif
    ax.axvspan(-0.5, 3.5, alpha=0.06, color='green')
    ax.text(1.5, 0.97, 'Kharif', transform=ax.get_xaxis_transform(),
            ha='center', va='top', fontsize=8, color='green', alpha=0.7)

    # Stats
    if valid.any():
        d = np.abs(ours[valid] - shuv[valid])
        ax.set_title(f"{label}\n{fid}\n"
                     f"mean|diff|={d.mean():.4f}  max|diff|={d.max():.4f}",
                     fontsize=9)
    else:
        ax.set_title(f"{label}\n{fid}\n(no overlapping data)", fontsize=9)

    ax.axhline(0.5, color='orange', ls=':', lw=1, alpha=0.7, label='Moderate stress (0.5)')
    ax.axhline(0.25, color='red', ls=':', lw=1, alpha=0.7, label='Severe stress (0.25)')
    ax.set_xticks(x)
    ax.set_xticklabels(MONTH_LABELS, rotation=45, ha='right', fontsize=8)
    ax.set_ylabel("MAI (AET/PET)", fontsize=9)
    ax.set_ylim(-0.05, 1.05)
    ax.legend(fontsize=7, loc='upper right')
    ax.grid(True, alpha=0.3)

plt.tight_layout()
p1 = f"{OUT_DIR}/mai_timeseries_tally_sanganer_2018.png"
plt.savefig(p1, dpi=150, bbox_inches='tight')
print(f"Saved: {p1}")
plt.close()


# ═══════════════════════════════════════════════════════════════════════════════
# PLOT 2: Gap-fill demo (left) + pipeline comparison (right)
# ═══════════════════════════════════════════════════════════════════════════════
print("Generating gap-fill demo...")

# Use the reference farm (known to have all 12 months)
actual_mai = our_series(ref_farm)

# Simulate 2 missing months: Aug (idx 1) and Sep (idx 2) in crop order
# But gap-fill works on calendar order. We need to convert.
# Crop order: Jul(0) Aug(1) Sep(2) Oct(3) Nov(4) Dec(5) Jan(6) Feb(7) Mar(8) Apr(9) May(10) Jun(11)
# Calendar:  Jan=0   Feb=1   Mar=2   Apr=3   May=4   Jun=5   Jul=6   Aug=7   Sep=8   Oct=9   Nov=10  Dec=11
# Mapping: crop_idx -> cal_month: {0->6, 1->7, 2->8, 3->9, 4->10, 5->11, 6->0, 7->1, 8->2, 9->3, 10->4, 11->5}
crop_to_cal = [6, 7, 8, 9, 10, 11, 0, 1, 2, 3, 4, 5]

# Build calendar-order matrix from actual values
cal_complete = np.full(12, np.nan)
for ci, cal_i in enumerate(crop_to_cal):
    cal_complete[cal_i] = actual_mai[ci]

# Now set Aug (cal idx 7) and Sep (cal idx 8) to NaN
cal_missing = cal_complete.copy()
cal_missing[7] = np.nan   # Aug
cal_missing[8] = np.nan   # Sep

# Apply gap-fill
cal_filled = _gap_fill_monthly_farms(cal_missing.reshape(1, 12))[0]

# Convert back to crop order for plotting
actual_crop = actual_mai.copy()
missing_crop = np.array([cal_missing[crop_to_cal[i]] for i in range(12)])
filled_crop  = np.array([cal_filled[crop_to_cal[i]] for i in range(12)])

# Shuvam for same farm
shuv_ref = shuvam_series(ref_farm)

fig2, axes2 = plt.subplots(1, 2, figsize=(18, 6))
fig2.suptitle(
    "Missing Data Handling & Gap-Fill Demonstration\n"
    f"Farm: {ref_farm}  |  Sanganer 2018",
    fontsize=13, fontweight='bold'
)

# ── Left: Before vs After gap-fill ───────────────────────────────────────────
ax = axes2[0]
ax.set_title("Before vs After Gap-Fill\n(Aug & Sep artificially set to NaN to demonstrate)", fontsize=10)

ax.plot(x, actual_crop, 'o-', color='green', linewidth=2, markersize=6,
        label='Actual (complete data)', zorder=4, alpha=0.5)
ax.plot(x, missing_crop, 'x--', color='#999', linewidth=1.5, markersize=10,
        label='Before gap-fill (Aug, Sep = NaN)', zorder=3, markeredgewidth=2)
ax.plot(x, filled_crop, 's-', color='#0077B6', linewidth=2.5, markersize=8,
        label='After gap-fill', zorder=5)

# Annotate the two filled points
for ci in range(12):
    if np.isnan(missing_crop[ci]) and np.isfinite(filled_crop[ci]):
        cal_m = crop_to_cal[ci]
        nbrs = [crop_to_cal.index(n) for n in [6, 8] if cal_m == 7] or \
               [crop_to_cal.index(n) for n in [7, 9] if cal_m == 8] or []
        # Get label text
        if cal_m == 7:  # Aug filled from Jul & Sep
            lbl = f"Filled: {filled_crop[ci]:.3f}\nmean(Jul, Sep)"
        elif cal_m == 8:  # Sep filled from Aug & Oct
            lbl = f"Filled: {filled_crop[ci]:.3f}\nmean(Aug, Oct)"
        else:
            lbl = f"Filled: {filled_crop[ci]:.3f}"

        yoff = 0.15 if ci == 1 else 0.12
        ax.annotate(lbl, xy=(ci, filled_crop[ci]),
                    xytext=(ci + 0.7, filled_crop[ci] + yoff),
                    fontsize=8, color='#0077B6',
                    arrowprops=dict(arrowstyle='->', color='#0077B6', lw=1.2),
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='#dbeafe', alpha=0.8))

ax.axvspan(-0.5, 3.5, alpha=0.07, color='green')
ax.text(1.5, 0.96, 'Kharif', transform=ax.get_xaxis_transform(),
        ha='center', va='top', fontsize=9, color='green', alpha=0.8)
ax.axhline(0.5, color='orange', ls=':', lw=1.2, label='Moderate stress (0.5)')
ax.axhline(0.25, color='red', ls=':', lw=1.2, label='Severe stress (0.25)')

rule_text = ("Gap-fill rules (mirrors Shuvam's ET_Applications/helper.py):\n"
             "  Aug → mean(Jul, Sep)\n"
             "  Sep → mean(Aug, Oct)\n"
             "  Jul → Aug only (crop-year start boundary)\n"
             "  Jun → May only (crop-year end boundary)\n"
             "  If no valid neighbour → stays NaN")
ax.text(0.55, 0.98, rule_text, transform=ax.transAxes, fontsize=8,
        va='top', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.85))

ax.set_xticks(x)
ax.set_xticklabels(MONTH_LABELS, rotation=45, ha='right', fontsize=9)
ax.set_ylabel("MAI (AET/PET)", fontsize=10)
ax.set_ylim(-0.05, 1.15)
ax.legend(fontsize=8.5, loc='upper left')
ax.grid(True, alpha=0.3)

# ── Right: Our pipeline vs Shuvam ────────────────────────────────────────────
ax2 = axes2[1]
valid = np.isfinite(actual_crop) & np.isfinite(shuv_ref)
d = np.abs(actual_crop[valid] - shuv_ref[valid])
ax2.set_title(f"Our Pipeline vs Shuvam's GEE Raster (same farm)\n"
              f"mean|diff|={d.mean():.4f}   max|diff|={d.max():.4f}", fontsize=10)

ax2.plot(x, shuv_ref, 'o-', color='#E85D04', linewidth=2.5, markersize=8, label="Shuvam (GEE)")
ax2.plot(x, actual_crop, 's--', color='#0077B6', linewidth=2.5, markersize=8, label="Our pipeline")
ax2.fill_between(x, actual_crop, shuv_ref, where=valid, alpha=0.15, color='purple', label='Difference')

ax2.axvspan(-0.5, 3.5, alpha=0.07, color='green')
ax2.text(1.5, 0.96, 'Kharif', transform=ax2.get_xaxis_transform(),
         ha='center', va='top', fontsize=9, color='green', alpha=0.8)
ax2.axhline(0.5, color='orange', ls=':', lw=1.2, label='Moderate stress (0.5)')
ax2.axhline(0.25, color='red', ls=':', lw=1.2, label='Severe stress (0.25)')
ax2.set_xticks(x)
ax2.set_xticklabels(MONTH_LABELS, rotation=45, ha='right', fontsize=9)
ax2.set_ylabel("MAI (AET/PET)", fontsize=10)
ax2.set_ylim(-0.05, 1.15)
ax2.legend(fontsize=8.5)
ax2.grid(True, alpha=0.3)

plt.tight_layout()
p2 = f"{OUT_DIR}/gap_fill_and_tally_demo_2018.png"
plt.savefig(p2, dpi=150, bbox_inches='tight')
print(f"Saved: {p2}")
plt.close()

print(f"\nAll plots saved to {OUT_DIR}/")
