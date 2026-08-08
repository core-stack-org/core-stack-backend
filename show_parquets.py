"""Print sample rows from all three output parquets for KT."""
import pandas as pd
import geopandas as gpd

BASE = "data/farm_boundaries/rajasthan/jaipur/sanganer"

print("=" * 80)
print("1. farm_static.parquet")
print("=" * 80)
static = gpd.read_parquet(f"{BASE}/farm_static.parquet")
print(f"Shape: {static.shape[0]:,} rows × {static.shape[1]} columns")
print(f"Columns: {list(static.columns)}")
print(f"CRS: {static.crs}")
print(f"\nSample (first 5 rows, geometry truncated):")
display = static.head(5).copy()
display['geometry'] = display['geometry'].apply(lambda g: str(g)[:60] + "...")
print(display.to_string(index=False))
print(f"\nMemory: {static.memory_usage(deep=True).sum()/1e6:.1f} MB")

print("\n" + "=" * 80)
print("2. farm_annual.parquet")
print("=" * 80)
annual = pd.read_parquet(f"{BASE}/farm_annual.parquet")
print(f"Shape: {annual.shape[0]:,} rows × {annual.shape[1]} columns")
print(f"Columns: {list(annual.columns)}")
print(f"\nSample (first 10 rows):")
print(annual.head(10).to_string(index=False))
print(f"\nYear distribution: {annual['year'].value_counts().to_dict()}")
print(f"Kharif stress farms: {annual['kharif_water_stress'].sum():,} / {len(annual):,}")
print(f"Severe stress farms: {annual['kharif_severe_stress'].sum():,} / {len(annual):,}")
print(f"MAI annual stats: mean={annual['mai_annual'].mean():.4f}, "
      f"median={annual['mai_annual'].median():.4f}, "
      f"min={annual['mai_annual'].min():.4f}, max={annual['mai_annual'].max():.4f}")
print(f"NaN count: {annual['mai_annual'].isna().sum():,}")

print("\n" + "=" * 80)
print("3. farm_monthly.parquet")
print("=" * 80)
monthly = pd.read_parquet(f"{BASE}/farm_monthly.parquet")
print(f"Shape: {monthly.shape[0]:,} rows × {monthly.shape[1]} columns")
print(f"Columns: {list(monthly.columns)}")
print(f"\nDate range: {monthly['date'].min()} to {monthly['date'].max()}")
print(f"\nSample (first 12 rows = one farm, all months):")
one_farm = monthly[monthly['farm_id'] == monthly['farm_id'].iloc[0]].sort_values('date')
print(one_farm.to_string(index=False))
print(f"\nMonthly MAI stats:")
for d in sorted(monthly['date'].unique()):
    subset = monthly[monthly['date'] == d]
    m = subset['mai']
    print(f"  {str(d)[:7]}: mean={m.mean():.4f}  median={m.median():.4f}  NaN={m.isna().sum():,}/{len(m):,}")
