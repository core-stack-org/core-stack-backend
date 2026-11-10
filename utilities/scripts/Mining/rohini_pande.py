import pandas as pd
import os

# Path to the dataset
data_path = 'full_application_envi_outcomes.dta'

# Read the Stata file
print("Reading Stata file...")
df = pd.read_stata(data_path)

print(f"Dataset shape: {df.shape}")
print(f"\nColumn names:\n{df.columns.tolist()}")

# Look for latitude/longitude columns
lat_cols = [col for col in df.columns if 'lat' in col.lower()]
lon_cols = [col for col in df.columns if 'lon' in col.lower() or 'lng' in col.lower()]

print(f"\nPotential latitude columns: {lat_cols}")
print(f"Potential longitude columns: {lon_cols}")

# If lat/lon columns are found, extract them
if lat_cols and lon_cols:
    # Use the first matching columns (adjust if needed)
    lat_col = lat_cols[0]
    lon_col = lon_cols[0]
    
    # Define columns to extract (removed tehsil_moefcc)
    columns_to_extract = [
        'state_cl', 'district_cl', 'tehsil_cl', 'village_1cl',
        'state_moefcc', 'district_moefcc',
        lat_col, lon_col,
        'sector_moefcc', 'proposal_name', 'company_type', 'company'
    ]
    
    # Check which columns exist
    available_cols = [col for col in columns_to_extract if col in df.columns]
    missing_cols = [col for col in columns_to_extract if col not in df.columns]
    
    if missing_cols:
        print(f"\nWarning: These columns not found: {missing_cols}")
    
    # Extract selected columns
    coords_df = df[available_cols].copy()
    
    # Remove rows with missing coordinates
    coords_df = coords_df.dropna(subset=[lat_col, lon_col])
    
    # Rename columns for clarity (removed tehsil_moefcc mapping)
    coords_df = coords_df.rename(columns={
        lat_col: 'latitude',
        lon_col: 'longitude',
        'proposal_name': 'proposal',
        'company': 'company_name',
        'state_cl': 'state',
        'district_cl': 'district',
        'tehsil_cl': 'tehsil',
        'village_1cl': 'village',
        'state_moefcc': 'state_official',
        'district_moefcc': 'district_official',
    })
    
    print(f"\nExtracted {len(coords_df)} mine locations with valid coordinates")
    print(f"\nColumns included: {list(coords_df.columns)}")
    print(f"\nFirst 10 rows:")
    print(coords_df.head(10))
    
    # Save selected columns to CSV
    output_file = 'mine_selected_data.csv'
    coords_df.to_csv(output_file, index=False)
    print(f"\nSelected data with {coords_df.shape[1]} columns saved to: {output_file}")
    
    # Basic statistics
    print(f"\nCoordinate ranges:")
    print(f"Latitude: {coords_df['latitude'].min():.6f} to {coords_df['latitude'].max():.6f}")
    print(f"Longitude: {coords_df['longitude'].min():.6f} to {coords_df['longitude'].max():.6f}")
    
else:
    print("\nNo obvious lat/lon columns found. Displaying first few rows:")
    print(df.head())
    print("\nPlease check the column names and update the script accordingly.")