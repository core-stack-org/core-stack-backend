import pandas as pd
import os
from datetime import datetime

# List of possible date formats
date_formats = [
    "%d-%b-%y %H:%M:%S.%f",
    "%d-%b-%y %H:%M:%S",
    "%d-%m-%y %H:%M:%S.%f",
    "%d-%m-%y %H:%M:%S",
    "%d-%b-%Y %H:%M:%S.%f",
    "%d-%b-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S.%f",
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
]

# Function to parse dates with multiple formats
def parse_date(date_str):
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue  # If format doesn't match, try the next one
    return pd.NaT  # If none of the formats work, return NaT (Not a Time)

import sys

def run_checkpointing(state_name):
    folder_path = os.path.join('C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\', state_name.upper())
    creation_folder = os.path.join('C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\nrega_data_files\\csv\\Creation_assets\\', state_name.upper())

    # If the user hasn't created the folder or there's no data, we gracefully skip
    # so that the main script fetches ALL data automatically.
    if not os.path.exists(folder_path):
        print(f"No existing data folder '{folder_path}' found. Pipeline will fetch ALL data from the server.")
        return

    # Ensure the output directory exists
    os.makedirs(creation_folder, exist_ok=True)

    # Loop through each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv") and not filename.endswith("_processed.csv") and not filename.endswith("_blank_data.csv"):
            file_path = os.path.join(folder_path, filename)
            district_name = filename.replace('_bhuvan_lat_lon', '').split('.')[0]
            
            try:
                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path)
                
                if 'creation_time' not in df.columns:
                    print(f"Skipping {filename} because it has no 'creation_time' column.")
                    continue
                
                # Apply the parse_date function to the 'creation_time' column
                df['creation_time'] = df['creation_time'].apply(parse_date)
                
                # Check for any errors in conversion (NaT means not a time, or failed parsing)
                if df['creation_time'].isna().sum() > 0:
                    print(f"Some entries could not be parsed in {filename}.")
                
                # Group by 'Panchayat_ID', 'Panchayat', and 'block', then get the latest 'creation_time'
                latest_creation_times = df.groupby(['Panchayat_ID', 'Panchayat', 'Block'], as_index=False, dropna=False)['creation_time'].max()
                
                # Convert 'creation_time' to only the date (YYYY-MM-DD)
                latest_creation_times['creation_time'] = latest_creation_times['creation_time'].dt.date
                
                # Add a new column 'district_name' (based on the file name)
                latest_creation_times['district_name'] = district_name
                
                # Save the result to an Excel file with district name (filename)
                output_file = os.path.join(creation_folder, f"{district_name.capitalize()}_latest_creation_times.xlsx")
                latest_creation_times.to_excel(output_file, index=False)  # Save to Excel
                
                print(f"Saved checkpoint: {output_file}")
            except Exception as e:
                print(f"Failed to create checkpoint for {filename}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        state = sys.argv[1]
    else:
        state = 'GUJARAT'  # Default fallback
    run_checkpointing(state)
