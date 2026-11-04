import pandas as pd
import glob
import os

# Path to the folder where your CSV files are stored
folder_path = "C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\nrega_data_files\\csv\\assets\\"  # Adjust this path

# Use glob to find all files that end with _bhuvan_lat_lon.csv
csv_files = glob.glob(os.path.join(folder_path, "*_bhuvan_lat_lon.csv"))

# Define the mapping of old column names to new column names
column_mapping = {
    'collection_sno': 'collection_sno',
    'assetid': 'Asset ID',
    'workcode': 'Work Code',
    'serial_no': 'serial_no',
    'path1': 'image_path1',
    'path2': 'image_path2',
    'accuracy': 'accuracy',
    'observername': 'observer_name',
    'gpname': 'Gram_panchayat',
    'creationtime': 'creation_time',
    'lat': 'lat',
    'lon': 'lon',
    'State': 'State',
    'District': 'District',
    'Block': 'Block',
    'Panchayat': 'Panchayat'
}

# Define the desired column order
desired_columns = [
    'State', 'District', 'Block', 'collection_sno', 'Asset ID', 'Work Code', 'serial_no',
    'image_path1', 'image_path2', 'accuracy', 'observer_name', 'Gram_panchayat', 'creation_time',
    'lat', 'lon', 'Panchayat_ID', 'Panchayat'
]

for file in csv_files:
    try:
        # Read the CSV file
        df = pd.read_csv(file)

        # Rename columns based on the column mapping
        df.rename(columns=column_mapping, inplace=True)

        # Check if 'Panchayat_ID' column exists, and if not, create it (based on some logic or just leave it empty)
        if 'Panchayat_ID' not in df.columns:
            df['Panchayat_ID'] = None  # Assuming you want to leave this column empty for now
        
        # Rearrange the columns to match the desired order
        df = df[desired_columns]

        # Save the modified CSV back (overwrite the file or save as a new one)
        df.to_csv(file, index=False)
        print(f"Successfully processed {file}")

    except Exception as e:
        print(f"Failed to process {file}: {e}")

