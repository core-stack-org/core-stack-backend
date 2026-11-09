import requests
import pandas as pd
import os
from pathlib import Path
from bhuvan_nrega_lat_lon import get_districts

def merge_csvs_by_district(state_name: str, state_code: str) -> None:
    """
    Merge CSV files by district name based on a common column.
    
    Args:
        state_name (str): Name of the state
        state_code (str): State code for fetching district data
    """
    try:
        # Get districts data
        districts_data = get_districts(state_code)
        if not districts_data:
            raise ValueError(f"No districts found for state code: {state_code}")
        
        districts = [district['district_name'] for district in districts_data]
        
        # Define paths using Path for cross-platform compatibility
        nrega_bhuvan_path = Path('C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\nrega_data_files\\csv\\assets') / state_name.upper()
        state_path = Path('C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\' + state_name.upper())
        
        # Columns to remove from the final output
        columns_to_remove = {
            'collection_sno', 'Category', 'Sub-Category',
            'Work Type Cleaned', 'serial_no', 'accuracy'
        }
        
        # Get all CSV files in the bhuvan directory
        csv_files = [f for f in nrega_bhuvan_path.glob('*.csv') if not f.name.endswith('_blank_data.csv')]
        
        for district in districts:
            if district == 'All':
                continue
            print(f"\nProcessing district: {district}")
            
            # Find relevant Bhuvan files for this district
            district_files = [f for f in csv_files if district.upper() in f.name.upper()]
            
            if not district_files:
                print(f"No Bhuvan files found for district {district}")
                # Try to process original NREGA file if no Bhuvan files exist
                nrega_file_path = state_path / f"{district.upper()}.csv"
                try:
                    nrega_df = pd.read_csv(nrega_file_path)
                    output_df = nrega_df.drop(columns=[col for col in columns_to_remove if col in nrega_df.columns])
                except FileNotFoundError:
                    print(f"Warning: No files available for {district}")
                    continue
            else:
                # Process Bhuvan files
                merged_df = None
                for file_path in district_files:
                    try:
                        current_df = pd.read_csv(file_path)
                        if merged_df is None:
                            merged_df = current_df
                        else:
                            merged_df = pd.merge(merged_df, current_df, 
                                                on='collection_sno', how='inner')
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")
                        continue
                
                if merged_df is not None:
                    # Try to merge with original NREGA file if it exists
                    nrega_file_path = state_path / f"{district.upper()}.csv"
                    try:
                        nrega_df = pd.read_csv(nrega_file_path)
                        nrega_df = nrega_df.drop(columns=[col for col in columns_to_remove 
                                                        if col in nrega_df.columns])
                        common_columns = list(set(merged_df.columns) & set(nrega_df.columns))
                        output_df = nrega_df[common_columns]
                    except FileNotFoundError:
                        # If original file not found, use Bhuvan data
                        print(f"Original NREGA file not found for {district}, using Bhuvan data only")
                        output_df = merged_df.drop(columns=[col for col in columns_to_remove 
                                                            if col in merged_df.columns])
                else:
                    print(f"Warning: Failed to process Bhuvan files for {district}")
                    continue
            
            # Save the final merged data
            output_df = output_df.drop_duplicates(subset='Work Code', keep='first')

            # Create the new folder inside the state_name folder
            new_bhuvan_folder = state_path / "new_bhuvan_files"
            new_bhuvan_folder.mkdir(parents=True, exist_ok=True)

            # Create the output path using Path object
            output_path = new_bhuvan_folder / f"{district.upper()}.csv"
            try:
                desired_column_order = [
                        'State', 'District', 'Block', 'Asset ID', 'Asset Name', 'Work Code','Work Name', 'Work Type', 'image_path1', 'image_path2', 
                        'observer_name', 'Gram_panchayat', 'creation_time', 'lat', 'lon', 'Panchayat_ID', 
                        'Panchayat', 'Estimated Cost', 'Start Location', 'End Location', 'Unskilled', 'Semi-Skilled', 'Skilled', 'Material', 'Total_Expenditure', 
                        'Unskilled_Persondays', 'Semi-skilled_Persondays', 'Total_persondays', 
                        'Unskilled_persons', 'Semi-skilled_persons', 'Total_persons', 'Work_start_date', 
                        'HyperLink', 'WorkCategory'
                    ]
                output_df = output_df[desired_column_order]
                output_df.to_csv(output_path, index=False)
            except:
                output_df = nrega_df.drop(columns=[col for col in columns_to_remove if col in nrega_df.columns])
                output_df.to_csv(output_path, index=False)


            
    except Exception as e:
        print(f"An error occurred: {str(e)}")


#merge_csvs_by_district('MAHARASHTRA', '18')
