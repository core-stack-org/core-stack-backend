import requests
import pandas as pd
import os
import time
import json
import glob
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock
import logging
import sys
from bhuvan_work_detail import *
from nrega_asset_categ import file_loop
from get_latest_creation_time import run_checkpointing
from csv_to_geojson import csv_to_geojson

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    filename='nrega_scraper.log'
)

STATE_DICT = {
    "01":"ANDAMAN AND NICOBAR",
    "02":"ANDHRA PRADESH",
    "03":"ARUNACHAL PRADESH",
    "04":"ASSAM",
    "05":"BIHAR",
    "33":"CHHATTISGARH",
    "07":"DN HAVELI AND DD",
    "10":"GOA",
    "11":"GUJARAT",
    "12":"HARYANA",
    "13":"HIMACHAL PRADESH",
    "14":"JAMMU AND KASHMIR",
    "34":"JHARKHAND",
    "15":"KARNATAKA",
    "16":"KERALA",
    "37":"LADAKH",
    "19":"LAKSHADWEEP",
    "17":"MADHYA PRADESH",
    "18":"MAHARASHTRA",
    "20":"MANIPUR",
    "21":"MEGHALAYA",
    "22":"MIZORAM",
    "23":"NAGALAND",
    "24":"ODISHA",
    "25":"PUDUCHERRY",
    "26":"PUNJAB",
    "27":"RAJASTHAN",
    "28":"SIKKIM",
    "29":"TAMIL NADU",
    "36":"TELANGANA",
    "30":"TRIPURA",
    "35":"UTTARAKHAND",
    "31":"UTTAR PRADESH",
    "32":"WEST BENGAL"
}



csv_path = 'nrega_data_files/csv/assets/'
BASE_URL = "https://bhuvan-app2.nrsc.gov.in/mgnrega/nrega_dashboard_phase2/php/"
# BASE_URL = "https://bhuvan-app2.nrsc.gov.in/bhugram/bhugram_dashboard/php/"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
MAX_WORKERS = 100
csv_write_lock = Lock()

def fetch_data(url, data, max_retries=50):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=data, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed after {max_retries} attempts: {str(e)}")
                raise
            
            logging.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
            time.sleep(2)  # Fixed 2-second delay between retries
    return None

def get_districts(state_code):
    url = BASE_URL + "location/getDistricts.php"
    return fetch_data(url, {"username": "unauthourized", "state_code": state_code, "financial_year": "All"})

def get_blocks(district_code):
    url = BASE_URL + "location/getBlocks.php"
    return fetch_data(url, {"username": "unauthourized", "district_code": district_code, "financial_year": "All"})

def get_panchayats(block_code):
    url = BASE_URL + "location/getPanchayats.php"
    return fetch_data(url, {"username": "unauthourized", "block_code": block_code, "financial_year": "All"})

def get_accepted_geotags(params):
    url = BASE_URL + "reports/accepted_geotags.php"
    return fetch_data(url, params)

def get_start_date_from_csv(state_name, district_name, block_name, panchayat_name):
    try:
        csv_file_path = f"nrega_data_files/csv/Creation_assets/{state_name.upper()}/{district_name.capitalize()}_latest_creation_times.xlsx"
        df = pd.read_excel(csv_file_path, engine='openpyxl')
        row = df[(df['Panchayat'] == panchayat_name) & (df['Block'] == block_name)]
        return row['creation_time'].iloc[0] if not row.empty else "2005-07-01"
    except Exception as e:
        logging.warning(f"Error getting start date: {e}")
        return "2005-01-01"

def process_panchayat_with_retry(args, max_retries=50):
    panchayat, params, block_name, district_name, state_name = args
   
    print("for block", block_name)
    for attempt in range(max_retries):
        try:
            accepted_geotags = get_accepted_geotags(params)
            
            if accepted_geotags is None:
                logging.warning(f"No response data for panchayat {panchayat['panchayat_name']} on attempt {attempt + 1}")
                continue
                
            if not accepted_geotags:
                logging.info(f"Empty data for panchayat {panchayat['panchayat_name']}")
                return pd.DataFrame()
            
            if not isinstance(accepted_geotags, list):
                logging.warning(f"Invalid response format for panchayat {panchayat['panchayat_name']}")
                continue
                
            # Add the extra information to each entry before converting to DataFrame
            for entry in accepted_geotags:
                entry['State'] = state_name
                entry['District'] = district_name
                entry['Block'] = block_name
                entry['Panchayat'] = panchayat['panchayat_name']

            # Convert to DataFrame
            df = pd.DataFrame(accepted_geotags)
            return df
        
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for panchayat {panchayat['panchayat_name']}: {str(e)}")
            time.sleep(2)  # Short fixed delay
    
    logging.error(f"All attempts failed for panchayat {panchayat['panchayat_name']}")
    return pd.DataFrame()


def save_district_data1(state_name, district_name, data):
    # Define the column renaming and rearranging logic
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
    if not data.empty:
        with csv_write_lock:
            try:
                data.rename(columns=column_mapping, inplace=True)
                data = data[desired_columns]

                # Define file path
                state_path = os.path.join(csv_path, state_name)
                os.makedirs(state_path, exist_ok=True)
                file_name = f'{district_name}_bhuvan_lat_lon.csv'
                file_path = os.path.join(state_path, file_name)
                
                # Check if file exists and append if it does
                if os.path.exists(file_path):
                    existing_data = pd.read_csv(file_path)
                    combined_data = pd.concat([existing_data, data], ignore_index=True)
                    # Remove duplicates based on all columns
                    combined_data = combined_data.drop_duplicates()
                    combined_data.to_csv(file_path, index=False)
                    logging.info(f"Updated data for {district_name}: total {len(combined_data)} records")
                else:
                    data.to_csv(file_path, index=False)
                    logging.info(f"Saved new data for {district_name}: {len(data)} records")
            except Exception as e:
                logging.error(f"Error saving data for {district_name}: {str(e)}")



def save_district_data(state_name, district_name, data):
    if not data.empty:
        with csv_write_lock:
            try:
                state_path = os.path.join(csv_path, state_name)
                os.makedirs(state_path, exist_ok=True)
                file_name = f'{district_name}_bhuvan_lat_lon.csv'
                file_path = os.path.join(state_path, file_name)
                
                # Check if file exists and append if it does
                if os.path.exists(file_path):
                    existing_data = pd.read_csv(file_path)
                    combined_data = pd.concat([existing_data, data], ignore_index=True)
                    # Remove duplicates based on all columns
                    combined_data = combined_data.drop_duplicates()
                    combined_data.to_csv(file_path, index=False)
                    logging.info(f"Updated data for {district_name}: total {len(combined_data)} records")
                else:
                    data.to_csv(file_path, index=False)
                    logging.info(f"Saved new data for {district_name}: {len(data)} records")
            except Exception as e:
                logging.error(f"Error saving data for {district_name}: {str(e)}")

def process_blocks_for_district(district_info, state_name, state_code):
    district_name = district_info['district_name']
    district_code = district_info['district_code']
    all_district_data = pd.DataFrame()

    try:
        logging.info(f"Processing District: {district_name}")
        blocks = get_blocks(district_code)
        financial_year = "All"

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            panchayat_futures = []

            for block_info in blocks:
                if block_info['block_name'] == "All":
                    continue

                block_name = block_info['block_name']
                block_code = block_info['block_code']

                try:
                    time.sleep(1)  # Short delay between blocks
                    panchayats = get_panchayats(block_code)
                    
                    for panchayat in panchayats:
                        if panchayat['panchayat_code'] == 'All':
                            continue

                        # start_date = get_start_date_from_csv(state_name, district_name, block_name, panchayat['panchayat_name'])
                        base_params = {
                            "username": "unauthourized",
                            "stage": 0,
                            "state_code": state_code,
                            "district_code": block_code[:4],
                            "block_code": block_code,
                            "panchayat_code": panchayat['panchayat_code'],
                            "financial_year": financial_year,
                            "accuracy": 0,
                            "category_id": "All",
                            "sub_category_id": "All",
                            "start_date": "2016-02-03",
                            "end_date": "2022-01-05"
                        }

                        future = executor.submit(
                            process_panchayat_with_retry,
                            (panchayat, base_params, block_name, district_name, state_name)
                        )
                        panchayat_futures.append(future)

                except Exception as e:
                    logging.error(f"Error processing block {block_name}: {str(e)}")
                    continue

            # Process results as they complete
            for completed in as_completed(panchayat_futures):
                try:
                    result_df = completed.result()
                    if not result_df.empty:
                        # Save data in smaller chunks
                        save_district_data(state_name, district_name, result_df)
                except Exception as e:
                    logging.error(f"Error processing panchayat result: {str(e)}")

    except Exception as e:
        logging.error(f"Error processing district {district_name}: {str(e)}")

def process_state(state_code, state_name):
    logging.info(f"Working on => {state_name}")
    try:
        districts = get_districts(state_code)
        
        # Process districts sequentially to reduce server load
        for district_info in districts:
            if district_info['district_name'] == 'All':
                continue
            print("----------------------------------", district_info)
            try:
                process_blocks_for_district(district_info, state_name, state_code)
                time.sleep(1)  # Short delay between districts
            except Exception as e:
                logging.error(f"Error processing district: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Exception in process_state: {str(e)}")


def merge_csvs_by_district(state_name: str, state_code: str) -> None:
    """
    Merge CSV files by district name based on a common column.
    
    Args:
        state_name (str): Name of the state
        state_code (str): State code for fetching district data
    """
    # try:
        # Get districts data
    districts_data = get_districts(state_code)
    if not districts_data:
        raise ValueError(f"No districts found for state code: {state_code}")
    
    districts = [district['district_name'] for district in districts_data]
    
    # Define paths using Path for cross-platform compatibility
    nrega_bhuvan_path = Path('nrega_data_files/csv/assets') / state_name.upper()
    state_path = Path(state_name.upper())
    
    # Columns to remove from the final output
    columns_to_remove = {
        'collection_sno', 'Category', 'Sub-Category',
        'Work Type Cleaned', 'serial_no', 'accuracy'
    }
    
    # Get all CSV files in the bhuvan directory
    csv_files = [f for f in nrega_bhuvan_path.glob('*.csv') if not f.name.endswith('_blank_data.csv') and not f.name.endswith('_processed.csv')]
    
    for district in districts:
        if district == 'All':
        #if 'FEROZEPUR' not in district:
            continue
        print(f"\nProcessing district: {district}")
        #district = 'FEROZEPUR'
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
            lat_lon_file = [f for f in district_files if 'bhuvan_lat_lon' in f.name]
            work_file = [f for f in district_files if 'work_data' in f.name]
            
            if lat_lon_file:
                try:
                    merged_df = pd.read_csv(lat_lon_file[0])
                    if work_file:
                        work_df = pd.read_csv(work_file[0])
                        # Use LEFT JOIN to ensure lat/lon data isn't lost if work_data is missing
                        # print("Lat Lon File:", lat_lon_file)
                        # print("Work File:", work_file)
                        # print("work df columns:",work_df.columns.tolist())
                        merged_df = pd.merge(merged_df, work_df, on='collection_sno', how='left')
                        # print("after merge:- ",merged_df.columns.tolist())
                except Exception as e:
                    print(f"Error processing Bhuvan files for {district}: {str(e)}")
            
            if merged_df is not None:
                # Try to merge with original NREGA file if it exists
                nrega_file_path = state_path / f"{district.upper()}.csv"
                # print("-----------------------", nrega_file_path)
                
                try:
                    nrega_df = pd.read_csv(nrega_file_path)
                    nrega_df = nrega_df.drop(columns=[col for col in columns_to_remove 
                                                    if col in nrega_df.columns])
                    merged_df_clean = merged_df.drop(columns=[col for col in columns_to_remove 
                                                    if col in merged_df.columns])
                    output_df = pd.concat([nrega_df, merged_df_clean], ignore_index=True)
                except FileNotFoundError:
                    # If original file not found, use Bhuvan data
                    print(f"Original NREGA file not found for {district}, using Bhuvan data only")
                    output_df = merged_df.drop(columns=[col for col in columns_to_remove 
                                                        if col in merged_df.columns])
            else:
                print(f"Warning: Failed to process Bhuvan files for {district}")
                continue
        
        # Save the final merged data
        print("-----------------------", nrega_file_path)
        
        # Drop rows where 'Asset ID' is missing or blank
        if 'Asset ID' in output_df.columns:
            output_df = output_df.dropna(subset=['Asset ID'])
            output_df = output_df[output_df['Asset ID'].astype(str).str.strip() != '']
            
        output_df = output_df.drop_duplicates(keep='first')

        # Create the new folder inside the state_name folder
        new_bhuvan_folder = state_path / "new_bhuvan_files"
        new_bhuvan_folder.mkdir(parents=True, exist_ok=True)

        # Create the output path using Path object
        output_path = new_bhuvan_folder / f"{district.upper()}.csv"
        try:
            desired_column_order = [
                    'State', 'District', 'Block', 'Asset ID', 'Asset Name', 'Asset location', 'Work Code','Work Name', 'Work Type', 'image_path1', 'image_path2', 
                    'observer_name', 'Gram_panchayat', 'creation_time', 'lat', 'lon', 'Panchayat_ID', 
                    'Panchayat', 'Estimated Cost', 'Start Location', 'End Location', 'Unskilled', 'Semi-Skilled', 'Skilled', 'Material', 'Total_Expenditure', 
                    'Unskilled_Persondays', 'Semi-skilled_Persondays', 'Total_persondays', 
                    'Unskilled_persons', 'Semi-skilled_persons', 'Total_persons', 'Work_start_date', 
                    'HyperLink', 'WorkCategory'
                ]
            # print(output_df.columns.tolist())
            output_df = output_df[desired_column_order]
            output_df.to_csv(output_path, index=False)
            csv_to_geojson(str(output_path), lat_col='lat', lon_col='lon')
        except:
            output_df = nrega_df.drop(columns=[col for col in columns_to_remove if col in nrega_df.columns])
            output_df.to_csv(output_path, index=False)
            csv_to_geojson(str(output_path), lat_col='lat', lon_col='lon')


            
    # except Exception as e:
    #     print(f"An error occurred: {str(e)}")



def process_state_csv(state_name: str) -> None:
    """
    Process CSV files for the given state, renaming and rearranging columns, and saving the modified files.

    Args:
    - state_name (str): The name of the state whose CSV files need to be processed.
    """
    # Path to the folder where the state-specific CSV files are stored
    folder_path = f"C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\nrega_data_files\\csv\\assets\\{state_name.upper()}"
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
        'lat': 'lon',
        'lon': 'lat',
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


def main(state_dict):
    start_time = time.time()
    logging.info(f"Start time: {start_time}")

    # Process states sequentially
    for state_code, state_name in state_dict.items():
        try:
            print(f"---------------Generating Checkpoints for {state_name}----------------")
            run_checkpointing(state_name)
            process_state(state_code, state_name)
        except Exception as e:
            logging.error(f"Error processing state {state_name}: {str(e)}")

    end_time = time.time()
    for state_code, state_name in state_dict.items():
        print("---------------download_html_for_all_districts for----------------", state_name)
        download_html_for_all_districts(state_name)
        print("---------------------process_html_files_and_extract_data for ------------", state_name)
        process_html_files_and_extract_data(state_name)
        print("--------------work data categorization----------------")
        file_loop(state_name)
        print("------------Merging task is started ---------", state_name)
        process_state_csv(state_name)
        merge_csvs_by_district(state_name, str(state_code))


    logging.info(f"End time: {end_time}")
    logging.info(f"Total execution time: {end_time - start_time} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the NREGA Bhuvan scraper pipeline.")
    parser.add_argument('--state_name', type=str, required=True, help="Name of the state to process (e.g., 'GOA' or 'MAHARASHTRA')")
    
    args = parser.parse_args()
    state_name_input = args.state_name.upper()
    
    # Find state code
    matched_code = None
    for code, name in STATE_DICT.items():
        if name == state_name_input:
            matched_code = code
            break
            
    if not matched_code:
        print(f"Error: State '{state_name_input}' not found in STATE_DICT.")
        sys.exit(1)
        
    main({matched_code: state_name_input})
