import requests
import pandas as pd
from os import path
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import PoolManager
import urllib3
from bs4 import BeautifulSoup
import os
from requests.packages.urllib3.util.retry import Retry
from .nrega_asset_categ import *
import argparse
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Constants
BASE_URL = "https://bhuvan-app2.nrsc.gov.in/mgnrega/nrega_dashboard_phase2/php/"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}

#state_dict = {
    # "01":"ANDAMAN AND NICOBAR",
    # "02":"ANDHRA PRADESH",
#     "03":"ARUNACHAL PRADESH",
#     "04":"ASSAM",
    # "05":"BIHAR",
#     "33":"CHHATTISGARH",
#     "07":"DN HAVELI AND DD",
#     "10":"GOA",
#     "11":"GUJARAT",
#     "12":"HARYANA",
#     "13":"HIMACHAL PRADESH",
#     "14":"JAMMU AND KASHMIR",
#     "34":"JHARKHAND",
    # "15":"KARNATAKA",
    # "16":"KERALA",
#     "37":"LADAKH",
#     "19":"LAKSHADWEEP",
    # "17":"MADHYA PRADESH",
    # "18":"MAHARASHTRA",
    # "20":"MANIPUR",
    # "21":"MEGHALAYA",
    # "22":"MIZORAM",
    # "23":"NAGALAND",
    # "24":"ODISHA",
    # "25":"PUDUCHERRY",
    # "26":"PUNJAB",
    # "27":"RAJASTHAN",
    # "28":"SIKKIM",
    # "29":"TAMIL NADU",
    # "36":"TELANGANA",
#     "30":"TRIPURA",
#     "35":"UTTARAKHAND",
    # "31":"UTTAR PRADESH",
    # "32":"WEST BENGAL"
 #   }




class CustomPoolManager(PoolManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pool_timeout = 60  # Increased timeout

class CustomAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = CustomPoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            timeout=60.0
        )

def check_file_exists(filename):
    return path.exists(filename)

def fetch_with_session(url, data):
    response = session.post(url, data=data, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def create_session():
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,  # Maximum number of retries
        backoff_factor=2,  # Exponential backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"]
    )
    
    # Create custom adapter with increased pool size and timeouts
    adapter = CustomAdapter(
        max_retries=retry_strategy,
        pool_connections=100,  # Increased from default
        pool_maxsize=100,     # Increased from default
        pool_block=True      # Will block when pool is full instead of dropping
    )
    
    # Mount the custom adapter
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    
    return session

session = requests.Session()

def check_file_exists(filename):
    return path.exists(filename)

def fetch_with_session(url, data):
    response = session.post(url, data=data, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def get_districts(session, state_code):
    url = BASE_URL + "location/getDistricts.php"
    return fetch_with_session(url, {"username": "unauthourized", "state_code": state_code, "financial_year": "All"})

def get_blocks(session, district_code):
    url = BASE_URL + "location/getBlocks.php"
    return fetch_with_session(url, {"username": "unauthourized", "district_code": district_code, "financial_year": "All"})

def get_panchayats(session, block_code):
    url = BASE_URL + "location/getPanchayats.php"
    return fetch_with_session(url, {"username": "unauthourized", "block_code": block_code, "financial_year": "All"})

def get_accepted_geotags(session, params):
    url = BASE_URL + "reports/accepted_geotags.php"
    return fetch_with_session(url, params)

def fetch_with_retry(session, url, data, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = session.post(url, data=data, timeout=60)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.ConnectionError, urllib3.exceptions.MaxRetryError) as e:
            if "Connection pool is full" in str(e):
                sleep_time = (attempt + 1) * 10  # Longer sleep for connection pool issues
                logging.warning(f"Connection pool full, sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
                # Create a new session if the pool is exhausted
                if attempt == max_retries - 1:
                    logging.info("Creating new session due to pool exhaustion")
                    session = create_session()
            else:
                sleep_time = (attempt + 1) * 5
                logging.warning(f"Connection error, sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed after {max_retries} attempts: {str(e)}")
                raise
            sleep_time = (attempt + 1) * 5
            time.sleep(sleep_time)
            
    return None

def process_panchayat_with_retry(args, max_retries=5):
    session, panchayat, params, block_name, district_name, state_name = args
    
    if panchayat['panchayat_name'] == "All":
        return []

    for attempt in range(max_retries):
        try:
            params.update({"panchayat_code": panchayat['panchayat_code']})
            accepted_geotags = get_accepted_geotags(session, params)
            
            if accepted_geotags:
                return [{
                    'State': state_name,
                    'District': district_name,
                    'Block': block_name,
                    'collection_sno': entry['collection_sno'],
                    'Asset ID': entry['assetid'],
                    'Work Code': entry['workcode'],
                    'serial_no': entry['serial_no'],
                    'image_path1': entry['path1'],
                    'image_path2': entry['path2'],
                    'accuracy': entry['accuracy'],
                    'observer_name': entry['observername'],
                    'Gram_panchayat': entry['gpname'],
                    'creation_time': entry['creationtime'],
                    'lat': entry['lat'],
                    'lon': entry['lon'],
                    'Panchayat_ID': panchayat['panchayat_code'],
                    'Panchayat': panchayat['panchayat_name']
                } for entry in accepted_geotags]
        except Exception as e:
            if "Connection pool is full" in str(e):
                sleep_time = (attempt + 1) * 10
                logging.warning(f"Connection pool full while processing panchayat {panchayat['panchayat_name']}, "
                              f"sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
                # Create new session on last attempt
                if attempt == max_retries - 1:
                    session = create_session()
            else:
                sleep_time = (attempt + 1) * 5
                logging.error(f"Error processing panchayat {panchayat['panchayat_name']}: {str(e)}")
                time.sleep(sleep_time)
            
            if attempt == max_retries - 1:
                logging.error(f"Failed to process panchayat {panchayat['panchayat_name']} after {max_retries} attempts")
                return []
            
    return []

def process_block(args):
    session, block_info, district_name, state_name, state_code, financial_year = args
    
    block_name = block_info['block_name']
    block_code = block_info['block_code']
    
    if block_name == "All" or district_name == "All":
        return []

    logging.info(f"Working on Block: {block_name}")
    
    failed_panchayats = []  # Track failed panchayats for retry
    
    try:
        panchayats = get_panchayats(session, block_code)
        base_params = {
            "username": "unauthourized",
            "stage": 0,
            "state_code": state_code,
            "district_code": block_code[:4],
            "block_code": block_code,
            "financial_year": financial_year,
            "accuracy": 0,
            "category_id": "All",
            "sub_category_id": "All",
            "start_date": "2011-07-01",
            "end_date": "2024-06-30"
        }

        valid_panchayats = [p for p in panchayats if p['panchayat_name'] != "All"]
        results = []
        
        # Process panchayats with smaller thread pool and retry mechanism
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_panchayat = {
                executor.submit(
                    process_panchayat_with_retry,
                    (session, panchayat, base_params.copy(), block_name, district_name, state_name)
                ): panchayat
                for panchayat in valid_panchayats
            }
            
            for future in future_to_panchayat:
                panchayat = future_to_panchayat[future]
                try:
                    result = future.result()
                    if result:
                        results.extend(result)
                    else:
                        failed_panchayats.append(panchayat)
                except Exception as e:
                    failed_panchayats.append(panchayat)
                    logging.error(f"Error processing panchayat {panchayat['panchayat_name']}: {str(e)}")

        # Retry failed panchayats with new session
        if failed_panchayats:
            logging.info(f"Retrying {len(failed_panchayats)} failed panchayats for block {block_name}")
            new_session = create_session()
            for panchayat in failed_panchayats:
                try:
                    retry_result = process_panchayat_with_retry(
                        (new_session, panchayat, base_params.copy(), block_name, district_name, state_name),
                        max_retries=3
                    )
                    if retry_result:
                        results.extend(retry_result)
                except Exception as e:
                    logging.error(f"Final retry failed for panchayat {panchayat['panchayat_name']}: {str(e)}")

        return results

    except Exception as e:
        logging.error(f"Error processing block {block_name}: {str(e)}")
        return []


def process_state(state_code, state_name):
    session = create_session()

    logging.info(f"Working on => {state_name}")

    try:
        districts = get_districts(session, state_code)
        for district_info in districts:
            all_data = []
            district_name = district_info['district_name']
            district_code = district_info['district_code']

            # Skip if not BARABANKI
            # if district_name != TARGET_DISTRICT:
            #     logging.info(f"Skipping district {district_name} - not {TARGET_DISTRICT}")
            #     continue

            logging.info(f"Processing District: {district_name}")

            blocks = get_blocks(session, district_code)
            
            # Process blocks for all years
            for year in range(2011, 2024):
                financial_year = f"{year}-{year+1}"
                logging.info(f"Processing Financial Year: {financial_year} for {district_name}")

                # Prepare arguments for block processing
                block_args = [
                    (session, block_info, district_name, state_name, state_code, financial_year)
                    for block_info in blocks
                    if block_info['block_name'] != "All"
                ]

                # Process blocks in parallel
                with ThreadPoolExecutor(max_workers=5) as executor:
                    results = list(executor.map(process_block, block_args))

                # Aggregate results for all years
                for block_data in results:
                    if block_data:  # Only extend if we have data
                        all_data.extend(block_data)
                
            if all_data:
                df = pd.DataFrame(all_data)
                process_dataframe(df, district_name)
                # file_name = f'{district_name}_{state_name}_collection.csv'
                # file_path = f'/home/amitesh/Documents/Project/nrega_asset_without_mis/data/ANDAMAN_AND_NICOBAR/{file_name}'
                # df.to_csv(file_path, index=False)
                logging.info(f"Saved all years data for {district_name}: {len(df)} records")

                
    except Exception as e:
        logging.error(f"Error processing state {state_name}: {str(e)}")
    
    # Save all data into a single file





def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("https://", adapter)
    return session

# Function to fetch work details with session reuse
def get_work_details(args):
    collection_no, session = args
    url = "https://bhuvan-app2.nrsc.gov.in/mgnrega/usrtasks/nrega_phase2/get/get_details.php"
    params = {"sno": collection_no}
    
    try:
        response = session.get(url, params=params, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        data = {
            "collection_sno": collection_no,
            "Category": None,
            "Sub-Category": None,
            "Asset Name": None,
            "Work Name": None,
            "Work Type": None,
            "Estimated Cost": None,
            "Start Location": "-1",
            "End Location": "-1",
            "Unskilled": None,
            "Semi-Skilled": "0",
            "Skilled": "0",
            "Material": None,
            "Contingency": "0",
            "Total_Expenditure": 0,
            "Unskilled_Persondays": "0",
            "Semi-skilled_Persondays": "0",
            "Total_persondays": "0",
            "Unskilled_persons": "0",
            "Semi-skilled_persons": "0",
            "Total_persons": "0",
            "Work_start_date": None,
            "HyperLink": "0",
        }

        # Batch find all td elements
        td_elements = soup.find_all('td')
        for i in range(len(td_elements) - 1):
            text = td_elements[i].get_text(strip=True)
            if text == "Category":
                data["Category"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Sub-Category":
                data["Sub-Category"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Asset Name":
                data["Asset Name"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Work Name":
                data["Work Name"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Work Type":
                data["Work Type"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Cumulative Cost of Asset":
                data["Estimated Cost"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Expenditure Unskilled":
                data["Unskilled"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Expenditure Material/Skilled":
                data["Material"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Work Start Date":
                data["Work_start_date"] = td_elements[i + 1].get_text(strip=True)

        # Calculate Total_Expenditure
        unskilled = float(data["Unskilled"] or 0)
        material = float(data["Material"] or 0)
        data["Total_Expenditure"] = unskilled + material

        return data
    except Exception as e:
        logging.error(f"Error processing collection {collection_no}: {str(e)}")
        return None

def process_dataframe(df, district_name):
    logging.info(f"Processing DataFrame with {len(df)} records")
    session = create_session()
    all_data = []

    chunk_size = 1000

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start+chunk_size]
        
        if 'collection_sno' in chunk.columns:
            unique_values = chunk['collection_sno'].unique()
            
            # Process chunk with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=20) as executor:
                future_to_collection = {
                    executor.submit(get_work_details, (collect, session)): collect 
                    for collect in unique_values
                }
                
                for future in as_completed(future_to_collection):
                    collection = future_to_collection[future]
                    try:
                        data = future.result()
                        if data:
                            all_data.append(data)
                            logging.info(f"Collection {collection} processed successfully")
                    except Exception as e:
                        logging.error(f"Collection {collection} failed: {str(e)}")

    if all_data:
        result_df = pd.DataFrame(all_data)
        merged_df = pd.merge(df, result_df, on='collection_sno', how='left')
        script(merged_df, district_name)
    else:
        logging.warning(f"No data processed")

def main(state_dict):
    start_time = time.time()
    with Pool(processes=4) as pool:
        pool.starmap(process_state, state_dict.items())
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Process a state dictionary.")
    parser.add_argument('--state_dict', type=str, required=True, help="JSON string of the state dictionary")
    
    # Parse arguments
    args = parser.parse_args()
    state_dict = json.loads(args.state_dict)

    main(state_dict)