import logging
import requests
import json

def call_api(state, district, block):
    url = "http://127.0.0.1:8000/api/v1/download_excel_layer"
    params = {
        'state': state,
        'district': district,
        'block': block
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"API Response for {state}, {district}, {block}: Status Code {response.status_code}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {state}, {district}, {block}: {e}")

def read_locations_from_json(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

def main():
    json_file = "utilities/excel_update_locations.json"
    locations = read_locations_from_json(json_file)
    
    logging.info(f"Calling API for {len(locations)} locations...")
    for location in locations:
        call_api(location['state'], location['district'], location['block'])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
