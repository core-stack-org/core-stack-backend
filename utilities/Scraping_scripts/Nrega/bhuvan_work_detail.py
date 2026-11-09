import os
import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Setup logging to capture errors or important logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

csv_path = 'C:\\Users\\LENOVO\\OneDrive\\Documents\\Nreaga_bhuvan_script\\nrega_data_files\\csv\\assets\\'


def fetch_work_using_nreaga_mis(state_code, state_name, financial_year):
    base_url = "https://mnregaweb4.nic.in/netnrega/assets_report.aspx"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract values for __VIEWSTATE and __VIEWSTATEGENERATOR
    viewstate_element = soup.find("input", {"id": "__VIEWSTATE"})
    if viewstate_element:
        viewstate = viewstate_element.get("value")
    else:
        viewstate = ""

    viewstate_generator_element = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    if viewstate_generator_element:
        viewstate_generator = viewstate_generator_element.get("value")
    else:
        viewstate_generator = ""

    # Generate Digest parameter
    digest_pattern = r"Digest=(\w+)"
    digest_match = re.search(digest_pattern, response.url)
    if digest_match:
        digest = digest_match.group(1)
    else:
        digest = "KRAjOaAIQjyPESs7kYMXcg"  # Use a default value

    # Construct the final URL
    final_url = f"{base_url}?page=S&lflag=eng&state_name={state_name}&state_code={state_code}&fin_year={financial_year}&source=national&Digest={digest}"

    # Make the final request with all required parameters
    payload = {
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator
    }
    response = requests.post(final_url, data=payload)

    # Parse the response and extract the data
    soup = BeautifulSoup(response.content, "html.parser")
    data_table = soup.find("table", {"class": "fontstyle"})

    if data_table:
        for row in data_table.find_all("tr")[1:]:
            cells = row.find_all("td")
            asset_id = cells[1].text.strip()
            asset_name = cells[2].text.strip()
            shared_with_nreg = cells[3].text.strip()
            work_code = cells[5].text.strip()
            work_name = cells[6].text.strip()
            work_type = cells[7].text.strip()
            shared_with_nrsc = cells[8].text.strip()
            print(f"Asset ID: {asset_id}")
            print(f"Asset Name: {asset_name}")
            print(f"Shared with NREG Soft: {shared_with_nreg}")
            print(f"Work Code: {work_code}")
            print(f"Work Name: {work_name}")
            print(f"Work Type: {work_type}")
            print(f"Shared with NRSC: {shared_with_nrsc}")
            print("---")
    else:
        print("No data table found on the page.")



    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    import time

    # Set up Chrome options for Selenium
    driver = webdriver.Chrome()

    # Example: Navigating to a page and extracting a Digest value
    driver.get("https://mnregaweb4.nic.in/netnrega/assets_report.aspx?page=S&lflag=eng&state_name={encoded_state}&state_code={state_code}&fin_year={financial_year}&source=national")

    # Allow the page to load
    time.sleep(5)

    # Find the Digest value in the page (example: assuming it’s in an input field with the name 'Digest')
    try:
        # Wait for up to 10 seconds for the Digest input field to become available
        digest_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "Digest"))
        )

        # Get the value of the Digest input field
        digest_value = digest_element.get_attribute("value")
        print(f"Digest Value: {digest_value}")

    except Exception as e:
        print(f"Error extracting Digest: {e}")

    finally:
        # Close the driver after scraping
        driver.quit()


def download_html_for_collection_sno(collection_sno, district_name, html_save_path, session):
    """
    This function downloads the HTML content for the given collection_sno and saves it to the specified directory.
    """
    url = "https://bhuvan-app2.nrsc.gov.in/mgnrega/usrtasks/nrega_phase2/get/get_details.php"
    params = {"sno": collection_sno}
    
    max_retries = 5
    import time
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                os.makedirs(html_save_path, exist_ok=True)
                
                file_name = f'{collection_sno}_work_data.html'
                file_path = os.path.join(html_save_path, file_name)
                
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(response.text)
                
                logging.info(f"Saved HTML for collection_sno {collection_sno} in {file_path}")
                return
            else:
                sleep_time = attempt + 1
                logging.warning(f"Attempt {attempt + 1} failed for collection_sno {collection_sno}. Status Code: {response.status_code}. Retrying in {sleep_time}s.")
                time.sleep(sleep_time)
        except Exception as e:
            sleep_time = attempt + 1
            logging.error(f"Attempt {attempt + 1} error while downloading HTML for collection_sno {collection_sno}: {e}. Retrying in {sleep_time}s.")
            time.sleep(sleep_time)
    
    logging.error(f"Completely failed to download HTML for collection_sno {collection_sno} after {max_retries} attempts.")

def extract_data_from_html(html_file):
    """
    This function extracts the relevant data from the saved HTML file.
    """
    try:
        with open(html_file, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')
            data = {
            "collection_sno": os.path.basename(html_file).split('_')[0],
            "Category": None,
            "Sub-Category": None,
            "Asset Name": None,
            "Asset location": None,
            "Work Name": None,
            "Work Type": None,
            "Estimated Cost": 0,
            "Start Location": -1,
            "End Location": -1,
            "Unskilled": 0,
            "Semi-Skilled": 0,
            "Skilled": 0,
            "Material": 0,
            "Total_Expenditure": 0,
            "Unskilled_Persondays": -1,
            "Semi-skilled_Persondays": -1,
            "Total_persondays": -1,
            "Unskilled_persons": -1,
            "Semi-skilled_persons": -1,
            "Total_persons": -1,
            "Work_start_date": -1,
            "HyperLink": -1,
        }

        td_elements = soup.find_all('td')
        for i in range(len(td_elements) - 1):
            text = td_elements[i].get_text(strip=True)
            if text == "Category":
                data["Category"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Sub-Category":
                data["Sub-Category"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Asset Name":
                data["Asset Name"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Asset Location":
                data["Asset location"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Work Name":
                data["Work Name"] = td_elements[i + 1].get_text(strip=True)
            elif text == "Work Type":
                data["Work Type"] = data["Sub-Category"]
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
        logging.error(f"Error while extracting data from {html_file}: {e}")
        return None

def download_html_for_all_districts(state_name):
    """
    This function downloads HTML files for all districts in the given state.
    """
    state_folder = os.path.join(csv_path, state_name)
    
    if not os.path.exists(state_folder):
        logging.error(f"State folder not found: {state_folder}")
        return
    
    # Loop through all CSV files in the state folder
    for district_file in os.listdir(state_folder):
        if district_file.endswith('_bhuvan_lat_lon.csv'):
            district_name = district_file.replace('_bhuvan_lat_lon.csv', '')
            csv_file_path = os.path.join(state_folder, district_file)
            logging.info(f"Downloading HTML for district: {district_name}")
            
            try:
                df = pd.read_csv(csv_file_path) 
                if 'collection_sno' in df.columns:
                    session = requests.Session()
                    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
                    session.mount('https://', adapter)
                    session.mount('http://', adapter)
                    district_html_folder = os.path.join(state_folder, district_name, "html_files")
                    os.makedirs(district_html_folder, exist_ok=True)
                    
                    with ThreadPoolExecutor(max_workers=100) as executor:
                        futures = {executor.submit(download_html_for_collection_sno, collection_sno, district_name, district_html_folder, session): collection_sno for collection_sno in df['collection_sno']}   
                        for future in futures:
                            future.result()
                    
                    logging.info(f"Completed HTML download for district {district_name}.")
                else:
                    logging.warning(f"Column 'collection_sno' not found in {district_file}. Skipping file.")
            except Exception as e:
                logging.error(f"Error while processing {district_file}: {e}")

def process_html_files_and_extract_data(state_name):
    """
    This function processes the saved HTML files and extracts the relevant data from each file.
    """
    state_folder = os.path.join(csv_path, state_name)
    
    if not os.path.exists(state_folder):
        logging.error(f"State folder not found: {state_folder}")
        return
    
    # Loop through all CSV files in the state folder
    for district_file in os.listdir(state_folder):
        if district_file.endswith('_bhuvan_lat_lon.csv'):
            district_name = district_file.replace('_bhuvan_lat_lon.csv', '')
            csv_file_path = os.path.join(state_folder, district_file)
            logging.info(f"Processing HTML files for district: {district_name}")
            
            try:
                df = pd.read_csv(csv_file_path)
                if 'collection_sno' in df.columns:
                    all_data = []
                    
                    for collection_sno in df['collection_sno']:
                        html_file_path = os.path.join(state_folder, district_name, "html_files", f"{collection_sno}_work_data.html")
                        
                        if os.path.exists(html_file_path):
                            data = extract_data_from_html(html_file_path)
                            if data:
                                all_data.append(data)
                    
                    if all_data:
                        new_df = pd.DataFrame(all_data)
                        
                        output_csv_file = os.path.join(state_folder, district_name + "_processed.csv")
                        new_df.to_csv(output_csv_file, index=False)
                        logging.info(f"Saved processed data to {output_csv_file}")
                    else:
                        logging.warning(f"No data extracted for district {district_name}.")
                else:
                    logging.warning(f"Column 'collection_sno' not found in {district_file}. Skipping file.")
            except Exception as e:
                logging.error(f"Error while processing {district_file}: {e}")

if __name__ == "__main__":
    import sys
    
    state_name = ''
    if len(sys.argv) > 1:
        state_name = sys.argv[1].upper()

    try:
        logging.info(f"Starting work detail extraction for state: {state_name}")
        download_html_for_all_districts(state_name)
        process_html_files_and_extract_data(state_name)
    except Exception as e:
        logging.error(f"Error while extracting data, falling back to nrega mis: {e}")
        
        # Determine state_code dynamically by importing STATE_DICT locally to avoid circular imports
        try:
            from bhuvan_nrega_lat_lon import STATE_DICT
            state_code = next((code for code, name in STATE_DICT.items() if name == state_name), None)
            financial_year = "All"
            
            if state_code:
                fetch_work_using_nreaga_mis(state_code, state_name, financial_year)
            else:
                logging.error(f"Could not find state code for {state_name}")
        except Exception as fallback_e:
            logging.error(f"Fallback fetch_work_using_nreaga_mis failed: {fallback_e}")
