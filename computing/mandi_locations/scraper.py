"""
Agmarknet Mandi Data Scraper

This module scrapes mandi (market) information from the Agmarknet website,
extracting details like mandi name, state, district, and commodities traded.
"""

import time
import json
import csv
from datetime import datetime
from typing import List, Dict, Optional
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MandiScraper:
    """
    Scrapes mandi data from Agmarknet website
    """

    BASE_URL = "https://agmarknet.gov.in"
    MANDI_URL = f"{BASE_URL}/SearchMarket.aspx"
    COMMODITY_URL = f"{BASE_URL}/SearchCommodity.aspx"

    def __init__(self, output_dir: str = "data/raw", headless: bool = True):
        """
        Initialize the scraper

        Args:
            output_dir: Directory to save scraped data
            headless: Run browser in headless mode
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        self.driver = None
        self.session = requests.Session()

    def _setup_driver(self):
        """Setup Selenium WebDriver with Chrome"""
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        try:
            self.driver = webdriver.Chrome(options=options)
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise

    def _close_driver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def scrape_states(self) -> List[Dict[str, str]]:
        """
        Scrape list of states from Agmarknet

        Returns:
            List of state dictionaries with state_code and state_name
        """
        states = []

        try:
            response = self.session.get(self.MANDI_URL)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find state dropdown
            state_select = soup.find('select', {'id': 'ddlState'})
            if state_select:
                for option in state_select.find_all('option')[1:]:  # Skip first empty option
                    state_code = option.get('value', '').strip()
                    state_name = option.text.strip()
                    if state_code and state_name:
                        states.append({
                            'state_code': state_code,
                            'state_name': state_name
                        })
                        logger.info(f"Found state: {state_name}")

        except Exception as e:
            logger.error(f"Error scraping states: {e}")

        return states

    def scrape_districts(self, state_code: str) -> List[Dict[str, str]]:
        """
        Scrape districts for a given state

        Args:
            state_code: State code from Agmarknet

        Returns:
            List of district dictionaries
        """
        districts = []

        if not self.driver:
            self._setup_driver()

        try:
            self.driver.get(self.MANDI_URL)
            wait = WebDriverWait(self.driver, 10)

            # Select state
            state_dropdown = wait.until(
                EC.presence_of_element_located((By.ID, "ddlState"))
            )
            Select(state_dropdown).select_by_value(state_code)

            # Wait for districts to load
            time.sleep(2)

            # Get districts
            district_dropdown = self.driver.find_element(By.ID, "ddlDistrict")
            district_select = Select(district_dropdown)

            for option in district_select.options[1:]:  # Skip first empty option
                district_code = option.get_attribute('value')
                district_name = option.text.strip()
                if district_code and district_name:
                    districts.append({
                        'district_code': district_code,
                        'district_name': district_name
                    })

        except Exception as e:
            logger.error(f"Error scraping districts for state {state_code}: {e}")

        return districts

    def scrape_mandis(self, state_code: str, district_code: str) -> List[Dict[str, any]]:
        """
        Scrape mandis for a given state and district

        Args:
            state_code: State code
            district_code: District code

        Returns:
            List of mandi dictionaries
        """
        mandis = []

        if not self.driver:
            self._setup_driver()

        try:
            self.driver.get(self.MANDI_URL)
            wait = WebDriverWait(self.driver, 10)

            # Select state
            state_dropdown = wait.until(
                EC.presence_of_element_located((By.ID, "ddlState"))
            )
            Select(state_dropdown).select_by_value(state_code)

            time.sleep(2)

            # Select district
            district_dropdown = self.driver.find_element(By.ID, "ddlDistrict")
            Select(district_dropdown).select_by_value(district_code)

            time.sleep(2)

            # Get mandis
            mandi_dropdown = self.driver.find_element(By.ID, "ddlMarket")
            mandi_select = Select(mandi_dropdown)

            for option in mandi_select.options[1:]:  # Skip first empty option
                mandi_code = option.get_attribute('value')
                mandi_name = option.text.strip()
                if mandi_code and mandi_name:
                    mandis.append({
                        'mandi_code': mandi_code,
                        'mandi_name': mandi_name,
                        'state_code': state_code,
                        'district_code': district_code
                    })

        except Exception as e:
            logger.error(f"Error scraping mandis for district {district_code}: {e}")

        return mandis

    def scrape_commodities(self, mandi_code: str) -> List[str]:
        """
        Scrape commodities available in a specific mandi

        Args:
            mandi_code: Mandi code

        Returns:
            List of commodity names
        """
        commodities = []

        try:
            # Use API endpoint if available, otherwise scrape
            response = self.session.get(
                f"{self.BASE_URL}/SearchCmdtyPrice.aspx?MarketID={mandi_code}"
            )
            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for commodity table or list
            commodity_table = soup.find('table', {'id': 'gvCommodity'})
            if commodity_table:
                for row in commodity_table.find_all('tr')[1:]:  # Skip header
                    cells = row.find_all('td')
                    if cells and len(cells) > 0:
                        commodity_name = cells[0].text.strip()
                        if commodity_name:
                            commodities.append(commodity_name)

        except Exception as e:
            logger.warning(f"Could not fetch commodities for mandi {mandi_code}: {e}")

        return commodities

    def scrape_all_mandis(self) -> List[Dict]:
        """
        Scrape all mandis across India

        Returns:
            List of all mandi dictionaries with complete information
        """
        all_mandis = []

        try:
            # Get all states
            states = self.scrape_states()
            logger.info(f"Found {len(states)} states")

            for state in states:
                state_code = state['state_code']
                state_name = state['state_name']

                logger.info(f"Processing state: {state_name}")

                # Get districts for state
                districts = self.scrape_districts(state_code)
                logger.info(f"Found {len(districts)} districts in {state_name}")

                for district in districts:
                    district_code = district['district_code']
                    district_name = district['district_name']

                    logger.info(f"Processing district: {district_name}")

                    # Get mandis for district
                    mandis = self.scrape_mandis(state_code, district_code)

                    for mandi in mandis:
                        # Add state and district names
                        mandi['state_name'] = state_name
                        mandi['district_name'] = district_name

                        # Get commodities for mandi
                        commodities = self.scrape_commodities(mandi['mandi_code'])
                        mandi['commodities'] = ','.join(commodities) if commodities else ''

                        all_mandis.append(mandi)
                        logger.info(f"Added mandi: {mandi['mandi_name']}")

                    # Add delay to avoid overwhelming the server
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Error in scraping all mandis: {e}")
        finally:
            self._close_driver()

        logger.info(f"Total mandis scraped: {len(all_mandis)}")
        return all_mandis

    def save_to_csv(self, mandis: List[Dict], filename: str = None):
        """
        Save mandi data to CSV file

        Args:
            mandis: List of mandi dictionaries
            filename: Output filename (optional)
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mandi_data_{timestamp}.csv"

        filepath = self.output_dir / filename

        if mandis:
            keys = mandis[0].keys()

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=keys)
                writer.writeheader()
                writer.writerows(mandis)

            logger.info(f"Data saved to {filepath}")
        else:
            logger.warning("No data to save")

    def save_to_json(self, mandis: List[Dict], filename: str = None):
        """
        Save mandi data to JSON file

        Args:
            mandis: List of mandi dictionaries
            filename: Output filename (optional)
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mandi_data_{timestamp}.json"

        filepath = self.output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(mandis, jsonfile, indent=2, ensure_ascii=False)

        logger.info(f"Data saved to {filepath}")

    def run(self, save_format: str = 'both'):
        """
        Run the complete scraping process

        Args:
            save_format: Format to save data ('csv', 'json', or 'both')

        Returns:
            List of scraped mandi data
        """
        logger.info("Starting Agmarknet mandi scraping...")

        mandis = self.scrape_all_mandis()

        if mandis:
            if save_format in ['csv', 'both']:
                self.save_to_csv(mandis)
            if save_format in ['json', 'both']:
                self.save_to_json(mandis)

        return mandis


if __name__ == "__main__":
    # Example usage
    scraper = MandiScraper(output_dir="data/raw", headless=True)
    mandi_data = scraper.run(save_format='both')
    print(f"Scraped {len(mandi_data)} mandis")