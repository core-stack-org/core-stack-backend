"""
Agriculture Census Scraper

Scrapes tehsil-level crop data from the Agriculture Census website
(https://agcensus.da.gov.in/) and the UP Agriculture portal
(https://upag.gov.in/) to build a structured dataset of crop types
and their area coverage at the tehsil/district level.

The agcensus website uses ASP.NET WebForms with postback-based navigation.
This scraper uses Selenium to handle the dynamic dropdowns and table rendering.

Output: CSV with columns
    state, district, tehsil, crop_name, area_hectares, year, source
"""

import os
import time
import csv
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


AGCENSUS_URL = "https://agcensus.da.gov.in/DatabaseHome.aspx"
DEFAULT_TIMEOUT = 15


def _create_driver(headless=True):
    """Create a Chrome WebDriver instance."""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver


def _wait_for_dropdown_populated(driver, select_id, timeout=DEFAULT_TIMEOUT):
    """Wait until a dropdown has more than 1 option (i.e., loaded via postback)."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(Select(d.find_element(By.ID, select_id)).options) > 1
        )
        return True
    except TimeoutException:
        return False


def _safe_select_by_index(driver, select_id, index):
    """Select a dropdown option by index with retry on stale element."""
    for attempt in range(3):
        try:
            select = Select(driver.find_element(By.ID, select_id))
            if index < len(select.options):
                select.select_by_index(index)
                time.sleep(1.5)  # Allow postback to complete
                return True
        except StaleElementReferenceException:
            time.sleep(1)
    return False


def _get_dropdown_options(driver, select_id):
    """Get all option texts from a dropdown."""
    try:
        select = Select(driver.find_element(By.ID, select_id))
        return [(i, opt.text.strip()) for i, opt in enumerate(select.options)]
    except NoSuchElementException:
        return []


def _extract_table_data(driver):
    """Extract data from the results table on the page."""
    rows = []
    try:
        table = driver.find_element(By.ID, "GridView1")
        trs = table.find_elements(By.TAG_NAME, "tr")
        for tr in trs[1:]:  # Skip header row
            tds = tr.find_elements(By.TAG_NAME, "td")
            row = [td.text.strip() for td in tds]
            if row and any(cell for cell in row):
                rows.append(row)
    except NoSuchElementException:
        pass
    return rows


def scrape_agcensus(
    output_dir,
    states=None,
    max_districts=None,
    headless=True,
    progress_callback=None,
):
    """Scrape crop data from the Agriculture Census website.

    The site has cascading dropdowns: Year -> Table -> State -> District -> Tehsil
    We iterate through available options to collect tehsil-level data.

    Args:
        output_dir: Directory to write output CSV
        states: List of state names to scrape (None = all available)
        max_districts: Limit districts per state (for testing)
        headless: Run browser in headless mode
        progress_callback: Optional function(state, district, msg) for progress

    Returns:
        pd.DataFrame of scraped data
    """
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(output_dir, "agriculture_census_raw.csv")

    driver = _create_driver(headless=headless)
    all_records = []

    try:
        driver.get(AGCENSUS_URL)
        time.sleep(3)

        # Identify dropdown IDs (these may vary; common patterns below)
        # The actual IDs need to be confirmed by inspecting the live site
        dropdown_ids = {
            "year": "ddlYear",
            "table": "ddlTable",
            "state": "ddlState",
            "district": "ddlDistrict",
            "tehsil": "ddlTehsil",
        }

        # Try to detect actual dropdown IDs from page
        selects = driver.find_elements(By.TAG_NAME, "select")
        found_ids = [s.get_attribute("id") for s in selects if s.get_attribute("id")]
        print(f"Found dropdown IDs on page: {found_ids}")

        # Map detected IDs
        for fid in found_ids:
            fid_lower = fid.lower()
            if "year" in fid_lower:
                dropdown_ids["year"] = fid
            elif "table" in fid_lower:
                dropdown_ids["table"] = fid
            elif "state" in fid_lower:
                dropdown_ids["state"] = fid
            elif "district" in fid_lower:
                dropdown_ids["district"] = fid
            elif "tehsil" in fid_lower or "block" in fid_lower:
                dropdown_ids["tehsil"] = fid

        print(f"Using dropdown IDs: {json.dumps(dropdown_ids, indent=2)}")

        # Select the most recent year
        year_options = _get_dropdown_options(driver, dropdown_ids["year"])
        if year_options:
            # Pick the latest year (usually last numeric option)
            latest_idx = year_options[-1][0] if len(year_options) > 1 else 0
            _safe_select_by_index(driver, dropdown_ids["year"], latest_idx)
            selected_year = year_options[latest_idx][1] if latest_idx < len(year_options) else "unknown"
            print(f"Selected year: {selected_year}")

        # Select table (crop-area related)
        table_options = _get_dropdown_options(driver, dropdown_ids["table"])
        table_idx = 1  # Usually index 1 is the first data table
        if len(table_options) > 1:
            # Try to find a table about "crop" or "area"
            for idx, text in table_options:
                if any(kw in text.lower() for kw in ["crop", "area", "holding"]):
                    table_idx = idx
                    break
            _safe_select_by_index(driver, dropdown_ids["table"], table_idx)
            print(f"Selected table: {table_options[table_idx][1] if table_idx < len(table_options) else 'unknown'}")

        time.sleep(2)

        # Iterate states
        _wait_for_dropdown_populated(driver, dropdown_ids["state"])
        state_options = _get_dropdown_options(driver, dropdown_ids["state"])
        print(f"Found {len(state_options)} states")

        for state_idx, state_name in state_options:
            if state_idx == 0 and state_name.lower() in ["select", "--select--", ""]:
                continue
            if states and state_name.lower().strip() not in [s.lower() for s in states]:
                continue

            print(f"\nProcessing state: {state_name}")
            _safe_select_by_index(driver, dropdown_ids["state"], state_idx)
            time.sleep(2)

            # Iterate districts
            _wait_for_dropdown_populated(driver, dropdown_ids["district"])
            district_options = _get_dropdown_options(driver, dropdown_ids["district"])
            districts_processed = 0

            for dist_idx, dist_name in district_options:
                if dist_idx == 0 and dist_name.lower() in ["select", "--select--", ""]:
                    continue
                if max_districts and districts_processed >= max_districts:
                    break

                print(f"  District: {dist_name}")
                _safe_select_by_index(driver, dropdown_ids["district"], dist_idx)
                time.sleep(2)

                # Try to get tehsil-level data
                tehsil_available = _wait_for_dropdown_populated(
                    driver, dropdown_ids["tehsil"], timeout=5
                )

                if tehsil_available:
                    tehsil_options = _get_dropdown_options(driver, dropdown_ids["tehsil"])
                    for teh_idx, teh_name in tehsil_options:
                        if teh_idx == 0 and teh_name.lower() in ["select", "--select--", ""]:
                            continue

                        _safe_select_by_index(driver, dropdown_ids["tehsil"], teh_idx)
                        time.sleep(1)

                        # Click submit/show button if present
                        try:
                            submit_btn = driver.find_element(By.ID, "btnSubmit")
                            submit_btn.click()
                            time.sleep(2)
                        except NoSuchElementException:
                            pass

                        # Extract table data
                        table_data = _extract_table_data(driver)
                        for row in table_data:
                            all_records.append({
                                "state": state_name,
                                "district": dist_name,
                                "tehsil": teh_name,
                                "data": row,
                                "source": "agcensus.da.gov.in",
                            })

                        if progress_callback:
                            progress_callback(state_name, dist_name, teh_name)
                else:
                    # No tehsil dropdown, try to get district-level data
                    try:
                        submit_btn = driver.find_element(By.ID, "btnSubmit")
                        submit_btn.click()
                        time.sleep(2)
                    except NoSuchElementException:
                        pass

                    table_data = _extract_table_data(driver)
                    for row in table_data:
                        all_records.append({
                            "state": state_name,
                            "district": dist_name,
                            "tehsil": "",
                            "data": row,
                            "source": "agcensus.da.gov.in",
                        })

                districts_processed += 1

    except Exception as e:
        print(f"Error during scraping: {e}")
        raise
    finally:
        driver.quit()

    # Save raw records
    if all_records:
        df = pd.DataFrame(all_records)
        df.to_csv(output_csv, index=False)
        print(f"\nSaved {len(all_records)} records to {output_csv}")
    else:
        df = pd.DataFrame()
        print("\nNo records scraped")

    return df
