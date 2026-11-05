# NREGA Bhuvan Data Scraping Workflow

## Data Sources

The NREGA data is collected from the following two sources:

1. **NREGA MIS:** [https://nreganarep.nic.in/netnrega/MISreport4.aspx](https://nreganarep.nic.in/netnrega/MISreport4.aspx)
2. **NREGA Bhuvan Dashboard:** [https://bhuvan-app2.nrsc.gov.in/bhugram/bhugram_dashboard](https://bhuvan-app2.nrsc.gov.in/bhugram/bhugram_dashboard)

Currently, we are using the Bhuvan one.

## Scraping Approach

- Data scraping is performed **state-wise at the district level**.
- The script processes the required state and fetches the NREGA Bhuvan data.
- The scraping date range must be configured before execution.

## Main Script

**Script:** `Nreaga_bhuvan_script/bhuvan_nrega_lat_lon.py`

This is the main script responsible for scraping NREGA Bhuvan latitude and longitude data.

### 1. Configure the Date Range

Before running the script, update the following parameters:

- **`start_date`**: The last date up to which data was previously scraped. This date should be retrieved from the existing GeoJSON.
- **`end_date`**: The latest date up to which new data needs to be scraped.

Update the `end_date` in the `base_params` dictionary:

```python
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
    "start_date": start_date,
    "end_date": "2025-01-01"
}
```

**Important:**
- Update `start_date` based on the last scraped date available in the existing GeoJSON.
- Update `end_date` to the target date for the current scraping operation.
- Ensure the date range covers only the data that needs to be fetched.

### 2. Execute the Scraping Script

Run the script state-wise using the following command:

```bash
nohup python -u bhuvan_nrega_lat_lon.py --state_name "UTTAR PRADESH" > UTTAR_PRADESH_nrega_terminal.log 2>&1 &
```

**Command parameters:**

| Parameter | Description |
|---|---|
| `nohup` | Allows the process to continue running after the terminal session is closed. |
| `python -u` | Runs Python in unbuffered mode so logs are written immediately. |
| `--state_name` | Specifies the state for which data needs to be scraped. |
| `> UTTAR_PRADESH_nrega_terminal.log` | Redirects standard output to a state-specific log file. |
| `2>&1` | Redirects error output to the same log file. |
| `&` | Runs the process in the background. |

### 3. State-wise Execution

Execute the command separately for each state by changing the `--state_name` argument and log filename.

Example:

```bash
nohup python -u bhuvan_nrega_lat_lon.py --state_name "UTTAR PRADESH" > UTTAR_PRADESH_nrega_terminal.log 2>&1 &
```

## Overall Workflow

1. Check the existing GeoJSON file to identify the last scraped date.
2. Update `start_date` and `end_date` in `bhuvan_nrega_lat_lon.py`.
3. Execute the script for the required state.
4. Monitor the execution through the generated log file.
5. Repeat the process for other states as required.

**Final Note:** Always verify the last scraped date from the existing GeoJSON before starting a new scraping operation to avoid missing data or unnecessarily scraping previously collected records.