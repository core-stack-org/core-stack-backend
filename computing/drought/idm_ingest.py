"""
India Drought Monitor (IDM) Alert Ingestion.

Fetches drought alert data from https://indiadroughtmonitor.in
and stores it in the DroughtAlert model.

IDM uses the following classification:
    D0 - Abnormally Dry
    D1 - Moderate Drought
    D2 - Severe Drought
    D3 - Extreme Drought
    D4 - Exceptional Drought
"""

import logging
import requests
from datetime import date, datetime

logger = logging.getLogger(__name__)

# IDM data endpoint (based on observed API patterns)
IDM_BASE_URL = "https://indiadroughtmonitor.in"
IDM_API_ENDPOINTS = {
    "current": f"{IDM_BASE_URL}/api/drought-current",
    "forecast": f"{IDM_BASE_URL}/api/drought-forecast",
    "historical": f"{IDM_BASE_URL}/api/drought-historical",
}

# Mapping IDM severity codes to our model
IDM_SEVERITY_MAP = {
    0: "D0",
    1: "D1",
    2: "D2",
    3: "D3",
    4: "D4",
    "D0": "D0",
    "D1": "D1",
    "D2": "D2",
    "D3": "D3",
    "D4": "D4",
}


def fetch_idm_data(endpoint="current", params=None):
    """
    Fetch drought data from India Drought Monitor.

    Args:
        endpoint: One of 'current', 'forecast', 'historical'
        params: Additional query parameters (e.g., date, state)

    Returns:
        dict with fetched data, or None on failure
    """
    url = IDM_API_ENDPOINTS.get(endpoint, IDM_API_ENDPOINTS["current"])

    try:
        headers = {
            "User-Agent": "CoreStack-DroughtMonitor/1.0",
            "Accept": "application/json",
        }
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                logger.warning("IDM response is not valid JSON")
                return None
        else:
            logger.warning(f"IDM API returned status {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch IDM data: {e}")
        return None


def parse_idm_alerts(idm_data, state=None, district=None, block=None):
    """
    Parse IDM response data into alert dicts compatible with DroughtAlert model.

    Args:
        idm_data: Raw response from IDM API
        state: Filter by state name
        district: Filter by district name
        block: Filter by block name

    Returns:
        list of alert dicts
    """
    alerts = []

    if not idm_data:
        return alerts

    # Handle different possible IDM response formats
    features = []
    if isinstance(idm_data, dict):
        features = idm_data.get("features", idm_data.get("data", []))
    elif isinstance(idm_data, list):
        features = idm_data

    for feature in features:
        try:
            # Extract properties
            if isinstance(feature, dict) and "properties" in feature:
                props = feature["properties"]
                geom = feature.get("geometry")
            elif isinstance(feature, dict):
                props = feature
                geom = None
            else:
                continue

            # Map severity
            raw_severity = props.get("severity", props.get("drought_class", None))
            severity = IDM_SEVERITY_MAP.get(raw_severity)
            if severity is None:
                continue

            # Extract location
            feat_state = props.get("state", props.get("STATE", ""))
            feat_district = props.get("district", props.get("DISTRICT", ""))
            feat_block = props.get("block", props.get("BLOCK", ""))

            # Apply location filters
            if state and feat_state and state.lower() != feat_state.lower():
                continue
            if district and feat_district and district.lower() != feat_district.lower():
                continue
            if block and feat_block and block.lower() != feat_block.lower():
                continue

            # Build AoI name
            aoi_parts = [p for p in [feat_state, feat_district, feat_block] if p]
            aoi_name = " / ".join(aoi_parts) if aoi_parts else "India"

            # Extract date
            alert_date_str = props.get("date", props.get("week_end", None))
            if alert_date_str:
                try:
                    alert_date = datetime.strptime(
                        alert_date_str[:10], "%Y-%m-%d"
                    ).date()
                except (ValueError, TypeError):
                    alert_date = date.today()
            else:
                alert_date = date.today()

            # Extract area
            area_sq_km = props.get("area_sq_km", props.get("area", None))
            if area_sq_km:
                area_sq_km = float(area_sq_km)

            alert = {
                "severity": severity,
                "alert_type": "idm",
                "spei_value": None,
                "aoi_name": aoi_name,
                "state": feat_state,
                "district": feat_district,
                "block": feat_block,
                "area_sq_km": area_sq_km,
                "geometry": geom,
                "alert_date": alert_date,
                "is_active": True,
                "metadata": {
                    "source": "India Drought Monitor",
                    "source_url": IDM_BASE_URL,
                    "raw_severity": str(raw_severity),
                    "ingested_at": datetime.now().isoformat(),
                },
            }
            alerts.append(alert)

        except Exception as e:
            logger.error(f"Error parsing IDM feature: {e}")
            continue

    return alerts


def ingest_idm_alerts(state=None, district=None, block=None):
    """
    Main entry point: fetch IDM data and return parsed alerts.

    Args:
        state: Optional state filter
        district: Optional district filter
        block: Optional block filter

    Returns:
        list of alert dicts ready to be saved as DroughtAlert objects
    """
    logger.info(
        f"Ingesting IDM alerts for state={state}, district={district}, block={block}"
    )

    # Try the current drought endpoint
    params = {}
    if state:
        params["state"] = state

    idm_data = fetch_idm_data("current", params)

    if idm_data is None:
        # Fallback: try the forecast endpoint
        logger.info("Current endpoint failed, trying forecast endpoint")
        idm_data = fetch_idm_data("forecast", params)

    alerts = parse_idm_alerts(idm_data, state, district, block)
    logger.info(f"Parsed {len(alerts)} IDM alerts")

    return alerts
