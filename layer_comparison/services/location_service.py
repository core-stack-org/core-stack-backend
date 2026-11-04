import json
import logging
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from geoadmin.models import (
    StateSOI,
    DistrictSOI,
    TehsilSOI,
    State_Disritct_Block_Properties,
)

logger = logging.getLogger(__name__)


def _geojson_urls(value):
    if isinstance(value, dict):
        for nested_value in value.values():
            yield from _geojson_urls(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            yield from _geojson_urls(nested_value)
    elif isinstance(value, str) and value.startswith(("https://", "http://")):
        yield value


def _fetch_geojson(url):
    parsed_url = urlsplit(url)
    if (
        parsed_url.scheme != "https"
        or parsed_url.hostname != "geoserver.core-stack.org"
        or parsed_url.port != 8443
    ):
        return None

    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            data = json.loads(response.read())
    except (OSError, URLError, TimeoutError, json.JSONDecodeError):
        logger.exception("Could not retrieve block boundary from GeoServer")
        return None

    if isinstance(data, dict) and data.get("type") in {"Feature", "FeatureCollection"}:
        return data
    return None


def get_states():
    print("getting states")
    states = StateSOI.objects.all()
    return list(states.order_by("state_name").values("id", "state_name"))


def get_districts(state_id):
    return list(
        DistrictSOI.objects.filter(state_id=state_id)
        .order_by("district_name")
        .values("id", "district_name")
    )


def get_blocks(district_id):
    # Layer.block is a FK to TehsilSOI. The dashboard label is "Block"
    # to match the user's workflow and existing terminology.
    return list(
        TehsilSOI.objects.filter(district_id=district_id)
        .order_by("tehsil_name")
        .values("id", "tehsil_name")
    )


def get_block_geometry(state_id, district_id, block_id):
    record = (
        State_Disritct_Block_Properties.objects.filter(
            state_id=state_id,
            district_id=district_id,
            tehsil_id=block_id,
        )
        .values_list("dashboard_geojson", flat=True)
        .first()
    )
    if isinstance(record, dict) and record.get("type") in {
        "Feature",
        "FeatureCollection",
    }:
        return record

    if not isinstance(record, (dict, list)):
        return None

    for url in _geojson_urls(record):
        geometry = _fetch_geojson(url)
        if geometry:
            return geometry
    return None
