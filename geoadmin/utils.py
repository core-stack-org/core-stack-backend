from pathlib import Path
from django.conf import settings
import json
import re
from operator import itemgetter
from typing import Optional

from .models import DistrictSOI, StateSOI, TehsilSOI


def normalize_name(name: Optional[str]) -> str:
    """
    Normalize names by removing special characters and extra whitespaces

    Examples:
        "Andaman & Nicobar" --> "Andaman Nicobar"
        "Andaman (Nicobar)" --> "Andaman Nicobar"

    Args:
        name (str): The name to be normalized

    Returns:
        str: Normalized name <state, district, block/tehsil>
    """
    if not name:
        return ""

    normalized = re.sub(r"[&\-()]", " ", name)
    normalized = re.sub(r"\s+", " ", normalized)

    return normalized.strip()


def activated_tehsils():
    """Returns all the activated Tehsils with tehsil id, tehsil name

    Returns:
        List: A list of JSON data
    """
    active_states = StateSOI.objects.filter(active_status=True).order_by("state_name")
    response_data = []
    for state in active_states:
        active_districts = DistrictSOI.objects.filter(
            state=state, active_status=True
        ).order_by("district_name")
        districts_data = []
        for district in active_districts:
            active_blocks = TehsilSOI.objects.filter(
                district=district, active_status=True
            ).order_by("tehsil_name")
            blocks_data = [
                {
                    "block_name": block.tehsil_name,
                    "block_id": block.id,
                }  # tehsil_name is block_name
                for block in active_blocks
            ]
            districts_data.append(
                {
                    "district_name": district.district_name,
                    "district_id": district.id,
                    "blocks": blocks_data,
                }
            )
        response_data.append(
            {
                "state_name": state.state_name,
                "state_id": state.id,
                "districts": districts_data,
            }
        )
    return response_data


def transform_data(data):
    return [
        {
            "label": state["state_name"],
            "state_id": str(state["state_id"]),
            "district": [
                {
                    "label": district["district_name"],
                    "district_id": str(district["district_id"]),
                    "blocks": [
                        {
                            "label": block["block_name"],
                            "block_id": str(block["block_id"]),
                            "tehsil_id": str(block["block_id"]),
                        }
                        for block in sorted(
                            district["blocks"], key=itemgetter("block_name")
                        )
                    ],
                }
                for district in sorted(
                    state["districts"], key=itemgetter("district_name")
                )
            ],
        }
        for state in sorted(data, key=itemgetter("state_name"))
    ]


def get_activated_location_json():
    """Read proposed blocks data from JSON file"""
    activate_locations_file_path = (
        Path(settings.BASE_DIR) / "data/activated_locations/active_locations.json"
    )
    try:
        if activate_locations_file_path.exists():
            with open(activate_locations_file_path, "r") as f:
                return json.load(f)
        return None
    except Exception as e:
        print(f"Error reading proposed blocks cache: {e}")
        return None
