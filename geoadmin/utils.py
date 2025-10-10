import re
from typing import Optional

from .models import Block, District, State


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


def activated_entities():
    """Returns all the activated Blocks with block id, block name

    Returns:
        List: A list of JSON data
    """
    active_states = State.objects.filter(active_status=True).order_by("state_name")
    response_data = []
    for state in active_states:
        active_districts = District.objects.filter(
            state=state, active_status=True
        ).order_by("district_name")
        districts_data = []
        for district in active_districts:
            active_blocks = Block.objects.filter(
                district=district, active_status=True
            ).order_by("block_name")
            blocks_data = [
                {"block_name": block.block_name, "block_id": block.id}
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
                "state_id": state.state_census_code,
                "districts": districts_data,
            }
        )
    return response_data


def transform_data(data):
    sorted_data = sorted(data, key=lambda x: x["state_name"])

    transformed_data = []
    state_value = 1234

    for state in sorted_data:
        sorted_districts = sorted(state["districts"], key=lambda x: x["district_name"])

        state_data = {
            "label": state["state_name"],
            "value": str(state_value),
            "state_id": state["state_id"],
            "district": [],
        }
        state_value += 1

        district_value = 1
        for district in sorted_districts:
            sorted_blocks = sorted(district["blocks"], key=lambda x: x["block_name"])

            district_data = {
                "label": district["district_name"],
                "value": str(district_value),
                "district_id": str(district["district_id"]),
                "blocks": [],
            }
            district_value += 1
            block_value = 1
            for block in sorted_blocks:
                block_data = {
                    "label": block["block_name"],
                    "value": str(block_value),
                    "block_id": str(block["block_id"]),
                }
                block_value += 1
                district_data["blocks"].append(block_data)

            state_data["district"].append(district_data)

        transformed_data.append(state_data)

    return transformed_data
