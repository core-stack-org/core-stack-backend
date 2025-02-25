from .models import Block, District, State


def activated_entities():
    """Returns all the activated Blocks with block id, block name

    Returns:
        List: A list of JSON data
    """
    active_states = State.objects.filter(active_status=True)
    response_data = []
    for state in active_states:
        active_districts = District.objects.filter(state=state, active_status=True)
        districts_data = []
        for district in active_districts:
            active_blocks = Block.objects.filter(district=district, active_status=True)
            print("active_blocks :: ", active_blocks)
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
    transformed_data = []
    state_value = 1234

    for state in data:
        state_data = {
            "label": state["state_name"],
            "value": str(state_value),
            "state_id": state["state_id"],
            "district": [],
        }
        state_value += 1

        district_value = 1
        for district in state["districts"]:
            district_data = {
                "label": district["district_name"],
                "value": str(district_value),
                "district_id": str(district["district_id"]),
                "blocks": [],
            }
            district_value += 1
            block_value = 1
            for block in district["blocks"]:
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
