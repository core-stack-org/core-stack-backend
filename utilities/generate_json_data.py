import pandas as pd
import json
import os


def gen_json_data(state_csv_path, output_json_path=None):
    """
    Converts the state csv's to JSON data in lists of dict

    Args:
        state_csv_path (str): The path to the state's csv path
        output_json_path (str)
    Returns:
        str: The generated JSON string
    """

    state_data = pd.read_csv(state_csv_path)
    name_ext = os.path.basename(state_csv_path)
    state_name, ext = os.path.splitext(name_ext)
    state_name = state_name.capitalize()

    districts_data = []
    unique_districts = state_data["District name"].str.capitalize().unique()

    for district in unique_districts:
        district_df = state_data[
            state_data["District name"].str.capitalize() == district
        ]
        district_census_code = str(district_df["District census code"].iloc[0])
        unique_blocks = district_df["Subdistrict name"].unique()

        blocks = []
        for block in unique_blocks:
            block_df = district_df[district_df["Subdistrict name"] == block]
            block_census_code = str(block_df["Subdistrict census code"].iloc[0])
            blocks.append({"name": block, "block_census_code": block_census_code})

        districts_data.append(
            {
                "name": district,
                "district_census_code": district_census_code,
                "blocks": blocks,
            }
        )

    json_data = {"states": [{"name": state_name, "districts": districts_data}]}

    if output_json_path:
        if os.path.exists(output_json_path):
            with open(output_json_path, "r") as f:
                existing_data = json.load(f)
                existing_data["states"].append(json_data["states"][0])
            with open(output_json_path, "w") as f:
                json.dump(existing_data, f, indent=4)
        else:
            with open(output_json_path, "w") as f:
                json.dump(json_data, f, indent=4)

    return json_data


json_string = gen_json_data(
    "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/admin-boundaries/odisha.csv",
    "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/output/all_states.json",
)
print(json_string)
