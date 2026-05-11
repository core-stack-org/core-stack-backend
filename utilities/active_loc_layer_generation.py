import requests
import os
import json

# Load the locations data from the JSON file
with open("/home/cfpt-jedi/developer/repos/core-stack-backend/utilities/active_location_for_layer_gen.json", "r") as file:
    locations = json.load(file)

# Base URL for API request


# Function to make the GET request and save the file
def download_excel(state, district, block):
    # Constructing the URL with query parameters
    base_url = "http://127.0.0.1:8000/api/v1/add_new_layer_data_to_excel/"
    params = {
        "state": state,
        "district": district,
        "block": block,
        "workspace": "nrega_assets",
    }

    print(f"Generation Excel for {state} {district}  {block}")

    try:
        # Make GET request to the API
        response = requests.get(base_url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Ensure directory exists
            # os.makedirs(os.path.dirname(save_path), exist_ok=True)

            # Saving the file (Excel file)
            # with open(save_path, 'wb') as file:
            #   file.write(response.content)
            print("File saved to:")
        else:
            print(
                f"Failed to fetch data for {state} - {district} - {block}. Status code: {response.status_code}"
            )
    except Exception as e:
        print(f"An error occurred: {e}")


def restoration_vector(state, district, block):
    # Updated base URL
    base_url = "http://localhost:8000/api/v1/generate_fabdem_raster/"

    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg0NTQwNTQ2LCJpYXQiOjE3NzY3NjQ1NDYsImp0aSI6IjIxYmVmZTMwYWE2MzQ4ZDM4YmE5M2EyNGQ3MjdhYzgyIiwidXNlcl9pZCI6NH0.FHqq8dRaI39L5zIDKotjYIlwnnc8JWyi9B1vDLWBhZQ",
        "Content-Type": "application/json",
    }

    # base_url = "https://geoserver.core-stack.org/api/v1/change_detection/"
    # base_url = "https://geoserver.core-stack.org/api/v1/generate_distance_nearest_DL/"
    # base_url = "https://geoserver.core-stack.org/api/v1/generate_ci_layer/"
    # base_url = "https://geoserver.core-stack.org/api/v1/generate_facilities_proximity/"

    # headers = {
    #     "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzc5NzE0OTU5LCJpYXQiOjE3NzE5Mzg5NTksImp0aSI6IjA5OGU0MzRjYWE0ZTQ2ZDVhYmE3MTc1YjljMDAwODQ3IiwidXNlcl9pZCI6NH0.BjE41D07gARbRXCdKOcVxJjpsVMZDxVYg7oaN4ABxCg",
    #     "Content-Type": "application/json",
    # }

    # Request bodyS
    body = {
        "state": state,
        "district": district,
        "block": block,
        "start_year": 2017,
        "end_year": 2024,
        "compute": "local"
    }

    print(f"Generating Layer for {state}, {district}, {block}")

    try:
        # Make POST request to the API with headers and JSON body
        response = requests.post(base_url, headers=headers, json=body)

        # Check if the request was successful
        if response.status_code == 200:
            print(f"Response: {response.text}")
            return True
        else:
            print(
                f"Failed to fetch data for {state} - {district} - {block}. Status code: {response.status_code}"
            )
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


# Loop through the JSON data to fetch state, district, and block and download files
for entry in locations:
    # state = entry["state"]
    # district = entry["district"]
    # block = entry["block"]
    state = entry["state"]
    district = entry["district"]
    block = entry["block"]

    # Define the save path (e.g., saving as a .xlsx file in a folder based on district)
    # save_path = os.path.join("downloaded_files", state, district, f"{block}.xlsx")

    # Download and save the Excel file
    #download_excel(state, district, block)
    restoration_vector(state, district, block)
