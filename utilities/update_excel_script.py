import json
from stats_generator.utils import get_vector_layer_geoserver

def read_locations_from_json(json_file):
    with open(json_file, 'r') as file:
        return json.load(file)

json_file = "utilities/excel_update_locations.json"
locations = read_locations_from_json(json_file)

for location in locations:
    print("Updating data for :", location)
    if location['update_required']==1:
        get_vector_layer_geoserver(location['state'].lower(), location['district'].lower(), location['block'].lower())

print("Excel Updated for all location")