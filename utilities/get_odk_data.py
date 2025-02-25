from utilities.constants import ODK_URL_settlement
import requests
import json
from nrm_app.settings import ODK_PASSWORD, ODK_USERNAME


def fetch_odk_data_sync(ODK_URL):
    """Fetch ODK data from the given ODK URL."""
    try:
        response = requests.get(ODK_URL, auth=(ODK_USERNAME, ODK_PASSWORD))
        response.raise_for_status()
        response_dict = json.loads(response.content)
        response_list = response_dict["value"]
        print("response list: ", response_list[0:3])

        return response_list
        # return response.json()  # Returns the ODK data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch ODK data from the given URL: {e}")
        return None


print("Waterbody Form")
odk_resp_list = fetch_odk_data_sync(ODK_URL_settlement)
