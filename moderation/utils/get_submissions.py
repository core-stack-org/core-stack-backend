import requests
from requests.auth import HTTPBasicAuth
from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
from plans.utils import fetch_bearer_token
from .form_mapping import corestack


class get_edited_updated_all_submissions:
    def __init__(self, username, password, base_url):
        self.auth = HTTPBasicAuth(username, password)
        self.base_url = base_url

    def get_edited_updated_submissions(self, project_id, form_id, filter_query):
        url = f"{self.base_url}/v1/projects/{project_id}/forms/{form_id}.svc/Submissions?{filter_query}"
        response = requests.get(url, auth=self.auth)
        response.raise_for_status()
        return response.json().get("value", [])


# flag required edited/updated


def form_submissions_updated_url(form_id, filter_query):
    project_id = 2
    base_url = "https://odk.core-stack.org"
    url = f"{base_url}/v1/projects/{project_id}/forms/{form_id}.svc/Submissions?{filter_query}"
    return url


filter_query_updated = "$filter=__system/submissionDate ge 2025-11-28T00:00:00.000Z"
filter_query_edited = "$filter=__system/submissionDate lt 2025-11-28T00:00:00.000Z and __system/updatedAt ge 2025-11-28T00:00:00.000Z"


class odk_submissions_updated:
    def __init__(self):
        self.token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
        self.results = {}

        self.settlement_submissions()
        self.well_submissions()
        self.waterbody_submissions()
        self.groundwater_submissions()
        self.agri_submissions()
        self.livelihood_submissions()
        self.cropping_submissions()
        self.agri_maintenance_submissions()
        self.gw_maintenance_submissions()
        self.swb_maintenance_submissions()
        self.swb_rs_maintenance_submissions()

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def settlement_submissions(self):
        form_name = "Settlement Form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def well_submissions(self):
        form_name = "Well Form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def waterbody_submissions(self):
        form_name = "water body form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def groundwater_submissions(self):
        form_name = "new recharge structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def agri_submissions(self):
        form_name = "new irrigation form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def livelihood_submissions(self):
        form_name = "livelihood form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name]: {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def cropping_submissions(self):
        form_name = "cropping pattern form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def agri_maintenance_submissions(self):
        form_name = "propose maintenance on existing irrigation form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def gw_maintenance_submissions(self):
        form_name = "propose maintenance on water structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def swb_maintenance_submissions(self):
        form_name = "propose maintenance on existing water recharge form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}

    def swb_rs_maintenance_submissions(self):
        form_name = "propose maintenance of remotely sensed water structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_updated)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_updated": False}
        else:
            self.results[form_name] = {"is_updated": True}


class odk_submissions_edited:
    def __init__(self):
        self.token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
        self.results = {}

        self.settlement_submissions()
        self.well_submissions()
        self.waterbody_submissions()
        self.groundwater_submissions()
        self.agri_submissions()
        self.livelihood_submissions()
        self.cropping_submissions()
        self.agri_maintenance_submissions()
        self.gw_maintenance_submissions()
        self.swb_maintenance_submissions()
        self.swb_rs_maintenance_submissions()

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def settlement_submissions(self):
        form_name = "Settlement Form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def well_submissions(self):
        form_name = "Well Form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def waterbody_submissions(self):
        form_name = "water body form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def groundwater_submissions(self):
        form_name = "new recharge structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def agri_submissions(self):
        form_name = "new irrigation form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def livelihood_submissions(self):
        form_name = "livelihood form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def cropping_submissions(self):
        form_name = "cropping pattern form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def agri_maintenance_submissions(self):
        form_name = "propose maintenance on existing irrigation form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def gw_maintenance_submissions(self):
        form_name = "propose maintenance on water structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def swb_maintenance_submissions(self):
        form_name = "propose maintenance on existing water recharge form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}

    def swb_rs_maintenance_submissions(self):
        form_name = "propose maintenance of remotely sensed water structure form"
        url = form_submissions_updated_url(corestack[form_name], filter_query_edited)
        response = (
            requests.get(url, headers=self._auth_headers()).json().get("value", [])
        )
        if not response:
            self.results[form_name] = {"is_edited": False}
        else:
            self.results[form_name] = {"is_edited": True}
