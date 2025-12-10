import requests
from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
from plans.utils import fetch_bearer_token
from .form_mapping import corestack
from utilities.constants import ODK_BASE_URL


class get_edited_updated_all_submissions:
    def __init__(self, username, password, base_url):
        self.base_url = base_url
        self.token = fetch_bearer_token(username, password)

    def get_edited_updated_submissions(self, project_id, form_id, filter_query):
        url = f"{self.base_url}{project_id}/forms/{form_id}.svc/Submissions?{filter_query}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("value", [])


def form_submissions_edited_updated_url(form_id, filter_query):
    """

    Args:
        form_id:
        filter_query:

    Returns:
        url for particular form on the basis of particular date

    """
    project_id = 2
    url = f"{ODK_BASE_URL}{project_id}/forms/{form_id}.svc/Submissions?{filter_query}"
    return url


filter_query_updated = "$filter=__system/submissionDate ge 2025-11-28T00:00:00.000Z"
filter_query_edited = "$filter=__system/submissionDate lt 2025-11-28T00:00:00.000Z and __system/updatedAt ge 2025-11-28T00:00:00.000Z"


class ODKSubmissionsChecker:
    def __init__(self):
        self.token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
        self.results = {}
        self.forms = list(corestack.keys())

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def process(self, mode="updated"):
        """
        mode = "updated" or "edited"
        """
        filter_query = (
            filter_query_updated if mode == "updated" else filter_query_edited
        )
        flag_key = "is_updated" if mode == "updated" else "is_edited"

        for form_name in self.forms:
            form_id = corestack.get(form_name)
            if not form_id:
                self.results[form_name] = {"error": "Form not found in corestack"}
                continue

            url = form_submissions_edited_updated_url(form_id, filter_query)
            self.set_flag(url, form_name, flag_key)

        return self.results

    def set_flag(self, url, form_name, flag_key):
        try:
            response = (
                requests.get(url, headers=self._auth_headers()).json().get("value", [])
            )
            self.results[form_name] = {flag_key: bool(response)}
        except Exception as e:
            self.results[form_name] = {"error": str(e)}
