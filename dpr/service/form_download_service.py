import json
from pathlib import Path
import requests
from utilities.constants import ODK_BASE_URL, ODK_PROJECT_ID
from django.conf import settings
from plans.utils import fetch_bearer_token
from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
import pandas as pd
from utilities.logger import setup_logger
import re

logger = setup_logger(__name__)


def sync_odk_forms():
    """
    Downloads ODK XLSX forms only when:
    1. Form version has changed, OR
    2. XLSX file is missing locally.

    Returns:
        dict: Summary of downloaded and skipped forms.
    """

    token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
    headers = {"Authorization": f"Bearer {token}"}

    forms_dir = Path(settings.BASE_DIR) / "data" / "dpr" / "forms"
    label_dirs = Path(settings.BASE_DIR) / "data" / "dpr" / "labels"
    versions_file = Path(settings.BASE_DIR) / "data" / "odk" / "form_version.json"

    forms_dir.mkdir(parents=True, exist_ok=True)
    versions_file.parent.mkdir(parents=True, exist_ok=True)

    if versions_file.exists():
        with open(versions_file, "r") as f:
            local_versions = json.load(f)
    else:
        local_versions = {}

    response = requests.get(
        f"{ODK_BASE_URL}{ODK_PROJECT_ID}/forms",
        headers=headers,
    )
    response.raise_for_status()

    forms = response.json()

    downloaded = []
    skipped = []

    for form in forms:
        form_id = form["xmlFormId"]
        current_version = form.get("version")

        saved_version = local_versions.get(form_id)
        file_path = forms_dir / f"{form_id}.xlsx"
        label_path = label_dirs / f"{form_id}.json"
        if (
            saved_version == current_version
            and file_path.exists()
            and label_path.exists()
        ):
            skipped.append(form_id)
            continue

        print(f"Downloading {form_id} " f"(old={saved_version}, new={current_version})")

        file_response = requests.get(
            f"{ODK_BASE_URL}{ODK_PROJECT_ID}/forms/{form_id}.xlsx",
            headers=headers,
        )
        file_response.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(file_response.content)

        local_versions[form_id] = current_version
        downloaded.append(form_id)
        try:
            generate_labels_json(
                file_path,
                label_path,
            )
        except Exception:
            logger.exception(f"Failed to generate labels for {form_id}")
    with open(versions_file, "w") as f:
        json.dump(local_versions, f, indent=2)

    return {
        "downloaded": downloaded,
        "skipped": skipped,
        "total_forms": len(forms),
    }


def get_select_question_mapping(survey_df):
    """
    Returns:
    {
        "gender": "gender",
        "caste_group": "caste"
    }
    """

    mapping = {}

    for _, row in survey_df.iterrows():

        question_type = str(row.get("type", "")).strip()
        question_name = row.get("name")

        if pd.isna(question_name):
            continue

        question_name = str(question_name).strip()

        parts = question_type.split()

        if len(parts) < 2:
            continue

        if parts[0] in ["select_one", "select_multiple"]:
            mapping[question_name] = str(parts[1]).strip()

    return mapping


def generate_labels_json(form_path, output_path):
    """
    Generate labels JSON from an XLSForm.
    """

    survey_df = pd.read_excel(form_path, sheet_name="survey")
    choices_df = pd.read_excel(form_path, sheet_name="choices")

    # normalize list_name column
    choices_df["list_name"] = choices_df["list_name"].astype(str).str.strip()

    question_mapping = get_select_question_mapping(survey_df)

    language_columns = [
        column for column in choices_df.columns if str(column).startswith("label::")
    ]

    labels = {}

    for question_name, list_name in question_mapping.items():

        question_name = str(question_name).strip()
        list_name = str(list_name).strip()

        labels[question_name] = {}

        choice_rows = choices_df[choices_df["list_name"] == list_name]

        for _, row in choice_rows.iterrows():

            choice_value = row.get("name")

            if pd.isna(choice_value):
                continue

            choice_value = str(choice_value).strip().lower()

            labels[question_name][choice_value] = {}

            for column in language_columns:

                language = str(column).replace("label::", "").split("(")[0].strip()

                value = row.get(column)

                labels[question_name][choice_value][language] = (
                    None if pd.isna(value) else str(value).strip()
                )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(labels, f, ensure_ascii=False, indent=2)

    return labels


LANGUAGE_MAP = {
    "en": "English",
    "hi": "Hindi",
    "gu": "Gujarati",
    "kn": "Kannada",
    "or": "Odia",
}


def load_form_labels(form_id):
    label_file = Path(settings.BASE_DIR) / "data" / "dpr" / "labels" / f"{form_id}.json"

    if not label_file.exists():
        return {}

    with open(label_file, encoding="utf-8") as f:
        return json.load(f)


def translate_choice(labels, field_name, value, language="en"):
    if not value:
        return value

    language_name = LANGUAGE_MAP.get(language, "English")

    normalized_labels = {str(key).strip(): val for key, val in labels.items()}

    field_labels = normalized_labels.get(
        str(field_name).strip(),
        {},
    )

    value_normalized = str(value).strip().lower().replace("'", "’")

    translations = field_labels.get(value_normalized)

    if translations:
        return translations.get(language_name, value)

    return value


def translate_multiple_choices(
    labels,
    field_name,
    value,
    language="en",
):
    if not value:
        return value

    values = [v.strip() for v in re.split(r"[,\s]+", str(value).strip()) if v.strip()]

    translated = [
        translate_choice(
            labels,
            field_name,
            item,
            language,
        )
        for item in values
    ]

    return ", ".join(translated)
