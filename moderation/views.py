from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
import requests
from moderation.utils.update_csdb import *
import xml.etree.ElementTree as ET
import re
from html import unescape
from bs4 import BeautifulSoup


FETCH_FIELD_MAP = {
    ODK_settlement: "data_settlement",
    ODK_well: "data_well",
    ODK_waterbody: "data_waterbody",
    ODK_groundwater: "data_groundwater",
    ODK_agri: "data_agri",
    ODK_livelihood: "data_crop",
    ODK_crop: "data_livelihood",
    Agri_maintenance: "data_agri_maintenance",
    GW_maintenance: "data_gw_maintenance",
    SWB_maintenance: "data_swb_maintenance",
    SWB_RS_maintenance: "data_swb_rs_maintenance",
}


def paginate_queryset(queryset, page=1, per_page=10):
    paginator = Paginator(queryset, per_page)

    try:
        obj_page = paginator.page(page)
    except PageNotAnInteger:
        obj_page = paginator.page(1)
    except EmptyPage:
        obj_page = paginator.page(paginator.num_pages)

    data = list(obj_page.object_list)

    return {
        "page": obj_page.number,
        "total_pages": paginator.num_pages,
        "total_objects": paginator.count,
        "data": data,
    }


class SubmissionsOfPlan:

    @staticmethod
    def _fetch(model, plan_id, page):
        field_name = FETCH_FIELD_MAP.get(model)
        if not field_name:
            raise ValueError(f"No fetch field configured for {model.__name__}")
        qs = model.objects.filter(plan_id=plan_id).values_list(field_name, flat=True)
        print(f"{qs=}")
        # quit()
        return paginate_queryset(qs, page)

    @staticmethod
    def get_settlement(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_settlement, plan_id, page)

    @staticmethod
    def get_well(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_well, plan_id, page)

    @staticmethod
    def get_waterbody(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_waterbody, plan_id, page)

    @staticmethod
    def get_groundwater(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_groundwater, plan_id, page)

    @staticmethod
    def get_agri(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_agri, plan_id, page)

    @staticmethod
    def get_livelihood(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_livelihood, plan_id, page)

    @staticmethod
    def get_crop(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_crop, plan_id, page)

    @staticmethod
    def get_agri_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(Agri_maintenance, plan_id, page)

    @staticmethod
    def get_gw_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(GW_maintenance, plan_id, page)

    @staticmethod
    def get_swb_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_maintenance, plan_id, page)

    @staticmethod
    def get_swb_rs_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_RS_maintenance, plan_id, page)


# ODK form XML parser
def parse_odk_xml_to_json(xml_content: str, language: str = "English(en)") -> dict:
    """
    Parse ODK XML form and extract itext translations as JSON.

    Args:
        xml_content (str): XML content returned by ODK
        language (str): Language to extract (default: English(en))

    Returns:
        dict: { text_id: label }
    """

    # Namespace mapping
    ns = {"h": "http://www.w3.org/1999/xhtml", "xf": "http://www.w3.org/2002/xforms"}

    root = ET.fromstring(xml_content)

    result = {}

    # Find itext -> translation
    translations = root.findall(".//xf:itext/xf:translation", ns)

    translation_node = None
    for t in translations:
        if t.attrib.get("lang") == language:
            translation_node = t
            break

    if translation_node is None:
        raise ValueError(f"Language '{language}' not found in XML")

    # Extract text nodes
    for text_el in translation_node.findall("xf:text", ns):
        text_id = text_el.attrib.get("id")
        value_el = text_el.find("xf:value", ns)

        if not text_id or value_el is None:
            continue

        value = "".join(value_el.itertext()).strip()

        if value:
            result[text_id] = value

    return result


# to fetch odk xml form
def parse_odk_form_service(
    odk_url, project_id, xml_form_id, token, language="English(en)"
):
    """
    Args:
        odk_url:
        project_id:
        xml_form_id:
        token:
        language:

    Returns:
        xml of particular form
    """
    url = f"{odk_url}{project_id}/forms/{xml_form_id}.xml"

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return parse_odk_xml_to_json(response.text, language)


def clean_html(text: str) -> str:
    """Remove span / html tags but keep text"""
    if "<" in text:
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text(strip=True)
    return text


def normalize_odk_labels(raw: dict):
    choices = {}
    questions = {}

    for key, value in raw.items():
        value = clean_html(unescape(value))

        # ------------------
        # CHOICES (Y_N-0)
        # ------------------
        if "-" in key and not key.startswith("/"):
            base, index = key.rsplit("-", 1)
            if index.isdigit():
                choices.setdefault(base, {})[index] = value
            continue

        # ------------------
        # QUESTION LABELS
        # ------------------
        if key.endswith(":label") and key.startswith("/data"):
            field = key.split("/")[-1].replace(":label", "")
            questions[field] = value
            continue

        # Ignore hints, constraints, jr metadata
        # :hint, :constraintMsg, :jr, etc.

    return {"choices": choices, "questions": questions}
