"""Load ``catalog.json``, the single store for public API text and units.

Route descriptions, property descriptions, and measurement units are edited
in that file. The catalog API and the v2 response unit maps both read it.
"""

import json
from functools import lru_cache
from pathlib import Path

_PATH = Path(__file__).with_name("catalog.json")
_PUBLIC_PROPERTY_KEYS = ("name", "type", "unit", "description", "selectable")


@lru_cache(maxsize=1)
def catalog_document():
    with _PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def catalog_api(api_id):
    for api in catalog_document()["apis"]:
        if api["id"] == api_id:
            return api
    raise KeyError(api_id)


def public_properties(properties):
    """Property objects returned by the catalog API."""
    return [{key: prop[key] for key in _PUBLIC_PROPERTY_KEYS} for prop in properties]


def response_unit_map(api_id):
    """Column or field units copied onto a v2 payload.

    Tehsil sheets store a unit object per property; those stay in the catalog
    and are not returned here. ``response_unit: false`` marks catalog-only
    fields such as geometry coordinates.
    """
    units = {}
    for prop in catalog_api(api_id)["properties"]:
        if prop.get("response_unit") is False:
            continue
        unit = prop.get("unit")
        if isinstance(unit, str):
            units[prop["name"]] = unit
    return units


def tehsil_data_type_docs():
    """``data=`` values and the sentence that describes each tehsil sheet."""
    document = catalog_document()
    all_entry = document["tehsil_data_all"]
    docs = [(all_entry["name"], all_entry["description"])]
    docs.extend(
        (prop["name"], prop["description"])
        for prop in catalog_api("get_tehsil_data")["properties"]
    )
    return docs
