import csv
from .services import get_maintenance_data, get_nrm_works_data, get_livelihood_data
from utilities.logger import setup_logger
from django.conf import settings
from pathlib import Path
import json
import pandas as pd
from xml.sax.saxutils import escape
from utilities.constants import YUKTDHARA_DEMAND_OUTPUT_DIR

logger = setup_logger(__name__)


def load_csv_column_config():
    config_path = (
        Path(settings.BASE_DIR) / "data" / "Yuktdhara" / "yuktdhara_file_config.json"
    )
    with open(config_path, "r") as f:
        return json.load(f)


CSV_COLUMNS = load_csv_column_config()


def is_community_demand(demand_type):
    if not demand_type:
        return False

    return demand_type.strip().lower().replace("_", " ") == "community demand"


MAINTENANCE_CATEGORY_MAP = {
    "gw": "Recharge Structure",
    "agri": "Irrigation Work",
    "swb": "Waterbody",
    "swb_rs": "Waterbody",
}


def format_maintenance_row(item):
    community = is_community_demand(item.get("demand_type"))
    maintenance_type = item.get("maintenance_category")
    category = MAINTENANCE_CATEGORY_MAP.get(maintenance_type, "")
    return {
        "Irrigation work or Recharge Structure": category,
        "new/maintenance": "Maintenance",
        "Type of demand": item.get("demand_type", ""),
        "Work demand": item.get("repair_activities", ""),
        "Name of Beneficiary's Settlement": item.get("beneficiary_settlement", ""),
        "Beneficiary's Name": ("NA" if community else item.get("beneficiary_name", "")),
        "Beneficiary's Father's Name": (
            "NA" if community else item.get("beneficiary_father_name", "")
        ),
        "Area(In acres)/Dimension(In ft)": "",
        "Latitude": item.get("latitude", ""),
        "Longitude": item.get("longitude", ""),
    }


def format_nrm_row(item):
    community = is_community_demand(item.get("demand_type"))
    return {
        "Irrigation work or Recharge Structure": item.get("work_category", ""),
        "new/maintenance": "New demand",
        "Type of demand": item.get("demand_type", ""),
        "Work demand": item.get("work_demand", ""),
        "Name of Beneficiary's Settlement": item.get("beneficiary_settlement", ""),
        "Beneficiary's Name": ("NA" if community else item.get("beneficiary_name", "")),
        "Beneficiary's Father's Name": (
            "NA" if community else item.get("beneficiary_father_name", "")
        ),
        "Area(In acres)/Dimension(In ft)": "",
        "Latitude": item.get("latitude", ""),
        "Longitude": item.get("longitude", ""),
    }


def format_livelihood_row(item):
    community = is_community_demand(item.get("demand_type"))
    return {
        "Irrigation work or Recharge Structure": item.get("livelihood_work", ""),
        "new/maintenance": "New demand",
        "Type of demand": item.get("demand_type", ""),
        "Work demand": item.get("work_demand", ""),
        "Name of Beneficiary's Settlement": item.get("beneficiary_settlement", ""),
        "Beneficiary's Name": ("NA" if community else item.get("beneficiary_name", "")),
        "Beneficiary's Father's Name": (
            "NA" if community else item.get("beneficiary_father_name", "")
        ),
        "Area(In acres)/Dimension(In ft)": item.get("total_acres", ""),
        "Latitude": item.get("latitude", ""),
        "Longitude": item.get("longitude", ""),
    }


def fetch_data(plan_id, csv_path):
    maintenance_data = []
    maintenance_list = ["gw", "agri", "swb", "swb_rs"]
    for maintenance in maintenance_list:
        data = get_maintenance_data(plan_id, maintenance)
        for item in data:
            item["maintenance_category"] = maintenance
        maintenance_data.extend(data)
    nrm_works_data = get_nrm_works_data(plan_id)
    livelihood_data = get_livelihood_data(plan_id)
    export_csv(
        maintenance_data,
        nrm_works_data,
        livelihood_data,
        csv_path,
    )


def export_csv(
    maintenance_data,
    nrm_data,
    livelihood_data,
    csv_path,
):
    rows = []

    # maintenance rows
    for item in maintenance_data:
        rows.append(format_maintenance_row(item))
    logger.info("Maintenance demand is added")

    # nrm rows
    for item in nrm_data:
        rows.append(format_nrm_row(item))
    logger.info("New demand is added")

    # livelihood rows
    for item in livelihood_data:
        rows.append(format_livelihood_row(item))
    logger.info("Livelihood demand is added")

    file_name = csv_path

    # write csv
    with open(file_name, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)

        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"CSV exported successfully: {file_name}")


def csv_to_kml(csv_file, output_kml):
    df = pd.read_csv(csv_file).fillna("")

    columns = list(df.columns)

    kml_parts = []

    # header
    kml_parts.append('<?xml version="1.0" encoding="utf-8" ?>')
    kml_parts.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    kml_parts.append('<Document id="root_doc">')

    # schema
    kml_parts.append('<Schema name="yuktdhara" id="yuktdhara">')

    for col in columns:

        field_type = "string"

        if col in ["Latitude", "Longitude"]:
            field_type = "float"

        kml_parts.append(
            f'<SimpleField name="{escape(col)}" type="{field_type}"></SimpleField>'
        )

    kml_parts.append("</Schema>")

    # folder
    kml_parts.append("<Folder>")
    kml_parts.append("<name>yuktdhara</name>")

    # placemarks
    for _, row in df.iterrows():

        lat = row.get("Latitude", "")
        lon = row.get("Longitude", "")

        if lat == "" or lon == "":
            continue

        kml_parts.append("<Placemark>")

        # extended data
        kml_parts.append('<ExtendedData><SchemaData schemaUrl="#yuktdhara">')

        for col in columns:

            value = row.get(col, "")

            kml_parts.append(
                f'<SimpleData name="{escape(col)}">{escape(str(value))}</SimpleData>'
            )

        kml_parts.append("</SchemaData></ExtendedData>")

        # point
        kml_parts.append(f"<Point><coordinates>{lon},{lat}</coordinates></Point>")

        kml_parts.append("</Placemark>")

    # closing tags
    kml_parts.append("</Folder>")
    kml_parts.append("</Document></kml>")

    # write file
    with open(output_kml, "w", encoding="utf-8") as f:
        f.write("\n".join(kml_parts))

    print(f"KML generated: {output_kml}")
