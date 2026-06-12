import csv
from .services import get_maintenance_data, get_nrm_works_data, get_livelihood_data
from utilities.logger import setup_logger
from django.conf import settings
from pathlib import Path
import json
import pandas as pd
from xml.sax.saxutils import escape
import os
from plans.models import PlanApp

logger = setup_logger(__name__)


def load_yuktdhara_config():

    config_path = (
        Path(settings.BASE_DIR) / "data" / "Yuktdhara" / "yuktdhara_file_config.json"
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"yuktdhara_file_config.json not found at: {config_path}"
        )

    with open(config_path, "r") as f:
        return json.load(f)


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


def build_row(item, mapping_type, config):

    mapping_config = config["mapping"][mapping_type]

    row = {}

    community = is_community_demand(item.get("demand_type"))

    for output_column, rules in mapping_config.items():

        # static values
        if "value" in rules:
            row[output_column] = rules["value"]
            continue

        value = item.get(
            rules.get("source", ""),
            "",
        )

        # community demand handling
        if rules.get("community_na") and community:
            value = "NA"

        # transformations
        transform = rules.get("transform")

        if transform == "maintenance_category_map":
            value = MAINTENANCE_CATEGORY_MAP.get(value, "")

        row[output_column] = value

    return row


def fetch_data(gp_id, csv_path):
    maintenance_data = []
    nrm_works_data = []
    livelihood_data = []
    plans = PlanApp.objects.filter(gp_id=gp_id, enabled=True)
    maintenance_list = [
        "gw",
        "agri",
        "swb",
        "swb_rs",
    ]
    for plan in plans:
        plan_id = plan.id
        # maintenance
        for maintenance in maintenance_list:

            data = get_maintenance_data(
                plan_id,
                maintenance,
            )
            for item in data:
                item["maintenance_category"] = maintenance

            maintenance_data.extend(data)
        # nrm
        nrm_works_data.extend(get_nrm_works_data(plan_id))
        # livelihood
        livelihood_data.extend(get_livelihood_data(plan_id))
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
    config = load_yuktdhara_config()
    rows = []

    # maintenance rows
    for item in maintenance_data:
        rows.append(build_row(item, "maintenance", config))
    logger.info("Maintenance demand is added")

    # nrm rows
    for item in nrm_data:
        rows.append(build_row(item, "nrm", config))
    logger.info("New demand is added")

    # livelihood rows
    for item in livelihood_data:
        rows.append(build_row(item, "livelihood", config))
    logger.info("Livelihood demand is added")

    file_name = csv_path

    # write csv
    with open(file_name, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=config["columns"])

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
