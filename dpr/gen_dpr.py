import io
import json
import os
import tempfile
from collections import defaultdict
from datetime import date, datetime
from io import BytesIO
from multiprocessing import Process

from selenium import webdriver
from nrm_app.settings import (
    EMAIL_HOST_USER,
    EMAIL_HOST_PASSWORD,
    EMAIL_USE_SSL,
    EMAIL_TIMEOUT,
    EMAIL_PORT,
    EMAIL_HOST,
)

import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from django.core.mail import EmailMessage
from django.core.mail.backends.smtp import EmailBackend
import socket
import ssl
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.shared import Inches
from PIL import Image
from shapely.geometry import MultiPolygon, Point, shape
from shapely.ops import unary_union

from plans.models import Plan
from nrm_app.settings import GEOSERVER_URL

from .models import (
    ODK_agri,
    ODK_groundwater,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    ODK_crop,
    ODK_livelihood,
    GW_maintenance,
    SWB_maintenance,
    SWB_RS_maintenance,
    Agri_maintenance,
)
from dpr.mapping import populate_maintenance_from_waterbody
from .utils import get_vector_layer_geoserver, sync_db_odk, format_text
from utilities.logger import setup_logger

logger = setup_logger(__name__)

def get_plan(plan_id):
    try:
        return Plan.objects.get(plan_id=plan_id)
    except Plan.DoesNotExist:
        return None


def create_dpr_document(plan):
    doc = initialize_document()
    logger.info("Generating DPR for plan ID: %s", plan.plan_id)
    logger.info("Syncing ODK database")

    sync_db_odk()
    logger.info("Database sync complete")
    logger.info("Details of the plan")
    logger.info(plan)
    logger.info(str(plan.district.district_name).lower())
    logger.info(str(plan.block.block_name).lower())

    total_settlements = get_settlement_count_for_plan(plan.plan_id)
    logger.info("1. MWS Fortnight")

    mws_fortnight = get_vector_layer_geoserver(
        geoserver_url=GEOSERVER_URL,
        workspace="mws_layers",
        layer_name="deltaG_fortnight_"
        + str(plan.district.district_name).lower().replace(" ", "_")
        + "_"
        + str(plan.block.block_name).lower().replace(" ", "_"),
    )

    add_section_a(doc, plan)

    settlement_mws_ids, mws_gdf = add_section_b(
        doc, plan, total_settlements, mws_fortnight
    )

    add_section_c(doc, plan)

    add_section_d(doc, plan, settlement_mws_ids, mws_gdf)

    add_section_e(doc, plan)

    add_section_f(doc, plan, mws_fortnight) # generates maps as well

    add_section_g(doc, plan, mws_fortnight)

    add_section_h(doc, plan, mws_fortnight)

    # MARK: local save /tmp/dpr/
    # operations on the document
    # file_path = "/tmp/dpr/"

    # if not os.path.exists(file_path):
    #     os.makedirs(file_path)
    # doc.save(file_path + plan.plan + ".docx")
    return doc


def send_dpr_email(doc, email_id, plan_name):
    try:
        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        doc_bytes = buffer.getvalue()
        buffer.close()

        email_body = f"""
        Hi,
        Find attached the Detailed Project Report for {plan_name}.
        Thanks.
        """

        backend = EmailBackend(
            host=EMAIL_HOST,
            port=EMAIL_PORT,
            username=EMAIL_HOST_USER,
            password=EMAIL_HOST_PASSWORD,
            use_ssl=EMAIL_USE_SSL,
            timeout=EMAIL_TIMEOUT,
            ssl_context=ssl.create_default_context(),
        )

        email = EmailMessage(
            subject=f"DPR of plan: {plan_name}",
            body=email_body,
            from_email=EMAIL_HOST_USER,
            to=[email_id],
            connection=backend,
        )

        email.attach(
            f"DPR_{plan_name}.docx",
            doc_bytes,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        logger.info("Sending DPR email to %s", email_id)
        email.send(fail_silently=False)
        logger.info("DPR email sent.")
        backend.close()

    except socket.error as e:
        logger.error(f"Socket error: {e}")
    except ssl.SSLError as e:
        logger.error(f"SSL error: {e}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def get_data_for_settlement(planid):
    return ODK_settlement.objects.filter(plan_id=planid)


def get_settlement_count_for_plan(planid):
    return ODK_settlement.objects.filter(plan_id=planid).count()


def get_settlement_coordinates_for_plan(planid):
    settlements = ODK_settlement.objects.filter(plan_id=planid).values(
        "settlement_name", "latitude", "longitude"
    )
    return [
        (
            settlement["settlement_name"],
            settlement["latitude"],
            settlement["longitude"],
        )
        for settlement in settlements
    ]


def get_mws_uid_for_settlement_gdf(mws_gdf, lat, lon):
    settlement_point = Point(lon, lat)
    intersecting_mws = mws_gdf[mws_gdf.intersects(settlement_point)]

    if not intersecting_mws.empty:
        mws_uid = intersecting_mws.iloc[0]["uid"]
        return mws_uid
    else:
        return None


def initialize_document():
    doc = Document()
    heading = doc.add_heading("Detailed Project Report", 0)
    heading.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    doc.add_paragraph(
        date.today().strftime("%B %d, %Y")
    ).alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    return doc


# MARK: - Section A
def add_section_a(doc, plan):
    """
    Brief about team details.
    """
    doc.add_heading("Section A: Team Details", level=1)
    para = doc.add_paragraph()
    para.add_run(
        "This section gives brief information about the Project Name, "
        "facilitator details responsible for the preparation of the Detailed "
        "Project Report (DPR). The process begins with Community Consultations, "
        "involving active engagement with community members "
        "to identify their needs and resources.\n\n"
    )
    create_team_details_table(doc, plan)


def create_team_details_table(doc, plan):
    table = doc.add_table(rows=1, cols=3)
    table.style = "Table Grid"
    hdr_cells = table.rows[0].cells
    hdr_cells[0].text = "Name of the Facilitator"
    hdr_cells[1].text = "Project Name"
    hdr_cells[2].text = "Process involved in the preparation of the DPR PRA"

    row_cells = table.add_row().cells
    row_cells[0].text = plan.facilitator_name
    row_cells[1].text = plan.plan
    row_cells[2].text = "PRA, Gram Sabha, Transect Walk, GIS Mapping"

    for cell in hdr_cells:
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.font.bold = True


# MARK: - Section B
def add_section_b(doc, plan, total_settlements, mws_fortnight):
    """
    Briefs about the village
    """
    doc.add_heading("Section B: Brief of Village")
    para = doc.add_paragraph()
    para.add_run(
        "This section gives a brief overview of the village, "
        "including its name, associated Gram Panchayat, location "
        "details (block, district, and state), the number of settlements, "
        "intersecting micro watershed IDs, and the geographic coordinates "
        "(latitude and longitude) of the village.\n\n"
    )

    mws_gdf = gpd.GeoDataFrame.from_features(mws_fortnight["features"])

    settlement_mws_ids = []
    settlement_coordinates = get_settlement_coordinates_for_plan(plan.plan_id)

    for settlement_name, latitude, longitude in settlement_coordinates:
        mws_uid = get_mws_uid_for_settlement_gdf(mws_gdf, latitude, longitude)
        if mws_uid:
            settlement_mws_ids.append((settlement_name, mws_uid))

    intersecting_mws_ids = "; ".join(
        [f"{name}: {mws_id}" for name, mws_id in settlement_mws_ids]
    )
    create_village_brief_table(
        intersecting_mws_ids, doc, plan, total_settlements, mws_gdf
    )

    return settlement_mws_ids, mws_gdf


def create_village_brief_table(
    intersecting_mws_ids, doc, plan, total_settlements, mws_gdf
):
    table = doc.add_table(rows=8, cols=2)
    table.style = "Table Grid"

    # Calculate the centroid of the intersecting MWS
    intersecting_mws = mws_gdf[
        mws_gdf["uid"].isin(
            [mws_id.split(": ")[1] for mws_id in intersecting_mws_ids.split("; ")]
        )
    ]
    centroid = intersecting_mws.geometry.unary_union.centroid

    headers_data = [
        ("Name of the Village", plan.village_name),
        ("Name of the Gram Panchayat", plan.gram_panchayat),
        ("Block", plan.block.block_name),
        ("District", plan.district.district_name),
        ("State", plan.state.state_name),
        ("Number of Settlements in the Village", str(total_settlements)),
        ("Intersecting Micro Watershed IDs", intersecting_mws_ids),
        (
            "Latitude and Longitude of the Village",
            f"{centroid.y:.6f}, {centroid.x:.6f}",
        ),
    ]

    for i, (key, value) in enumerate(headers_data):
        row_cells = table.rows[i].cells
        cell_0_paragraphs = row_cells[0].paragraphs
        if cell_0_paragraphs:
            cell_0_paragraphs[0].clear()
        paragraph = (
            cell_0_paragraphs[0] if cell_0_paragraphs else row_cells[0].add_paragraph()
        )
        run = paragraph.add_run(key)
        run.bold = True
        row_cells[1].text = value


# MARK: - Section C
def add_section_c(doc, plan):
    """
    Adds information on Settlement, Vulnerability, NREGA from the settlement's form

    Livelihood Profile: From Livelihood form and Settlement's form (Livestock Info)
    """
    doc.add_heading("Section C: Social Economic Ecological Profile", level=1)
    para = doc.add_paragraph()
    para.add_run(
        "This section provides an overview of the settlement's social, economic, and ecological characteristics. It includes information on vulnerabilities, NREGA details, livelihoods, and crop and livestock profiles. The specific details on caste groups, economic conditions, NREGA works, and infrastructure, as well as previous NRM demands, contribute to a comprehensive understanding of the settlement's dynamics. \n\n"
    )

    create_socio_eco_table(doc, plan)

    doc.add_heading("Livelihood Profile", level=3)

    create_livelihood_table(doc, plan)


def create_socio_eco_table(doc, plan):
    headers_socio = [
        "Name of the Settlement",
        "Total Number of Households",
        "Settlement Type",
        "Caste Group",
        "Total marginal farmers (<2 acres)",
    ]

    data_settlement = get_data_for_settlement(plan.plan_id)
    table_socio = doc.add_table(rows=1, cols=len(headers_socio))
    table_socio.style = "Table Grid"

    hdr_cells = table_socio.rows[0].cells
    for i, header in enumerate(headers_socio):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for item in data_settlement:
        if item.status_re != "rejected":
            row_cells = table_socio.add_row().cells
            row_cells[0].text = item.settlement_name
            row_cells[1].text = str(item.number_of_households)
            row_cells[2].text = item.largest_caste

            # Determine the caste group based on settlement type
            if item.largest_caste.lower() == "single caste group":
                row_cells[3].text = item.smallest_caste
            elif item.largest_caste.lower() == "mixed caste group":
                row_cells[3].text = item.settlement_status
            else:
                row_cells[3].text = "NA"

            row_cells[4].text = str(item.farmer_family.get("marginal_farmers", "")) or "NA"

    headers_nrega = [
        "Settlement's Name",
        "Households - applied \n- having NREGA job cards",
        "NREGA work days in previous year",
        "Previous NRM demands made in the settlement",
        "Were demands raised by you, and were you involved in the new NRM planning?",
        "Issues",
    ]

    doc.add_heading("NREGA Info", level=3)

    table_nrega = doc.add_table(rows=1, cols=len(headers_nrega))
    table_nrega.style = "Table Grid"
    hdr_cells = table_nrega.rows[0].cells
    for i, header in enumerate(headers_nrega):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for settlement_nrega in data_settlement:
        if settlement_nrega.status_re != "rejected":
            row_cells = table_nrega.add_row().cells
            row_cells[0].text = settlement_nrega.settlement_name
            row_cells[1].text = (
                "applied: "
                + str(settlement_nrega.nrega_job_applied)
                + "\n"
                + "having: "
                + str(settlement_nrega.nrega_job_card)
            )
            row_cells[2].text = str(settlement_nrega.nrega_work_days)
            row_cells[3].text = format_text(settlement_nrega.nrega_past_work)
            row_cells[4].text = settlement_nrega.nrega_demand
            row_cells[5].text = format_text(settlement_nrega.nrega_issues)


def create_livelihood_table(doc, plan):
    # Crop Info section remains the same
    doc.add_heading("Crop Info", level=4)
    crops_in_plan = ODK_crop.objects.filter(plan_id=plan.plan_id)
    headers_cropping_pattern = [
        "Name of the Settlement",
        "Irrigation Source",
        "Land Classification",
        "Type of Grid (Uncropped/Barren)",
        "Cropping Pattern (Kharif)",
        "Cropping Pattern (Rabi)",
        "Cropping Pattern (Zaid)",
        "Cropping Intensity",
    ]
    table_cropping_pattern = doc.add_table(rows=1, cols=len(headers_cropping_pattern))
    table_cropping_pattern.style = "Table Grid"
    hdr_cells = table_cropping_pattern.rows[0].cells
    for i, header in enumerate(headers_cropping_pattern):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for crops in crops_in_plan:
        row_cells = table_cropping_pattern.add_row().cells
        row_cells[0].text = crops.beneficiary_settlement
        row_cells[1].text = crops.irrigation_source
        row_cells[2].text = crops.land_classification
        row_cells[3].text = crops.data_crop.get("Uncropped_barren_land") or "None"
        row_cells[4].text = format_text(crops.cropping_patterns_kharif)
        row_cells[5].text = format_text(crops.cropping_patterns_rabi)
        row_cells[6].text = format_text(crops.cropping_patterns_zaid)
        row_cells[7].text = crops.agri_productivity

    # Livestock Info section with modified value handling
    doc.add_heading("Livestock Info", level=4)
    livelihood_in_plan = ODK_settlement.objects.filter(plan_id=plan.plan_id)
    headers_livelihood = [
        "Name of the Settlement",
        "Goats",
        "Sheep",
        "Cattle",
        "Piggery",
        "Poultry",
    ]
    table_livelihood = doc.add_table(rows=1, cols=len(headers_livelihood))
    table_livelihood.style = "Table Grid"
    hdr_cells = table_livelihood.rows[0].cells
    for i, header in enumerate(headers_livelihood):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    def format_livestock_value(value):
        """Helper function to format livestock values"""
        if value in [None, "", "0", 0, "None"]:
            return "None"
        return str(value)

    for livelihood in livelihood_in_plan:
        row_cells = table_livelihood.add_row().cells
        row_cells[0].text = livelihood.settlement_name

        # Get livestock census data with proper handling of None/empty values
        livestock_data = livelihood.livestock_census or {}

        # Set values for each livestock type
        livestock_types = ["Goats", "Sheep", "Cattle", "Piggery", "Poultry"]
        for i, livestock_type in enumerate(livestock_types, start=1):
            value = livestock_data.get(livestock_type, "")
            row_cells[i].text = format_livestock_value(value)


# MARK: - Section D
def add_section_d(doc, plan, settlement_mws_ids, mws_gdf):
    doc.add_heading(
        "Section D: Information of all Micro Watersheds intersecting the village",
        level=1,
    )
    para = doc.add_paragraph()
    para.add_run(
        "This section provides an overview of all micro watersheds intersecting the village, detailing completed NRM works on individual and common lands, non-NRM works, changes in land use over the last 5 years, and information on wells, water structures, and household beneficiaries."
    )

    create_mws_table(doc, plan, settlement_mws_ids, mws_gdf)
    unique_mws_ids = sorted(set([mws_id for _, mws_id in settlement_mws_ids]))

    for mws_id in unique_mws_ids:
        populate_mws(doc, plan, mws_id, mws_gdf)


def populate_mws(doc, plan, mws_id, mws_gdf):
    doc.add_heading("MWS: " + mws_id, level=1)

    populate_land_use(doc, plan, mws_id, mws_gdf)  # Land Use Section
    populate_water_structures(doc, plan, mws_id, mws_gdf)  # Water Structures
    populate_water_budgeting(doc, plan, mws_id, mws_gdf)


def create_mws_table(doc, plan, settlement_mws_ids, mws_gdf):
    unique_mws_ids = sorted(set([mws_id for _, mws_id in settlement_mws_ids]))

    headers_mws = ["Microwatershed ID", "Latitude and Longitude (Centroid)"]
    table_mws = doc.add_table(rows=len(unique_mws_ids) + 1, cols=len(headers_mws))
    table_mws.style = "Table Grid"

    hdr_cells = table_mws.rows[0].cells
    for i, header in enumerate(headers_mws):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for i, mws_id in enumerate(unique_mws_ids, start=1):
        row_cells = table_mws.rows[i].cells
        row_cells[0].text = mws_id

        matching_feature = mws_gdf[mws_gdf["uid"] == mws_id]
        if not matching_feature.empty:
            centroid = matching_feature.geometry.centroid.iloc[0]
            row_cells[1].text = f"{centroid.y:.2f}, {centroid.x:.2f}"
        else:
            row_cells[1].text = "Not found"


def populate_land_use(doc, plan, mws_id, mws_gdf):
    doc.add_heading("Information on Land Use", level=2)
    doc.add_heading("Change in cropping area over last 5 years", level=3)

    logger.info("2. Cropping Intensity Layer")

    try:
        ci_layer_name = (
            str(plan.district.district_name).lower().replace(" ", "_")
            + "_"
            + str(plan.block.block_name).lower().replace(" ", "_")
            + "_"
            + "intensity"
        )
        logger.debug("Attempting to fetch layer: %s", ci_layer_name)

        cropping_intensity_layer = get_vector_layer_geoserver(
            geoserver_url=GEOSERVER_URL,
            workspace="cropping_intensity",
            layer_name=ci_layer_name,
        )

        if not cropping_intensity_layer or "features" not in cropping_intensity_layer:
            logger.error(
                "Failed to fetch cropping intensity layer or layer has no features"
            )
            return

        logger.debug(
            "Successfully fetched cropping intensity layer with %d features",
            len(cropping_intensity_layer["features"]),
        )

        ci_gdf = gpd.GeoDataFrame.from_features(cropping_intensity_layer["features"])
        joined_gdf = gpd.sjoin(mws_gdf, ci_gdf, how="left", predicate="intersects")

    except Exception as e:
        logger.exception("Error processing cropping intensity layer: %s", str(e))
        return

    specific_mws_row = joined_gdf[joined_gdf["uid_left"] == mws_id]
    table_ci = create_land_use_table(doc, specific_mws_row)
    plot_graph_ci(table_ci, doc)


def create_land_use_table(doc, specific_mws_row):
    headers_cropping_intensity = [
        "Year",
        "Area under Single Crop (in ha)",
        "Area under Double Crop (in ha)",
        "Area under Triple Crop (in ha)",
        "Uncropped Area (in ha)",
    ]
    table_cropping_intensity = doc.add_table(
        rows=8, cols=len(headers_cropping_intensity)
    )
    table_cropping_intensity.style = "Table Grid"

    hdr_cells = table_cropping_intensity.rows[0].cells
    for i, header in enumerate(headers_cropping_intensity):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for i, year in enumerate(range(2017, 2023)):
        total_crop = specific_mws_row["total_crop"].iloc[0]
        single_kharif_cropping = (
            (specific_mws_row[f"single_k_{i}"].iloc[0] / total_crop) * 100
            if i > 0
            else (specific_mws_row["single_kha"].iloc[0] / total_crop) * 100
        )
        single_non_kharif_cropping = (
            (specific_mws_row[f"single_n_{i}"].iloc[0] / total_crop) * 100
            if i > 0
            else (specific_mws_row["single_non"].iloc[0] / total_crop) * 100
        )
        single_cropping = single_kharif_cropping + single_non_kharif_cropping
        double_cropping = (
            (specific_mws_row[f"doubly_c_{i}"].iloc[0] / total_crop) * 100
            if i > 0
            else (specific_mws_row["doubly_cro"].iloc[0] / total_crop) * 100
        )
        triple_cropping = (
            (specific_mws_row[f"triply_c_{i}"].iloc[0] / total_crop) * 100
            if i > 0
            else (specific_mws_row["triply_cro"].iloc[0] / total_crop) * 100
        )
        uncropped_area = 100 - (single_cropping + double_cropping + triple_cropping)

        # +1 to skip the header row
        row_cells = table_cropping_intensity.rows[i + 1].cells
        row_cells[0].text = str(year)
        row_cells[1].text = str(round(single_cropping, 2))
        row_cells[2].text = str(round(double_cropping, 2))
        row_cells[3].text = str(round(triple_cropping, 2))
        row_cells[4].text = str(round(uncropped_area, 2))

    return table_cropping_intensity


def plot_graph_ci(table_ci, doc):
    years = list(range(2017, 2023))
    single_cropping_values = []
    double_cropping_values = []
    triple_cropping_values = []
    uncropped_land_values = []

    for i, year in enumerate(years):
        row_cells = table_ci.rows[i + 1].cells
        single_cropping_values.append(float(row_cells[1].text))
        double_cropping_values.append(float(row_cells[2].text))
        triple_cropping_values.append(float(row_cells[3].text))
        uncropped_land_values.append(float(row_cells[4].text))

    plt.bar(years, single_cropping_values, color="#eee05d", label="Single Cropping")
    plt.bar(
        years,
        double_cropping_values,
        bottom=single_cropping_values,
        color="#f9b249",
        label="Double Cropping",
    )
    plt.bar(
        years,
        triple_cropping_values,
        bottom=[i + j for i, j in zip(single_cropping_values, double_cropping_values)],
        color="#fb5139",
        label="Triple Cropping",
    )
    plt.bar(
        years,
        uncropped_land_values,
        bottom=[
            i + j + k
            for i, j, k in zip(
                single_cropping_values, double_cropping_values, triple_cropping_values
            )
        ],
        color="#a9a9a9",
        label="Uncropped Area",
    )

    # Add labels and title
    plt.xlabel("Year")
    plt.ylabel("Percentage of area")
    plt.title("Land use")
    plt.legend()

    image_stream = BytesIO()
    plt.savefig(image_stream, format="png")
    plt.close()

    image_stream.seek(0)

    doc.add_picture(image_stream)


def populate_water_structures(doc, plan, mws_id, mws_gdf):
    doc.add_heading("Information on Water Structures", level=2)

    populate_well(doc, plan, mws_id, mws_gdf)
    populate_waterbody(doc, plan, mws_id, mws_gdf)


def populate_well(doc, plan, mws_id, mws_gdf):
    mws_polygon = mws_gdf[mws_gdf["uid"] == mws_id].geometry.iloc[0]
    wells_in_plan = ODK_well.objects.filter(plan_id=plan.plan_id)

    wells_in_mws = []
    for well in wells_in_plan:
        well_point = Point(well.longitude, well.latitude)
        well_found = False

        for current_mws_id, current_mws_polygon in zip(
            mws_gdf["uid"], mws_gdf["geometry"]
        ):
            if well_point.within(current_mws_polygon):
                well_found = True
                wells_in_mws.append(well)
                break  # Stop checking other MWS if found in one

        if not well_found:
            logger.info(
                f"Well at ({well.latitude}, {well.longitude}) does not belong to any MWS"
            )

    wells_count = defaultdict(int)
    households_count = defaultdict(int)
    for well in wells_in_mws:
        wells_count[well.beneficiary_settlement] += 1
        households_count[well.beneficiary_settlement] += int(well.households_benefitted)

    wells_info = [
        (settlement, wells_count[settlement], households_count[settlement])
        for settlement in wells_count
    ]

    doc.add_heading(f"Well Information for MWS: {mws_id}", level=2)
    headers_well_info = [
        "Name of Beneficiary Settlement",
        "Number of Wells",
        "Total Number of Household Benefitted",
    ]
    table_well_info = doc.add_table(
        rows=len(wells_info) + 1, cols=len(headers_well_info)
    )
    table_well_info.style = "Table Grid"

    hdr_cells = table_well_info.rows[0].cells
    for i, header in enumerate(headers_well_info):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for i, (settlement, num_wells, num_households) in enumerate(wells_info, start=1):
        row_cells = table_well_info.rows[i].cells
        row_cells[0].text = settlement
        row_cells[1].text = str(num_wells)
        row_cells[2].text = str(num_households)

    doc.add_heading("Well Info", level=3)
    headers_well = [
        "Name of Beneficiary Settlement",
        "Who owns the Well",
        "Households Benefitted",
        "Which Caste uses the well?",
        "Functional or Non-functional",
        "Need Maintenance",
        "Latitude",
        "Longitude",
    ]
    table_well = doc.add_table(rows=1, cols=len(headers_well))
    table_well.style = "Table Grid"
    hdr_cells = table_well.rows[0].cells
    for i, header in enumerate(headers_well):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for well in wells_in_mws:
        row_cells = table_well.add_row().cells
        row_cells[0].text = well.beneficiary_settlement
        row_cells[1].text = well.owner
        row_cells[2].text = str(well.households_benefitted)
        row_cells[3].text = well.caste_uses
        row_cells[4].text = well.is_functional
        row_cells[5].text = well.need_maintenance
        row_cells[6].text = str(well.latitude)
        row_cells[7].text = str(well.longitude)


def populate_waterbody(doc, plan, mws_id, mws_gdf):
    mws_polygon = mws_gdf[mws_gdf["uid"] == mws_id].geometry.iloc[0]
    waterbodies_in_plan = ODK_waterbody.objects.filter(plan_id=plan.plan_id)
    waterbody_in_mws = []
    for waterbody in waterbodies_in_plan:
        waterbody_point = Point(waterbody.longitude, waterbody.latitude)
        waterbody_found = False

        for current_mws_id, current_mws_polygon in zip(
            mws_gdf["uid"], mws_gdf["geometry"]
        ):
            if waterbody_point.within(current_mws_polygon):
                waterbody_found = True
                waterbody_in_mws.append(waterbody)
                break  # Stop checking other MWS if found in one

        if not waterbody_found:
            logger.info(
                f"Waterbody at ({waterbody.latitude}, {waterbody.longitude}) does not belong to any MWS"
            )

    # Count the waterbodies and households benefitted for each settlement
    waterbody_count = defaultdict(int)
    households_count = defaultdict(int)
    for waterbody in waterbody_in_mws:
        waterbody_count[
            (waterbody.beneficiary_settlement, waterbody.water_structure_type)
        ] += 1
        households_count[
            (waterbody.beneficiary_settlement, waterbody.water_structure_type)
        ] += int(waterbody.household_benefitted)

    # Convert the counts to a list of tuples for table population
    waterbody_info = [
        (
            settlement,
            waterbody_type,
            waterbody_count[(settlement, waterbody_type)],
            households_count[(settlement, waterbody_type)],
        )
        for (settlement, waterbody_type) in waterbody_count
    ]

    doc.add_heading(f"Waterbody Information for MWS: {mws_id}", level=2)
    headers_waterstructure = [
        "Name of the Beneficiary Settlement",
        "Who manages?",
        "Who owns the water structure?",
        "Which Caste uses the water structure?",
        "Households Benefitted",
        "Type of Water Structure",
        "Identified By",
        "Need Maintenance",
        "Latitude",
        "Longitude",
    ]
    table_water_structure = doc.add_table(rows=1, cols=len(headers_waterstructure))
    table_water_structure.style = "Table Grid"

    hdr_cells = table_water_structure.rows[0].cells
    for i, header in enumerate(headers_waterstructure):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for water_st in waterbody_in_mws:
        row_cells = table_water_structure.add_row().cells
        row_cells[0].text = water_st.beneficiary_settlement
        if water_st.who_manages.lower() == "other":
            row_cells[1].text = "Other: " + water_st.specify_other_manager
        else:
            row_cells[1].text = water_st.who_manages
        row_cells[2].text = water_st.owner
        row_cells[3].text = water_st.caste_who_uses
        row_cells[4].text = str(water_st.household_benefitted)
        if water_st.water_structure_type.lower() == "other":
            row_cells[5].text = "Other: " + water_st.water_structure_other
        else:
            row_cells[5].text = water_st.water_structure_type
        row_cells[6].text = water_st.identified_by
        row_cells[7].text = water_st.need_maintenance
        row_cells[8].text = str(water_st.latitude)
        row_cells[9].text = str(water_st.longitude)


def populate_water_budgeting(doc, plan, mws_id, mws_gdf):
    doc.add_heading("Water Budgeting at Micro Watershed level", level=2)
    para = doc.add_paragraph()
    para.add_run(
        "This table below provides year wise information on water budgeting at the micro watershed level, "
        "including details on precipitation, runoff, ET, change in groundwater, and well depth.\n\n"
    )

    mws_well_depth_layer = get_vector_layer_geoserver(
        geoserver_url=GEOSERVER_URL,
        workspace="mws_layers",
        layer_name="deltaG_well_depth_"
        + str(plan.district.district_name).lower().replace(" ", "_")
        + "_"
        + str(plan.block.block_name).lower().replace(" ", "_"),
    )

    filtered_features = [
        feature
        for feature in mws_well_depth_layer["features"]
        if "properties" in feature and feature["properties"].get("uid") == mws_id
    ]

    for feature in filtered_features:
        properties = feature["properties"]
        properties = properties.items()  # Convert to list of tuples
        yearly_data = extract_yearly_data(properties)
        fill_yearly_table(doc, yearly_data)


def extract_yearly_data(properties):
    data = {}
    for year_range, values_str in properties:
        if not (year_range.startswith("20") and "_" in year_range):
            continue

        year = int(year_range.split("_")[0])
        values = json.loads(values_str)

        data[year] = {
            "Precipitation": values["Precipitation"],
            "RunOff": values["RunOff"],
            "ET": values["ET"],
            "G": values["G"],
            "WellDepth": values["WellDepth"],
        }

    return data


def fill_yearly_table(doc, yearly_data):
    headers = [
        "Year",
        "Precipitation (in mm)",
        "Run-off (in mm)",
        "ET (in mm)",
        "Change in Groundwater (G in mm)",
        "Well Depth (in m)",
    ]
    years = sorted(yearly_data.keys())

    table = doc.add_table(rows=len(years) + 1, cols=len(headers))
    table.style = "Table Grid"

    # Fill headers
    for i, header in enumerate(headers):
        cell = table.cell(0, i)
        cell.text = header
        cell.paragraphs[0].runs[0].font.bold = True

    # Fill data
    for i, year in enumerate(years):
        row = yearly_data[year]
        row_cells = table.rows[i + 1].cells
        row_cells[0].text = str(year)
        row_cells[1].text = f"{row['Precipitation']:.2f}"
        row_cells[2].text = f"{row['RunOff']:.2f}"
        row_cells[3].text = f"{row['ET']:.2f}"
        row_cells[4].text = f"{row['G']:.2f}"
        row_cells[5].text = f"{row['WellDepth']:.2f}"

    # Create and add the graph
    create_and_add_graph(doc, yearly_data)


def create_and_add_graph(doc, yearly_data):
    years = sorted(yearly_data.keys())
    precipitation = [yearly_data[year]["Precipitation"] for year in years]
    runoff = [yearly_data[year]["RunOff"] for year in years]
    et = [yearly_data[year]["ET"] for year in years]
    g = [yearly_data[year]["G"] for year in years]

    # Set the style
    sns.set_style("darkgrid")

    # Create individual figures for each metric
    metrics = [
        ("Precipitation", precipitation, "blue"),
        ("Run-off", runoff, "pink"),
        ("ET", et, "green"),
        ("Change in Groundwater", g, "brown"),
    ]

    for title, data, color in metrics:
        # Create new figure for each plot
        plt.figure(figsize=(6, 4))

        # Create bar plot
        sns.barplot(x=years, y=data, color=color)

        # Customize plot
        plt.title(f"Yearly {title}", fontsize=10, pad=10)
        plt.xlabel("Year", fontsize=10)
        plt.ylabel(f"{title} (mm)", fontsize=8)
        plt.xticks(rotation=45)

        # Adjust layout
        plt.tight_layout()

        # Save to stream and add to document
        image_stream = BytesIO()
        plt.savefig(image_stream, format="png", dpi=300, bbox_inches="tight")
        plt.close()

        # Add to document
        image_stream.seek(0)
        doc.add_picture(image_stream)

        # Add small spacing between graphs
        doc.add_paragraph().add_run().add_break()

    # Add final spacing after all graphs
    doc.add_paragraph()


# MARK: - Section E
def add_section_e(doc, plan):
    doc.add_heading(
        "Section E: Remote sensing data- Total Area under Surface Water Structures",
        level=1,
    )
    para = doc.add_paragraph()
    para.add_run(
        "This section includes information on  the total area under  surface water during the Kharif, Rabi, and Zaid season for each specified year.\n\n"
    )

    doc.add_heading("Total area under Surface Water", level=2)
    create_surface_wb_table(doc, plan)


def find_closest_mws(point_coords, mws_polygons):
    """Find the closest MWS polygon to a point."""
    point = shape({"type": "Point", "coordinates": point_coords})
    min_distance = float("inf")
    closest_mws = None

    for uid, polygon in mws_polygons.items():
        distance = point.distance(polygon)
        if distance < min_distance:
            min_distance = distance
            closest_mws = uid

    return closest_mws


def create_surface_wb_table(doc, plan):
    """
    Total area under surface water body. Generates a table about surface water availability during Kharib, Rabi and Zaid seasons
    """
    headers = [
        "Year",
        "Total area under Surface Water (hectare)",
        "Water availability in Kharif (pixel percentage)",
        "Water availability in Rabi (pixel percentage)",
        "Water availability in Zaid (pixel percentage)",
    ]

    swb = get_vector_layer_geoserver(
        geoserver_url=GEOSERVER_URL,
        workspace="water_bodies",
        layer_name="surface_waterbodies_"
        + str(plan.district.district_name).lower().replace(" ", "_")
        + "_"
        + str(plan.block.block_name).lower().replace(" ", "_"),
    )
    columns = []
    if swb is not None:
        features = swb.get("features", [])
        if features:
            columns = list(features[0]["properties"].keys())

    column_mapping = {
        "2018-2019": {
            "total_area": "area_18-19",
            "kharif": "k_18-19",
            "rabi": "kr_18-19",
            "zaid": "krz_18-19",
        },
        "2019-2020": {
            "total_area": "area_19-20",
            "kharif": "k_19-20",
            "rabi": "kr_19-20",
            "zaid": "krz_19-20",
        },
        "2020-2021": {
            "total_area": "area_20-21",
            "kharif": "k_20-21",
            "rabi": "kr_20-21",
            "zaid": "krz_20-21",
        },
        "2021-2022": {
            "total_area": "area_21-22",
            "kharif": "k_21-22",
            "rabi": "kr_21-22",
            "zaid": "krz_21-22",
        },
        "2022-2023": {
            "total_area": "area_22-23",
            "kharif": "k_22-23",
            "rabi": "kr_22-23",
            "zaid": "krz_22-23",
        },
    }

    data = []
    for year, season_columns in column_mapping.items():
        total_area = (
            features[0]["properties"].get(season_columns["total_area"], "N/A")
            if season_columns["total_area"] in columns
            else "N/A"
        )

        kharif_value = (
            features[0]["properties"].get(season_columns["kharif"], "N/A")
            if season_columns["kharif"] in columns
            else "N/A"
        )
        rabi_value = (
            features[0]["properties"].get(season_columns["rabi"], "N/A")
            if season_columns["rabi"] in columns
            else "N/A"
        )
        zaid_value = (
            features[0]["properties"].get(season_columns["zaid"], "N/A")
            if season_columns["zaid"] in columns
            else "N/A"
        )

        total_area = f"{float(total_area/10000):.2f}" if total_area != "N/A" else "N/A"

        kharif_value = f"{float(kharif_value):.2f}" if kharif_value != "N/A" else "N/A"
        rabi_value = f"{float(rabi_value):.2f}" if rabi_value != "N/A" else "N/A"
        zaid_value = f"{float(zaid_value):.2f}" if zaid_value != "N/A" else "N/A"

        data.append([year, total_area, kharif_value, rabi_value, zaid_value])

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"

    header_row = table.rows[0]
    for i, header in enumerate(headers):
        header_row.cells[i].text = header
        header_row.cells[i].paragraphs[0].runs[0].bold = True

    for row_data in data:
        row = table.add_row()
        for i, cell_data in enumerate(row_data):
            row.cells[i].text = str(cell_data)


# MARK: - Section F
def add_section_f(doc, plan, mws):
    doc.add_heading("Section F: Proposed New NRM works on basis through Gram Sabha")
    para = doc.add_paragraph()
    para.add_run(
        "This section outlines the details of proposed new NRM works based on community inputs put up to the Gram Sabha.\n\n\n\n"
    )

    create_nrm_works_table(doc, plan, mws)


def create_nrm_works_table(doc, plan, mws):
    recharge_st_in_plan = ODK_groundwater.objects.filter(plan_id=plan.plan_id)
    irrigation_works_in_plan = ODK_agri.objects.filter(plan_id=plan.plan_id)
    settlement_resources_in_plan = ODK_settlement.objects.filter(plan_id=plan.plan_id)
    well_resources_in_plan = ODK_well.objects.filter(plan_id=plan.plan_id)
    waterbody_resources_in_plan = ODK_waterbody.objects.filter(plan_id=plan.plan_id)

    mws_uids = list(set([feature["properties"]["uid"] for feature in mws["features"]]))

    mws_polygons = {}
    for feature in mws["features"]:
        uid = feature["properties"]["uid"]
        polygon_coords = feature["geometry"]["coordinates"][0]
        polygon = shape({"type": "Polygon", "coordinates": polygon_coords})
        mws_polygons[uid] = polygon

    # Pre-process all resources to find their MWS assignments
    resource_mws_assignments = {}

    logger.info("\nAssigning resources to closest MWS:")
    for settlement in settlement_resources_in_plan:
        coords = [settlement.longitude, settlement.latitude]
        mws_uid = find_closest_mws(coords, mws_polygons)
        logger.info(
            f"Settlement at ({settlement.latitude}, {settlement.longitude}) assigned to MWS {mws_uid}"
        )
        if mws_uid not in resource_mws_assignments:
            resource_mws_assignments[mws_uid] = {
                "settlement": [],
                "well": [],
                "waterbody": [],
            }
        resource_mws_assignments[mws_uid]["settlement"].append(settlement)

    for well in well_resources_in_plan:
        coords = [well.longitude, well.latitude]
        mws_uid = find_closest_mws(coords, mws_polygons)
        logger.info(
            f"Well at ({well.latitude}, {well.longitude}) assigned to MWS {mws_uid}"
        )
        if mws_uid not in resource_mws_assignments:
            resource_mws_assignments[mws_uid] = {
                "settlement": [],
                "well": [],
                "waterbody": [],
            }
        resource_mws_assignments[mws_uid]["well"].append(well)

    for waterbody in waterbody_resources_in_plan:
        coords = [waterbody.longitude, waterbody.latitude]
        mws_uid = find_closest_mws(coords, mws_polygons)
        logger.info(
            f"Waterbody at ({waterbody.latitude}, {waterbody.longitude}) assigned to MWS {mws_uid}"
        )
        if mws_uid not in resource_mws_assignments:
            resource_mws_assignments[mws_uid] = {
                "settlement": [],
                "well": [],
                "waterbody": [],
            }
        resource_mws_assignments[mws_uid]["waterbody"].append(waterbody)

    for uid in mws_uids:
        polygon = mws_polygons[uid]
        mws_filtered = {
            "type": "FeatureCollection",
            "features": [
                feature
                for feature in mws["features"]
                if feature["properties"]["uid"] == uid
            ],
        }

        # Get resources for this MWS
        resources = resource_mws_assignments.get(
            uid, {"settlement": [], "well": [], "waterbody": []}
        )

        # Process works for this specific MWS
        recharge_works = [
            structure
            for structure in recharge_st_in_plan
            if find_closest_mws([structure.longitude, structure.latitude], mws_polygons)
            == uid
        ]

        irrigation_works = [
            work
            for work in irrigation_works_in_plan
            if find_closest_mws([work.longitude, work.latitude], mws_polygons) == uid
        ]

        # Only create table if there are works (not resources) present
        if len(recharge_works) > 0 or len(irrigation_works) > 0:
            doc.add_heading(f"MWS UID: {uid}", level=2)
            headers = [
                "S.No",
                "Work Category : Irrigation work or Recharge Structure",
                "Name of Beneficiary's Settlement",
                "Beneficiary Name",
                "Type of work",
                "Work ID",
                "Latitude",
                "Longitude",
                "Work Dimensions",
            ]
            table = doc.add_table(
                rows=1 + len(recharge_works) + len(irrigation_works), cols=len(headers)
            )
            table.style = "Table Grid"

            for i, header in enumerate(headers):
                table.cell(0, i).text = header
                table.cell(0, i).paragraphs[0].runs[0].font.bold = True

            # Add rows for recharge structures
            for i, structure in enumerate(recharge_works, start=1):
                row_cells = table.rows[i].cells
                row_cells[0].text = str(i)  # S.No
                row_cells[1].text = "Recharge Structure"  # Work Category
                row_cells[
                    2
                ].text = (
                    structure.beneficiary_settlement
                )
                row_cells[3].text = structure.data_groundwater.get(
                    "Beneficiary_Name"
                ) or "No Data Provided"
                row_cells[4].text = structure.work_type
                row_cells[5].text = structure.recharge_structure_id
                row_cells[6].text = str(structure.latitude)
                row_cells[7].text = str(structure.longitude)
                row_cells[8].text = format_work_dimensions(
                    structure.work_dimensions, structure.work_type.lower()
                )

            # Add rows for irrigation works
            offset = len(recharge_works) + 1
            for i, work in enumerate(irrigation_works, start=offset):
                row_cells = table.rows[i].cells
                row_cells[0].text = str(i)  # S.No
                row_cells[1].text = "Irrigation Work"  
                row_cells[
                    2
                ].text = work.beneficiary_settlement  
                row_cells[3].text = (
                    work.data_agri.get("Beneficiary_Name") or "No Data Provided"
                )  

                if (
                    work.work_type.lower() == "other"
                    and work.data_agri
                    and "TYPE_OF_WORK_ID_other" in work.data_agri
                ):
                    custom_work_type = work.data_agri.get("TYPE_OF_WORK_ID_other")
                    row_cells[4].text = (
                        str(custom_work_type)
                        if custom_work_type is not None
                        else "Other (unspecified)"
                    )
                else:
                    row_cells[4].text = work.work_type

                row_cells[5].text = work.irrigation_work_id  # Work ID
                row_cells[6].text = str(work.latitude)  # Latitude
                row_cells[7].text = str(work.longitude)  # Longitude
                row_cells[8].text = format_work_dimensions(
                    work.work_dimensions, work.work_type.lower()
                )  # Work Dimension

        # Always show map if there are any resources or works
        has_resources = any(len(r) > 0 for r in resources.values())
        if has_resources or len(recharge_works) > 0 or len(irrigation_works) > 0:
            if len(recharge_works) == 0 and len(irrigation_works) == 0:
                doc.add_heading(f"MWS UID: {uid}", level=2)
            show_marked_works(doc, plan, uid, mws_filtered, polygon, resources)
            doc.add_page_break()

    show_all_mws(doc, plan, mws)


def format_work_dimensions(work_dimensions, work_type):
    dimensions_str = ""
    if work_type in work_dimensions:
        dimensions = work_dimensions[work_type]
        for key, value in dimensions.items():
            if value is not None:
                dimensions_str += f"{key}: {value}, "
    return dimensions_str.rstrip(", ")


# TODO: fix the marked works selenium webdriver issue
def show_marked_works(doc, plan, uid, mws_filtered, polygon, resources):
    logger.info(f"\nDEBUG: Starting show_marked_works for MWS: {uid}")
    logger.info(f"DEBUG: Polygon bounds: {polygon.bounds}")

    layers = {
        "settlement": {
            "workspace": "resources",
            "layer_name": f"settlement_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png",
            "label_key": "Settleme_1",
            "legend_name": "Settlement (Resource)",
        },
        "well": {
            "workspace": "resources",
            "layer_name": f"well_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png",
            "label_key": "well_id",
            "legend_name": "Well (Resource)",
        },
        "waterbody": {
            "workspace": "resources",
            "layer_name": f"waterbody_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-violet.png",
            "label_key": "wbs_type",
            "legend_name": "Water Structure (Resource)",
        },
        "recharge": {
            "workspace": "works",
            "layer_name": f"plan_gw_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png",
            "label_key": "work_type",
            "legend_name": "Recharge Structure (Proposed Work)",
        },
        "irrigation": {
            "workspace": "works",
            "layer_name": f"plan_agri_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-orange.png",
            "label_key": "work_type",
            "legend_name": "Irrigation Work (Proposed Work)",
        },
    }

    centroid = polygon.centroid
    map_center = [centroid.y, centroid.x]
    fol_map = folium.Map(location=map_center, zoom_start=14)

    folium.GeoJson(
        mws_filtered,
        name="MWS boundary",
        style_function=lambda feature: {
            "fillColor": "blue",
            "color": "black",
            "weight": 2,
            "fillOpacity": 0.3,
        },
    ).add_to(fol_map)

    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px; height: 180px; 
    border:2px solid grey; z-index:9999; font-size:14px; background-color:white;
    ">&nbsp;<b>Legend:</b><br>
    """

    has_features = False

    for key in ["settlement", "well", "waterbody"]:
        if resources[key]:
            has_features = True
            feature_group = folium.FeatureGroup(name=layers[key]["legend_name"])

            for resource in resources[key]:
                icon = folium.features.CustomIcon(
                    layers[key]["icon_url"], icon_size=(25, 41)
                )
                coords = [resource.latitude, resource.longitude]
                folium.Marker(
                    location=coords,
                    icon=icon,
                    popup=getattr(resource, layers[key]["label_key"], "N/A"),
                ).add_to(feature_group)

            feature_group.add_to(fol_map)
            legend_html += f"""
            <img src="{layers[key]["icon_url"]}" alt="{layers[key]["legend_name"]}" width="15" height="25">
            {layers[key]["legend_name"]}<br>
            """

    for key in ["recharge", "irrigation"]:
        try:
            layer = get_vector_layer_geoserver(
                geoserver_url=GEOSERVER_URL,
                workspace=layers[key]["workspace"],
                layer_name=layers[key]["layer_name"],
            )
            if layer is not None:
                features = [
                    feature
                    for feature in layer["features"]
                    if shape(feature["geometry"]).within(polygon)
                ]
                if features:
                    has_features = True
                    feature_group = folium.FeatureGroup(name=layers[key]["legend_name"])
                    for feature in features:
                        icon = folium.features.CustomIcon(
                            layers[key]["icon_url"], icon_size=(25, 41)
                        )
                        label_text = feature["properties"].get(
                            layers[key]["label_key"], "N/A"
                        )
                        folium.Marker(
                            location=[
                                feature["geometry"]["coordinates"][1],
                                feature["geometry"]["coordinates"][0],
                            ],
                            icon=icon,
                            popup=label_text,
                        ).add_to(feature_group)
                    feature_group.add_to(fol_map)

                    # Add to legend
                    legend_html += f"""
                    <img src="{layers[key]["icon_url"]}" alt="{layers[key]["legend_name"]}" width="15" height="25">
                    {layers[key]["legend_name"]}<br>
                    """

        except Exception as e:
            logger.error(f"DEBUG: Error processing {key} layer: {str(e)}")
            continue

    if not has_features:
        logger.info(f"DEBUG: No features found for MWS: {uid}")
        return

    logger.info("DEBUG: Features were found, completing map creation")

    legend_html += "</div>"
    fol_map.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl().add_to(fol_map)

    with tempfile.TemporaryDirectory() as temp_dir:
        map_filename = os.path.join(temp_dir, f"marked_works_{uid}.html")
        fol_map.save(map_filename)
        img_data = fol_map._to_png(5)
        img = Image.open(BytesIO(img_data))
        img_filename = os.path.join(temp_dir, f"marked_works_{uid}.png")
        img.save(img_filename)

        doc.add_picture(img_filename, width=Inches(6))
def show_all_mws(doc, plan, mws):
    """
    Creates a map showing all MWS polygons with resources and proposed works,
    regardless of intersections.

    Args:
        doc: Document object to add the map to
        plan: Plan object containing plan details
        mws: GeoJSON object containing MWS features
    """

    layers = {
        "settlement": {
            "workspace": "resources",
            "layer_name": f"settlement_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png",
            "label_key": "Settleme_1",
            "legend_name": "Settlement (Resource)",
        },
        "well": {
            "workspace": "resources",
            "layer_name": f"well_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png",
            "label_key": "well_id",
            "legend_name": "Well (Resource)",
        },
        "waterbody": {
            "workspace": "resources",
            "layer_name": f"waterbody_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-violet.png",
            "label_key": "wbs_type",
            "legend_name": "Water Structure (Resource)",
        },
        "recharge": {
            "workspace": "works",
            "layer_name": f"plan_gw_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png",
            "label_key": "work_type",
            "legend_name": "Recharge Structure (Proposed Work)",
        },
        "irrigation": {
            "workspace": "works",
            "layer_name": f"plan_agri_{plan.plan_id}_{plan.district.district_name.lower()}_{plan.block.block_name.lower()}",
            "icon_url": "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-orange.png",
            "label_key": "work_type",
            "legend_name": "Irrigation Work (Proposed Work)",
        },
    }

    all_polygons = [shape(feature["geometry"]) for feature in mws["features"]]
    combined_polygon = unary_union(all_polygons)
    map_center = [combined_polygon.centroid.y, combined_polygon.centroid.x]

    fol_map = folium.Map(location=map_center, zoom_start=11)

    for feature in mws["features"]:
        uid = feature["properties"]["uid"]
        folium.GeoJson(
            {"type": "FeatureCollection", "features": [feature]},
            name=f"MWS {uid}",
            style_function=lambda x: {
                "fillColor": "blue",
                "color": "black",
                "weight": 2,
                "fillOpacity": 0.3,
            },
            popup=folium.Popup(f"MWS UID: {uid}", max_width=300),
        ).add_to(fol_map)

    features = {}
    for key, layer_info in layers.items():
        try:
            layer = get_vector_layer_geoserver(
                geoserver_url=GEOSERVER_URL,
                workspace=layer_info["workspace"],
                layer_name=layer_info["layer_name"],
            )
            features[key] = layer["features"] if layer is not None else []
        except Exception as e:
            logger.error(f"Error retrieving {key} layer: {str(e)}")
            features[key] = []

    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px; 
    border:2px solid grey; z-index:9999; font-size:14px; background-color:white;
    ">&nbsp;<b>Legend:</b><br>
    """

    for key, feature_list in features.items():
        if len(feature_list) > 0:
            feature_group = folium.FeatureGroup(name=layers[key]["legend_name"])
            for feature in feature_list:
                icon = folium.features.CustomIcon(
                    layers[key]["icon_url"], icon_size=(25, 41)
                )
                label_text = feature["properties"].get(layers[key]["label_key"], "N/A")
                folium.Marker(
                    location=[
                        feature["geometry"]["coordinates"][1],
                        feature["geometry"]["coordinates"][0],
                    ],
                    icon=icon,
                    popup=label_text,
                ).add_to(feature_group)
            feature_group.add_to(fol_map)

            legend_html += f"""
            <img src="{layers[key]["icon_url"]}" alt="{layers[key]["legend_name"]}" width="15" height="25">
            {layers[key]["legend_name"]}<br>
            """

    legend_html += """
    <div style="background-color:blue;opacity:0.3;border:1px solid black;width:15px;height:15px;display:inline-block;"></div>
    MWS Boundary<br>
    </div>
    """

    fol_map.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl().add_to(fol_map)

    with tempfile.TemporaryDirectory() as temp_dir:
        map_filename = os.path.join(temp_dir, "all_mws_map.html")
        fol_map.save(map_filename)

        img_data = fol_map._to_png(5)
        img = Image.open(BytesIO(img_data))
        img_filename = os.path.join(temp_dir, "all_mws_map.png")
        img.save(img_filename)

        doc.add_heading("Overview Map of All MWS", level=1)
        doc.add_picture(img_filename, width=Inches(6))
        doc.add_page_break()

# MARK: - Section G

def add_section_g(doc, plan, mws):
    doc.add_heading("Section G: Propose New Livelihood Works", level=1)

    livelihood_records = ODK_livelihood.objects.filter(plan_id=plan.plan_id)
    
    # Table for Livestock and Fisheries
    doc.add_heading("G.1 Livestock and Fisheries", level=2)
    headers = [
        "Livelihood Works",
        "Name of Beneficiary Settlement",
        "Name of Beneficiary",
        "Beneficiary Father's Name",
        "Type of Work Demand",
        "Latitude",
        "Longitude",
    ]
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    hdr_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for record in livelihood_records:
        # Handle Livestock category
        if record.data_livelihood.get("select_one_demand_promoting_livestock") == "Yes":
            row_cells = table.add_row().cells
            row_cells[0].text = "Livestock"
            row_cells[1].text = record.beneficiary_settlement or "No Data Provided"
            row_cells[2].text = record.data_livelihood.get(
                "beneficiary_name", "No Data Provided"
            )
            row_cells[3].text = "No Data"  # Father's name not specified

            # Get livestock demand type
            livestock_demand = record.data_livelihood.get(
                "select_one_promoting_livestock"
            )
            if livestock_demand == "other":
                livestock_demand = record.data_livelihood.get(
                    "select_one_promoting_livestock_other", "No Data Provided"
                )
            row_cells[4].text = format_text(livestock_demand) or "No Data Provided"

            row_cells[5].text = (
                "{:.2f}".format(record.latitude) if record.latitude else "No Data Provided"
            )
            row_cells[6].text = (
                "{:.2f}".format(record.longitude) if record.longitude else "No Data Provided"
            )

        # Handle Fisheries category
        if record.data_livelihood.get("select_one_demand_promoting_fisheries") == "Yes":
            row_cells = table.add_row().cells
            row_cells[0].text = "Fisheries"
            row_cells[1].text = record.beneficiary_settlement or "No Data Provided"
            row_cells[2].text = record.data_livelihood.get(
                "beneficiary_name", "No Data Provided"
            )
            row_cells[3].text = "No Data"  # Father's name not specified

            # Get fisheries demand type
            fisheries_demand = record.data_livelihood.get(
                "select_one_promoting_fisheries"
            )
            if fisheries_demand == "other":
                fisheries_demand = record.data_livelihood.get(
                    "select_one_promoting_fisheries_other", "No Data Provided"
                )
            row_cells[4].text = format_text(fisheries_demand) or "No Data Provided"

            row_cells[5].text = (
                "{:.2f}".format(record.latitude) if record.latitude else "No Data Provided"
            )
            row_cells[6].text = (
                "{:.2f}".format(record.longitude) if record.longitude else "No Data Provided"
            )

    # Table for Plantation
    doc.add_heading("G.2 Plantations", level=2)
    plantation_headers = [
        "Livelihood Works",
        "Name of Beneficiary Settlement",
        "Name of Beneficiary",
        "Beneficiary Father's Name",
        "Name of Plantation Crop",
        "Total Acres",
        "Latitude",
        "Longitude",
    ]
    plantation_table = doc.add_table(rows=1, cols=len(plantation_headers))
    plantation_table.style = "Table Grid"
    plantation_hdr_cells = plantation_table.rows[0].cells
    for i, header in enumerate(plantation_headers):
        plantation_hdr_cells[i].paragraphs[0].add_run(header).bold = True

    for record in livelihood_records:
        # Handle Plantation category
        if record.data_livelihood.get("select_one_demand_plantation") == "Yes":
            row_cells = plantation_table.add_row().cells
            row_cells[0].text = "Plantations"
            row_cells[1].text = record.beneficiary_settlement or "No Data Provided"
            row_cells[2].text = record.data_livelihood.get(
                "beneficiary_name", "No Data Provided"
            )
            row_cells[3].text = "No Data"  # Father's name not specified

            # Get plantation type
            plantation_type = record.data_livelihood.get("Plantation")
            row_cells[4].text = plantation_type or "No Data Provided"

            # Get plantation area/crop
            plantation_area = record.data_livelihood.get("Plantation_crop")
            row_cells[5].text = plantation_area or "No Data Provided"

            row_cells[6].text = (
                "{:.2f}".format(record.latitude) if record.latitude else "No Data Provided"
            )
            row_cells[7].text = (
                "{:.2f}".format(record.longitude) if record.longitude else "No Data Provided"
            )


# MARK: - Section H
# TODO: Fix the sync between the settlements marked for maintenance in resource mapping that 
# they are also treated as first class citizens and added to the maintenance tables

def add_section_h(doc, plan, mws):
    populate_maintenance_from_waterbody(plan)

    doc.add_heading(
        "Section H: Proposed Maintenance Works on existing Assets on basis through Gram Sabha",
        level=1,
    )
    para = doc.add_paragraph()
    para.add_run(
        "This section presents information on proposed maintenance works for existing assets based on inputs from the Gram Sabha. For each maintenance work, include details such as the beneficiary settlement, work ID, type of work, latitude, and longitude."
    )
    para.add_run("\n\n")

    asset_types = [
        "Water Recharge Structures",
        "Irrigation Structures",
        "Surface Water Structures",
        "Remote Sensed Surface Water Structures",
    ]

    doc.add_heading("Maintenance Works by Asset Type", level=2)

    table = doc.add_table(rows=len(asset_types) + 1, cols=1)
    table.style = "Table Grid"

    header_cells = table.rows[0].cells
    header_cells[0].text = "Asset Type"
    header_cells[0].paragraphs[0].runs[0].bold = True

    for i, asset_type in enumerate(asset_types):
        row_cells = table.rows[i].cells
        row_cells[0].text = asset_type

        doc.add_heading(f"Maintenance Works for {asset_type}", level=3)

        if asset_type == "Water Recharge Structures":
            maintenance_gw_table(doc, plan, mws)
        elif asset_type == "Irrigation Structures":
            maintenance_agri_table(doc, plan, mws)
        elif asset_type == "Surface Water Structures":
            maintenance_waterstructures_table(doc, plan, mws)
        elif asset_type == "Remote Sensed Surface Water Structures":
            maintenance_rs_waterstructures_table(doc, plan, mws)

        doc.add_page_break()


def maintenance_gw_table(doc, plan, mws):
    headers = [
        "Name of the Beneficiary Settlement",
        "Beneficiary Name",
        "Work ID",
        "Corresponding Work ID",
        "Type of Recharge Structure",
        "Previous Maintenance Activity",
        "Latitude",
        "Longitude",
    ]

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    header_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        header_cells[i].text = header
        header_cells[i].paragraphs[0].runs[0].bold = True

    # Add data rows
    for maintenance in GW_maintenance.objects.filter(plan_id=plan.plan_id):
        row_cells = table.add_row().cells
        row_cells[0].text = maintenance.data_gw_maintenance.get(
            "beneficiary_settlement"
        ) or "No Data"
        row_cells[1].text = (
            maintenance.data_gw_maintenance.get("Beneficiary_Name") or "No Data"
        )
        row_cells[2].text = maintenance.work_id
        row_cells[3].text = maintenance.corresponding_work_id
        row_cells[4].text = (
            maintenance.data_gw_maintenance.get("select_one_water_structure")
            or "No Data"
        )
        row_cells[5].text = (
            maintenance.data_gw_maintenance.get("select_one_activities") or "No Data"
        )
        row_cells[6].text = str(maintenance.latitude)
        row_cells[7].text = str(maintenance.longitude)


def maintenance_agri_table(doc, plan, mws):
    headers = [
        "Name of the Beneficiary Settlement",
        "Beneficiary Name",
        "Work ID",
        "Corresponding Work ID",
        "Type of Irrigation Structure",
        "Previous Maintenance Activity",
        "Latitude",
        "Longitude",
    ]

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    header_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        header_cells[i].text = header
        header_cells[i].paragraphs[0].runs[0].bold = True

    # Add data rows
    for maintenance in Agri_maintenance.objects.filter(plan_id=plan.plan_id):
        row_cells = table.add_row().cells
        row_cells[0].text = maintenance.data_agri_maintenance.get(
            "beneficiary_settlement"
        )
        row_cells[1].text = (
            maintenance.data_agri_maintenance.get("Beneficiary_Name")
            or "No Data Provided"
        )
        row_cells[2].text = maintenance.work_id
        row_cells[3].text = maintenance.corresponding_work_id
        row_cells[4].text = maintenance.data_agri_maintenance.get(
            "select_one_irrigation_structure"
        ) or "No Data"
        row_cells[5].text = maintenance.data_agri_maintenance.get(
            "select_one_activities"
        ) or "No Data"
        row_cells[6].text = str(maintenance.latitude)
        row_cells[7].text = str(maintenance.longitude)


def maintenance_waterstructures_table(doc, plan, mws):
    headers = [
        "Name of the Beneficiary Settlement",
        "Beneficiary Name",
        "Work ID",
        "Corresponding Work ID",
        "Type of Work",
        "Latitude",
        "Longitude",
    ]

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    header_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        header_cells[i].text = header
        header_cells[i].paragraphs[0].runs[0].bold = True

    # Add data rows
    for maintenance in SWB_maintenance.objects.filter(plan_id=plan.plan_id):
        row_cells = table.add_row().cells
        row_cells[0].text = maintenance.data_swb_maintenance.get(
            "beneficiary_settlement"
        ) or "No Data"
        row_cells[1].text = maintenance.data_swb_maintenance.get("Beneficiary_Name") or "No Data"
        row_cells[2].text = maintenance.work_id
        row_cells[3].text = maintenance.corresponding_work_id
        row_cells[4].text = maintenance.data_swb_maintenance.get("TYPE_OF_WORK") or "No Data"
        row_cells[5].text = str(maintenance.latitude)
        row_cells[6].text = str(maintenance.longitude)


def maintenance_rs_waterstructures_table(doc, plan, mws):
    headers = [
        "Name of the Beneficiary Settlement",
        "Beneficiary Name",
        "Work ID",
        "Corresponding Work ID",
        "Type of Work",
        "Latitude",
        "Longitude",
    ]
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    header_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        header_cells[i].text = header
        header_cells[i].paragraphs[0].runs[0].bold = True

    # Add data rows
    for maintenance in SWB_RS_maintenance.objects.filter(plan_id=plan.plan_id):
        row_cells = table.add_row().cells
        row_cells[0].text = maintenance.data_swb_rs_maintenance.get(
            "beneficiary_settlement"
        ) or "No Data"
        row_cells[1].text = maintenance.data_swb_rs_maintenance.get("Beneficiary_Name") or "No Data"
        row_cells[2].text = maintenance.work_id
        row_cells[3].text = maintenance.corresponding_work_id
        row_cells[4].text = maintenance.data_swb_rs_maintenance.get("TYPE_OF_WORK") or "No Data"
        row_cells[5].text = str(maintenance.latitude) 
        row_cells[6].text = str(maintenance.longitude) 
