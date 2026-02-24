import json
import re
import ssl
import socket
from io import BytesIO
import warnings
import time
from urllib.parse import urlparse
import pytz
import requests
from django.db.models import Max
from django.utils import timezone
from django.core.mail import EmailMessage
from django.core.mail.backends.smtp import EmailBackend
from docx import Document

from nrm_app.settings import EMAIL_HOST, EMAIL_HOST_PASSWORD, EMAIL_HOST_USER, EMAIL_PORT, EMAIL_TIMEOUT, EMAIL_USE_SSL, ODK_PASSWORD, ODK_USERNAME
from utilities.constants import (
    ODK_URL_AGRI_MAINTENANCE,
    ODK_URL_GW_MAINTENANCE,
    ODK_URL_RS_WATERBODY_MAINTENANCE,
    ODK_URL_WATERBODY_MAINTENANCE,
    ODK_URL_agri,
    ODK_URL_crop,
    ODK_URL_gw,
    ODK_URL_livelihood,
    ODK_URL_settlement,
    ODK_URL_waterbody,
    ODK_URL_well,
)
from utilities.logger import setup_logger

from .models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_agri,
    ODK_crop,
    ODK_groundwater,
    ODK_livelihood,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    SWB_maintenance,
    SWB_RS_maintenance,
)

import boto3
from nrm_app.settings import DPR_S3_ACCESS_KEY, DPR_S3_SECRET_KEY, DPR_S3_REGION, DPR_S3_BUCKET, DPR_S3_FOLDER
from botocore.exceptions import ClientError

warnings.filterwarnings("ignore")

logger = setup_logger(__name__)


def get_url(geoserver_url, workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{geoserver_url}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    return geojson_url


def get_vector_layer_geoserver(geoserver_url, workspace, layer_name):
    """Fetch vector layer data from GeoServer and return as GeoJSON."""
    url = get_url(geoserver_url, workspace, layer_name)
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Check if the response content is not empty and is valid JSON
        if response.content:
            return response.json()
        else:
            print(f"Empty response for layer '{layer_name}'.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the vector layer '{layer_name}' from GeoServer: {e}")
        print(f"Request URL: {url}")
        if response is not None:
            print(f"Response status code: {response.status_code}")
            # print(f"Response content: {response.text}")
        return None


def determine_caste_fields(record):
    """
    Determine caste group whether it's a Single Caste Group or Mixed Caste Group
    """
    count_sc = record.get("count_sc")
    count_st = record.get("count_st")
    count_obc = record.get("count_obc")
    count_general = record.get("count_general")

    caste_counts_mapping = {
        "SC": count_sc,
        "ST": count_st,
        "OBC": count_obc,
        "GENERAL": count_general,
    }

    valid_castes = []
    for caste, count in caste_counts_mapping.items():
        if count is not None and count != "":
            try:
                count_value = float(count) if isinstance(count, str) else count
                if count_value > 0:
                    valid_castes.append(caste)
            except (ValueError, TypeError):
                if count:
                    valid_castes.append(caste)

    if not valid_castes:
        return None, None, None, True

    if len(valid_castes) == 1:
        largest_caste = "Single Caste Group"
        smallest_caste = valid_castes[0]
        settlement_status = "NA"
        return largest_caste, smallest_caste, settlement_status, False

    else:
        largest_caste = "Mixed Caste Group"
        smallest_caste = "NA"
        settlement_status = ", ".join(sorted(valid_castes))
        return largest_caste, smallest_caste, settlement_status, False


def validate_email(emailid):
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if not re.match(email_regex, emailid):
        return False
    else:
        return True


def check_submission_time(record, model):
    submission_date = timezone.datetime.strptime(
        record.get("__system", {}).get("submissionDate", ""), "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    submission_date = timezone.make_aware(submission_date, pytz.UTC)
    latest_submission_time = model.objects.aggregate(Max("submission_time"))[
        "submission_time__max"
    ]

    if latest_submission_time and submission_date <= latest_submission_time:
        return False, True
    return True, False


def extract_coordinates(record):
    try:
        gps_point = record.get("GPS_point", {})
        if not gps_point:
            return None, None

        # Check for both possible key names
        maps_appearance = gps_point.get("point_mapsappearance") or gps_point.get(
            "point_mapappearance"
        )
        if not maps_appearance:
            return None, None

        coordinates = maps_appearance.get("coordinates", [])
        if len(coordinates) < 2:
            return None, None

        return coordinates[1], coordinates[0]  # latitude, longitude
    except (AttributeError, IndexError, TypeError):
        return None, None


def format_text_demands(text):
    """
    Helps in converting demands in proper text
    """
    if not text:
        return ""

    items = text.split()
    formatted_items = []

    for item in items:
        item_with_spaces = item.replace("_", " ")
        formatted_item = " ".join(
            word.capitalize() for word in item_with_spaces.split()
        )
        formatted_items.append(formatted_item)

    formatted_text = "\n".join(formatted_items)
    return formatted_text


def ensure_str(value):
    """Normalize a value that may be a list (from Kobo multi-select fields) into a string."""
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return value if value is not None else ""


def format_text(text):
    """
    Converts text with underscores to properly formatted text.
    Example: 'Delayed_payments_for_works' -> 'Delayed Payments For Works'
    """
    if not text:
        return ""

    text = ensure_str(text)
    formatted_text = text.replace("_", " ")
    return formatted_text.capitalize() + "\n\n"


def get_waterbody_repair_activities(data_waterbody, water_structure_type):
    """
    Extract repair activities based on water structure type from data_waterbody.
    Handles 'other' cases where the specific repair activity is in a separate field.

    Args:
        data_waterbody (dict): The nested waterbody data dictionary
        water_structure_type (str): The type of water structure

    Returns:
        str: The repair activities or "NA" if none found
    """
    if not data_waterbody or not water_structure_type:
        return "NA"

    structure_type_mapping = {
        "canal": "Repair_of_canal",
        "bunding": "Repair_of_bunding",
        "check dam": "Repair_of_check_dam",
        "farm bund": "Repair_of_farm_bund",
        "farm pond": "Repair_of_farm_ponds",
        "soakage pits": "Repair_of_soakage_pits",
        "recharge pits": "Repair_of_recharge_pits",
        "rock fill dam": "Repair_of_rock_fill_dam",
        "stone bunding": "Repair_of_stone_bunding",
        "community pond": "Repair_of_community_pond",
        "diversion drains": "Repair_of_diversion_drains",
        "large water body": "Repair_of_large_water_body",
        "model5 structure": "Repair_of_model5_structure",
        "percolation tank": "Repair_of_percolation_tank",
        "earthen gully plug": "Repair_of_earthen_gully_plug",
        "30-40 model structure": "Repair_of_30_40_model_structure",
        "loose boulder structure": "Repair_of_loose_boulder_structure",
        "trench cum bund network": "Repair_of_trench_cum_bund_network",
        "water absorption trenches": "Repair_of_Water_absorption_trenches",
        "drainage soakage channels": "Repair_of_drainage_soakage_channels",
        "staggered contour trenches": "Repair_of_Staggered_contour_trenches",
        "continuous contour trenches": "Repair_of_Continuous_contour_trenches",
    }

    structure_type_lower = water_structure_type.lower().strip()
    if structure_type_lower.startswith("other:"):
        repair_fields = [
            key for key in data_waterbody.keys() if key.startswith("Repair_of_")
        ]
        for field in repair_fields:
            if data_waterbody.get(field):
                repair_value = ensure_str(data_waterbody.get(field))
                other_field = field + "_other"
                if (
                    repair_value
                    and repair_value.lower() == "other"
                    and data_waterbody.get(other_field)
                ):
                    return f"Other: {data_waterbody.get(other_field)}"
                elif repair_value:
                    return repair_value.replace("_", " ").title()
        return "NA"

    repair_field = structure_type_mapping.get(structure_type_lower)

    if not repair_field:
        return "NA"

    repair_activity = ensure_str(data_waterbody.get(repair_field))

    if not repair_activity:
        return "NA"

    if repair_activity.lower() == "other":
        other_field = repair_field + "_other"
        other_value = data_waterbody.get(other_field)
        if other_value:
            return f"Other: {other_value}"
        else:
            return "Other"

    return repair_activity.replace("_", " ").title()


def sort_key(settlement):
    return (settlement == "NA", settlement.lower() if settlement != "NA" else "")


def transform_name(name):
    if not name:
        return name

    name = re.sub(r"[()]", "", name)
    name = re.sub(r"[-\s]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = re.sub(r"^_|_$", "", name)
    return name.lower()

def to_utf8(value):
    """Ensure value is a properly encoded UTF-8 string for Word document.
    
    Handles cases where UTF-8 text was incorrectly decoded as Latin-1,
    resulting in garbled characters like 'à²ªà²¾à²...' for Kannada/Hindi text.
    """
    if value is None:
        return "NA"
    if isinstance(value, list):
        value = " ".join(str(v) for v in value)
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.decode('latin-1')
    if not isinstance(value, str):
        value = str(value)
    try:
        return value.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return value


def send_dpr_email(
    email_id,
    plan_name,
    mws_reports,
    mws_Ids,
    resource_report,
    resource_report_url,
    dpr_s3_url,
    state_name="",
    district_name="",
    tehsil_name="",
):
    try:
        mws_table_html = ""
        if mws_reports and mws_Ids:
            mws_rows = "".join(
                f'<tr><td style="padding: 10px 16px; border-bottom: 1px solid #eee;">{mws_id}</td>'
                f'<td style="padding: 10px 16px; border-bottom: 1px solid #eee; text-align: center;">'
                f'<a href="{report_url}" style="color: #2563eb; text-decoration: none;">View Report</a></td></tr>'
                for mws_id, report_url in zip(mws_Ids, mws_reports)
            )
            mws_table_html = f"""
            <div style="margin: 24px 0;">
                <p style="font-weight: 600; color: #374151; margin-bottom: 12px;">MWS Reports</p>
                <table style="width: 100%; border-collapse: collapse; background: #f9fafb; border-radius: 8px; overflow: hidden;">
                    <thead>
                        <tr style="background: #f3f4f6;">
                            <th style="padding: 12px 16px; text-align: left; font-weight: 600; color: #374151;">MWS ID</th>
                            <th style="padding: 12px 16px; text-align: center; font-weight: 600; color: #374151;">Report</th>
                        </tr>
                    </thead>
                    <tbody>{mws_rows}</tbody>
                </table>
            </div>
            """

        email_body = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="margin: 0; padding: 0; background-color: #f3f4f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto; padding: 40px 20px;">
                <div style="background: #ffffff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden;">
                    <div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%); padding: 32px; text-align: center;">
                        <h1 style="color: #ffffff; margin: 0; font-size: 24px; font-weight: 600;">Detailed Project Report</h1>
                        <p style="color: #bfdbfe; margin: 12px 0 0 0; font-size: 16px; font-weight: 500;">{to_utf8(plan_name)}</p>
                        <p style="color: #93c5fd; margin: 8px 0 0 0; font-size: 13px;">{to_utf8(tehsil_name)} · {to_utf8(district_name)} · {to_utf8(state_name)}</p>
                    </div>
                    <div style="padding: 32px;">
                        <p style="color: #374151; font-size: 15px; line-height: 1.6; margin: 0 0 20px 0;">
                            Hi,<br><br>
                            Your Detailed Project Report for <strong>{to_utf8(plan_name)}</strong> is ready.
                        </p>
                        <div style="margin: 24px 0; padding: 16px; background: #eff6ff; border-radius: 8px; border-left: 4px solid #3b82f6; text-align: center;">
                            <p style="margin: 0 0 12px 0; color: #1e40af; font-weight: 600;">Download DPR Report</p>
                            <a href="{dpr_s3_url}" style="display: inline-block; background: #2563eb; color: #ffffff; padding: 12px 24px; border-radius: 6px; text-decoration: none; font-weight: 500;">Download DPR →</a>
                        </div>
                        {mws_table_html}
                        <div style="margin: 24px 0; padding: 16px; background: #f0fdf4; border-radius: 8px; border-left: 4px solid #22c55e;">
                            <p style="margin: 0; color: #166534; font-weight: 600;">Resource Report</p>
                            <a href="{resource_report_url}" style="color: #15803d; text-decoration: none; font-size: 14px;">View Report →</a>
                        </div>
                    </div>
                    <div style="background: #f9fafb; padding: 24px 32px; border-top: 1px solid #e5e7eb;">
                        <p style="margin: 0; color: #6b7280; font-size: 14px;">
                            Thanks and Regards,<br>
                            <strong style="color: #374151;">CoRE Stack Team</strong>
                        </p>
                    </div>
                </div>
                <p style="text-align: center; color: #9ca3af; font-size: 12px; margin-top: 24px;">
                    This is an automated email from CoRE Stack.
                </p>
            </div>
        </body>
        </html>
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

        email.content_subtype = "html"

        if resource_report is not None:
            email.attach(
                f"Resource Report_{plan_name}.pdf", resource_report, "application/pdf"
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


def upload_dpr_to_s3(doc, plan_id, plan_name): 
    doc_bytes = BytesIO()
    doc.save(doc_bytes)
    doc_bytes.seek(0)
    
    safe_plan_name = transform_name(plan_name)
    s3_key = f"{DPR_S3_FOLDER}/{plan_id}_{safe_plan_name}.docx"
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=DPR_S3_ACCESS_KEY,
        aws_secret_access_key=DPR_S3_SECRET_KEY,
        region_name=DPR_S3_REGION,
    )
    
    s3_client.upload_fileobj(
        doc_bytes,
        DPR_S3_BUCKET,
        s3_key,
        ExtraArgs={
            "ContentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "ContentDisposition": f'attachment; filename="DPR_{safe_plan_name}.docx"',
            "CacheControl": "no-cache, no-store, must-revalidate",
        }
    )
    
    ts = int(time.time())
    s3_url = f"https://{DPR_S3_BUCKET}.s3.{DPR_S3_REGION}.amazonaws.com/{s3_key}?v={ts}"
    logger.info(f"DPR uploaded to S3: {s3_url}")
    return s3_url


def _extract_s3_key(s3_url):
    
    parsed = urlparse(s3_url)
    return parsed.path.lstrip("/")


def check_dpr_exists_on_s3(s3_url):
    if not s3_url:
        return False
    
    try:
        s3_key = _extract_s3_key(s3_url)
    except (IndexError, AttributeError):
        return False
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=DPR_S3_ACCESS_KEY,
        aws_secret_access_key=DPR_S3_SECRET_KEY,
        region_name=DPR_S3_REGION,
    )
    
    try:
        s3_client.head_object(Bucket=DPR_S3_BUCKET, Key=s3_key)
        return True
    except ClientError:
        logger.warning(f"DPR not found on S3: {s3_url}")
        return False


def download_dpr_from_s3(s3_url):
    s3_key = _extract_s3_key(s3_url)
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=DPR_S3_ACCESS_KEY,
        aws_secret_access_key=DPR_S3_SECRET_KEY,
        region_name=DPR_S3_REGION,
    )
    
    doc_bytes = BytesIO()
    s3_client.download_fileobj(DPR_S3_BUCKET, s3_key, doc_bytes)
    doc_bytes.seek(0)
    
    doc = Document(doc_bytes)
    logger.info(f"DPR downloaded from S3: {s3_url}")
    return doc