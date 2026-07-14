from datetime import date
from django.template.loader import render_to_string
from dpr.service.translation_service import load_translations

from weasyprint import HTML
from django.conf import settings
from pathlib import Path
from .gen_dpr import get_settlement_count_for_plan
from .utils import get_vector_layer_geoserver, transform_name
from nrm_app.settings import GEOSERVER_URL
from .get_dpr_sectionwise_data import (
    get_section_b_data,
    get_section_c_data,
    get_section_d_data,
    get_section_e_data,
    get_section_f_data,
    get_section_g_data,
)
from .service.form_download_service import sync_odk_forms

font_regular = (
    Path(settings.BASE_DIR)
    / "dpr"
    / "static"
    / "fonts"
    / "NotoSansDevanagari-Regular.ttf"
).as_uri()

font_bold = (
    Path(settings.BASE_DIR) / "dpr" / "static" / "fonts" / "NotoSansDevanagari-Bold.ttf"
).as_uri()


def generate_dpr_html(plan, language="en"):
    translations = load_translations(language)
    total_settlements = get_settlement_count_for_plan(plan.id)
    mws_fortnight = get_vector_layer_geoserver(
        geoserver_url=GEOSERVER_URL,
        workspace="mws_layers",
        layer_name="deltaG_fortnight_"
        + transform_name(str(plan.district_soi.district_name))
        + "_"
        + transform_name(str(plan.tehsil_soi.tehsil_name)),
    )
    section_b_data, settlement_mws_ids, mws_gdf = get_section_b_data(
        plan, total_settlements, mws_fortnight
    )
    section_c_data = get_section_c_data(plan, language)
    section_d_data = get_section_d_data(plan, settlement_mws_ids, mws_gdf, language)
    section_e_data = get_section_e_data(plan, language)
    section_f_data = get_section_f_data(plan, language)
    section_g_data = get_section_g_data(plan, language)
    html = render_to_string(
        "dpr/base.html",
        {
            "t": translations,
            "current_date": date.today().strftime("%B %d, %Y"),
            "section_a": plan,
            "section_b": section_b_data,
            "section_c": section_c_data,
            "section_d": section_d_data,
            "section_e": section_e_data,
            "section_f": section_f_data,
            "section_g": section_g_data,
            "footnote": f"DPR supported by {plan.organization.name} in {plan.created_at.year}",
            "font_regular": font_regular,
            "font_bold": font_bold,
        },
    )

    return html


def generate_dpr_pdf(plan, language="en"):
    sync_odk_forms()
    html = generate_dpr_html(plan, language)

    pdf = HTML(
        string=html,
        base_url=settings.BASE_DIR,
    ).write_pdf()

    return pdf
