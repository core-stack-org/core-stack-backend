"""
Export DPR data to Excel.

One workbook is written per plan with one sheet per DPR sub-table; column
labels match the DPR docx exactly so internal reviewers see the same
headers (Type of demand, Name of the Beneficiary Settlement, Beneficiary
Name, Beneficiary's Father's Name, Type of <structure>, Repair Activities,
Latitude, Longitude, etc.).

Usage:
    python manage.py export_dpr_excel --org-name "Foundation for Ecological Security"
    python manage.py export_dpr_excel --org-id <uuid> --plan-id 12 --plan-id 34
    python manage.py export_dpr_excel --org-name "FES" --consolidated
"""

import re
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError

from dpr.services import (
    get_crops_data,
    get_livelihood_data,
    get_livestock_data,
    get_maintenance_data,
    get_nrm_works_data,
    get_settlements_data,
    get_team_details,
    get_village_brief,
    get_waterbodies_data,
    get_wells_data,
)
from organization.models import Organization
from plans.models import PlanApp


# ---------------------------------------------------------------------------
# Column maps: (DPR header, key in service dict). A tuple key descends into
# nested dicts (e.g. caste_counts.sc).
# ---------------------------------------------------------------------------

SOCIO_COLUMNS = [
    ("Name of the Settlement", "settlement_name"),
    ("Total Number of Households", "number_of_households"),
    ("Settlement Type", "settlement_type"),
    ("Caste Group", "caste_group_detail"),
    ("Total Households (SC)", ("caste_counts", "sc")),
    ("Total Households (ST)", ("caste_counts", "st")),
    ("Total Households (OBC)", ("caste_counts", "obc")),
    ("Total Households (General)", ("caste_counts", "general")),
    ("Total marginal farmers (<2 acres)", "marginal_farmers"),
]

NREGA_COLUMNS = [
    ("Settlement's Name", "settlement_name"),
    ("Total Households - applied", "nrega_job_applied"),
    ("Total Households - have NREGA job cards in previous year", "nrega_job_card"),
    ("Total work days in previous year", "nrega_work_days"),
    ("Work demands made in previous year", "nrega_past_work"),
    ("Were you involved in the village level planning?", "nrega_demand"),
    ("Issues", "nrega_issues"),
]

CROP_COLUMNS = [
    ("Name of the Settlement", "beneficiary_settlement"),
    ("Irrigation Source", "irrigation_source"),
    ("Crops grown in Kharif", "kharif_crops"),
    ("Kharif acreage (acres)", "kharif_acres"),
    ("Crops grown in Rabi", "rabi_crops"),
    ("Rabi acreage (acres)", "rabi_acres"),
    ("Crops grown in Zaid", "zaid_crops"),
    ("Zaid acreage (acres)", "zaid_acres"),
    ("Cropping Intensity", "cropping_intensity"),
    ("Land Classification", "land_classification"),
]

LIVESTOCK_COLUMNS = [
    ("Name of the Settlement", "settlement_name"),
    ("Goats", "goats"),
    ("Sheep", "sheep"),
    ("Cattle", "cattle"),
    ("Piggery", "piggery"),
    ("Poultry", "poultry"),
]

WELL_COLUMNS = [
    ("Name of Beneficiary Settlement", "beneficiary_settlement"),
    ("Type of Well", "well_type"),
    ("Who owns the Well", "owner"),
    ("Beneficiary Name", "beneficiary_name"),
    ("Beneficiary's Father's Name", "beneficiary_father_name"),
    ("Water Availability", "water_availability"),
    ("Households Benefitted", "households_benefitted"),
    ("Which Caste uses the well?", "caste_uses"),
    ("Well Usage", "well_usage"),
    ("Need Maintenance?", "need_maintenance"),
    ("Repair Activities", "repair_activities"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
]

WATERBODY_COLUMNS = [
    ("Name of the Beneficiary's Settlement", "beneficiary_settlement"),
    ("Who owns the water structure?", "owner"),
    ("Beneficiary Name", "beneficiary_name"),
    ("Beneficiary's Father's Name", "beneficiary_father_name"),
    ("Who manages?", "who_manages"),
    ("Which Caste uses the water structure?", "caste_who_uses"),
    ("Households Benefitted", "households_benefitted"),
    ("Type of Water Structure", "water_structure_type"),
    ("Usage of Water Structure", "usage"),
    ("Need Maintenance?", "need_maintenance"),
    ("Repair Activities", "repair_activities"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
]


def _maintenance_columns(structure_label):
    return [
        ("Type of demand", "demand_type"),
        ("Name of the Beneficiary Settlement", "beneficiary_settlement"),
        ("Beneficiary Name", "beneficiary_name"),
        ("Beneficiary's Father's Name", "beneficiary_father_name"),
        (structure_label, "structure_type"),
        ("Repair Activities", "repair_activities"),
        ("Latitude", "latitude"),
        ("Longitude", "longitude"),
    ]


NRM_COLUMNS = [
    ("Work Category : Irrigation work or Recharge Structure", "work_category"),
    ("Type of demand", "demand_type"),
    ("Work demand", "work_demand"),
    ("Name of Beneficiary's Settlement", "beneficiary_settlement"),
    ("Beneficiary's Name", "beneficiary_name"),
    ("Gender", "gender"),
    ("Beneficiary's Father's Name", "beneficiary_father_name"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
]

LF_COLUMNS = [
    ("Livelihood Works", "livelihood_work"),
    ("Type of Demand", "demand_type"),
    ("Work Demand", "work_demand"),
    ("Name of Beneficiary Settlement", "beneficiary_settlement"),
    ("Beneficiary's Name", "beneficiary_name"),
    ("Gender", "gender"),
    ("Beneficiary Father's Name", "beneficiary_father_name"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
]

PLANTATION_COLUMNS = [
    ("Livelihood Works", "livelihood_work"),
    ("Type of demand", "demand_type"),
    ("Name of Beneficiary Settlement", "beneficiary_settlement"),
    ("Name of Beneficiary", "beneficiary_name"),
    ("Gender", "gender"),
    ("Beneficiary's Father's Name", "beneficiary_father_name"),
    ("Name of Plantation Crop", "work_demand"),
    ("Total Acres", "total_acres"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FILENAME_INVALID = re.compile(r"[^A-Za-z0-9._-]+")
_KV_HEADERS = ("A_Team_Details", "B_Village_Brief")


def _pluck(record, key):
    if isinstance(key, tuple):
        cur = record
        for part in key:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur
    return record.get(key)


def _df(records, columns):
    headers = [h for h, _ in columns]
    rows = [{h: _pluck(r, k) for h, k in columns} for r in records]
    return pd.DataFrame(rows, columns=headers)


def _team_df(plan):
    info = get_team_details(plan)
    rows = [
        ("Organization", info["organization"]),
        ("Project", info["project"]),
        ("Plan", info["plan"]),
        ("Facilitator", info["facilitator"]),
        ("Process involved in the preparation of DPR PRA", info["process"]),
    ]
    return pd.DataFrame(rows, columns=["Field", "Value"])


def _village_df(plan):
    info = get_village_brief(plan)
    lat, lon = info.get("latitude"), info.get("longitude")
    latlon = f"{lat:.8f}, {lon:.8f}" if lat is not None and lon is not None else None
    rows = [
        ("Name of the Village", info["village_name"]),
        ("Name of the Gram Panchayat", info["gram_panchayat"]),
        ("Tehsil", info["tehsil"]),
        ("District", info["district"]),
        ("State", info["state"]),
        ("Number of Settlements in the Village", info["total_settlements"]),
        ("Latitude and Longitude of the Village", latlon),
    ]
    return pd.DataFrame(rows, columns=["Field", "Value"])


def _section_dfs(plan):
    """Return ordered (sheet_name, dataframe) pairs for a single plan."""
    pid = plan.id
    livelihood = get_livelihood_data(pid)
    lf = [r for r in livelihood if r.get("livelihood_work") in ("Livestock", "Fisheries")]
    plantations = [r for r in livelihood if r.get("livelihood_work") not in ("Livestock", "Fisheries")]
    settlements = get_settlements_data(pid)

    return [
        ("A_Team_Details", _team_df(plan)),
        ("B_Village_Brief", _village_df(plan)),
        ("C1_Socio_Economic", _df(settlements, SOCIO_COLUMNS)),
        ("C2_MGNREGA", _df(settlements, NREGA_COLUMNS)),
        ("C3_Cropping_Pattern", _df(get_crops_data(pid), CROP_COLUMNS)),
        ("C4_Livestock", _df(get_livestock_data(pid), LIVESTOCK_COLUMNS)),
        ("D1_Wells", _df(get_wells_data(pid), WELL_COLUMNS)),
        ("D2_Waterbodies", _df(get_waterbodies_data(pid), WATERBODY_COLUMNS)),
        ("E1_GW_Maintenance", _df(get_maintenance_data(pid, "gw"), _maintenance_columns("Type of Recharge Structure"))),
        ("E2_Agri_Maintenance", _df(get_maintenance_data(pid, "agri"), _maintenance_columns("Type of Irrigation Structure"))),
        ("E3_SWB_Maintenance", _df(get_maintenance_data(pid, "swb"), _maintenance_columns("Type of Work"))),
        ("E4_RS_SWB_Maintenance", _df(get_maintenance_data(pid, "swb_rs"), _maintenance_columns("Type of Work"))),
        ("F_NRM_Works", _df(get_nrm_works_data(pid), NRM_COLUMNS)),
        ("G1_Livestock_Fisheries", _df(lf, LF_COLUMNS)),
        ("G2_Plantations", _df(plantations, PLANTATION_COLUMNS)),
    ]


def _safe_name(value, fallback="unnamed"):
    cleaned = _FILENAME_INVALID.sub("_", str(value or "")).strip("_")
    return cleaned or fallback


def _write_workbook(path, sheets):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets:
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------

class Command(BaseCommand):
    help = "Export DPR data to Excel (one workbook per plan) for an organization."

    def add_arguments(self, parser):
        parser.add_argument("--org-id", help="Organization UUID")
        parser.add_argument("--org-name", help="Organization name (exact match)")
        parser.add_argument(
            "--plan-id",
            action="append",
            default=[],
            type=int,
            help="Limit to specific plan id(s); repeat the flag to pass multiple",
        )
        parser.add_argument(
            "--output-dir",
            default="dpr_exports",
            help="Output directory root (default: dpr_exports)",
        )
        parser.add_argument(
            "--include-disabled",
            action="store_true",
            help="Include plans where enabled=False (default: only enabled plans)",
        )
        parser.add_argument(
            "--consolidated",
            action="store_true",
            help="Also write one org-wide workbook with all plans appended",
        )

    def handle(self, *args, **opts):
        org = self._resolve_org(opts["org_id"], opts["org_name"])
        plans = list(self._resolve_plans(org, opts["plan_id"], opts["include_disabled"]))
        if not plans:
            raise CommandError("No plans matched the given filters.")

        out_root = Path(opts["output_dir"]) / _safe_name(org.name, str(org.id))
        out_root.mkdir(parents=True, exist_ok=True)

        consolidated = {} if opts["consolidated"] else None

        for plan in plans:
            try:
                sheets = _section_dfs(plan)
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"Plan {plan.id}: failed - {exc}"))
                continue

            file_path = out_root / f"{plan.id}_{_safe_name(plan.plan)}.xlsx"
            _write_workbook(file_path, sheets)
            self.stdout.write(self.style.SUCCESS(f"Wrote {file_path}"))

            if consolidated is not None:
                self._accumulate_consolidated(consolidated, plan, sheets)

        if consolidated:
            cons_path = out_root / f"_consolidated_{_safe_name(org.name)}.xlsx"
            merged = [(name, pd.concat(frames, ignore_index=True)) for name, frames in consolidated.items()]
            _write_workbook(cons_path, merged)
            self.stdout.write(self.style.SUCCESS(f"Wrote consolidated workbook: {cons_path}"))

    def _resolve_org(self, org_id, org_name):
        if not org_id and not org_name:
            raise CommandError("Provide --org-id or --org-name")
        try:
            if org_id:
                return Organization.objects.get(id=org_id)
            return Organization.objects.get(name=org_name)
        except Organization.DoesNotExist:
            raise CommandError(f"Organization not found: {org_id or org_name}")

    def _resolve_plans(self, org, plan_ids, include_disabled):
        qs = PlanApp.objects.filter(organization=org)
        if not include_disabled:
            qs = qs.filter(enabled=True)
        if plan_ids:
            qs = qs.filter(id__in=plan_ids)
        return qs.select_related(
            "organization", "project", "state_soi", "district_soi", "tehsil_soi"
        ).order_by("id")

    @staticmethod
    def _accumulate_consolidated(bucket, plan, sheets):
        for sheet_name, df in sheets:
            if sheet_name in _KV_HEADERS or df.empty:
                continue
            enriched = df.copy()
            enriched.insert(0, "Plan Name", plan.plan)
            enriched.insert(0, "Village", plan.village_name)
            enriched.insert(0, "Plan ID", plan.id)
            bucket.setdefault(sheet_name, []).append(enriched)
