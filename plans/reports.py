import csv
import io
from calendar import monthrange

from django.db.models import Count, Q
from django.utils import timezone


def _get_month_boundaries(report_date=None):
    if report_date is None:
        report_date = timezone.now()
    first_of_month = report_date.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    _, last_day = monthrange(report_date.year, report_date.month)
    end_of_month = report_date.replace(
        day=last_day, hour=23, minute=59, second=59, microsecond=999999
    )
    return first_of_month, end_of_month


def _base_queryset(cutoff):
    from .models import PlanApp

    return PlanApp.objects.filter(
        created_at__lte=cutoff,
        enabled=True,
    ).exclude(
        Q(plan__icontains="test")
        | Q(plan__icontains="demo")
        | Q(facilitator_name__icontains="demo")
    )


DETAILS_HEADER = [
    "plan_id", "plan_name", "village_name", "gram_panchayat",
    "facilitator_name", "created_at", "is_completed", "is_dpr_generated",
    "latitude", "longitude", "org_id", "org_name", "project_id",
    "project_name", "app_type", "state_soi_id", "state_soi_name",
    "district_soi_id", "district_soi_name", "tehsil_soi_id",
    "tehsil_soi_name", "created_by",
]


def generate_plan_details_csv(report_date=None):
    _, end_of_month = _get_month_boundaries(report_date)
    plans = (
        _base_queryset(end_of_month)
        .select_related(
            "organization", "project", "state_soi",
            "district_soi", "tehsil_soi", "created_by",
        )
        .order_by("-created_at")
        .distinct()
    )

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(DETAILS_HEADER)

    for p in plans.iterator():
        w.writerow([
            p.id,
            p.plan,
            p.village_name,
            p.gram_panchayat,
            p.facilitator_name,
            p.created_at.strftime("%Y-%m-%d %H:%M:%S") if p.created_at else "",
            p.is_completed,
            p.is_dpr_generated,
            p.latitude,
            p.longitude,
            p.organization_id,
            getattr(p.organization, "name", ""),
            p.project_id,
            getattr(p.project, "name", ""),
            getattr(p.project, "app_type", ""),
            p.state_soi_id,
            getattr(p.state_soi, "state_name", ""),
            p.district_soi_id,
            getattr(p.district_soi, "district_name", ""),
            p.tehsil_soi_id,
            getattr(p.tehsil_soi, "tehsil_name", ""),
            getattr(p.created_by, "username", ""),
        ])

    return buf.getvalue().encode("utf-8")


def generate_summary_csv(report_date=None):
    first_of_month, end_of_month = _get_month_boundaries(report_date)

    all_plans = _base_queryset(end_of_month)
    new_plans = all_plans.filter(created_at__gte=first_of_month)

    total_count = all_plans.count()
    new_count = new_plans.count()

    from organization.models import Organization

    org_ids = all_plans.values_list("organization_id", flat=True).distinct()
    orgs = Organization.objects.filter(id__in=org_ids).order_by("name")

    rows = []
    for org in orgs:
        org_all = all_plans.filter(organization=org)
        org_new = new_plans.filter(organization=org)

        projects = ", ".join(
            filter(None, org_all.values_list("project__name", flat=True).distinct())
        )

        _strip = {None, ""}
        all_facilitators = (
            set(org_all.values_list("facilitator_name", flat=True).distinct())
            - _strip
        )
        existing_facilitators = (
            set(
                org_all.filter(created_at__lt=first_of_month)
                .values_list("facilitator_name", flat=True)
                .distinct()
            )
            - _strip
        )
        new_facilitators = (
            set(org_new.values_list("facilitator_name", flat=True).distinct())
            - _strip
        ) - existing_facilitators

        rows.append([
            org.name,
            projects,
            len(all_facilitators),
            len(new_facilitators),
            org_all.count(),
            org_new.count(),
        ])

    buf = io.StringIO()
    w = csv.writer(buf)

    month_label = end_of_month.strftime("%B %Y")
    w.writerow(["Metric", "Value"])
    w.writerow(["Report Month", month_label])
    w.writerow(["Total Plans", total_count])
    w.writerow(["New Plans (This Month)", new_count])
    w.writerow([])
    w.writerow([
        "Organization", "Projects", "Total Facilitators",
        "New Facilitators", "Total Plans", "New Plans",
    ])
    for row in rows:
        w.writerow(row)

    return buf.getvalue().encode("utf-8")


def _tally_demand_types(demand_totals, raw_values):
    from dpr.mapping import classify_demand_type

    for raw in raw_values:
        demand_totals["total"] += 1
        classified = classify_demand_type(raw)
        if classified == "Community Demand":
            demand_totals["community"] += 1
            demand_totals["total_classified"] += 1
        elif classified == "Individual Demand":
            demand_totals["individual"] += 1
            demand_totals["total_classified"] += 1
        else:
            demand_totals["unclassified"] += 1


def generate_dpr_demand_overview_csv(report_date=None):
    """
    DPR review/approval activity and demand-type overview for the report
    month only (not cumulative). Each metric is scoped by its own source
    timestamp field:
      - plan flags        -> PlanApp.updated_at
      - DPR report status -> DPR_Report.last_updated_at
      - demand records    -> each ODK/maintenance model's submission_time

    Note: PlanApp.updated_at / DPR_Report.last_updated_at are bumped on ANY
    save, not just the specific transition being counted, so a record last
    touched this month for an unrelated reason will still show up here.
    """
    from dpr.models import (
        Agri_maintenance,
        DPR_Report,
        GW_maintenance,
        ODK_agri,
        ODK_agrohorticulture,
        ODK_groundwater,
        ODK_livelihood,
        SWB_maintenance,
        SWB_RS_maintenance,
    )
    from dpr.services import CFPT_ORG_ID

    first_of_month, end_of_month = _get_month_boundaries(report_date)
    month_window = (first_of_month, end_of_month)

    plans = _base_queryset(end_of_month)
    plan_id_strs = [str(pid) for pid in plans.values_list("id", flat=True)]

    plan_meta = plans.filter(updated_at__range=month_window).aggregate(
        completed=Count("id", filter=Q(is_completed=True)),
        dpr_reviewed=Count("id", filter=Q(is_dpr_reviewed=True)),
        dpr_approved=Count("id", filter=Q(is_dpr_approved=True)),
    )

    dpr_counts = (
        DPR_Report.objects.filter(plan_id__in=plans, last_updated_at__range=month_window)
        .exclude(plan_id__organization_id=CFPT_ORG_ID)
        .aggregate(
            submitted=Count(
                "dpr_report_id", filter=Q(status__in=["SUBMITTED", "APPROVED"])
            ),
            approved=Count("dpr_report_id", filter=Q(status="APPROVED")),
        )
    )

    demand_totals = {
        "community": 0,
        "individual": 0,
        "unclassified": 0,
        "total": 0,
        "total_classified": 0,
    }

    def in_month(qs):
        return qs.filter(submission_time__range=month_window)

    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type")
            for d in in_month(
                GW_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True)
            ).values_list("data_gw_maintenance", flat=True)
        ),
    )
    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type")
            for d in in_month(
                Agri_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True)
            ).values_list("data_agri_maintenance", flat=True)
        ),
    )
    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type")
            for d in in_month(
                SWB_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True)
            ).values_list("data_swb_maintenance", flat=True)
        ),
    )
    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type")
            for d in in_month(
                SWB_RS_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True)
            ).values_list("data_swb_rs_maintenance", flat=True)
        ),
    )
    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type")
            for d in in_month(
                ODK_groundwater.objects.filter(plan_id__in=plan_id_strs)
                .exclude(is_deleted=True)
                .exclude(status_re="rejected")
            ).values_list("data_groundwater", flat=True)
        ),
    )
    _tally_demand_types(
        demand_totals,
        (
            (d or {}).get("demand_type_irrigation")
            for d in in_month(
                ODK_agri.objects.filter(plan_id__in=plan_id_strs)
                .exclude(is_deleted=True)
                .exclude(status_re="rejected")
            ).values_list("data_agri", flat=True)
        ),
    )

    for _, dl in (
        in_month(
            ODK_livelihood.objects.filter(plan_id__in=plan_id_strs)
            .exclude(is_deleted=True)
            .exclude(status_re="rejected")
        ).values_list("submission_time", "data_livelihood")
    ):
        dl = dl or {}
        livestock = dl.get("Livestock") or {}
        fisheries = dl.get("fisheries") or {}
        plantations = dl.get("plantations") or {}
        kitchen_garden = dl.get("kitchen_gardens") or {}
        raws = []
        if (
            str(livestock.get("is_demand_livestock", "")).lower() == "yes"
            or str(dl.get("select_one_demand_promoting_livestock", "")).lower() == "yes"
        ):
            raws.append(livestock.get("livestock_demand"))
        if (
            str(fisheries.get("is_demand_fisheris", "")).lower() == "yes"
            or str(dl.get("select_one_demand_promoting_fisheries", "")).lower() == "yes"
        ):
            raws.append(fisheries.get("demand_type_fisheries"))
        if (
            str(dl.get("select_one_demand_plantation", "")).lower() == "yes"
            or str(plantations.get("select_plantation_demands", "")).lower() == "yes"
        ):
            raws.append(plantations.get("demand_type_plantations"))
        if (
            str(dl.get("indi_assets", "")).lower() == "yes"
            or str(kitchen_garden.get("assets_kg", "")).lower() == "yes"
        ):
            raws.append(kitchen_garden.get("demand_type_kitchen_garden"))
        _tally_demand_types(demand_totals, raws)

    # ODK_agrohorticulture has no submission_time field (removed in dpr
    # migration 0004), so its demand records can't be scoped to this month
    # and are intentionally excluded here — see the note row in the CSV.

    buf = io.StringIO()
    w = csv.writer(buf)
    month_label = end_of_month.strftime("%B %Y")

    w.writerow(["Report Month", month_label])
    w.writerow(
        [
            "Report Window",
            f"{first_of_month.strftime('%Y-%m-%d')} to {end_of_month.strftime('%Y-%m-%d')}",
        ]
    )
    w.writerow([])
    w.writerow(["DPR & Plan Status", "Count", "Source", "Timestamp Field"])
    w.writerow(["Plans Completed", plan_meta["completed"], "PlanApp.is_completed", "PlanApp.updated_at"])
    w.writerow(["Plans DPR Reviewed", plan_meta["dpr_reviewed"], "PlanApp.is_dpr_reviewed", "PlanApp.updated_at"])
    w.writerow(["Plans DPR Approved", plan_meta["dpr_approved"], "PlanApp.is_dpr_approved", "PlanApp.updated_at"])
    w.writerow(
        [
            "DPR Submitted (Submitted + Approved)",
            dpr_counts["submitted"] or 0,
            "DPR_Report.status",
            "DPR_Report.last_updated_at",
        ]
    )
    w.writerow(["DPR Approved", dpr_counts["approved"] or 0, "DPR_Report.status", "DPR_Report.last_updated_at"])
    w.writerow([])
    w.writerow(["Demand Overview", "Count"])
    w.writerow(["Community Demands", demand_totals["community"]])
    w.writerow(["Individual Demands", demand_totals["individual"]])
    w.writerow(["Unclassified Demands", demand_totals["unclassified"]])
    w.writerow(["Total Classified Demands", demand_totals["total_classified"]])
    w.writerow(["Total Demands", demand_totals["total"]])
    w.writerow([])
    w.writerow(
        [
            "Note",
            "Agrohorticulture demand records have no submission timestamp "
            "and are excluded from this month-scoped report.",
        ]
    )

    return buf.getvalue().encode("utf-8")


def generate_resource_demand_status_csv(report_date=None):
    """
    Per-type status counts for every resource and demand model (mirrors
    dpr.services RESOURCE_TYPE_MAP / DEMAND_TYPE_MAP, imported directly so
    this never drifts from the real status-tracking API), scoped to records
    submitted during the report month via each model's submission_time.
    """
    from dpr.services import DEMAND_TYPE_MAP, RESOURCE_TYPE_MAP

    first_of_month, end_of_month = _get_month_boundaries(report_date)
    month_window = (first_of_month, end_of_month)

    plans = _base_queryset(end_of_month)
    plan_id_strs = [str(pid) for pid in plans.values_list("id", flat=True)]

    def has_submission_time(model):
        return "submission_time" in {f.name for f in model._meta.get_fields()}

    def status_rows(type_map):
        rows = []
        for type_name, (model, pk_field, demand_field) in type_map.items():
            timestamped = has_submission_time(model)
            qs = model.objects.exclude(is_deleted=True).filter(plan_id__in=plan_id_strs)
            if timestamped:
                qs = qs.filter(submission_time__range=month_window)
            else:
                qs = qs.none()
            counts = dict(qs.values_list(demand_field).annotate(count=Count(pk_field)))
            rows.append(
                {
                    "type": type_name,
                    "pending": counts.get("PENDING", 0),
                    "submitted": counts.get("SUBMITTED", 0) + counts.get("APPROVED", 0),
                    "approved": counts.get("APPROVED", 0),
                    "reverted": counts.get("REVERTED", 0),
                    "rejected": counts.get("REJECTED", 0),
                    "total": sum(counts.values()),
                    "timestamped": timestamped,
                }
            )
        return rows

    buf = io.StringIO()
    w = csv.writer(buf)
    month_label = end_of_month.strftime("%B %Y")

    w.writerow(["Report Month", month_label])
    w.writerow(
        [
            "Report Window",
            f"{first_of_month.strftime('%Y-%m-%d')} to {end_of_month.strftime('%Y-%m-%d')}",
        ]
    )
    w.writerow([])
    w.writerow(
        [
            "Section",
            "Type",
            "Pending",
            "Submitted (incl. Approved)",
            "Approved",
            "Reverted",
            "Rejected",
            "Total",
            "Timestamp Available",
        ]
    )
    for row in status_rows(RESOURCE_TYPE_MAP):
        w.writerow(
            [
                "Resource",
                row["type"],
                row["pending"],
                row["submitted"],
                row["approved"],
                row["reverted"],
                row["rejected"],
                row["total"],
                row["timestamped"],
            ]
        )
    for row in status_rows(DEMAND_TYPE_MAP):
        w.writerow(
            [
                "Demand",
                row["type"],
                row["pending"],
                row["submitted"],
                row["approved"],
                row["reverted"],
                row["rejected"],
                row["total"],
                row["timestamped"],
            ]
        )

    return buf.getvalue().encode("utf-8")
