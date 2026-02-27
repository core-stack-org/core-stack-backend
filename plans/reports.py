import csv
import io
from calendar import monthrange

from django.db.models import Q
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
