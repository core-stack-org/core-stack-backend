# plans/views.py
from django.db.models import Avg, Case, CharField as CharFieldOutput, Count, F, Max, Min, Q, Value, When
from django.db.models.functions import Concat, Length, Substr, Trim
from django.utils import timezone
from rest_framework import permissions, status, viewsets
from rest_framework.authentication import BaseAuthentication
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication

from geoadmin.models import UserAPIKey
from organization.models import Organization
from projects.models import AppType, Project
from users.models import User, UserProjectGroup

from .models import PlanApp
from .serializers import (
    PlanAppSerializer,
    PlanCreateSerializer,
    PlanUpdateSerializer,
)

from dpr.mapping import classify_demand_type
from dpr.models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_agri,
    ODK_groundwater,
    SWB_RS_maintenance,
    SWB_maintenance,
)

STATE_CENTROIDS = {
    "Jammu & Kashmir": {"lat": 34.0837, "lon": 74.7973},
    "Himachal Pradesh": {"lat": 31.1048, "lon": 77.1734},
    "Punjab": {"lat": 31.1471, "lon": 75.3412},
    "Uttarakhand": {"lat": 30.0668, "lon": 79.0193},
    "Haryana": {"lat": 29.0588, "lon": 76.0856},
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Uttar Pradesh": {"lat": 26.8467, "lon": 80.9462},
    "Rajasthan": {"lat": 26.4499, "lon": 74.6399},
    "Madhya Pradesh": {"lat": 22.9734, "lon": 78.6569},
    "Gujarat": {"lat": 22.2587, "lon": 71.1924},
    "Maharashtra": {"lat": 19.7515, "lon": 75.7139},
    "Karnataka": {"lat": 15.3173, "lon": 75.7139},
    "Goa": {"lat": 15.2993, "lon": 74.1240},
    "Ladakh": {"lat": 34.1526, "lon": 77.5771},
    "Kerala": {"lat": 10.8505, "lon": 76.2711},
    "Tamil Nadu": {"lat": 11.1271, "lon": 78.6569},
    "Andhra Pradesh": {"lat": 15.9129, "lon": 79.7400},
    "Odisha": {"lat": 20.9517, "lon": 85.0985},
    "West Bengal": {"lat": 22.9868, "lon": 87.8550},
    "Jharkhand": {"lat": 23.6102, "lon": 85.2799},
    "Bihar": {"lat": 25.0961, "lon": 85.3131},
    "Assam": {"lat": 26.2006, "lon": 92.9376},
    "Sikkim": {"lat": 27.5330, "lon": 88.5122},
    "Mizoram": {"lat": 23.1645, "lon": 92.9376},
    "Manipur": {"lat": 24.6637, "lon": 93.9063},
    "Meghalaya": {"lat": 25.4670, "lon": 91.3662},
    "Arunachal Pradesh": {"lat": 27.1004, "lon": 93.6166},
    "Nagaland": {"lat": 25.6751, "lon": 94.1086},
    "Tripura": {"lat": 23.7451, "lon": 91.7468},
    "Chhattisgarh": {"lat": 21.2787, "lon": 81.8661},
    "Lakshadweep": {"lat": 10.5667, "lon": 72.6417},
    "Puducherry": {"lat": 11.9416, "lon": 79.8083},
    "Chandigarh": {"lat": 30.7333, "lon": 76.7794},
    "Andaman & Nicobar": {"lat": 11.7401, "lon": 92.6586},
    "Telangana": {"lat": 17.1232, "lon": 79.2088},
    "Dadra & Nagar Haveli and Daman & Diu": {"lat": 20.2376, "lon": 73.0167},
}

TEST_FACILITATOR_EXCLUSIONS = (
    Q(facilitator_name__isnull=True)
    | Q(facilitator_name="")
    | Q(facilitator_name__icontains="test")
    | Q(facilitator_name__icontains="demo")
    | Q(facilitator_name__icontains="facilitator")
)


def _app_user_steward_names_qs():
    return (
        User.objects.filter(groups__name="App User")
        .exclude(organization__name__iexact="CFPT")
        .annotate(full_name=Concat("first_name", Value(" "), "last_name"))
        .values_list("full_name", flat=True)
    )


def _count_demand_types(plan_id_strs):
    from dpr.mapping import classify_demand_type
    from dpr.models import (
        Agri_maintenance,
        GW_maintenance,
        ODK_agri,
        ODK_groundwater,
        SWB_RS_maintenance,
        SWB_maintenance,
    )

    community = 0
    individual = 0

    def _tally(raw_values):
        nonlocal community, individual
        for raw in raw_values:
            classified = classify_demand_type(raw)
            if classified == "Community Demand":
                community += 1
            elif classified == "Individual Demand":
                individual += 1

    # Section E — maintenance models
    # Fetch full JSON dict and extract key in Python to avoid ->operator issues on text columns
    _tally(
        (d or {}).get("demand_type")
        for d in GW_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).values_list("data_gw_maintenance", flat=True)
    )
    _tally(
        (d or {}).get("demand_type")
        for d in Agri_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).values_list("data_agri_maintenance", flat=True)
    )
    _tally(
        (d or {}).get("demand_type")
        for d in SWB_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).values_list("data_swb_maintenance", flat=True)
    )
    _tally(
        (d or {}).get("demand_type")
        for d in SWB_RS_maintenance.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).values_list("data_swb_rs_maintenance", flat=True)
    )

    # Section F — NRM works models
    _tally(
        (d or {}).get("demand_type")
        for d in ODK_groundwater.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).exclude(status_re="rejected").values_list("data_groundwater", flat=True)
    )
    _tally(
        (d or {}).get("demand_type_irrigation")
        for d in ODK_agri.objects.filter(plan_id__in=plan_id_strs).exclude(is_deleted=True).exclude(status_re="rejected").values_list("data_agri", flat=True)
    )

    return {"community_demands": community, "individual_demands": individual}


def _build_steward_meta_stats(queryset):
    per_steward = queryset.values("facilitator_name").annotate(
        plan_count=Count("id"),
        completed_count=Count("id", filter=Q(is_completed=True)),
        in_progress_count=Count("id", filter=Q(is_completed=False)),
    )

    # Single query: total stewards + active stewards + per-steward plan stats
    combined = per_steward.aggregate(
        total_stewards=Count("facilitator_name"),
        active_stewards=Count("facilitator_name", filter=Q(in_progress_count__gt=0)),
        avg_plans=Avg("plan_count"),
        min_plans=Min("plan_count"),
        max_plans=Max("plan_count"),
        avg_completion=Avg(
            Case(
                When(plan_count__gt=0, then=F("completed_count") * 100.0 / F("plan_count")),
                default=Value(0.0),
            )
        ),
    )

    dpr_agg = queryset.aggregate(
        total_dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
        total_dpr_reviewed=Count("id", filter=Q(is_dpr_reviewed=True)),
        pending_dpr_generation=Count(
            "id", filter=Q(is_completed=True, is_dpr_generated=False)
        ),
        pending_dpr_review=Count(
            "id", filter=Q(is_dpr_generated=True, is_dpr_reviewed=False)
        ),
    )

    by_organization = [
        {
            "organization_id": s["organization"],
            "organization_name": s["organization__name"],
            "steward_count": s["steward_count"],
        }
        for s in (
            queryset.values("organization", "organization__name")
            .annotate(steward_count=Count("facilitator_name", distinct=True))
            .order_by("-steward_count")
        )
    ]

    state_level = [
        {
            "state_id": s["state_soi"],
            "state_name": s["state_soi__state_name"],
            "steward_count": s["steward_count"],
        }
        for s in (
            queryset.filter(state_soi__isnull=False)
            .values("state_soi", "state_soi__state_name")
            .annotate(steward_count=Count("facilitator_name", distinct=True))
            .order_by("-steward_count")
        )
    ]

    district_level = [
        {
            "district_id": s["district_soi"],
            "district_name": s["district_soi__district_name"],
            "state_name": s["state_soi__state_name"],
            "steward_count": s["steward_count"],
        }
        for s in (
            queryset.filter(district_soi__isnull=False)
            .values("district_soi", "district_soi__district_name", "state_soi__state_name")
            .annotate(steward_count=Count("facilitator_name", distinct=True))
            .order_by("-steward_count")
        )
    ]

    tehsil_level = [
        {
            "tehsil_id": s["tehsil_soi"],
            "tehsil_name": s["tehsil_soi__tehsil_name"],
            "district_name": s["district_soi__district_name"],
            "steward_count": s["steward_count"],
        }
        for s in (
            queryset.filter(tehsil_soi__isnull=False)
            .values(
                "tehsil_soi",
                "tehsil_soi__tehsil_name",
                "district_soi__district_name",
            )
            .annotate(steward_count=Count("facilitator_name", distinct=True))
            .order_by("-steward_count")
        )
    ]

    total_stewards = combined["total_stewards"] or 0
    active_stewards = combined["active_stewards"] or 0

    return {
        "total_stewards": total_stewards,
        "plans_per_steward": {
            "avg": round(combined["avg_plans"] or 0, 2),
            "min": combined["min_plans"] or 0,
            "max": combined["max_plans"] or 0,
        },
        "avg_completion_rate": round(combined["avg_completion"] or 0, 2),
        "dpr_stats": dpr_agg,
        "active_stewards": active_stewards,
        "inactive_stewards": total_stewards - active_stewards,
        "by_organization": by_organization,
        "state_level": state_level,
        "district_level": district_level,
        "tehsil_level": tehsil_level,
    }


def _build_steward_listing(queryset):
    effective_village = Case(
        When(
            ~Q(village_name="") & Q(village_name__isnull=False),
            then=Trim(F("village_name")),
        ),
        When(
            plan__startswith="Plan ",
            then=Trim(Substr("plan", 6, Length("plan") - Value(5))),
        ),
        default=Trim(F("plan")),
        output_field=CharFieldOutput(max_length=255),
    )
    qs = queryset.annotate(effective_village=effective_village)

    per_steward = (
        qs.values("facilitator_name")
        .annotate(
            plan_count=Count("id"),
            completed_count=Count("id", filter=Q(is_completed=True)),
        )
        .order_by("facilitator_name")
    )

    steward_names = [s["facilitator_name"] for s in per_steward]

    plans_by_steward = {}
    villages_by_steward = {}
    orgs_by_steward = {}
    projects_by_steward = {}
    states_by_steward = {}
    all_states = {}
    for row in qs.filter(facilitator_name__in=steward_names).values(
        "facilitator_name", "id", "plan", "is_completed", "effective_village",
        "organization", "organization__name", "project", "project__name",
        "state_soi", "state_soi__state_name",
    ):
        name = row["facilitator_name"]
        plans_by_steward.setdefault(name, []).append(
            {
                "id": row["id"],
                "plan": row["plan"],
                "is_completed": row["is_completed"],
                "village_name": row["effective_village"],
            }
        )
        villages_by_steward.setdefault(name, set()).add(row["effective_village"])
        if row["organization"]:
            orgs_by_steward.setdefault(name, {})[row["organization"]] = row["organization__name"]
        if row["project"]:
            projects_by_steward.setdefault(name, {})[row["project"]] = row["project__name"]
        if row["state_soi"]:
            states_by_steward.setdefault(name, {})[row["state_soi"]] = row["state_soi__state_name"]
            all_states[row["state_soi"]] = row["state_soi__state_name"]

    stewards = [
        {
            "facilitator_name": s["facilitator_name"],
            "plan_count": s["plan_count"],
            "completed_count": s["completed_count"],
            "organization": next(
                ({"id": k, "name": v} for k, v in orgs_by_steward.get(s["facilitator_name"], {}).items()),
                None,
            ),
            "projects": [
                {"id": k, "name": v}
                for k, v in projects_by_steward.get(s["facilitator_name"], {}).items()
            ],
            "states": [
                {"id": k, "name": v}
                for k, v in states_by_steward.get(s["facilitator_name"], {}).items()
            ],
            "villages": sorted(villages_by_steward.get(s["facilitator_name"], [])),
            "plans": plans_by_steward.get(s["facilitator_name"], []),
        }
        for s in per_steward
    ]

    working_states = [{"id": k, "name": v} for k, v in sorted(all_states.items(), key=lambda x: x[1])]

    return {
        "total_stewards": len(steward_names),
        "working_states": working_states,
        "stewards": stewards,
    }


class PlanPermission(permissions.BasePermission):
    """
    Custom permission for PlanApp:
    - All authenticated users can view plans
    - Only superadmins, org admins, administrators, and project managers can create/edit plans
    - Plans must be enabled to be visible
    """

    schema = None

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        if request.method in permissions.SAFE_METHODS:
            return True

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        project_id = view.kwargs.get("project_pk")
        if not project_id:
            return False

        if request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            try:
                project = Project.objects.get(id=project_id)
                return project.organization == request.user.organization
            except Project.DoesNotExist:
                return False

        if request.method == "POST":
            return request.user.has_project_permission(
                project_id=project_id, codename="add_watershed"
            )
        elif request.method in ["PUT", "PATCH"]:
            return request.user.has_project_permission(
                project_id=project_id, codename="change_watershed"
            )
        elif request.method == "DELETE":
            return request.user.has_project_permission(
                project_id=project_id, codename="delete_watershed"
            )

        return False

    def has_object_permission(self, request, view, obj):
        if hasattr(obj, "enabled") and not obj.enabled:
            return False

        if request.method in permissions.SAFE_METHODS:
            return True

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        project = None
        if hasattr(obj, "project"):
            project = obj.project

        if not project:
            return False

        if request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            return project.organization == request.user.organization

        if request.method in ["PUT", "PATCH"]:
            return request.user.has_project_permission(
                project=project, codename="change_watershed"
            )
        elif request.method == "DELETE":
            return request.user.has_project_permission(
                project=project, codename="delete_watershed"
            )

        return False


class APIKeyOrJWTAuth(BaseAuthentication):
    """
    Custom authentication class that supports both JWT tokens and API keys
    """

    def authenticate(self, request):
        jwt_auth = JWTAuthentication()
        try:
            jwt_result = jwt_auth.authenticate(request)
            if jwt_result:
                return jwt_result
        except Exception as e:
            raise e

        api_key = request.headers.get("X-API-Key")
        if api_key:
            try:
                api_key_obj = UserAPIKey.objects.get_from_key(api_key)
                if api_key_obj and api_key_obj.is_active and not api_key_obj.is_expired:
                    api_key_obj.last_used_at = timezone.now()
                    api_key_obj.save()
                    return (api_key_obj.user, api_key_obj)
            except Exception as e:
                raise e
        return None


class GlobalPlanPermission(permissions.BasePermission):
    """
    Custom permission that allows:
        - Superadmin and superusers
        - Users with API Key
    """

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        if hasattr(request, "auth") and isinstance(request.auth, UserAPIKey):
            return True

        return False


class SuperAdminPlanPermission(permissions.BasePermission):
    """
    Custom permission for superadmin only plan endpoints
    """

    schema = None

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        return request.user.is_superadmin or request.user.is_superuser

    def has_object_permission(self, request, view, obj):
        if hasattr(obj, "enabled") and not obj.enabled:
            return False

        return request.user.is_superadmin or request.user.is_superuser


class GlobalPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for global watershed planning operations
    Allows superadmin to view all plans across all organizations and projects
    URL: /api/v1/watershed/plans/
    """

    schema = None
    serializer_class = PlanAppSerializer
    authentication_classes = [APIKeyOrJWTAuth]
    permission_classes = [GlobalPlanPermission]

    def get_queryset(self):
        """
        Return all plans for superadmins and API key users
        """

        queryset = PlanApp.objects.filter(enabled=True)

        tehsil_id = self.request.query_params.get("tehsil", None)
        district_id = self.request.query_params.get("district", None)
        state_id = self.request.query_params.get("state", None)

        if tehsil_id:
            queryset = queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            queryset = queryset.filter(district_soi_id=district_id)
        elif state_id:
            queryset = queryset.filter(state_soi_id=state_id)

        filter_test_demo = self.request.query_params.get("filter_test_plan", "").lower() == "true"
        if filter_test_demo:
            queryset = queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

        return queryset.order_by("-created_at")

    @action(detail=False, methods=["get"], url_path="meta-stats")
    def meta_stats(self, request, *args, **kwargs):
        """
        Get global meta statistics about watershed plans.
        Excludes Test/Demo plans from counts.
        Only accessible to superadmins and API key users.

        Query Parameters:
        - state: Filter by state ID
        - district: Filter by district ID
        - tehsil: Filter by tehsil ID
        - project: Filter by project ID
        - organization: Filter by organization ID

        URL: /api/v1/watershed/plans/meta-stats/

        Returns comprehensive statistics across all plans.
        """
        base_queryset = PlanApp.objects.filter(enabled=True)

        base_queryset = base_queryset.exclude(
            Q(plan__icontains="test") | Q(plan__icontains="demo")
        )

        organization_id = request.query_params.get("organization")
        project_id = request.query_params.get("project")
        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if organization_id:
            base_queryset = base_queryset.filter(organization_id=organization_id)

        if project_id:
            base_queryset = base_queryset.filter(project_id=project_id)

        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        plan_id_strs = [str(pid) for pid in base_queryset.values_list("id", flat=True)]

        total_plans = base_queryset.count()
        completed_plans = base_queryset.filter(is_completed=True).count()
        dpr_generated = base_queryset.filter(is_dpr_generated=True).count()
        dpr_reviewed = base_queryset.filter(is_dpr_reviewed=True).count()

        in_progress_plans = base_queryset.filter(is_completed=False).count()

        pending_dpr_generation = base_queryset.filter(
            is_completed=True, is_dpr_generated=False
        ).count()

        pending_dpr_review = base_queryset.filter(
            is_dpr_generated=True, is_dpr_reviewed=False
        ).count()

        cc_operational_queryset = base_queryset.filter(tehsil_soi__active_status=True)
        cc_active_tehsils = (
            cc_operational_queryset.values("tehsil_soi").distinct().count()
        )
        cc_active_districts = (
            cc_operational_queryset.values("district_soi").distinct().count()
        )
        cc_active_states = (
            cc_operational_queryset.values("state_soi").distinct().count()
        )

        demand_type_counts = _count_demand_types(plan_id_strs)

        steward_queryset = base_queryset.exclude(
            Q(facilitator_name__isnull=True)
            | Q(facilitator_name="")
            | Q(facilitator_name__icontains="test")
            | Q(facilitator_name__icontains="demo")
        )

        steward_queryset = steward_queryset.filter(facilitator_name__in=_app_user_steward_names_qs())

        total_stewards = steward_queryset.values("facilitator_name").distinct().count()

        active_facilitator_names = steward_queryset.values_list("facilitator_name", flat=True).distinct()
        gender_counts = {
            row["gender"]: row["count"]
            for row in (
                User.objects.filter(groups__name="App User")
                .exclude(organization__name__iexact="CFPT")
                .annotate(full_name=Concat("first_name", Value(" "), "last_name"))
                .filter(full_name__in=active_facilitator_names)
                .values("gender")
                .annotate(count=Count("id"))
            )
        }
        steward_gender_breakdown = {
            "male": gender_counts.get("M", 0),
            "female": gender_counts.get("F", 0),
            "other": gender_counts.get("O", 0),
        }

        steward_by_org = []
        if not organization_id:
            org_steward_stats = (
                steward_queryset.values("organization", "organization__name")
                .annotate(steward_count=Count("facilitator_name", distinct=True))
                .order_by("-steward_count")
            )
            for stat in org_steward_stats:
                steward_by_org.append(
                    {
                        "organization_id": stat["organization"],
                        "organization_name": stat["organization__name"],
                        "steward_count": stat["steward_count"],
                    }
                )

        organization_breakdown = []
        state_breakdown = []
        district_breakdown = []
        tehsil_breakdown = []

        if not organization_id:
            org_stats = (
                base_queryset.values("organization", "organization__name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                    dpr_reviewed=Count("id", filter=Q(is_dpr_reviewed=True)),
                )
                .order_by("-total")
            )

            for stat in org_stats:
                organization_breakdown.append(
                    {
                        "organization_id": stat["organization"],
                        "organization_name": stat["organization__name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                        "dpr_reviewed": stat["dpr_reviewed"],
                    }
                )

        if not tehsil_id and not district_id:
            state_stats = (
                base_queryset.values("state_soi", "state_soi__state_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in state_stats:
                state_name = stat["state_soi__state_name"]
                centroid = STATE_CENTROIDS.get(state_name, {})
                state_breakdown.append(
                    {
                        "state_id": stat["state_soi"],
                        "state_name": state_name,
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                        "centroid": centroid if centroid else None,
                    }
                )

        if not tehsil_id and (district_id or state_id):
            district_stats = (
                base_queryset.values("district_soi", "district_soi__district_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in district_stats:
                district_breakdown.append(
                    {
                        "district_id": stat["district_soi"],
                        "district_name": stat["district_soi__district_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        if district_id or state_id or tehsil_id:
            tehsil_stats = (
                base_queryset.filter(tehsil_soi__isnull=False)
                .values("tehsil_soi", "tehsil_soi__tehsil_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in tehsil_stats:
                tehsil_breakdown.append(
                    {
                        "tehsil_id": stat["tehsil_soi"],
                        "tehsil_name": stat["tehsil_soi__tehsil_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        response_data = {
            "summary": {
                "total_plans": total_plans,
                "completed_plans": completed_plans,
                "in_progress_plans": in_progress_plans,
                "dpr_generated": dpr_generated,
                "dpr_reviewed": dpr_reviewed,
                "pending_dpr_generation": pending_dpr_generation,
                "pending_dpr_review": pending_dpr_review,
            },
            "demand_overview": demand_type_counts,
            "commons_connect_operational": {
                "active_tehsils": cc_active_tehsils,
                "active_districts": cc_active_districts,
                "active_states": cc_active_states,
            },
            "landscape_stewards": {
                "total_stewards": total_stewards,
                "gender_breakdown": steward_gender_breakdown,
                "by_organization": steward_by_org if steward_by_org else None,
            },
            "completion_rate": round((completed_plans / total_plans * 100), 2)
            if total_plans > 0
            else 0,
            "dpr_generation_rate": round((dpr_generated / total_plans * 100), 2)
            if total_plans > 0
            else 0,
        }

        if organization_breakdown:
            response_data["organization_breakdown"] = organization_breakdown
        if state_breakdown:
            response_data["state_breakdown"] = state_breakdown
        if district_breakdown:
            response_data["district_breakdown"] = district_breakdown
        if tehsil_breakdown:
            response_data["tehsil_breakdown"] = tehsil_breakdown

        response_data["filters_applied"] = {
            "organization_id": organization_id,
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }

        return Response(response_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="steward-meta-stats")
    def steward_meta_stats(self, request, *args, **kwargs):
        base_queryset = PlanApp.objects.filter(enabled=True).exclude(
            Q(plan__icontains="test") | Q(plan__icontains="demo")
        )

        organization_id = request.query_params.get("organization")
        project_id = request.query_params.get("project")
        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if organization_id:
            base_queryset = base_queryset.filter(organization_id=organization_id)
        if project_id:
            base_queryset = base_queryset.filter(project_id=project_id)
        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        steward_qs = (
            base_queryset
            .exclude(TEST_FACILITATOR_EXCLUSIONS)
            .filter(facilitator_name__in=_app_user_steward_names_qs())
        )
        response_data = _build_steward_meta_stats(steward_qs)
        response_data["filters_applied"] = {
            "organization_id": organization_id,
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }
        return Response(response_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="steward-listing")
    def steward_listing(self, request, *args, **kwargs):
        base_queryset = PlanApp.objects.filter(enabled=True).exclude(
            Q(plan__icontains="test") | Q(plan__icontains="demo")
        )

        organization_id = request.query_params.get("organization")
        project_id = request.query_params.get("project")
        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if organization_id:
            base_queryset = base_queryset.filter(organization_id=organization_id)
        if project_id:
            base_queryset = base_queryset.filter(project_id=project_id)
        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        steward_qs = (
            base_queryset
            .exclude(TEST_FACILITATOR_EXCLUSIONS)
            .filter(facilitator_name__in=_app_user_steward_names_qs())
        )
        response_data = _build_steward_listing(steward_qs)

        if organization_id:
            org = Organization.objects.filter(pk=organization_id).values("id", "name").first()
            response_data["organization"] = org

        response_data["filters_applied"] = {
            "organization_id": organization_id,
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }
        return Response(response_data, status=status.HTTP_200_OK)


class OrganizationPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for organization level watershed planning ops
    Allows superadmins to view plans for a specific organization
    URL: /api/v1/organization/{organization_id}/watershed/plans/
    """

    schema = None

    serializer_class = PlanAppSerializer
    permissions_classes = [permissions.IsAuthenticated, SuperAdminPlanPermission]

    def get_queryset(self):
        """
        Filter plans by organizations for superadmins
        """
        if not (self.request.user.is_superadmin or self.request.user.is_superuser):
            return PlanApp.objects.none()

        organization_id = self.kwargs.get("organization_pk")
        if organization_id:
            try:
                organization = Organization.objects.get(pk=organization_id)
                queryset = PlanApp.objects.filter(
                    organization=organization, enabled=True
                )
            except Organization.DoesNotExist:
                return PlanApp.objects.none()
        else:
            return PlanApp.objects.none()

        filter_test_demo = self.request.query_params.get("filter_test_plan", "").lower() == "true"
        if filter_test_demo:
            queryset = queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

        return queryset.order_by("-created_at")

    @action(
        detail=False,
        methods=["get"],
        url_path="steward-details",
        authentication_classes=[APIKeyOrJWTAuth],
    )
    def steward_details(self, request, *args, **kwargs):
        """
        Get details of a facilitator (steward) by facilitator_name at organization level.

        Query Parameters:
        - facilitator_name: The facilitator's full name (required)

        URL: /api/v1/organization/{organization_id}/watershed/plans/steward-details/?facilitator_name=xxx
        """
        facilitator_name = request.query_params.get("facilitator_name")
        if not facilitator_name:
            return Response(
                {"message": "facilitator_name query parameter is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = (
            User.objects.select_related("organization")
            .annotate(full_name=Concat("first_name", Value(" "), "last_name"))
            .filter(full_name__iexact=facilitator_name)
            .first()
        )

        organization_id = self.kwargs.get("organization_pk")
        plans_queryset = PlanApp.objects.filter(
            facilitator_name__iexact=facilitator_name, enabled=True
        )

        if organization_id:
            plans_queryset = plans_queryset.filter(organization_id=organization_id)

        total_plans = plans_queryset.count()
        dpr_completed = plans_queryset.filter(is_dpr_approved=True).count()

        working_locations = plans_queryset.values(
            "state_soi",
            "state_soi__state_name",
            "district_soi",
            "district_soi__district_name",
            "tehsil_soi",
            "tehsil_soi__tehsil_name",
        ).distinct()

        states = {}
        districts = {}
        tehsils = {}
        for loc in working_locations:
            if loc["state_soi"]:
                states[loc["state_soi"]] = loc["state_soi__state_name"]
            if loc["district_soi"]:
                districts[loc["district_soi"]] = loc["district_soi__district_name"]
            if loc["tehsil_soi"]:
                tehsils[loc["tehsil_soi"]] = loc["tehsil_soi__tehsil_name"]

        projects = {}
        for p in plans_queryset.values("project", "project__name"):
            if p["project"]:
                projects[p["project"]] = p["project__name"]

        plans = list(plans_queryset.values("id", "plan", "is_completed"))

        profile_picture_url = None
        if user and user.profile_picture:
            profile_picture_url = request.build_absolute_uri(user.profile_picture.url)

        response_data = {
            "facilitator_name": facilitator_name,
            "username": user.username if user else None,
            "first_name": user.first_name if user else None,
            "last_name": user.last_name if user else None,
            "age": user.age if user else None,
            "gender": user.get_gender_display() if user and user.gender else None,
            "education_qualification": user.education_qualification if user else None,
            "organization": {
                "id": user.organization.id,
                "name": user.organization.name,
            }
            if user and user.organization
            else None,
            "projects": [{"id": k, "name": v} for k, v in projects.items()],
            "plans": [
                {"id": p["id"], "name": p["plan"], "is_completed": p["is_completed"]}
                for p in plans
            ],
            "profile_picture": profile_picture_url,
            "statistics": {
                "total_plans": total_plans,
                "dpr_completed": dpr_completed,
            },
            "working_locations": {
                "states": [{"id": k, "name": v} for k, v in states.items()],
                "districts": [{"id": k, "name": v} for k, v in districts.items()],
                "tehsils": [{"id": k, "name": v} for k, v in tehsils.items()],
            },
        }

        return Response(response_data, status=status.HTTP_200_OK)


class PlanViewSet(viewsets.ModelViewSet):
    """
    ViewSet for watershed planning operations
    """

    serializer_class = PlanAppSerializer
    permission_classes = [permissions.IsAuthenticated, PlanPermission]
    schema = None
    app_type = AppType.WATERSHED

    def get_queryset(self):
        """
        Filter plans by project
        Superadmins: can see all the plans from all the projects from all the organizations
        Org Admins: can see all plans from all the projects for an organization
        App Users: can see all the plans from a project they are associated with
        """
        project_id = self.kwargs.get("project_pk")

        if self.request.user.groups.filter(name="Test Plan Reviewer").exists():
            base_queryset = PlanApp.objects.filter(enabled=True).filter(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )
            if project_id:
                base_queryset = base_queryset.filter(project_id=project_id)
            tehsil_id = self.request.query_params.get("tehsil")
            if tehsil_id:
                base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
            return base_queryset

        if self.request.user.is_superuser or self.request.user.is_superadmin:
            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    base_queryset = PlanApp.objects.filter(
                        project=project, enabled=True
                    )
                except Project.DoesNotExist:
                    return PlanApp.objects.none()
            else:
                base_queryset = PlanApp.objects.filter(enabled=True)

        elif self.request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            base_queryset = PlanApp.objects.filter(
                organization=self.request.user.organization, enabled=True
            )

            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    if project.organization == self.request.user.organization:
                        base_queryset = base_queryset.filter(project=project)
                    else:
                        return PlanApp.objects.none()
                except Project.DoesNotExist:
                    return PlanApp.objects.none()

        else:
            # regular user
            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    base_queryset = PlanApp.objects.filter(
                        project=project, enabled=True
                    )
                except Project.DoesNotExist:
                    return PlanApp.objects.none()
            else:
                return PlanApp.objects.none()

        tehsil_id = self.request.query_params.get("tehsil", None)

        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)

        filter_test_demo = self.request.query_params.get("filter_test_plan", "").lower() == "true"
        if filter_test_demo:
            base_queryset = base_queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

        return base_queryset

    def get_serializer_class(self):
        """
        Use different serializers based on the action
        """
        if self.action in ["create"]:
            return PlanCreateSerializer
        elif self.action in ["update", "partial_update"]:
            return PlanUpdateSerializer
        elif self.action in ["list", "retrieve"]:
            return PlanAppSerializer
        return PlanAppSerializer

    def create(self, request, *args, **kwargs):
        """
        Create a new watershed plan
        """
        project_id = self.kwargs.get("project_pk")
        if not project_id:
            return Response(
                {"message": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.WATERSHED, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"message": "Watershed Planning is not enabled for this project."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        plan = serializer.save(
            project=project, organization=project.organization, created_by=request.user
        )

        response_data = {
            "plan_data": PlanAppSerializer(plan).data,
            "message": f"Successfully created the watershed plan,{plan.plan}",
        }

        return Response(response_data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        """
        Update a watershed plan
        """
        partial = kwargs.pop("partial", False)
        instance = self.get_object()

        update_serializer = PlanUpdateSerializer(
            instance, data=request.data, partial=partial, context={"request": request}
        )
        update_serializer.is_valid(raise_exception=True)

        updated_instance = update_serializer.save()

        response_data = {
            "plan_data": PlanAppSerializer(updated_instance).data,
            "message": f"Successfully updated the watershed plan,{updated_instance.plan}",
        }

        return Response(response_data, status=status.HTTP_200_OK)

    def perform_destroy(self, instance):
        """
        Delete a watershed plan
        """
        instance.delete()

    @action(detail=False, methods=["get"], url_path="my-plans")
    def my_plans(self, request, *args, **kwargs):
        """
        Get all plans for the authenticated user.
        Returns plans from projects the user belongs to.
        URL: /api/v1/projects/{project_id}/watershed/plans/my-plans/
        """
        user = request.user
        project_id = self.kwargs.get("project_pk")

        if user.groups.filter(name="Test Plan Reviewer").exists():
            plans = PlanApp.objects.filter(enabled=True).filter(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )
            if project_id:
                plans = plans.filter(project_id=project_id)
            tehsil_id = request.query_params.get("tehsil")
            if tehsil_id:
                plans = plans.filter(tehsil_soi_id=tehsil_id)
            serializer = PlanAppSerializer(plans, many=True)
            return Response(
                {"count": plans.count(), "plans": serializer.data},
                status=status.HTTP_200_OK,
            )

        if project_id:
            try:
                project = Project.objects.get(
                    id=project_id, app_type=AppType.WATERSHED, enabled=True
                )
            except Project.DoesNotExist:
                return Response(
                    {"message": "Project not found or watershed planning not enabled."},
                    status=status.HTTP_404_NOT_FOUND,
                )

            user_project_exists = UserProjectGroup.objects.filter(
                user=user, project=project
            ).exists()

            if not user_project_exists and not (
                user.is_superadmin or user.is_superuser
            ):
                if not (
                    user.groups.filter(
                        name__in=["Organization Admin", "Org Admin", "Administrator"]
                    ).exists()
                    and project.organization == user.organization
                ):
                    return Response(
                        {"message": "You do not have access to this project."},
                        status=status.HTTP_403_FORBIDDEN,
                    )

            plans = PlanApp.objects.filter(project=project, enabled=True)
        else:
            user_projects = UserProjectGroup.objects.filter(user=user).values_list(
                "project_id", flat=True
            )
            plans = PlanApp.objects.filter(project_id__in=user_projects, enabled=True)

        tehsil_id = request.query_params.get("tehsil", None)

        if tehsil_id:
            plans = plans.filter(tehsil_soi_id=tehsil_id)

        serializer = PlanAppSerializer(plans, many=True)
        return Response(
            {
                "count": plans.count(),
                "plans": serializer.data,
            },
            status=status.HTTP_200_OK,
        )

    @action(detail=False, methods=["get"], url_path="meta-stats")
    def meta_stats(self, request, *args, **kwargs):
        """
        Get meta statistics about watershed plans.
        Excludes Test/Demo plans from counts.

        Query Parameters:
        - state: Filter by state ID
        - district: Filter by district ID
        - block: Filter by block ID
        - project: Filter by project ID (optional when called from project context)

        URL: /api/v1/projects/{project_id}/watershed/plans/meta-stats/
        or /api/v1/watershed/plans/meta-stats/

        Returns statistics like:
        - Total enabled plans (excluding test/demo)
        - Completed plans count
        - DPR generated count
        - DPR reviewed count
        - DPR approved count
        - Plans by state/district/block breakdown
        """
        user = request.user
        project_id = self.kwargs.get("project_pk") or request.query_params.get(
            "project"
        )

        is_test_plan_reviewer = user.groups.filter(name="Test Plan Reviewer").exists()

        base_queryset = PlanApp.objects.filter(enabled=True)

        if is_test_plan_reviewer:
            base_queryset = base_queryset.filter(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )
            if project_id:
                base_queryset = base_queryset.filter(project_id=project_id)
        else:
            base_queryset = base_queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )

                    if not (user.is_superadmin or user.is_superuser):
                        if user.groups.filter(
                            name__in=["Organization Admin", "Org Admin", "Administrator"]
                        ).exists():
                            if project.organization != user.organization:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )
                        else:
                            user_project_exists = UserProjectGroup.objects.filter(
                                user=user, project=project
                            ).exists()
                            if not user_project_exists:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )

                    base_queryset = base_queryset.filter(project=project)
                except Project.DoesNotExist:
                    return Response(
                        {"message": "Project not found."},
                        status=status.HTTP_404_NOT_FOUND,
                    )
            else:
                if not (user.is_superadmin or user.is_superuser):
                    if user.groups.filter(
                        name__in=["Organization Admin", "Org Admin", "Administrator"]
                    ).exists():
                        base_queryset = base_queryset.filter(organization=user.organization)
                    else:
                        user_projects = UserProjectGroup.objects.filter(
                            user=user
                        ).values_list("project_id", flat=True)
                        base_queryset = base_queryset.filter(project_id__in=user_projects)

        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        total_plans = base_queryset.count()
        completed_plans = base_queryset.filter(is_completed=True).count()
        dpr_generated = base_queryset.filter(is_dpr_generated=True).count()
        dpr_reviewed = base_queryset.filter(is_dpr_reviewed=True).count()

        in_progress_plans = base_queryset.filter(is_completed=False).count()

        pending_dpr_generation = base_queryset.filter(
            is_completed=True, is_dpr_generated=False
        ).count()

        pending_dpr_review = base_queryset.filter(
            is_dpr_generated=True, is_dpr_reviewed=False
        ).count()

        cc_operational_queryset = base_queryset.filter(tehsil_soi__active_status=True)
        cc_active_tehsils = (
            cc_operational_queryset.values("tehsil_soi").distinct().count()
        )
        cc_active_districts = (
            cc_operational_queryset.values("district_soi").distinct().count()
        )
        cc_active_states = (
            cc_operational_queryset.values("state_soi").distinct().count()
        )

        steward_queryset = base_queryset.exclude(
            Q(facilitator_name__isnull=True)
            | Q(facilitator_name="")
            | Q(facilitator_name__icontains="test")
            | Q(facilitator_name__icontains="demo")
        )
        total_stewards = steward_queryset.values("facilitator_name").distinct().count()

        steward_by_org = (
            steward_queryset.values("organization", "organization__name")
            .annotate(steward_count=Count("facilitator_name", distinct=True))
            .order_by("-steward_count")
        )
        steward_by_org_list = [
            {
                "organization_id": stat["organization"],
                "organization_name": stat["organization__name"],
                "steward_count": stat["steward_count"],
            }
            for stat in steward_by_org
        ]

        state_breakdown = []
        district_breakdown = []
        tehsil_breakdown = []

        if not tehsil_id and not district_id:
            state_stats = (
                base_queryset.values("state_soi", "state_soi__state_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in state_stats:
                state_name = stat["state_soi__state_name"]
                centroid = STATE_CENTROIDS.get(state_name, {})
                state_breakdown.append(
                    {
                        "state_id": stat["state_soi"],
                        "state_name": state_name,
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                        "centroid": centroid if centroid else None,
                    }
                )

        if not tehsil_id and (district_id or state_id):
            district_stats = (
                base_queryset.values("district_soi", "district_soi__district_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in district_stats:
                district_breakdown.append(
                    {
                        "district_id": stat["district_soi"],
                        "district_name": stat["district_soi__district_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        if district_id or state_id or tehsil_id:
            tehsil_stats = (
                base_queryset.filter(tehsil_soi__isnull=False)
                .values("tehsil_soi", "tehsil_soi__tehsil_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in tehsil_stats:
                tehsil_breakdown.append(
                    {
                        "tehsil_id": stat["tehsil_soi"],
                        "tehsil_name": stat["tehsil_soi__tehsil_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        response_data = {
            "summary": {
                "total_plans": total_plans,
                "completed_plans": completed_plans,
                "in_progress_plans": in_progress_plans,
                "dpr_generated": dpr_generated,
                "dpr_reviewed": dpr_reviewed,
                "pending_dpr_generation": pending_dpr_generation,
                "pending_dpr_review": pending_dpr_review,
            },
            "commons_connect_operational": {
                "active_tehsils": cc_active_tehsils,
                "active_districts": cc_active_districts,
                "active_states": cc_active_states,
            },
            "landscape_stewards": {
                "total_stewards": total_stewards,
                "by_organization": steward_by_org_list if steward_by_org_list else None,
            },
            "completion_rate": round((completed_plans / total_plans * 100), 2)
            if total_plans > 0
            else 0,
            "dpr_generation_rate": round((dpr_generated / total_plans * 100), 2)
            if total_plans > 0
            else 0,
        }

        if state_breakdown:
            response_data["state_breakdown"] = state_breakdown
        if district_breakdown:
            response_data["district_breakdown"] = district_breakdown
        if tehsil_breakdown:
            response_data["tehsil_breakdown"] = tehsil_breakdown

        response_data["filters_applied"] = {
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }

        return Response(response_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="steward-meta-stats")
    def steward_meta_stats(self, request, *args, **kwargs):
        user = request.user
        project_id = self.kwargs.get("project_pk") or request.query_params.get(
            "project"
        )

        is_test_plan_reviewer = user.groups.filter(name="Test Plan Reviewer").exists()

        base_queryset = PlanApp.objects.filter(enabled=True)

        if is_test_plan_reviewer:
            base_queryset = base_queryset.filter(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )
            if project_id:
                base_queryset = base_queryset.filter(project_id=project_id)
        else:
            base_queryset = base_queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )

                    if not (user.is_superadmin or user.is_superuser):
                        if user.groups.filter(
                            name__in=["Organization Admin", "Org Admin", "Administrator"]
                        ).exists():
                            if project.organization != user.organization:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )
                        else:
                            user_project_exists = UserProjectGroup.objects.filter(
                                user=user, project=project
                            ).exists()
                            if not user_project_exists:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )

                    base_queryset = base_queryset.filter(project=project)
                except Project.DoesNotExist:
                    return Response(
                        {"message": "Project not found."},
                        status=status.HTTP_404_NOT_FOUND,
                    )
            else:
                if not (user.is_superadmin or user.is_superuser):
                    if user.groups.filter(
                        name__in=["Organization Admin", "Org Admin", "Administrator"]
                    ).exists():
                        base_queryset = base_queryset.filter(organization=user.organization)
                    else:
                        user_projects = UserProjectGroup.objects.filter(
                            user=user
                        ).values_list("project_id", flat=True)
                        base_queryset = base_queryset.filter(project_id__in=user_projects)

        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        steward_qs = base_queryset.exclude(TEST_FACILITATOR_EXCLUSIONS)
        response_data = _build_steward_meta_stats(steward_qs)
        response_data["filters_applied"] = {
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }
        return Response(response_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"], url_path="steward-listing")
    def steward_listing(self, request, *args, **kwargs):
        user = request.user
        project_id = self.kwargs.get("project_pk") or request.query_params.get(
            "project"
        )

        is_test_plan_reviewer = user.groups.filter(name="Test Plan Reviewer").exists()

        base_queryset = PlanApp.objects.filter(enabled=True)

        if is_test_plan_reviewer:
            base_queryset = base_queryset.filter(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )
            if project_id:
                base_queryset = base_queryset.filter(project_id=project_id)
        else:
            base_queryset = base_queryset.exclude(
                Q(plan__icontains="test") | Q(plan__icontains="demo")
            )

            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )

                    if not (user.is_superadmin or user.is_superuser):
                        if user.groups.filter(
                            name__in=["Organization Admin", "Org Admin", "Administrator"]
                        ).exists():
                            if project.organization != user.organization:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )
                        else:
                            user_project_exists = UserProjectGroup.objects.filter(
                                user=user, project=project
                            ).exists()
                            if not user_project_exists:
                                return Response(
                                    {"message": "You do not have access to this project."},
                                    status=status.HTTP_403_FORBIDDEN,
                                )

                    base_queryset = base_queryset.filter(project=project)
                except Project.DoesNotExist:
                    return Response(
                        {"message": "Project not found."},
                        status=status.HTTP_404_NOT_FOUND,
                    )
            else:
                if not (user.is_superadmin or user.is_superuser):
                    if user.groups.filter(
                        name__in=["Organization Admin", "Org Admin", "Administrator"]
                    ).exists():
                        base_queryset = base_queryset.filter(organization=user.organization)
                    else:
                        user_projects = UserProjectGroup.objects.filter(
                            user=user
                        ).values_list("project_id", flat=True)
                        base_queryset = base_queryset.filter(project_id__in=user_projects)

        state_id = request.query_params.get("state")
        district_id = request.query_params.get("district")
        tehsil_id = request.query_params.get("tehsil")

        if tehsil_id:
            base_queryset = base_queryset.filter(tehsil_soi_id=tehsil_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_soi_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_soi_id=state_id)

        steward_qs = (
            base_queryset
            .exclude(TEST_FACILITATOR_EXCLUSIONS)
            .filter(facilitator_name__in=_app_user_steward_names_qs())
        )
        response_data = _build_steward_listing(steward_qs)
        response_data["filters_applied"] = {
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "tehsil_id": tehsil_id,
        }
        return Response(response_data, status=status.HTTP_200_OK)

    @action(
        detail=False,
        methods=["get"],
        url_path="steward-details",
        authentication_classes=[APIKeyOrJWTAuth],
    )
    def steward_details(self, request, *args, **kwargs):
        """
        Get details of a facilitator (steward) by facilitator_name.

        Query Parameters:
        - facilitator_name: The facilitator's full name (required)

        URL: /api/v1/projects/{project_id}/watershed/plans/steward-details/?facilitator_name=xxx

        Returns:
        - User profile details
        - Plan statistics (count, DPR completed)
        - Working locations (states, districts, tehsils)
        """
        facilitator_name = request.query_params.get("facilitator_name")
        if not facilitator_name:
            return Response(
                {"message": "facilitator_name query parameter is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = (
            User.objects.select_related("organization")
            .annotate(full_name=Concat("first_name", Value(" "), "last_name"))
            .filter(full_name__iexact=facilitator_name)
            .first()
        )

        plans_queryset = PlanApp.objects.filter(
            facilitator_name__iexact=facilitator_name, enabled=True
        )

        project_id = self.kwargs.get("project_pk")
        if project_id:
            plans_queryset = plans_queryset.filter(project_id=project_id)

        total_plans = plans_queryset.count()
        dpr_completed = plans_queryset.filter(is_dpr_approved=True).count()

        working_locations = plans_queryset.values(
            "state_soi",
            "state_soi__state_name",
            "district_soi",
            "district_soi__district_name",
            "tehsil_soi",
            "tehsil_soi__tehsil_name",
        ).distinct()

        states = {}
        districts = {}
        tehsils = {}
        for loc in working_locations:
            if loc["state_soi"]:
                states[loc["state_soi"]] = loc["state_soi__state_name"]
            if loc["district_soi"]:
                districts[loc["district_soi"]] = loc["district_soi__district_name"]
            if loc["tehsil_soi"]:
                tehsils[loc["tehsil_soi"]] = loc["tehsil_soi__tehsil_name"]

        projects = {}
        for p in plans_queryset.values("project", "project__name"):
            if p["project"]:
                projects[p["project"]] = p["project__name"]

        plans = list(plans_queryset.values("id", "plan", "is_completed"))

        profile_picture_url = None
        if user and user.profile_picture:
            profile_picture_url = request.build_absolute_uri(user.profile_picture.url)

        response_data = {
            "facilitator_name": facilitator_name,
            "username": user.username if user else None,
            "first_name": user.first_name if user else None,
            "last_name": user.last_name if user else None,
            "age": user.age if user else None,
            "gender": user.get_gender_display() if user and user.gender else None,
            "education_qualification": user.education_qualification if user else None,
            "organization": {
                "id": user.organization.id,
                "name": user.organization.name,
            }
            if user and user.organization
            else None,
            "projects": [{"id": k, "name": v} for k, v in projects.items()],
            "plans": [
                {"id": p["id"], "name": p["plan"], "is_completed": p["is_completed"]}
                for p in plans
            ],
            "profile_picture": profile_picture_url,
            "statistics": {
                "total_plans": total_plans,
                "dpr_completed": dpr_completed,
            },
            "working_locations": {
                "states": [{"id": k, "name": v} for k, v in states.items()],
                "districts": [{"id": k, "name": v} for k, v in districts.items()],
                "tehsils": [{"id": k, "name": v} for k, v in tehsils.items()],
            },
        }

        return Response(response_data, status=status.HTTP_200_OK)
