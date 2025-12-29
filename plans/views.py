# plans/views.py
from django.db.models import Count, Q, Value
from django.db.models.functions import Concat
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
    PlanSerializer,
    PlanUpdateSerializer,
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

        block_id = self.request.query_params.get("block", None)
        district_id = self.request.query_params.get("district", None)
        state_id = self.request.query_params.get("state", None)

        if block_id:
            queryset = queryset.filter(block=block_id)
        elif district_id:
            queryset = queryset.filter(district=district_id)
        elif state_id:
            queryset = queryset.filter(state=state_id)
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
        - block: Filter by block ID
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
        block_id = request.query_params.get("block")

        if organization_id:
            base_queryset = base_queryset.filter(organization_id=organization_id)

        if project_id:
            base_queryset = base_queryset.filter(project_id=project_id)

        if block_id:
            base_queryset = base_queryset.filter(block_id=block_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_id=state_id)

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

        cc_operational_queryset = base_queryset.filter(block__active_status=True)
        cc_active_blocks = cc_operational_queryset.values("block").distinct().count()
        cc_active_districts = (
            cc_operational_queryset.values("district").distinct().count()
        )
        cc_active_states = cc_operational_queryset.values("state").distinct().count()

        steward_queryset = base_queryset.exclude(
            Q(facilitator_name__isnull=True)
            | Q(facilitator_name="")
            | Q(facilitator_name__icontains="test")
            | Q(facilitator_name__icontains="demo")
        )
        total_stewards = steward_queryset.values("facilitator_name").distinct().count()

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
        block_breakdown = []

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

        if not block_id and not district_id:
            state_stats = (
                base_queryset.values("state", "state__state_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in state_stats:
                state_name = stat["state__state_name"]
                centroid = STATE_CENTROIDS.get(state_name, {})
                state_breakdown.append(
                    {
                        "state_id": stat["state"],
                        "state_name": state_name,
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                        "centroid": centroid if centroid else None,
                    }
                )

        if not block_id and (district_id or state_id):
            district_stats = (
                base_queryset.values("district", "district__district_name")
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
                        "district_id": stat["district"],
                        "district_name": stat["district__district_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        if district_id or state_id or block_id:
            block_stats = (
                base_queryset.values("block", "block__block_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in block_stats:
                block_breakdown.append(
                    {
                        "block_id": stat["block"],
                        "block_name": stat["block__block_name"],
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
                "active_blocks": cc_active_blocks,
                "active_districts": cc_active_districts,
                "active_states": cc_active_states,
            },
            "landscape_stewards": {
                "total_stewards": total_stewards,
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
        if block_breakdown:
            response_data["block_breakdown"] = block_breakdown

        response_data["filters_applied"] = {
            "organization_id": organization_id,
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "block_id": block_id,
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
                return PlanApp.objects.filter(
                    organization=organization, enabled=True
                ).order_by("-created_at")
            except Organization.DoesNotExist:
                return PlanApp.objects.none()

        return PlanApp.objects.none()

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
            "state",
            "state__state_name",
            "district",
            "district__district_name",
            "block",
            "block__block_name",
        ).distinct()

        states = {}
        districts = {}
        blocks = {}
        for loc in working_locations:
            if loc["state"]:
                states[loc["state"]] = loc["state__state_name"]
            if loc["district"]:
                districts[loc["district"]] = loc["district__district_name"]
            if loc["block"]:
                blocks[loc["block"]] = loc["block__block_name"]

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
                "blocks": [{"id": k, "name": v} for k, v in blocks.items()],
            },
        }

        return Response(response_data, status=status.HTTP_200_OK)


class PlanViewSet(viewsets.ModelViewSet):
    """
    ViewSet for watershed planning operations
    """

    serializer_class = PlanSerializer
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

        block_id = self.request.query_params.get("block", None)
        if block_id:
            base_queryset = base_queryset.filter(block=block_id)

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
        return PlanSerializer

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

        block_id = request.query_params.get("block", None)
        if block_id:
            plans = plans.filter(block=block_id)

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

        base_queryset = PlanApp.objects.filter(enabled=True)

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
        block_id = request.query_params.get("block")

        if block_id:
            base_queryset = base_queryset.filter(block_id=block_id)
        elif district_id:
            base_queryset = base_queryset.filter(district_id=district_id)
        elif state_id:
            base_queryset = base_queryset.filter(state_id=state_id)

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

        cc_operational_queryset = base_queryset.filter(block__active_status=True)
        cc_active_blocks = cc_operational_queryset.values("block").distinct().count()
        cc_active_districts = (
            cc_operational_queryset.values("district").distinct().count()
        )
        cc_active_states = cc_operational_queryset.values("state").distinct().count()

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
        block_breakdown = []

        if not block_id and not district_id:
            state_stats = (
                base_queryset.values("state", "state__state_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in state_stats:
                state_name = stat["state__state_name"]
                centroid = STATE_CENTROIDS.get(state_name, {})
                state_breakdown.append(
                    {
                        "state_id": stat["state"],
                        "state_name": state_name,
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                        "centroid": centroid if centroid else None,
                    }
                )

        if not block_id and (district_id or state_id):
            district_stats = (
                base_queryset.values("district", "district__district_name")
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
                        "district_id": stat["district"],
                        "district_name": stat["district__district_name"],
                        "total_plans": stat["total"],
                        "completed_plans": stat["completed"],
                        "dpr_generated": stat["dpr_generated"],
                    }
                )

        if district_id or state_id or block_id:
            block_stats = (
                base_queryset.values("block", "block__block_name")
                .annotate(
                    total=Count("id"),
                    completed=Count("id", filter=Q(is_completed=True)),
                    dpr_generated=Count("id", filter=Q(is_dpr_generated=True)),
                )
                .order_by("-total")
            )

            for stat in block_stats:
                block_breakdown.append(
                    {
                        "block_id": stat["block"],
                        "block_name": stat["block__block_name"],
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
                "active_blocks": cc_active_blocks,
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
        if block_breakdown:
            response_data["block_breakdown"] = block_breakdown

        response_data["filters_applied"] = {
            "project_id": project_id,
            "state_id": state_id,
            "district_id": district_id,
            "block_id": block_id,
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
            "state",
            "state__state_name",
            "district",
            "district__district_name",
            "block",
            "block__block_name",
        ).distinct()

        states = {}
        districts = {}
        blocks = {}
        for loc in working_locations:
            if loc["state"]:
                states[loc["state"]] = loc["state__state_name"]
            if loc["district"]:
                districts[loc["district"]] = loc["district__district_name"]
            if loc["block"]:
                blocks[loc["block"]] = loc["block__block_name"]

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
                "blocks": [{"id": k, "name": v} for k, v in blocks.items()],
            },
        }

        return Response(response_data, status=status.HTTP_200_OK)
