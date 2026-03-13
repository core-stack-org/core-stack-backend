# Python Coding Principles for CoRE Stack Backend

This document outlines the best practices and principles for writing Python code in the Core Stack Backend Django
application, focusing on maintainability, readability, and performance.

## Table of Contents

- [Naming Conventions](#naming-conventions)
- [Code Structure and Organization](#code-structure-and-organization)
- [Django-Specific Best Practices](#django-specific-best-practices)
- [Design Patterns and Architecture](#design-patterns-and-architecture)
- [Error Handling and Logging](#error-handling-and-logging)
- [Performance and Optimization](#performance-and-optimization)
- [Testing Guidelines](#testing-guidelines)
- [Documentation Standards](#documentation-standards)

## Naming Conventions

### Variables and Functions

Use `snake_case` for variables, functions, and methods:

```python
# Good
user_count = 0
max_retry_attempts = 3

def calculate_project_area():
    pass

def get_user_by_organization(org_id):
    pass
```

### Classes

Use `PascalCase` for class names:

```python
# Good
class UserManager:
    pass

class ProjectPermissionHandler:
    pass

class WatershedPlanService:
    pass
```

### Constants

Use `UPPER_SNAKE_CASE` for constants:

```python
# Good
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
DEFAULT_PAGINATION_SIZE = 20
JWT_ACCESS_TOKEN_LIFETIME = 2  # days
```

### Django Model Fields

Use `snake_case` for model field names:

```python
class Organization(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.ForeignKey(User, on_delete=models.CASCADE)
```

### URL Patterns and API Endpoints

Use `kebab-case` for URL paths:

```python
# urls.py
urlpatterns = [
    path('api/v1/watershed/plans/', WatershedPlanListView.as_view()),
    path('api/v1/auth/change-password/', ChangePasswordView.as_view()),
    path('api/v1/projects/<int:project_id>/kml-files/', KMLFileView.as_view()),
]
```

### File and Directory Names

Use `snake_case` for Python files and directories:

```
core_stack_backend/
├── apps/
│   ├── users/
│   │   ├── models.py
│   │   ├── views.py
│   │   ├── serializers.py
│   │   └── permission_handlers.py
│   └── watershed/
│       ├── plan_service.py
│       └── resource_manager.py
```

### Environment Variables

Use `UPPER_SNAKE_CASE` for environment variables:

```python
# settings.py
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
JWT_ACCESS_TOKEN_LIFETIME = int(os.getenv('JWT_ACCESS_TOKEN_LIFETIME', 2))
```

## Code Structure and Organization

### Function Design

Keep functions small and focused on a single responsibility:

```python
# Good
def validate_kml_file(file):
    """Validate if uploaded file is a valid KML file."""
    if not file.name.endswith('.kml'):
        raise ValidationError("File must have .kml extension")

    if file.size > MAX_FILE_SIZE:
        raise ValidationError(f"File size cannot exceed {MAX_FILE_SIZE} bytes")

def calculate_file_hash(file):
    """Calculate SHA-256 hash of file content."""
    hasher = hashlib.sha256()
    for chunk in file.chunks():
        hasher.update(chunk)
    return hasher.hexdigest()

# Bad - doing too many things
def process_kml_file(file):
    # Validation logic
    if not file.name.endswith('.kml'):
        raise ValidationError("File must have .kml extension")

    # Hash calculation
    hasher = hashlib.sha256()
    for chunk in file.chunks():
        hasher.update(chunk)
    file_hash = hasher.hexdigest()

    # Duplicate check
    if KMLFile.objects.filter(kml_hash=file_hash).exists():
        raise ValidationError("Duplicate file")

    # File conversion
    # ... conversion logic

    # Database save
    # ... save logic
```

### Class Organization

Structure classes with a logical method order:

```python
class WatershedPlanService:
    """Service class for watershed plan operations."""

    def __init__(self, user, project):
        self.user = user
        self.project = project
        self._validate_permissions()

    # Public methods first
    def create_plan(self, plan_data):
        """Create a new watershed plan."""
        self._validate_plan_data(plan_data)
        plan = self._build_plan_instance(plan_data)
        return self._save_plan(plan)

    def update_plan(self, plan_id, plan_data):
        """Update an existing watershed plan."""
        plan = self._get_plan_or_404(plan_id)
        self._validate_update_permissions(plan)
        return self._update_plan_fields(plan, plan_data)

    # Private methods last
    def _validate_permissions(self):
        """Validate user has necessary permissions."""
        if not self.user.has_project_permission(self.project, 'add_watershed'):
            raise PermissionError("User lacks watershed creation permission")

    def _validate_plan_data(self, data):
        """Validate plan data integrity."""
        required_fields = ['plan', 'state', 'district', 'block', 'village_name']
        for field in required_fields:
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")

    def _build_plan_instance(self, data):
        """Build plan instance with processed data."""
        return Plan(
            project=self.project,
            organization=self.project.organization,
            created_by=self.user,
            **data
        )
```

### Module Organization

Organize imports and code sections consistently:

```python
# Standard library imports
import hashlib
import logging
from datetime import datetime, timedelta

# Third-party imports
from django.db import models, transaction
from django.core.exceptions import ValidationError
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response

# Local application imports
from core_stack_backend.apps.users.models import User
from core_stack_backend.apps.projects.models import Project
from .models import WatershedPlan
from .serializers import WatershedPlanSerializer
from .permissions import WatershedPermission

# Constants
MAX_PLAN_NAME_LENGTH = 255
DEFAULT_PLAN_STATUS = 'draft'

# Logger setup
logger = logging.getLogger(__name__)
```

## Django-Specific Best Practices

### Model Design

Follow Django model best practices:

```python
class WatershedPlan(models.Model):
    """Model representing a watershed management plan."""

    # Use descriptive field names
    plan = models.CharField(
        max_length=255,
        help_text="Name of the watershed plan"
    )

    # Add appropriate constraints
    state = models.ForeignKey(
        'geoadmin.State',
        on_delete=models.CASCADE,
        help_text="State where the plan is located"
    )

    # Use appropriate field types
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Add meaningful choices
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('active', 'Active'),
        ('completed', 'Completed'),
        ('archived', 'Archived'),
    ]
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='draft'
    )

    class Meta:
        db_table = 'watershed_plan'
        verbose_name = 'Watershed Plan'
        verbose_name_plural = 'Watershed Plans'
        ordering = ['-created_at']

        # Add database constraints
        constraints = [
            models.UniqueConstraint(
                fields=['project', 'plan'],
                name='unique_plan_per_project'
            )
        ]

    def __str__(self):
        return f"{self.plan} - {self.project.name}"

    def clean(self):
        """Model-level validation."""
        super().clean()
        if self.village_name and len(self.village_name.strip()) == 0:
            raise ValidationError("Village name cannot be empty")
```

### ViewSet Design

Structure ViewSets with clear separation of concerns:

```python
class WatershedPlanViewSet(viewsets.ModelViewSet):
    """ViewSet for watershed plan management."""

    serializer_class = WatershedPlanSerializer
    permission_classes = [IsAuthenticated, WatershedPermission]
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['status', 'state', 'district', 'block']
    search_fields = ['plan', 'village_name', 'gram_panchayat']
    ordering_fields = ['created_at', 'updated_at', 'plan']
    ordering = ['-created_at']

    def get_queryset(self):
        """Return filtered queryset based on user permissions."""
        user = self.request.user
        project_id = self.kwargs.get('project_id')

        if user.is_superadmin:
            queryset = WatershedPlan.objects.all()
        elif user.groups.filter(name='Organization Admin').exists():
            queryset = WatershedPlan.objects.filter(
                organization=user.organization
            )
        else:
            queryset = WatershedPlan.objects.filter(
                project__userprojectgroup__user=user
            )

        if project_id:
            queryset = queryset.filter(project_id=project_id)

        return queryset.select_related(
            'project', 'organization', 'created_by'
        ).prefetch_related('state', 'district', 'block')

    def perform_create(self, serializer):
        """Handle plan creation with proper context."""
        project = self._get_project_from_url()

        serializer.save(
            project=project,
            organization=project.organization,
            created_by=self.request.user
        )

        logger.info(
            f"Watershed plan created: {serializer.instance.plan} "
            f"by user {self.request.user.username}"
        )

    @action(detail=True, methods=['patch'])
    def update_status(self, request, pk=None):
        """Update plan status with validation."""
        plan = self.get_object()
        status = request.data.get('status')

        if status not in dict(WatershedPlan.STATUS_CHOICES):
            return Response(
                {'error': 'Invalid status'},
                status=status.HTTP_400_BAD_REQUEST
            )

        plan.status = status
        plan.updated_by = request.user
        plan.save(update_fields=['status', 'updated_by', 'updated_at'])

        return Response({'message': 'Status updated successfully'})

    def _get_project_from_url(self):
        """Extract and validate project from URL parameters."""
        project_id = self.kwargs.get('project_id')
        try:
            return Project.objects.get(id=project_id)
        except Project.DoesNotExist:
            raise ValidationError(f"Project with id {project_id} not found")
```

### Serializer Design

Create clear, maintainable serializers:

```python
class WatershedPlanSerializer(serializers.ModelSerializer):
    """Serializer for watershed plan operations."""

    # Add computed fields
    project_name = serializers.CharField(
        source='project.name',
        read_only=True
    )
    organization_name = serializers.CharField(
        source='organization.name',
        read_only=True
    )
    created_by_name = serializers.CharField(
        source='created_by.get_full_name',
        read_only=True
    )

    class Meta:
        model = WatershedPlan
        fields = [
            'id', 'plan', 'facilitator_name', 'village_name',
            'gram_panchayat', 'created_at', 'updated_at',
            'enabled', 'is_completed', 'is_dpr_generated',
            'project', 'project_name', 'organization',
            'organization_name', 'state', 'district', 'block',
            'created_by', 'created_by_name', 'updated_by'
        ]
        read_only_fields = [
            'id', 'created_at', 'updated_at', 'project',
            'organization', 'created_by', 'updated_by'
        ]

    def validate_plan(self, value):
        """Validate plan name is unique within project."""
        project = self.context.get('project')
        if not project:
            return value

        queryset = WatershedPlan.objects.filter(
            project=project,
            plan=value
        )

        # Exclude current instance during updates
        if self.instance:
            queryset = queryset.exclude(pk=self.instance.pk)

        if queryset.exists():
            raise serializers.ValidationError(
                "Plan with this name already exists in the project"
            )

        return value

    def validate(self, data):
        """Cross-field validation."""
        if data.get('is_dpr_approved') and not data.get('is_dpr_reviewed'):
            raise serializers.ValidationError(
                "DPR must be reviewed before it can be approved"
            )

        return data
```

## Design Patterns and Architecture

### Service Layer Pattern

Implement a service layer for complex business logic:

```python
class OrganizationService:
    """Service layer for organization-related operations."""

    @staticmethod
    def create_organization_with_admin(org_data, admin_data):
        """Create organization and assign an admin in a single transaction."""
        with transaction.atomic():
            # Create organization
            organization = Organization.objects.create(**org_data)

            # Create admin user
            admin_user = User.objects.create_user(
                organization=organization,
                **admin_data
            )

            # Assign admin role
            org_admin_group = Group.objects.get(name='Organization Admin')
            admin_user.groups.add(org_admin_group)

            logger.info(f"Created organization {organization.name} with admin {admin_user.username}")

            return organization, admin_user

    @staticmethod
    def transfer_project_ownership(project, new_organization, user):
        """Transfer project to a different organization."""
        if not user.is_superadmin:
            raise PermissionError("Only superadmins can transfer projects")

        old_org = project.organization

        with transaction.atomic():
            project.organization = new_organization
            project.updated_by = user
            project.save()

            # Log the transfer
            logger.info(
                f"Project {project.name} transferred from "
                f"{old_org.name} to {new_organization.name} by {user.username}"
            )

        return project
```

### Factory Pattern

Use factories for complex object creation:

```python
class ProjectFactory:
    """Factory for creating projects with proper setup."""

    @staticmethod
    def create_plantation_project(user, project_data):
        """Create a project configured for plantation management."""
        project_data.update({
            'app_type': 'plantation',
            'created_by': user,
            'updated_by': user
        })

        with transaction.atomic():
            project = Project.objects.create(**project_data)

            # Enable plantation app
            ProjectApp.objects.create(
                project=project,
                app_type='plantation',
                enabled=True
            )

            # Assign creator as project manager
            ProjectFactory._assign_project_manager(user, project)

            return project

    @staticmethod
    def create_watershed_project(user, project_data):
        """Create a project configured for watershed planning."""
        project_data.update({
            'app_type': 'watershed',
            'created_by': user,
            'updated_by': user
        })

        with transaction.atomic():
            project = Project.objects.create(**project_data)

            # Enable watershed app
            ProjectApp.objects.create(
                project=project,
                app_type='watershed',
                enabled=True
            )

            # Assign creator as project manager
            ProjectFactory._assign_project_manager(user, project)

            return project

    @staticmethod
    def _assign_project_manager(user, project):
        """Assign user as project manager."""
        manager_group = Group.objects.get(name='Project Manager')
        UserProjectGroup.objects.create(
            user=user,
            project=project,
            group=manager_group
        )
```

### Repository Pattern

Abstract data access with repository pattern:

```python
class WatershedPlanRepository:
    """Repository for watershed plan data access."""

    @staticmethod
    def get_plans_for_user(user, filters=None):
        """Get plans accessible by user with optional filters."""
        queryset = WatershedPlan.objects.none()

        if user.is_superadmin:
            queryset = WatershedPlan.objects.all()
        elif user.groups.filter(name='Organization Admin').exists():
            queryset = WatershedPlan.objects.filter(
                organization=user.organization
            )
        else:
            queryset = WatershedPlan.objects.filter(
                project__userprojectgroup__user=user
            )

        if filters:
            queryset = WatershedPlanRepository._apply_filters(queryset, filters)

        return queryset.select_related(
            'project', 'organization', 'state', 'district', 'block'
        )

    @staticmethod
    def get_plans_by_geography(state_id=None, district_id=None, block_id=None):
        """Get plans filtered by geographical hierarchy."""
        queryset = WatershedPlan.objects.all()

        if state_id:
            queryset = queryset.filter(state_id=state_id)
        if district_id:
            queryset = queryset.filter(district_id=district_id)
        if block_id:
            queryset = queryset.filter(block_id=block_id)

        return queryset

    @staticmethod
    def _apply_filters(queryset, filters):
        """Apply additional filters to queryset."""
        if 'status' in filters:
            queryset = queryset.filter(status=filters['status'])
        if 'is_completed' in filters:
            queryset = queryset.filter(is_completed=filters['is_completed'])
        if 'project_id' in filters:
            queryset = queryset.filter(project_id=filters['project_id'])

        return queryset
```

### Strategy Pattern

Implement different strategies for similar operations:

```python
class FileProcessorStrategy:
    """Base strategy for file processing."""

    def process(self, file):
        raise NotImplementedError

class KMLProcessorStrategy(FileProcessorStrategy):
    """Strategy for processing KML files."""

    def process(self, file):
        # Validate KML format
        self._validate_kml(file)

        # Calculate hash for deduplication
        file_hash = self._calculate_hash(file)

        # Convert to GeoJSON
        geojson_data = self._convert_to_geojson(file)

        return {
            'hash': file_hash,
            'geojson': geojson_data,
            'type': 'kml'
        }

    def _validate_kml(self, file):
        # KML validation logic
        pass

    def _calculate_hash(self, file):
        # Hash calculation logic
        pass

    def _convert_to_geojson(self, file):
        # KML to GeoJSON conversion logic
        pass

class FileProcessor:
    """Context class for file processing."""

    def __init__(self, strategy: FileProcessorStrategy):
        self._strategy = strategy

    def process_file(self, file):
        return self._strategy.process(file)

# Usage
processor = FileProcessor(KMLProcessorStrategy())
result = processor.process_file(uploaded_file)
```

## Error Handling and Logging

### Exception Handling

Implement proper exception handling:

```python
class WatershedPlanService:
    def create_plan(self, plan_data):
        """Create watershed plan with comprehensive error handling."""
        try:
            self._validate_plan_data(plan_data)

            with transaction.atomic():
                plan = WatershedPlan.objects.create(**plan_data)
                self._post_creation_tasks(plan)

            logger.info(f"Successfully created plan: {plan.plan}")
            return plan

        except ValidationError as e:
            logger.warning(f"Validation error creating plan: {str(e)}")
            raise
        except IntegrityError as e:
            logger.error(f"Database integrity error: {str(e)}")
            raise ValidationError("Plan with this name already exists")
        except Exception as e:
            logger.error(f"Unexpected error creating plan: {str(e)}", exc_info=True)
            raise

    def _validate_plan_data(self, data):
        """Validate plan data with specific error messages."""
        if not data.get('plan'):
            raise ValidationError("Plan name is required")

        if not data.get('village_name'):
            raise ValidationError("Village name is required")

        # Check geographical hierarchy consistency
        if data.get('block') and data.get('district'):
            if not self._is_block_in_district(data['block'], data['district']):
                raise ValidationError("Block must belong to the specified district")
```

### Logging Configuration

Set up structured logging:

```python
import logging
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

class WatershedPlanService:
    def __init__(self):
        self.logger = structlog.get_logger(__name__)

    def create_plan(self, plan_data):
        self.logger.info(
            "Creating watershed plan",
            user_id=plan_data.get('created_by'),
            project_id=plan_data.get('project'),
            plan_name=plan_data.get('plan')
        )

        try:
            # Plan creation logic
            pass
        except Exception as e:
            self.logger.error(
                "Failed to create watershed plan",
                error=str(e),
                plan_data=plan_data,
                exc_info=True
            )
            raise
```

## Performance and Optimization

### Database Query Optimization

Optimize database queries:

```python
class WatershedPlanViewSet(viewsets.ModelViewSet):
    def get_queryset(self):
        """Optimized queryset with proper joins."""
        return WatershedPlan.objects.select_related(
            'project',
            'organization',
            'created_by',
            'updated_by',
            'state',
            'district',
            'block'
        ).prefetch_related(
            'project__userprojectgroup_set__user',
            'project__userprojectgroup_set__group'
        ).annotate(
            total_resources=Count('resources'),
            completion_percentage=Case(
                When(is_completed=True, then=Value(100)),
                When(is_dpr_approved=True, then=Value(80)),
                When(is_dpr_reviewed=True, then=Value(60)),
                When(is_dpr_generated=True, then=Value(40)),
                default=Value(20),
                output_field=IntegerField()
            )
        )

# Use bulk operations for mass updates
def bulk_update_plan_status(plan_ids, status, user):
    """Bulk update plan status for multiple plans."""
    WatershedPlan.objects.filter(
        id__in=plan_ids
    ).update(
        status=status,
        updated_by=user,
        updated_at=timezone.now()
    )
```

### Caching Strategy

Implement appropriate caching:

```python
from django.core.cache import cache
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page

class OrganizationService:
    @staticmethod
    def get_organization_stats(org_id):
        """Get organization statistics with caching."""
        cache_key = f"org_stats_{org_id}"
        stats = cache.get(cache_key)

        if stats is None:
            stats = {
                'total_projects': Project.objects.filter(organization_id=org_id).count(),
                'active_users': User.objects.filter(
                    organization_id=org_id,
                    is_active=True
                ).count(),
                'completed_plans': WatershedPlan.objects.filter(
                    organization_id=org_id,
                    is_completed=True
                ).count()
            }

            # Cache for 15 minutes
            cache.set(cache_key, stats, 900)

        return stats

    @staticmethod
    def invalidate_organization_cache(org_id):
        """Invalidate organization-related cache entries."""
        cache_keys = [
            f"org_stats_{org_id}",
            f"org_projects_{org_id}",
            f"org_users_{org_id}"
        ]
        cache.delete_many(cache_keys)

# Use method decorators for view-level caching
@method_decorator(cache_page(60 * 5), name='list')  # 5-minute cache
class OrganizationViewSet(viewsets.ReadOnlyModelViewSet):
    pass
```

## Testing Guidelines

### Unit Testing

Write comprehensive unit tests:

```python
import pytest
from django.test import TestCase
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from core_stack_backend.apps.watershed.services import WatershedPlanService
from core_stack_backend.apps.projects.models import Project

User = get_user_model()

class WatershedPlanServiceTest(TestCase):
    """Test cases for WatershedPlanService."""

    def setUp(self):
        """Set up test data."""
        self.organization = Organization.objects.create(
            name="Test Organization"
        )
        self.user = User.objects.create_user(
            username="testuser",
            email="test@example.com",
            organization=self.organization
        )
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
            created_by=self.user
        )
        self.service = WatershedPlanService(self.user, self.project)

    def test_create_plan_success(self):
        """Test successful plan creation."""
        plan_data = {
            'plan': 'Test Plan',
            'state': 1,
            'district': 101,
            'block': 10101,
            'village_name': 'Test Village',
            'gram_panchayat': 'Test GP'
        }

        plan = self.service.create_plan(plan_data)

        self.assertEqual(plan.plan, 'Test Plan')
        self.assertEqual(plan.project, self.project)
        self.assertEqual(plan.created_by, self.user)

    def test_create_plan_missing_required_field(self):
        """Test plan creation with missing required field."""
        plan_data = {
            'plan': 'Test Plan',
            # Missing state, district, block, etc.
        }

        with self.assertRaises(ValidationError):
            self.service.create_plan(plan_data)

    def test_create_plan_insufficient_permissions(self):
        """Test plan creation without permissions."""
        # Create user without permissions
        user_without_perms = User.objects.create_user(
            username="noperms",
            email="noperms@example.com",
            organization=self.organization
        )
        service = WatershedPlanService(user_without_perms, self.project)

        plan_data = {
            'plan': 'Test Plan',
            'state': 1,
            'district': 101,
            'block': 10101,
            'village_name': 'Test Village',
            'gram_panchayat': 'Test GP'
        }

        with self.assertRaises(PermissionError):
            service.create_plan(plan_data)

# Use pytest for more advanced testing
@pytest.mark.django_db
class TestWatershedPlanRepository:
    """Test cases for WatershedPlanRepository."""

    @pytest.fixture
    def setup_data(self):
        """Set up test data using pytest fixtures."""
        org = Organization.objects.create(name="Test Org")
        user = User.objects.create_user(
            username="testuser",
            organization=org
        )
        project = Project.objects.create(
            name="Test Project",
            organization=org,
            created_by=user
        )
        return {
            'organization': org,
            'user': user,
            'project': project
        }

    def test_get_plans_for_superadmin(self, setup_data):
        """Test that superadmin can see all plans."""
        # Create superadmin
        superadmin = User.objects.create_user(
            username="superadmin",
            is_superadmin=True,
            organization=setup_data['organization']
        )

        # Create plans in different organizations
        WatershedPlan.objects.create(
            plan="Plan 1",
            project=setup_data['project'],
            organization=setup_data['organization'],
            created_by=setup_data['user']
        )

        plans = WatershedPlanRepository.get_plans_for_user(superadmin)
        assert plans.count() >= 1
```

## Documentation Standards

### Docstring Conventions

Use Google-style docstrings:

```python
def create_watershed_plan(user, project, plan_data):
    """Create a new watershed plan with validation and permissions.

    This function creates a watershed plan after validating the input data
    and checking user permissions. It also handles related tasks like
    logging and cache invalidation.

    Args:
        user (User): The user creating the plan
        project (Project): The project the plan belongs to
        plan_data (dict): Dictionary containing plan data with keys:
            - plan (str): Name of the plan
            - state (int): State ID from geoadmin
            - district (int): District ID from geoadmin
            - block (int): Block ID from geoadmin
            - village_name (str): Name of the village
            - gram_panchayat (str): Name of the gram panchayat

    Returns:
        WatershedPlan: The created watershed plan instance

    Raises:
        ValidationError: If plan data is invalid
        PermissionError: If user lacks necessary permissions
        IntegrityError: If plan name already exists in project

    Example:
        >>> user = User.objects.get(id=1)
        >>> project = Project.objects.get(id=1)
        >>> plan_data = {
        ...     'plan': 'New Watershed Plan',
        ...     'state': 69,
        ...     'district': 3110101,
        ...     'block': 311011,
        ...     'village_name': 'Example Village',
        ...     'gram_panchayat': 'Example GP'
        ... }
        >>> plan = create_watershed_plan(user, project, plan_data)
        >>> print(plan.plan)
        'New Watershed Plan'
    """
    # Function implementation here
    pass

class WatershedPlanService:
    """Service for managing watershed plans.

    This service handles all watershed plan operations including creation,
    validation, and permissions checking. It acts as a business logic layer
    between the API views and the data models.

    Attributes:
        user (User): The user performing operations
        project (Project): The project context
        logger (Logger): Logger instance for this service
    """

    def __init__(self, user, project):
        """Initialize the service with user and project context.

        Args:
            user (User): User performing operations
            project (Project): Project context for operations
        """
        self.user = user
        self.project = project
        self.logger = logging.getLogger(__name__)
```

### Type Hints

Use type hints for better code documentation and IDE support:

```python
from typing import List, Dict, Optional, Union, Tuple
from django.http import HttpRequest, HttpResponse

def get_user_projects(
    user: User,
    filters: Optional[Dict[str, Any]] = None
) -> List[Project]:
    """Get projects accessible to user with optional filters.

    Args:
        user: User instance
        filters: Optional dictionary of filters

    Returns:
        List of Project instances
    """
    queryset = Project.objects.filter(
        userprojectgroup__user=user
    )

    if filters:
        if 'app_type' in filters:
            queryset = queryset.filter(app_type=filters['app_type'])
        if 'enabled' in filters:
            queryset = queryset.filter(enabled=filters['enabled'])

    return list(queryset)

class ProjectService:
    """Service for project management operations."""

    def create_project(
        self,
        user: User,
        data: Dict[str, Any]
    ) -> Tuple[Project, bool]:
        """Create a new project.

        Args:
            user: User creating the project
            data: Project data dictionary

        Returns:
            Tuple of (created_project, was_created)
        """
        # Implementation here
        pass
```

## Django App Architecture

### App Structure

Organize Django apps with clear separation of concerns:

```
watershed/
├── __init__.py
├── models/
│   ├── __init__.py
│   ├── plan.py
│   └── resource.py
├── services/
│   ├── __init__.py
│   ├── plan_service.py
│   └── resource_service.py
├── serializers/
│   ├── __init__.py
│   ├── plan_serializer.py
│   └── resource_serializer.py
├── views/
│   ├── __init__.py
│   ├── plan_views.py
│   └── resource_views.py
├── permissions.py
├── urls.py
├── admin.py
├── apps.py
└── tests/
    ├── __init__.py
    ├── test_models.py
    ├── test_services.py
    ├── test_views.py
    └── test_serializers.py
```

### Model Organization

Split large models into separate files:

```python
# models/__init__.py
from .plan import WatershedPlan
from .resource import Resource, ResourceType

__all__ = ['WatershedPlan', 'Resource', 'ResourceType']

# models/plan.py
from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()

class WatershedPlan(models.Model):
    """Model for watershed management plans."""

    plan = models.CharField(max_length=255)
    project = models.ForeignKey(
        'projects.Project',
        on_delete=models.CASCADE,
        related_name='watershed_plans'
    )
    # ... other fields

    class Meta:
        db_table = 'watershed_plan'
        verbose_name = 'Watershed Plan'
        verbose_name_plural = 'Watershed Plans'
        ordering = ['-created_at']
```

### Custom Managers and QuerySets

Implement custom managers for complex queries:

```python
class WatershedPlanQuerySet(models.QuerySet):
    """Custom queryset for WatershedPlan."""

    def completed(self):
        """Filter to completed plans only."""
        return self.filter(is_completed=True)

    def for_user(self, user):
        """Filter plans accessible by user."""
        if user.is_superadmin:
            return self
        elif user.groups.filter(name='Organization Admin').exists():
            return self.filter(organization=user.organization)
        else:
            return self.filter(project__userprojectgroup__user=user)

    def by_geography(self, state_id=None, district_id=None, block_id=None):
        """Filter by geographical hierarchy."""
        qs = self
        if state_id:
            qs = qs.filter(state_id=state_id)
        if district_id:
            qs = qs.filter(district_id=district_id)
        if block_id:
            qs = qs.filter(block_id=block_id)
        return qs

    def with_stats(self):
        """Annotate with statistical data."""
        return self.annotate(
            resource_count=Count('resources'),
            work_count=Count('works'),
            completion_score=Case(
                When(is_completed=True, then=100),
                When(is_dpr_approved=True, then=80),
                default=20,
                output_field=IntegerField()
            )
        )

class WatershedPlanManager(models.Manager):
    """Custom manager for WatershedPlan."""

    def get_queryset(self):
        """Return custom queryset."""
        return WatershedPlanQuerySet(self.model, using=self._db)

    def completed(self):
        """Get completed plans."""
        return self.get_queryset().completed()

    def for_user(self, user):
        """Get plans for specific user."""
        return self.get_queryset().for_user(user)

class WatershedPlan(models.Model):
    """Watershed plan model with custom manager."""

    # ... fields

    objects = WatershedPlanManager()

    class Meta:
        # ... meta options
```

### Permission Classes

Create reusable permission classes:

```python
from rest_framework.permissions import BasePermission

class WatershedPermission(BasePermission):
    """Custom permission class for watershed operations."""

    def has_permission(self, request, view):
        """Check if user has basic watershed access."""
        if not request.user.is_authenticated:
            return False

        project_id = view.kwargs.get('project_id')
        if not project_id:
            return request.user.is_superadmin

        return self._has_project_access(request.user, project_id)

    def has_object_permission(self, request, view, obj):
        """Check if user can access specific watershed object."""
        user = request.user

        if user.is_superadmin:
            return True

        if user.groups.filter(name='Organization Admin').exists():
            return obj.organization == user.organization

        return user.has_project_permission(obj.project, self._get_required_permission(request.method))

    def _has_project_access(self, user, project_id):
        """Check if user has access to project."""
        if user.is_superadmin:
            return True

        try:
            project = Project.objects.get(id=project_id)
            if user.groups.filter(name='Organization Admin').exists():
                return project.organization == user.organization

            return UserProjectGroup.objects.filter(
                user=user,
                project=project
            ).exists()
        except Project.DoesNotExist:
            return False

    def _get_required_permission(self, method):
        """Map HTTP method to required permission."""
        permission_map = {
            'GET': 'view_watershed',
            'POST': 'add_watershed',
            'PUT': 'change_watershed',
            'PATCH': 'change_watershed',
            'DELETE': 'delete_watershed',
        }
        return permission_map.get(method, 'view_watershed')
```

## Security Best Practices

### Input Validation

Always validate and sanitize user input:

```python
from django.core.validators import RegexValidator, MinLengthValidator
from django.core.exceptions import ValidationError
import re

class WatershedPlanSerializer(serializers.ModelSerializer):
    """Serializer with comprehensive validation."""

    # Add custom validators
    plan = serializers.CharField(
        max_length=255,
        validators=[
            MinLengthValidator(3),
            RegexValidator(
                regex=r'^[a-zA-Z0-9\s\-_]+$',
                message='Plan name can only contain letters, numbers, spaces, hyphens, and underscores'
            )
        ]
    )

    village_name = serializers.CharField(
        max_length=100,
        validators=[
            RegexValidator(
                regex=r'^[a-zA-Z\s]+$',
                message='Village name can only contain letters and spaces'
            )
        ]
    )

    def validate_facilitator_name(self, value):
        """Custom validation for facilitator name."""
        if value and len(value.strip()) == 0:
            raise serializers.ValidationError("Facilitator name cannot be empty")

        # Check for potentially malicious input
        if re.search(r'[<>"\';]', value):
            raise serializers.ValidationError("Facilitator name contains invalid characters")

        return value.strip()

    def validate(self, data):
        """Cross-field validation."""
        # Validate geographical hierarchy
        if data.get('district') and data.get('state'):
            if not self._is_district_in_state(data['district'], data['state']):
                raise serializers.ValidationError(
                    "District must belong to the specified state"
                )

        return data

    def _is_district_in_state(self, district_id, state_id):
        """Validate district belongs to state."""
        try:
            district = District.objects.get(id=district_id)
            return district.state_id == state_id
        except District.DoesNotExist:
            return False
```

### SQL Injection Prevention

Use Django ORM properly to prevent SQL injection:

```python
# Good - Using ORM
def get_plans_by_name(plan_name):
    """Get plans by name using ORM."""
    return WatershedPlan.objects.filter(plan__icontains=plan_name)

# Good - Using parameterized queries when raw SQL is necessary
def get_plan_statistics(state_id):
    """Get plan statistics using raw SQL with parameters."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) as total_plans,
                   COUNT(CASE WHEN is_completed = %s THEN 1 END) as completed_plans
            FROM watershed_plan
            WHERE state_id = %s
            """,
            [True, state_id]
        )
        return cursor.fetchone()

# Bad - Never do this (vulnerable to SQL injection)
def bad_get_plans(plan_name):
    """Example of what NOT to do."""
    query = f"SELECT * FROM watershed_plan WHERE plan LIKE '%{plan_name}%'"
    return WatershedPlan.objects.extra(where=[query])
```

### Authentication and Authorization

Implement proper authentication checks:

```python
from django.contrib.auth.decorators import login_required
from rest_framework.decorators import authentication_classes, permission_classes
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated

class WatershedPlanViewSet(viewsets.ModelViewSet):
    """ViewSet with proper authentication and permissions."""

    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated, WatershedPermission]

    def get_queryset(self):
        """Filter queryset based on user permissions."""
        user = self.request.user

        # Always check authentication first
        if not user.is_authenticated:
            return WatershedPlan.objects.none()

        # Apply user-specific filtering
        queryset = WatershedPlan.objects.for_user(user)

        # Apply additional filters
        project_id = self.kwargs.get('project_id')
        if project_id:
            queryset = queryset.filter(project_id=project_id)

        return queryset

@login_required
@require_http_methods(["POST"])
def create_plan_view(request):
    """Function-based view with proper decorators."""
    # Check specific permissions
    if not request.user.has_perm('watershed.add_watershedplan'):
        return HttpResponseForbidden("Permission denied")

    # Process request
    pass
```

## Code Quality and Standards

### Code Formatting

Use consistent formatting with tools like `black` and `isort`:

```python
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 22.3.0
    hooks:
      - id: black
        language_version: python3.9

  - repo: https://github.com/pycqa/isort
    rev: 5.10.1
    hooks:
      - id: isort
        args: ["--profile", "black"]

  - repo: https://github.com/pycqa/flake8
    rev: 4.0.1
    hooks:
      - id: flake8
        args: [--max-line-length=88, --extend-ignore=E203]

# pyproject.toml
[tool.black]
line-length = 88
target-version = ['py39']
include = '\.pyi?$'
extend-exclude = '''
/(
  migrations
)/
'''

[tool.isort]
profile = "black"
multi_line_output = 3
line_length = 88
skip_glob = ["*/migrations/*"]
```

### Linting Configuration

Set up comprehensive linting:

```python
# .flake8
[flake8]
max-line-length = 88
extend-ignore = E203, E501, W503
exclude =
    migrations,
    __pycache__,
    manage.py,
    settings.py,
    env,
    venv

# pylint configuration
[tool.pylint.messages_control]
disable = [
    "missing-docstring",
    "too-few-public-methods",
    "import-error",
]

[tool.pylint.format]
max-line-length = 88
```

### Static Type Checking

Use mypy for static type checking:

```python
# mypy.ini
[mypy]
python_version = 3.9
warn_return_any = True
warn_unused_configs = True
disallow_untyped_defs = True
ignore_missing_imports = True

[mypy-*.migrations.*]
ignore_errors = True

# Type-annotated service example
from typing import Protocol, List, Optional, Dict, Any

class UserPermissionChecker(Protocol):
    """Protocol for user permission checking."""

    def has_permission(self, user: User, permission: str) -> bool:
        """Check if user has specific permission."""
        ...

class WatershedPlanService:
    """Type-annotated service class."""

    def __init__(self, permission_checker: UserPermissionChecker) -> None:
        self.permission_checker = permission_checker

    def get_accessible_plans(
        self,
        user: User,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[WatershedPlan]:
        """Get plans accessible to user with optional filters."""
        if not self.permission_checker.has_permission(user, 'view_watershed'):
            return []

        queryset = WatershedPlan.objects.for_user(user)

        if filters:
            queryset = self._apply_filters(queryset, filters)

        return list(queryset)

    def _apply_filters(
        self,
        queryset: models.QuerySet[WatershedPlan],
        filters: Dict[str, Any]
    ) -> models.QuerySet[WatershedPlan]:
        """Apply filters to queryset."""
        # Implementation here
        return queryset
```

## Conclusion

These Python coding principles provide a comprehensive framework for writing maintainable, secure, and efficient code in
the Core Stack Backend. By following these guidelines:

1. **Consistency**: Code becomes more predictable and easier to understand
2. **Maintainability**: Clear structure and documentation make future changes easier
3. **Security**: Proper validation and authentication prevent common vulnerabilities
4. **Performance**: Optimized queries and caching improve application speed
5. **Testability**: Well-structured code with clear dependencies is easier to test
6. **Scalability**: Design patterns and service layers support growth

### Key Takeaways

- Use descriptive naming conventions consistently across the codebase
- Structure code with clear separation of concerns
- Implement comprehensive error handling and logging
- Optimize database queries and use appropriate caching strategies
- Write thorough tests for all critical functionality
- Document code thoroughly with clear docstrings
- Follow Django best practices for models, views, and serializers
- Implement security measures at every level
- Use design patterns to solve common problems elegantly

By adhering to these principles, the Core Stack Backend will remain a robust, maintainable, and secure application that
can evolve with changing requirements while maintaining high code quality standards.
