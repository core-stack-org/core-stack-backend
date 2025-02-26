from rest_framework import status, viewsets, mixins, generics, permissions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.authentication import JWTAuthentication
from django.contrib.auth.models import Group
from .models import User, UserProjectGroup
from .serializers import (
    UserSerializer, UserRegistrationSerializer, 
    GroupSerializer, UserProjectGroupSerializer
)
from projects.models import Project
from projects.serializers import ProjectSerializer
from organization.models import Organization
from organization.serializers import OrganizationSerializer


class RegisterView(viewsets.GenericViewSet, generics.CreateAPIView):
    """API endpoint for user registration."""
    queryset = User.objects.all()
    serializer_class = UserRegistrationSerializer
    permission_classes = [permissions.AllowAny]
    
    @action(detail=False, methods=['get'])
    def available_organizations(self, request):
        """Get list of organizations that users can register for."""
        
        # Get all organizations
        organizations = Organization.objects.all()
        
        # Return only the names of the organizations
        organization_names = [org.name for org in organizations]
        
        return Response(organization_names)
    
    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        
        # Generate tokens for the new user
        refresh = RefreshToken.for_user(user)
        
        # Return user info and tokens
        return Response({
            'user': UserSerializer(user).data,
            'token': str(refresh.access_token),
            'refresh_token': str(refresh)
        }, status=status.HTTP_201_CREATED)


class LoginView(TokenObtainPairView):
    """
    API endpoint for user login.
    Extends SimpleJWT's TokenObtainPairView to customize the response.
    """
    def post(self, request, *args, **kwargs):
        # Call parent class method to validate credentials and get tokens
        response = super().post(request, *args, **kwargs)
        
        # Get the user from the validated data
        token = response.data.get('access')
        jwt_auth = JWTAuthentication()
        validated_token = jwt_auth.get_validated_token(token)
        user = jwt_auth.get_user(validated_token)
        
        # Add user info to response
        response.data['user'] = UserSerializer(user).data
        
        return response


class LogoutView(generics.GenericAPIView):
    """API endpoint for user logout - invalidates refresh token."""
    permission_classes = [permissions.IsAuthenticated]
    
    def post(self, request):
        try:
            # Get the refresh token from request
            refresh_token = request.data.get('refresh_token')
            
            # Blacklist the token
            token = RefreshToken(refresh_token)
            token.blacklist()
            
            return Response({"success": True}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"success": False, "error": str(e)}, 
                           status=status.HTTP_400_BAD_REQUEST)


class IsSuperAdminOrOrgAdmin(permissions.BasePermission):
    """Permission to allow only superadmins or organization admins."""
    def has_permission(self, request, view):
        return request.user.is_authenticated and (
            request.user.is_superadmin or 
            request.user.groups.filter(name='Organization Admin').exists()
        )


class UserViewSet(viewsets.ModelViewSet):
    """API endpoint for user management."""
    queryset = User.objects.all()
    serializer_class = UserSerializer
    
    def get_permissions(self):
        """Set custom permissions based on action."""
        if self.action in ['list', 'create']:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        elif self.action in ['retrieve', 'update', 'partial_update']:
            # Users can view/edit their own profile
            permission_classes = [permissions.IsAuthenticated]
        else:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        return [permission() for permission in permission_classes]

    def get_queryset(self):
        """Filter users based on permissions."""
        user = self.request.user
        
        # Superadmins can see all users
        if user.is_superadmin:
            return User.objects.all()
            
        # Organization admins can see users in their organization
        if user.organization and user.groups.filter(name='Organization Admin').exists():
            return User.objects.filter(organization=user.organization)
            
        # Regular users can only see themselves
        return User.objects.filter(id=user.id)

    def retrieve(self, request, *args, **kwargs):
        """Get user details."""
        instance = self.get_object()
        
        # Allow users to view their own details
        if request.user.id != instance.id and not (
            request.user.is_superadmin or 
            (request.user.organization == instance.organization and 
             request.user.groups.filter(name='Organization Admin').exists())
        ):
            return Response(
                {"detail": "You do not have permission to view this user."},
                status=status.HTTP_403_FORBIDDEN
            )
            
        serializer = self.get_serializer(instance)
        return Response(serializer.data)

    def update(self, request, *args, **kwargs):
        """Update user profile."""
        instance = self.get_object()
        
        # Allow users to update their own profile
        if request.user.id != instance.id and not (
            request.user.is_superadmin or 
            (request.user.organization == instance.organization and 
             request.user.groups.filter(name='Organization Admin').exists())
        ):
            return Response(
                {"detail": "You do not have permission to update this user."},
                status=status.HTTP_403_FORBIDDEN
            )
            
        # Prevent changing specific fields unless superadmin
        if not request.user.is_superadmin:
            # Remove is_superadmin and is_staff from the data if present
            mutable_data = request.data.copy()
            mutable_data.pop('is_superadmin', None)
            mutable_data.pop('is_staff', None)
            mutable_data.pop('is_active', None)
            
            # Only org admins can change organization
            if not request.user.groups.filter(name='Organization Admin').exists():
                mutable_data.pop('organization', None)
                
            partial = kwargs.pop('partial', False)
            serializer = self.get_serializer(instance, data=mutable_data, partial=partial)
        else:
            partial = kwargs.pop('partial', False)
            serializer = self.get_serializer(instance, data=request.data, partial=partial)
            
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        
        return Response(serializer.data)

    @action(detail=True, methods=['put'])
    def set_organization(self, request, pk=None):
        """Assign user to an organization."""
        user = self.get_object()
        organization_id = request.data.get('organization_id')
        
        if not (request.user.is_superadmin or 
                (request.user.groups.filter(name='Organization Admin').exists() and 
                 request.user.organization.id == organization_id)):
            return Response(
                {"detail": "You do not have permission to assign users to this organization."},
                status=status.HTTP_403_FORBIDDEN
            )
            
        # Update user's organization
        from organization.models import Organization
        try:
            organization = Organization.objects.get(id=organization_id)
            user.organization = organization
            user.save()
            
            return Response(UserSerializer(user).data)
        except Organization.DoesNotExist:
            return Response(
                {"detail": "Organization not found."},
                status=status.HTTP_404_NOT_FOUND
            )

    @action(detail=True, methods=['put'])
    def set_group(self, request, pk=None):
        """Assign a user to a group (role)."""
        user = self.get_object()
        group_id = request.data.get('group_id')
        
        # Only superadmins can assign users to groups
        if not request.user.is_superadmin:
            return Response(
                {"detail": "You do not have permission to assign users to groups."},
                status=status.HTTP_403_FORBIDDEN
            )
            
        # Get the group
        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."},
                status=status.HTTP_404_NOT_FOUND
            )
            
        # Special handling for Organization Admin group
        if group.name == 'Organization Admin':
            # Ensure the user has an organization assigned
            if not user.organization:
                return Response(
                    {"detail": "User must be assigned to an organization before being made an Organization Admin."},
                    status=status.HTTP_400_BAD_REQUEST
                )
                
            # If the user is being assigned to an organization different from the requester's
            # organization, only superadmins can do this
            if (request.user.organization != user.organization and 
                not request.user.is_superadmin):
                return Response(
                    {"detail": "You can only assign users in your organization as Organization Admins."},
                    status=status.HTTP_403_FORBIDDEN
                )
        
        # Add the user to the group
        user.groups.add(group)
        
        return Response({
            "message": f"User {user.username} successfully added to group {group.name}",
            "user": UserSerializer(user).data
        })

    @action(detail=True, methods=['put'])
    def remove_group(self, request, pk=None):
        """Remove a user from a group (role)."""
        user = self.get_object()
        group_id = request.data.get('group_id')
        
        # Only superadmins can remove users from groups
        if not request.user.is_superadmin:
            return Response(
                {"detail": "You do not have permission to remove users from groups."},
                status=status.HTTP_403_FORBIDDEN
            )
            
        # Get the group
        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."},
                status=status.HTTP_404_NOT_FOUND
            )
            
        # Remove the user from the group
        user.groups.remove(group)
        
        return Response({
            "message": f"User {user.username} successfully removed from group {group.name}",
            "user": UserSerializer(user).data
        })


class GroupViewSet(viewsets.ReadOnlyModelViewSet):
    """API endpoint for group management (read-only)."""
    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class UserProjectGroupViewSet(viewsets.ModelViewSet):
    """API endpoint for managing user-project assignments."""
    serializer_class = UserProjectGroupSerializer
    permission_classes = [permissions.IsAuthenticated]
    
    def get_queryset(self):
        project_id = self.kwargs.get('project_pk')
        if project_id:
            return UserProjectGroup.objects.filter(project_id=project_id)
        return UserProjectGroup.objects.none()
    
    def get_serializer_class(self):
        if self.action == 'list':
            return UserProjectGroupSerializer
        return UserProjectGroupSerializer
    
    def create(self, request, *args, **kwargs):
        project_id = self.kwargs.get('project_pk')
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Add project_id to request data
        mutable_data = request.data.copy()
        mutable_data['project'] = project_id
        
        serializer = self.get_serializer(data=mutable_data)
        serializer.is_valid(raise_exception=True)
        
        # Verify the user belongs to the same organization as the project
        project = Project.objects.get(id=project_id)
        user = User.objects.get(id=mutable_data.get('user'))
        
        if user.organization != project.organization:
            return Response(
                {"detail": "User must belong to the same organization as the project."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Check if the user already has a role in this project
        existing = UserProjectGroup.objects.filter(user=user, project_id=project_id).first()
        if existing:
            # Update existing role
            existing.group_id = mutable_data.get('group')
            existing.save()
            return Response(
                UserProjectGroupSerializer(existing).data,
                status=status.HTTP_200_OK
            )
        
        # Create new role assignment
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        return Response(
            serializer.data, 
            status=status.HTTP_201_CREATED, 
            headers=headers
        )