from django.db import models
import uuid
from django.contrib.auth.models import AbstractUser, Group, Permission
from django.utils.translation import gettext_lazy as _
from organization.models import Organization
from projects.models import Project


class User(AbstractUser):
    id = models.AutoField(primary_key=True)
    organization = models.ForeignKey(
        Organization, 
        on_delete=models.SET_NULL, 
        null=True, 
        related_name='users'
    )
    contact_number = models.CharField(max_length=20, blank=True, null=True)
    is_superadmin = models.BooleanField(default=False)

    # Override the ManyToMany fields from AbstractUser with custom related_name
    groups = models.ManyToManyField(
        Group,
        verbose_name=_('groups'),
        blank=True,
        help_text=_(
            'The groups this user belongs to. A user will get all permissions '
            'granted to each of their groups.'
        ),
        related_name='custom_user_set',
        related_query_name='user',
    )
    user_permissions = models.ManyToManyField(
        Permission,
        verbose_name=_('user permissions'),
        blank=True,
        help_text=_('Specific permissions for this user.'),
        related_name='custom_user_permission_set',  
        related_query_name='user',
    )
    
    class Meta:
        verbose_name = _('user')
        verbose_name_plural = _('users')

    def has_project_permission(self, project=None, project_id=None, codename=None):
        """
        Check if the user has a specific permission for a project.
        
        Args:
            project: The project object to check permissions for
            project_id: ID of the project to check permissions for (alternative to project)
            codename: The permission codename (e.g., 'view_plantation')
            
        Returns:
            bool: True if the user has permission, False otherwise
        """
        # Super admins have all permissions
        if self.is_superadmin or self.is_superuser:
            return True
        
        # Get project object if only ID was provided
        if not project and project_id:
            from projects.models import Project
            try:
                project = Project.objects.get(id=project_id)
            except Project.DoesNotExist:
                return False
        
        # Check if user has any role in this project
        try:
            user_project_group = UserProjectGroup.objects.get(user=self, project=project)
            group = user_project_group.group
            return group.permissions.filter(codename=codename).exists()
        except UserProjectGroup.DoesNotExist:
            return False

    def get_project_group(self, project):
        """Get the user's group (role) for a specific project."""
        try:
            return UserProjectGroup.objects.get(user=self, project=project).group
        except UserProjectGroup.DoesNotExist:
            return None


class UserProjectGroup(models.Model):
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='project_groups')
    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)  # using Django's Group model
    
    class Meta:
        unique_together = ('user', 'project')
        verbose_name = 'User Project Role'
        verbose_name_plural = 'User Project Roles'
        
    def __str__(self):
        return f"{self.user.username} - {self.project.name} - {self.group.name}"