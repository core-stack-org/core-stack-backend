from django.db.models.signals import post_save, m2m_changed
from django.dispatch import receiver
from django.contrib.auth.models import Group

from projects.models import Project, AppType
from users.models import User, UserProjectGroup


@receiver(post_save, sender=Project)
def map_test_plan_reviewers_to_new_project(sender, instance, created, **kwargs):
    if not created or instance.app_type != AppType.WATERSHED:
        return

    try:
        group = Group.objects.get(name="Test Plan Reviewer")
    except Group.DoesNotExist:
        return

    for user in User.objects.filter(groups=group):
        existing_role = UserProjectGroup.objects.filter(user=user).values_list("group", flat=True).first()
        if not existing_role:
            continue
        UserProjectGroup.objects.get_or_create(
            user=user, project=instance, defaults={"group_id": existing_role},
        )


@receiver(m2m_changed, sender=User.groups.through)
def map_user_to_all_projects_on_reviewer_group_add(sender, instance, action, pk_set, **kwargs):
    if action != "post_add":
        return

    try:
        reviewer_group = Group.objects.get(name="Test Plan Reviewer")
    except Group.DoesNotExist:
        return

    if reviewer_group.pk not in pk_set:
        return

    existing_role = UserProjectGroup.objects.filter(user=instance).values_list("group", flat=True).first()
    if not existing_role:
        return

    for project in Project.objects.filter(app_type=AppType.WATERSHED, enabled=True):
        UserProjectGroup.objects.get_or_create(
            user=instance, project=project, defaults={"group_id": existing_role},
        )