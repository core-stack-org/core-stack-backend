# signals.py
from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Project


@receiver(post_save, sender=Project)
def handle_project_created(sender, instance, created, **kwargs):
    if created:
        print(f"New project created: {instance.name}")
        if instance.app_type == "community_engagement":
            from community_engagement.utils import create_community_for_project
            create_community_for_project(instance)
