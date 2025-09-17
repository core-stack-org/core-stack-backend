from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Organization

from django.contrib.auth import get_user_model
User = get_user_model()
from nrm_app.settings import BASE_URL
from utilities.mailutils import send_email

@receiver(post_save, sender=Organization)
def send_email_on_org_creation(sender, instance, created, **kwargs):
    print ("singla trigger")
    if created:  # True only when a new org is created
        subject = f"New Organization Created: {instance.name}"
        user_approval_url = f"{BASE_URL}admin/users/userprojectgroup/{instance.created_by.id}/change/"
        org_approval_url = f"{BASE_URL}admin/users/userprojectgroup/"
        superuser_emails = list(
            User.objects.filter(is_superuser=True).values_list('email', flat=True)
        )

        print("super user emai")
        print (superuser_emails)
        message = f"""
        A new organization '{instance.name}' was created from the dashboard.  

        Please take the following actions:

        1. Approve the organization: {org_approval_url}  
        2. Grant organization admin rights to the user: {user_approval_url}  

        Thanks,  
        CoRE Stack Team
        """

        send_email(
            subject,
            message,
            superuser_emails
        )