from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Organization

from django.contrib.auth import get_user_model
User = get_user_model()
from nrm_app.settings import BASE_URL
from geoadmin.tasks import send_email, send_email_notification


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
        <html>
  <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; background-color: #f4f4f4; margin: 0; padding: 0;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" width="100%" style="max-width: 600px; margin: auto; background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px;">
      <!-- Header with Logo -->
      <tr>
        <td style="background-color: #2c3e50; padding: 20px; text-align: center; border-top-left-radius: 8px; border-top-right-radius: 8px;">
          <img src="https://www.explorer.core-stack.org/static/media/newlogoWhite.49a9a6f4f7debe5a6ad8.png" alt="CoRE Stack" style="max-height: 50px;">
        </td>
      </tr>

      <!-- Body -->
      <tr>
        <td style="padding: 30px;">
          <h2 style="color: #2c3e50; margin-top: 0; font-size: 22px; text-align: center;">
            🚀 New Organization Created
          </h2>

          <p style="font-size: 16px; margin-bottom: 20px;">
            A new organization <strong>{instance.name}</strong> was created from the dashboard.
          </p>

          <p style="font-size: 16px; margin-bottom: 15px;">Please take the following actions:</p>

          <ol style="padding-left: 20px; font-size: 15px;">
            <li style="margin-bottom: 10px;">
              <a href="{org_approval_url}" 
                 style="color: #1a73e8; text-decoration: none; font-weight: bold;">
                 ✅ Approve the organization
              </a>
            </li>
            <li>
              <a href="{user_approval_url}" 
                 style="color: #1a73e8; text-decoration: none; font-weight: bold;">
                 👤 Grant organization admin rights to the user
              </a>
            </li>
          </ol>

          <p style="margin-top: 30px; font-size: 15px;">Thanks,</p>
          <p style="font-weight: bold; color: #2c3e50; font-size: 16px;">CoRE Stack Team</p>
        </td>
      </tr>

      <!-- Footer -->
      <tr>
        <td style="background-color: #f9f9f9; padding: 15px; text-align: center; font-size: 12px; color: #888; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;">
          © 2025 CoRE Stack. All rights reserved.  
        </td>
      </tr>
    </table>
  </body>
</html>

        """

        send_email_notification.delay(
            subject,
            '',
            message,
            ['kapil.dadheech@gramvaani.org']
        )