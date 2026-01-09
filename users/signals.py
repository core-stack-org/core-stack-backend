from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model

User = get_user_model()
from nrm_app.settings import BASE_URL
from geoadmin.tasks import send_email_notification


@receiver(post_save, sender=User)
def send_email_to_org_admin(sender, instance, created, **kwargs):
    if created:
        subject = f"New User Registered – Assign to Project"
        user_approval_url = f"{BASE_URL}admin/users/userprojectgroup/"
        superuser_emails = list(
            User.objects.filter(is_superuser=True).values_list("email", flat=True)
        )

        message = f"""
        <html>
  <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; background-color: #f4f4f4; margin: 0; padding: 0;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" width="100%" style="max-width: 600px; margin: auto; background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px;">
      <tr>
        <td style="background-color: #2c3e50; padding: 20px; text-align: center; border-top-left-radius: 8px; border-top-right-radius: 8px;">
          <img src="https://www.explorer.core-stack.org/static/media/newlogoWhite.49a9a6f4f7debe5a6ad8.png" alt="CoRE Stack" style="max-height: 50px;">
        </td>
      </tr>
      <tr>
        <td style="padding: 30px;">
          <h2 style="color: #2c3e50; margin-top: 0; font-size: 22px; text-align: center;">
            New User Registered
          </h2>
          <p style="font-size: 16px; margin-bottom: 20px;">
            A new user <strong>{instance.email}</strong> has registered on the dashboard.
          </p>
          <p style="font-size: 16px; margin-bottom: 15px;">Please take the following action:</p>
          <ol style="padding-left: 20px; font-size: 15px;">
            <li>
              <a href="{user_approval_url}" 
                 style="color: #1a73e8; text-decoration: none; font-weight: bold;">
                 Assign user to a project
              </a>
            </li>
          </ol>
          <p style="margin-top: 30px; font-size: 15px;">Thanks,</p>
          <p style="font-weight: bold; color: #2c3e50; font-size: 16px;">CoRE Stack Team</p>
        </td>
      </tr>
      <tr>
        <td style="background-color: #f9f9f9; padding: 15px; text-align: center; font-size: 12px; color: #888; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;">
          © 2025 CoRE Stack. All rights reserved.  
        </td>
      </tr>
    </table>
  </body>
</html>
        """

        recipients = (
            superuser_emails
            if superuser_emails
            else [
                "ankit.kumar@oniondev.com",
                "aman.verma@oniondev.com",
                "kapil.dadheech@gramvaani.org",
            ]
        )
        send_email_notification.delay(subject, "", message, recipients)
