import ssl
import socket
from django.core.mail import EmailMessage, get_connection
from django.core.mail.backends.smtp import EmailBackend
import logging

from nrm_app.settings import (
    EMAIL_HOST,
    EMAIL_HOST_PASSWORD,
    EMAIL_HOST_USER,
    EMAIL_PORT,
    EMAIL_TIMEOUT,
    EMAIL_USE_SSL,
)

logger = logging.getLogger(__name__)

def send_email(subject: str, body: str, to_emails: list, attachments: list = None):
    """
    Send email via SMTP.

    Args:
        subject (str): Email subject
        body (str): Email body (plain text)
        to_emails (list): List of recipient email addresses
        attachments (list): Optional list of attachments, each as dict:
            {
                "filename": str,
                "content": bytes,
                "mimetype": str
            }
    """
    try:
        backend = EmailBackend(
            host=EMAIL_HOST,
            port=EMAIL_PORT,
            username=EMAIL_HOST_USER,
            password=EMAIL_HOST_PASSWORD,
            use_ssl=EMAIL_USE_SSL,
            timeout=EMAIL_TIMEOUT,
            ssl_context=ssl.create_default_context(),
        )

        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=EMAIL_HOST_USER,
            to=to_emails,
            connection=backend,
        )

        # Attach files if provided
        if attachments:
            for attachment in attachments:
                filename = attachment.get("filename")
                content = attachment.get("content")
                mimetype = attachment.get("mimetype", "application/octet-stream")
                email.attach(filename, content, mimetype)

        logger.info("Sending email to %s", ", ".join(to_emails))
        email.send(fail_silently=False)
        logger.info("Email sent successfully.")
        backend.close()

    except socket.error as e:
        logger.error(f"Socket error: {e}")
    except ssl.SSLError as e:
        logger.error(f"SSL error: {e}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
