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

import ssl
import socket
import logging
from django.core.mail import EmailMultiAlternatives, get_connection
from django.conf import settings

logger = logging.getLogger(__name__)


def send_email(
        subject: str,
        text_body: str,
        to_emails: list,
        html_body: str = None,
        attachments: list = None,
):
    """
    Send email via SMTP (supports HTML + plain text + attachments).

    Args:
        subject (str): Email subject
        text_body (str): Plain text body
        to_emails (list): List of recipient email addresses
        html_body (str, optional): HTML body (if provided, added as alternative)
        attachments (list, optional): List of attachments, each dict:
            {
                "filename": str,
                "content": bytes,
                "mimetype": str
            }
    """
    try:
        backend = get_connection(
            host=settings.EMAIL_HOST,
            port=settings.EMAIL_PORT,
            username=settings.EMAIL_HOST_USER,
            password=settings.EMAIL_HOST_PASSWORD,
            use_ssl=getattr(settings, "EMAIL_USE_SSL", False),
            use_tls=getattr(settings, "EMAIL_USE_TLS", False),
            timeout=getattr(settings, "EMAIL_TIMEOUT", 30),
            ssl_context=ssl.create_default_context(),
        )

        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to_emails,
            connection=backend,
        )

        # Add HTML part
        if html_body:
            msg.attach_alternative(html_body, "text/html")

        # Attach files
        if attachments:
            for attachment in attachments:
                filename = attachment.get("filename")
                content = attachment.get("content")
                mimetype = attachment.get("mimetype", "application/octet-stream")
                msg.attach(filename, content, mimetype)

        logger.info("Sending email to %s", ", ".join(to_emails))
        msg.send(fail_silently=False)
        logger.info("Email sent successfully.")

    except socket.error as e:
        logger.error(f"Socket error: {e}")
    except ssl.SSLError as e:
        logger.error(f"SSL error: {e}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
