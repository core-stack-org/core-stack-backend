from nrm_app.celery import app
from utilities.mailutils import send_email
import logging

logger = logging.getLogger("geoadmin")


@app.task(bind=True)
def send_email_notification(self, subject, text_body, html_body='', to_emails=['support@core-stack.org'],
                            attachments=None):
    logger.info(f"sending email to {', '.join(to_emails)}")
    try:
        # Call the utility properly
        send_email(
            subject,
            text_body,  # fallback to html if text is empty
            to_emails,
            html_body,
            attachments=attachments
        )
        logger.info(f"Mail sent successfully to {', '.join(to_emails)}")
    except Exception as e:
        logger.error("Error while sending email", exc_info=True)
