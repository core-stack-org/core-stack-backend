"""
Django signals for bot interface events
"""

import threading
import logging
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from community_engagement.api import item_type
from .models import UserLogs
from .interface.whatsapp import WhatsAppInterface
from .tasks import process_and_submit_work_demand

logger = logging.getLogger(__name__)


# Add debug print to confirm signal registration
# print("bot_interface.signals module loaded - registering post_save signal")


def async_process_work_demand(user_log_id, item_type="Asset Demand"):
    """
    Async function to process work demand without blocking the main thread
    """
    try:
        print(f"Generating demand for item type {item_type}")
        # Initialize WhatsApp interface and process the work demand
        whatsapp_interface = WhatsAppInterface()
        whatsapp_interface.process_and_submit_work_demand(user_log_id)

        logger.info(
            f"Successfully processed work demand for UserLogs ID: {user_log_id}"
        )

    except Exception as e:
        logger.error(
            f"Error in async_process_work_demand for UserLogs ID {user_log_id}: {e}"
        )

        # Update UserLogs with error status
        try:
            user_log = UserLogs.objects.get(id=user_log_id)
            user_log.key2 = "upload"
            user_log.value2 = "failure"
            user_log.key3 = "retries"
            user_log.value3 = "0"
            user_log.key4 = "error"
            user_log.value4 = str(e)
            user_log.save()
        except Exception as update_error:
            logger.error(f"Failed to update UserLogs with error status: {update_error}")


@receiver(post_save, sender=UserLogs)
def process_work_demand_on_completion(sender, instance, created, **kwargs):
    """
    Signal handler to automatically process work demand when UserLogs is created
    with work_demand data.

    Args:
        sender: The UserLogs model class
        instance: The UserLogs instance that was saved
        created: Boolean indicating if this is a new record
    """
    # Add debug logging to track signal firing
    logger.info(f"Signal fired for UserLogs ID: {instance.id}, created: {created}")

    if not created:
        logger.info(f"Skipping non-new record for UserLogs ID: {instance.id}")
        return  # Only process new records

    # Check if this is a work demand completion log
    if (
        instance.key1 == "useraction"
        and instance.value1 == "work_demand"
        and instance.misc
        and "flow_data" in instance.misc
    ):

        logger.info(f"Work demand completion detected for UserLogs ID: {instance.id}")
        process_and_submit_work_demand.apply_async(
            kwargs={"user_log_id": instance.id},
            queue="whatsapp",
        )
        logger.info(f"Started async processing thread for UserLogs ID: {instance.id}")

    else:

        logger.info(
            f"UserLogs ID {instance.id} does not match work demand criteria: key1={instance.key1}, value1={instance.value1}, misc_keys={list(instance.misc.keys()) if instance.misc else None}"
        )
