"""
Django signals for bot interface events
"""
import threading
import logging
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from .models import UserLogs
from .interface.whatsapp import WhatsAppInterface

logger = logging.getLogger(__name__)

# Add debug print to confirm signal registration
# print("bot_interface.signals module loaded - registering post_save signal")


def async_process_asset_demand(user_log_id):
    """
    Async function to process asset demand without blocking the main thread
    """
    try:
        # Initialize WhatsApp interface and process the asset demand
        whatsapp_interface = WhatsAppInterface()
        whatsapp_interface.process_and_submit_asset_demand(user_log_id)

        logger.info(f"Successfully processed asset demand for UserLogs ID: {user_log_id}")

    except Exception as e:
        logger.error(f"Error in async_process_asset_demand for UserLogs ID {user_log_id}: {e}")

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
def process_asset_demand_on_completion(sender, instance, created, **kwargs):
    """
    Signal handler to automatically process asset demand when UserLogs is created
    with asset_demand data.

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
    if (instance.key1 == "useraction" and 
        instance.value1 == "asset_demand" and 
        instance.misc and 
        "asset_demand_data" in instance.misc):
        
        logger.info(f"Asset demand completion detected for UserLogs ID: {instance.id}")
        
        # Process asynchronously to avoid blocking the SMJ flow
        thread = threading.Thread(
            target=async_process_asset_demand,
            args=(instance.id,),
            daemon=True
        )
        thread.start()
        logger.info(f"Started async processing thread for UserLogs ID: {instance.id}")
    else:
        logger.info(f"UserLogs ID {instance.id} does not match asset demand criteria: key1={instance.key1}, value1={instance.value1}, misc_keys={list(instance.misc.keys()) if instance.misc else None}")

def async_process_story(user_log_id):
    """
    Async function to process story without blocking the main thread
    """
    try:
        # Initialize WhatsApp interface and process the story
        whatsapp_interface = WhatsAppInterface()
        whatsapp_interface.process_and_submit_story(user_log_id)

        logger.info(f"Successfully processed story for UserLogs ID: {user_log_id}")

    except Exception as e:
        logger.error(f"Error in async_process_story for UserLogs ID {user_log_id}: {e}")

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
def process_story_on_completion(sender, instance, created, **kwargs):
    """
    Signal handler to automatically process asset demand when UserLogs is created
    with asset_demand data.

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
    if (instance.key1 == "useraction" and 
        instance.value1 == "story" and 
        instance.misc and 
        "story_data" in instance.misc):
        
        logger.info(f"Story completion detected for UserLogs ID: {instance.id}")
        
        # Process asynchronously to avoid blocking the SMJ flow
        thread = threading.Thread(
            target=async_process_story,
            args=(instance.id,),
            daemon=True
        )
        thread.start()
        logger.info(f"Started async processing thread for UserLogs ID: {instance.id}")
    else:
        logger.info(f"UserLogs ID {instance.id} does not match asset demand criteria: key1={instance.key1}, value1={instance.value1}, misc_keys={list(instance.misc.keys()) if instance.misc else None}")
