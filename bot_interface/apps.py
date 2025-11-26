from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class BotInterfaceConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'bot_interface'

    def ready(self):
        """Import signals when the app is ready"""
        logger.info("BotInterfaceConfig.ready() called - registering signals")
        import bot_interface.signals
        logger.info("Signals imported successfully")
