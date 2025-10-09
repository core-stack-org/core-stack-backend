from django.db import models
from django.utils import timezone
from users.models import User
from bot_interface.interface.whatsapp import WhatsAppInterface
import json

APP_TYPE_CHOICES = [
    ("WA", "WhatsApp"),
    ("TG", "Telegram"),
    ("FB", "Facebook"),
]

LANGUAGE_CHOICES = [
    ("hi", "Hindi"),
    ("en", "English"),
    # Add more languages as needed
]


class FactoryInterface:
    """
    Factory pattern implementation for creating appropriate interfaces based on app type.
    """

    @staticmethod
    def build_interface(app_type):
        """
        Returns the appropriate interface instance based on app type.
        
        Args:
            app_type (str): The type of application ("WA", "TG", "FB")
            
        Returns:
            Interface: An instance of the appropriate interface
        """
        if app_type == "WA":
            return WhatsAppInterface()
        elif app_type == "TG":
            return TelegramInterface()
        elif app_type == "FB":
            return FacebookInterface()
        else:
            raise ValueError(f"Unsupported app type: {app_type}")


class SMJ(models.Model):
    """
    State Machine JSON model for storing state machine definitions for messaging flows.
    """
    name = models.CharField(max_length=20, help_text="Flow Name")
    desc = models.CharField(max_length=100, help_text="Description of flow")
    creation_time = models.DateTimeField(auto_now_add=True)
    last_updated_time = models.DateTimeField(auto_now=True)
    smj_json = models.JSONField(help_text="JSON containing state machine definition")

    def __str__(self):
        return self.name


class Bot(models.Model):
    """
    Configuration for bot application instances.
    """

    app_type = models.CharField(max_length=2, choices=APP_TYPE_CHOICES, help_text="Application platform")
    bot_name = models.CharField(max_length=32, help_text="Name of the bot")
    desc = models.CharField(max_length=100, help_text="Description of the bot")
    bot_number = models.CharField(max_length=12, help_text="Bot phone number")
    config_json = models.JSONField(help_text="Bot specific configuration", default=dict)
    is_active = models.BooleanField(default=True, help_text="Active status of the bot")
    smj = models.ForeignKey(SMJ, on_delete=models.CASCADE, help_text="Flow to start with")
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="hi", help_text="Language code")
    init_state = models.CharField(max_length=50, help_text="Initial state identifier")
    creation_at = models.DateTimeField(auto_now_add=True)
    last_updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.bot_name} {self.bot_number}"


class BotUsers(models.Model):
    """
    Bot Users model extending the Users model to accommodate bot-specific data.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="bot_users")
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="users")
    first_interaction_at = models.DateTimeField(auto_now_add=True)
    last_updated_at = models.DateTimeField(auto_now=True)
    user_misc = models.JSONField(default=dict, help_text="Miscellaneous user data")

    class Meta:
        verbose_name_plural = "Bot Users"
        unique_together = ('user', 'bot')

    def __str__(self):
        return f"{self.user.username} - {self.bot.bot_name}"


class UserSessions(models.Model):
    """
    Manages user information and session state.
    """
    APP_TYPE_CHOICES = [
        ("WA", "WhatsApp"),
        ("TG", "Telegram"),
        ("FB", "Facebook"),
    ]

    RESPONSE_TYPE_CHOICES = [
        ("text", "Text"),
        ("button", "Button"),
        ("media", "Media"),
        ("location", "Location"),
        ("contact", "Contact"),
        # Add more types as needed
    ]

    app_type = models.CharField(max_length=2, choices=APP_TYPE_CHOICES)
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="sessions")
    user = models.ForeignKey(BotUsers, on_delete=models.CASCADE, related_name="sessions")
    phone = models.CharField(max_length=15, help_text="User's mobile number")
    user_config = models.JSONField(default=dict, help_text="User specific configuration")
    started_at = models.DateTimeField(auto_now_add=True)
    last_updated_at = models.DateTimeField(auto_now=True)
    current_session = models.JSONField(default=dict)
    current_smj = models.ForeignKey(SMJ, on_delete=models.SET_NULL, null=True, blank=True)
    current_state = models.CharField(max_length=50, help_text="User's current state in the flow")
    expected_response_type = models.CharField(max_length=20, choices=RESPONSE_TYPE_CHOICES, default="text")
    misc_data = models.JSONField(default=dict, help_text="Miscellaneous session data")

    class Meta:
        verbose_name_plural = "User Sessions"

    def __str__(self):
        return f"{self.user.user.username} - {self.bot.bot_name} Session"


class UserLogs(models.Model):
    """
    Logs user interactions and events.
    """
    app_type = models.CharField(max_length=2, choices=APP_TYPE_CHOICES, help_text="Application platform")
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="logs")
    user = models.ForeignKey(BotUsers, on_delete=models.CASCADE, related_name="logs")
    key1 = models.CharField(max_length=50, blank=True, null=True)
    value1 = models.TextField(blank=True, null=True)
    key2 = models.CharField(max_length=50, blank=True, null=True)
    value2 = models.TextField(blank=True, null=True)
    key3 = models.CharField(max_length=50, blank=True, null=True)
    value3 = models.TextField(blank=True, null=True)
    key4 = models.CharField(max_length=50, blank=True, null=True)
    value4 = models.TextField(blank=True, null=True)
    misc = models.JSONField(default=dict, help_text="Additional log data beyond 4 parameters")
    smj = models.ForeignKey(SMJ, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "User Logs"

    def __str__(self):
        return f"{self.user.user.username} - {self.bot.bot_name} Log {self.id}"


class UserArchive(models.Model):
    """
    Archives user current session after user completed the flow and there is no continuation.
    """
    app_type = models.CharField(max_length=2, choices=APP_TYPE_CHOICES)
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, related_name="archives")
    user = models.ForeignKey(BotUsers, on_delete=models.CASCADE, related_name="archives")
    archived_at = models.DateTimeField(default=timezone.now)
    session_data = models.JSONField(help_text="Archived session data")

    def __str__(self):
        return f"{self.user.user.username} - {self.bot.bot_name} Archive {self.id}"
