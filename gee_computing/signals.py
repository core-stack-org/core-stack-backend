# signals.py
from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import GEEAccount
from cryptography.fernet import Fernet
from django.conf import settings
import os
import environ
from nrm_app.settings import  FERNET_KEY






@receiver(post_save, sender=GEEAccount)
def encrypt_credentials(sender, instance, created, **kwargs):
    if instance.credentials_file and not instance.credentials_encrypted:
        # Read file content
        with open(instance.credentials_file.path, "rb") as f:
            raw_data = f.read()

        # Encrypt and save
        fernet = Fernet(FERNET_KEY)
        instance.credentials_encrypted = fernet.encrypt(raw_data)
        instance.save(update_fields=["credentials_encrypted"])

        # (Optional) delete original uploaded file
        os.remove(instance.credentials_file.path)
        instance.credentials_file.delete(save=False)
