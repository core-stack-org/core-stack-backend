# signals.py
from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import GEEAccount
from cryptography.fernet import Fernet
from django.conf import settings
import os
import environ
from nrm_app.settings import FERNET_KEY


@receiver(post_save, sender=GEEAccount)
def encrypt_credentials(sender, instance, created, **kwargs):
    # A later save of only the encrypted blob must not run this again.
    update_fields = kwargs.get("update_fields")
    if update_fields is not None and "credentials_file" not in update_fields:
        return
    if not instance.credentials_file:
        return
    path = instance.credentials_file.path
    if not os.path.exists(path):
        return

    with open(path, "rb") as f:
        raw_data = f.read()

    # Replace any previous ciphertext. Re-uploads were ignored once
    # credentials_encrypted was already set, so decrypt kept failing.
    encrypted = Fernet(FERNET_KEY).encrypt(raw_data)
    GEEAccount.objects.filter(pk=instance.pk).update(
        credentials_encrypted=encrypted,
        credentials_file="",
    )
    instance.credentials_encrypted = encrypted
    instance.credentials_file = None
    try:
        os.remove(path)
    except OSError:
        pass
