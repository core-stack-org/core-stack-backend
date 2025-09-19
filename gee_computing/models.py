from django.db import models
from cryptography.fernet import Fernet
import environ
from nrm_app.settings import FERNET_KEY

# Create your models here.


class GEEAccount(models.Model):
    name = models.CharField(max_length=100, unique=True)
    service_account_email = models.EmailField()

    def __str__(self):
        return f"{self.name}"

    # store uploaded JSON temporarily
    credentials_file = models.FileField(upload_to="gee_uploads/", null=True, blank=True)

    # permanently store encrypted JSON inside DB
    credentials_encrypted = models.BinaryField(null=True, blank=True)

    def get_credentials(self):
        """Decrypt and return JSON key file content"""
        if self.credentials_encrypted:
            fernet = Fernet(FERNET_KEY)
            return fernet.decrypt(bytes(self.credentials_encrypted))  # 👈 cast to bytes
        return None