from django.db import models

# Create your models here.

# create a dummy MWS model with mws id
class MWS:
    id = models.IntegerField(primary_key=True)
