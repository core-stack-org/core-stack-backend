from django.db import models
from geoadmin.models import State, District, Block
from computing.models import MWS


# Create your models here.
class Plan(models.Model):
    plan_id = models.AutoField(primary_key=True)
    facilitator_name = models.CharField(max_length=300)
    plan = models.TextField(default="Default Plan")
    village_name = models.CharField(max_length=300)
    gram_panchayat = models.CharField(max_length=300)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    block = models.ForeignKey(Block, on_delete=models.CASCADE)
    
    def __str__(self):
        return str(self.plan)