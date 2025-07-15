from django.db import models

# Create your models here.
# models for state, district and blocks
class State(models.Model):
    state_census_code = models.CharField(max_length=20, primary_key=True)
    state_name = models.CharField(max_length=100)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.state_name


class District(models.Model):
    id = models.AutoField(primary_key=True)
    district_census_code = models.CharField(max_length=20)
    district_name = models.CharField(max_length=100)
    state = models.ForeignKey(State, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.district_name


class Block(models.Model):
    id = models.AutoField(primary_key=True)
    block_name = models.CharField(max_length=100)
    block_census_code = models.CharField(max_length=20)
    district = models.ForeignKey(District, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.block_name


class State_SOI(models.Model):
    id = models.IntegerField(primary_key=True)
    state_name = models.CharField(max_length=255, unique=True)
    active_status = models.BooleanField(default=False)

    class Meta:
        verbose_name_plural = 'State_SOI'

    def __str__(self) -> str:
        return self.state_name

class District_SOI(models.Model):
    id = models.IntegerField(primary_key=True)
    district_name = models.CharField(max_length=255)
    state = models.ForeignKey(State_SOI, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)


    class Meta:
        verbose_name_plural = 'District_SOI'

    def __str__(self) -> str:
        return self.district_name

class Block_SOI(models.Model):
    id = models.IntegerField(primary_key=True)
    block_name = models.CharField(max_length=255)
    district = models.ForeignKey(District_SOI, on_delete=models.CASCADE)
    active_status = models.BooleanField(default=False)


    class Meta:
        verbose_name_plural = 'Block_SOI'

    def __str__(self) -> str:
        return self.block_name
