from django.db import models


class ODK_settlement(models.Model):
    settlement_id = models.CharField(max_length=255, primary_key=True)
    settlement_name = models.TextField()
    submission_time = models.DateTimeField()
    submitted_by = models.TextField()
    status_re = models.TextField()
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    block_name = models.TextField()
    number_of_households = models.IntegerField()
    largest_caste = models.TextField()
    smallest_caste = models.TextField()
    settlement_status = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    uuid = models.TextField()
    system = models.JSONField(default=dict)
    gps_point = models.JSONField(default=dict)
    farmer_family = models.JSONField()
    livestock_census = models.JSONField()
    nrega_job_aware = models.IntegerField()
    nrega_job_applied = models.IntegerField()
    nrega_job_card = models.IntegerField(default=0)
    nrega_without_job_card = models.IntegerField(default=0)
    nrega_work_days = models.IntegerField(default=0)
    nrega_past_work = models.TextField()
    nrega_raise_demand = models.TextField()
    nrega_demand = models.TextField()
    nrega_issues = models.TextField()
    nrega_community = models.TextField()
    data_settlement = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.settlement_name


class ODK_well(models.Model):
    well_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField()
    submission_time = models.DateTimeField()
    beneficiary_settlement = models.TextField()
    block_name = models.TextField()
    owner = models.TextField()
    households_benefitted = models.BigIntegerField()
    caste_uses = models.TextField()
    is_functional = models.TextField()
    need_maintenance = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    status_re = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    system = models.JSONField(default=dict)
    gps_point = models.JSONField(default=dict)
    data_well = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.well_id


class ODK_waterbody(models.Model):
    waterbody_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField()
    submission_time = models.DateTimeField()
    block_name = models.TextField()
    beneficiary_settlement = models.TextField()
    beneficiary_contact = models.TextField()
    who_manages = models.TextField()
    specify_other_manager = models.TextField()
    owner = models.TextField()
    caste_who_uses = models.TextField()
    household_benefitted = models.IntegerField()
    water_structure_type = models.TextField()
    water_structure_other = models.TextField()
    water_structure_dimension = models.JSONField(default=dict)
    identified_by = models.TextField()
    need_maintenance = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    system = models.JSONField(default=dict)
    gps_point = models.JSONField(default=dict)
    data_waterbody = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.waterbody_id


class ODK_groundwater(models.Model):
    recharge_structure_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField()
    submission_time = models.DateTimeField()
    beneficiary_settlement = models.TextField()
    block_name = models.TextField()
    work_type = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    system = models.JSONField()
    gps_point = models.JSONField()
    work_dimensions = models.JSONField(default=dict)
    data_groundwater = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.recharge_structure_id

    def update_work_dimensions(self, work_type, work_details):
        work_dimensions = self.work_dimensions.copy()
        work_dimensions[work_type] = work_details
        self.work_dimensions = work_dimensions
        self.save()


class ODK_agri(models.Model):
    irrigation_work_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField()
    submission_time = models.DateTimeField()
    beneficiary_settlement = models.TextField()
    block_name = models.TextField()
    work_type = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    system = models.JSONField()
    gps_point = models.JSONField()
    work_dimensions = models.JSONField(default=dict)
    data_agri = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.irrigation_work_id

    def update_work_dimensions(self, work_type, work_details):
        work_dimensions = self.work_dimensions.copy()
        work_dimensions[work_type] = work_details
        self.work_dimensions = work_dimensions
        self.save()


class ODK_crop(models.Model):
    crop_grid_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField(max_length=255)
    beneficiary_settlement = models.TextField()
    irrigation_source = models.TextField()
    submission_time = models.DateTimeField()
    land_classification = models.TextField()
    cropping_patterns_kharif = models.TextField()
    cropping_patterns_rabi = models.TextField()
    cropping_patterns_zaid = models.TextField()
    agri_productivity = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    status_re = models.TextField()
    system = models.JSONField()
    data_crop = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.crop_grid_id


class ODK_livelihood(models.Model):
    livelihood_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=42)
    beneficiary_settlement = models.TextField()
    block_name = models.TextField()
    beneficiary_contact = models.TextField()
    livestock_development = models.TextField()
    submission_time = models.DateTimeField()
    fisheries = models.TextField()
    common_asset = models.TextField()
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    system = models.JSONField()
    gps_point = models.JSONField()
    data_livelihood = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self) -> str:
        return self.livelihood_id


class GW_maintenance(models.Model):
    gw_maintenance_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=255)
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    work_id = models.CharField(max_length=255)
    corresponding_work_id = models.CharField(max_length=255)
    data_gw_maintenance = models.JSONField(default=dict, null=True, blank=True)


class SWB_RS_maintenance(models.Model):
    swb_rs_maintenance_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=255)
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    work_id = models.CharField(max_length=255)
    corresponding_work_id = models.CharField(max_length=255)
    data_swb_rs_maintenance = models.JSONField(default=dict, null=True, blank=True)


class SWB_maintenance(models.Model):
    swb_maintenance_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=255)
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    work_id = models.CharField(max_length=255)
    corresponding_work_id = models.CharField(max_length=255)
    data_swb_maintenance = models.JSONField(default=dict, null=True, blank=True)


class Agri_maintenance(models.Model):
    agri_maintenance_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=255)
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    work_id = models.CharField(max_length=255)
    corresponding_work_id = models.CharField(max_length=255)
    data_agri_maintenance = models.JSONField(default=dict, null=True, blank=True)
