from django.db import models
from users.models import User
from plans.models import PlanApp

from django.db.models import Max
from django.db.models.functions import Greatest


DPR_STATUS_CHOICES = [
    ("PENDING", "PENDING"),
    ("SUBMITTED", "SUBMITTED"),
    ("APPROVED", "APPROVED"),
    ("REVERTED", "REVERTED"),
    ("REJECTED", "REJECTED"),
]

DEMAND_STATUS_CHOICES = [
    ("PENDING", "PENDING"),
    ("SUBMITTED", "SUBMITTED"),
    ("APPROVED", "APPROVED"),
    ("REVERTED", "REVERTED"),
    ("REJECTED", "REJECTED"),
]


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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    settlement_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_settlements")

    class Meta:
        verbose_name = "Settlement"
        verbose_name_plural = "Settlements"
        db_table = "odk_settlement"

    def __str__(self) -> str:
        return self.settlement_name or self.settlement_id or "Unknown"


class ODK_well(models.Model):
    well_id = models.CharField(max_length=255, primary_key=True)
    uuid = models.TextField()
    submission_time = models.DateTimeField()
    beneficiary_settlement = models.TextField()
    block_name = models.TextField()
    owner = models.TextField()
    households_benefitted = models.IntegerField()
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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    well_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_wells")

    class Meta:
        verbose_name = "Well"
        verbose_name_plural = "Wells"
        db_table = "odk_well"

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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    waterbody_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_waterbodies")

    class Meta:
        verbose_name = "Waterbody Structure"
        verbose_name_plural = "Waterbody Structures"
        db_table = "odk_waterbody"

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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    recharge_structure_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_groundwater")

    class Meta:
        verbose_name = "Groundwater Structure"
        verbose_name_plural = "Groundwater Structures"
        db_table = "odk_groundwater"

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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    irrigation_work_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_agri")

    class Meta:
        verbose_name = "Irrigation Structure"
        verbose_name_plural = "Irrigation Structures"
        db_table = "odk_irrigation"

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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    crop_pattern_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_crops")

    class Meta:
        verbose_name = "Cropping Pattern"
        verbose_name_plural = "Cropping Patterns"
        db_table = "odk_crop"

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
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    livelihood_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_livelihoods")

    class Meta:
        verbose_name = "Livelihood"
        verbose_name_plural = "Livelihoods"
        db_table = "odk_livelihood"

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
    submission_time = models.DateTimeField(null=True, blank=True)
    data_gw_maintenance = models.JSONField(default=dict, null=True, blank=True)
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    recharge_structure_maintenance_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_gw_maintenance")

    class Meta:
        verbose_name = "Groundwater Maintenance"
        verbose_name_plural = "Groundwater Maintenance"
        db_table = "odk_gw_maintenance"

    def __str__(self) -> str:
        return self.uuid or str(self.gw_maintenance_id)


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
    submission_time = models.DateTimeField(null=True, blank=True)
    data_swb_rs_maintenance = models.JSONField(default=dict, null=True, blank=True)
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    swb_rs_maintenance_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_swb_rs_maintenance")

    class Meta:
        verbose_name = "SWB-RS Maintenance"
        verbose_name_plural = "SWB-RS Maintenance"
        db_table = "odk_swb_rs_maintenance"

    def __str__(self) -> str:
        return self.uuid or str(self.swb_rs_maintenance_id)


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
    submission_time = models.DateTimeField(null=True, blank=True)
    data_swb_maintenance = models.JSONField(default=dict, null=True, blank=True)
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    swb_maintenance_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_swb_maintenance")

    class Meta:
        verbose_name = "SWB Maintenance"
        verbose_name_plural = "SWB Maintenance"
        db_table = "odk_swb_maintenance"

    def __str__(self) -> str:
        return self.uuid or str(self.swb_maintenance_id)


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
    submission_time = models.DateTimeField(null=True, blank=True)
    data_agri_maintenance = models.JSONField(default=dict, null=True, blank=True)
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    irrigation_structure_maintenance_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_agri_maintenance")

    class Meta:
        verbose_name = "Agri Maintenance"
        verbose_name_plural = "Agri Maintenance"
        db_table = "odk_agri_maintenance"

    def __str__(self) -> str:
        return self.uuid or str(self.agri_maintenance_id)


class ODK_agrohorticulture(models.Model):
    agrohorticulture_id = models.AutoField(primary_key=True)
    uuid = models.CharField(max_length=255)
    plan_id = models.TextField()
    plan_name = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    status_re = models.TextField()
    data_agohorticulture = models.JSONField(default=dict, null=True, blank=True)
    is_moderated = models.BooleanField(default=False, blank=True, null=True)
    moderated_at = models.DateTimeField(null=True, blank=True)
    moderated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    moderation_reason = models.TextField(null=True, blank=True)
    moderation_bookmark = models.BooleanField(default=False, blank=True, null=True)
    agrohorticulture_demand_status = models.CharField(max_length=255, choices=DEMAND_STATUS_CHOICES, default="PENDING")
    data_before_moderation = models.JSONField(default=dict, null=True, blank=True)
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="deleted_agrohorticulture")

    class Meta:
        verbose_name = "Agrohorticulture"
        verbose_name_plural = "Agrohorticulture"
        db_table = "odk_agrohorticulture"

    def __str__(self) -> str:
        return str(self.agrohorticulture_id)


class Overpass_Block_Details(models.Model):
    block_details_id = models.AutoField(primary_key=True)
    location = models.TextField(max_length=511, null=False)
    overpass_response = models.JSONField(null=True, blank=True)

    class Meta:
        verbose_name = "Overpass Block Details"
        verbose_name_plural = "Overpass Block Details"
        db_table = "overpass_block_details"


class DPR_Report(models.Model):
    dpr_report_id = models.AutoField(primary_key=True)
    plan_id = models.ForeignKey(PlanApp, on_delete=models.CASCADE)
    plan_name = models.TextField()
    dpr_report_s3_url = models.TextField(null=True, blank=True)
    dpr_generated_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="dpr_report_created_by")
    status = models.CharField(max_length=255, choices=DPR_STATUS_CHOICES, default="PENDING")
    last_updated_at = models.DateTimeField(null=True, blank=True)
    last_updated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="dpr_report_last_updated_by")

    class Meta:
        verbose_name = "DPR Report"
        verbose_name_plural = "DPR Reports"
        db_table = "dpr_report"

    def __str__(self) -> str:
        return f"{self.plan_name} - {self.dpr_report_id}"

    @staticmethod
    def get_latest_change_time(plan_id):
        pid = str(plan_id)
        all_models = [
            ODK_settlement, ODK_well, ODK_waterbody,
            ODK_groundwater, ODK_agri, ODK_crop, ODK_livelihood,
            GW_maintenance, SWB_RS_maintenance, SWB_maintenance, Agri_maintenance,
        ]
        times = []
        for m in all_models:
            agg = m.objects.filter(plan_id=pid).aggregate(
                latest_submission=Max('submission_time'),
                latest_deletion=Max('deleted_at'),
            )
            if agg['latest_submission']:
                times.append(agg['latest_submission'])
            if agg['latest_deletion']:
                times.append(agg['latest_deletion'])
        return max(times) if times else None

    def needs_regeneration(self):
        if not self.dpr_generated_at or not self.dpr_report_s3_url:
            return True
        latest_change = self.get_latest_change_time(self.plan_id_id)
        if not latest_change:
            return False
        return latest_change > self.dpr_generated_at