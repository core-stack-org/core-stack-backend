from rest_framework import serializers


class DPRSummarySerializer(serializers.Serializer):
    plan_id = serializers.IntegerField()
    sections = serializers.DictField()


class TeamDetailsSerializer(serializers.Serializer):
    organization = serializers.CharField(allow_null=True)
    project = serializers.CharField(allow_null=True)
    plan = serializers.CharField()
    facilitator = serializers.CharField(allow_null=True)
    process = serializers.CharField()


class VillageBriefSerializer(serializers.Serializer):
    village_name = serializers.CharField()
    gram_panchayat = serializers.CharField()
    tehsil = serializers.CharField(allow_null=True)
    district = serializers.CharField(allow_null=True)
    state = serializers.CharField(allow_null=True)
    total_settlements = serializers.IntegerField()
    latitude = serializers.FloatField(allow_null=True)
    longitude = serializers.FloatField(allow_null=True)


class SettlementSerializer(serializers.Serializer):
    settlement_id = serializers.CharField()
    settlement_name = serializers.CharField()
    number_of_households = serializers.IntegerField()
    settlement_type = serializers.CharField(allow_null=True)
    caste_group_detail = serializers.CharField(allow_null=True)
    caste_counts = serializers.DictField(child=serializers.CharField(allow_null=True))
    marginal_farmers = serializers.CharField(allow_null=True)
    nrega_job_applied = serializers.IntegerField()
    nrega_job_card = serializers.IntegerField()
    nrega_work_days = serializers.IntegerField()
    nrega_past_work = serializers.CharField(allow_null=True)
    nrega_demand = serializers.CharField(allow_null=True)
    nrega_issues = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField(allow_null=True)
    longitude = serializers.FloatField(allow_null=True)


class CropSerializer(serializers.Serializer):
    crop_grid_id = serializers.CharField()
    beneficiary_settlement = serializers.CharField()
    irrigation_source = serializers.CharField(allow_null=True)
    land_classification = serializers.CharField(allow_null=True)
    kharif_crops = serializers.CharField(allow_null=True)
    kharif_acres = serializers.FloatField(allow_null=True)
    rabi_crops = serializers.CharField(allow_null=True)
    rabi_acres = serializers.FloatField(allow_null=True)
    zaid_crops = serializers.CharField(allow_null=True)
    zaid_acres = serializers.FloatField(allow_null=True)
    cropping_intensity = serializers.CharField(allow_null=True)


class LivestockSerializer(serializers.Serializer):
    settlement_id = serializers.CharField()
    settlement_name = serializers.CharField()
    goats = serializers.CharField(allow_null=True)
    sheep = serializers.CharField(allow_null=True)
    cattle = serializers.CharField(allow_null=True)
    piggery = serializers.CharField(allow_null=True)
    poultry = serializers.CharField(allow_null=True)


class WellSerializer(serializers.Serializer):
    well_id = serializers.CharField()
    beneficiary_settlement = serializers.CharField()
    well_type = serializers.CharField(allow_null=True)
    owner = serializers.CharField(allow_null=True)
    beneficiary_name = serializers.CharField(allow_null=True)
    beneficiary_father_name = serializers.CharField(allow_null=True)
    water_availability = serializers.CharField(allow_null=True)
    households_benefitted = serializers.IntegerField()
    caste_uses = serializers.CharField(allow_null=True)
    well_usage = serializers.CharField(allow_null=True)
    need_maintenance = serializers.CharField(allow_null=True)
    repair_activities = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()


class WaterbodySerializer(serializers.Serializer):
    waterbody_id = serializers.CharField()
    beneficiary_settlement = serializers.CharField()
    owner = serializers.CharField(allow_null=True)
    beneficiary_name = serializers.CharField(allow_null=True)
    beneficiary_father_name = serializers.CharField(allow_null=True)
    who_manages = serializers.CharField(allow_null=True)
    caste_who_uses = serializers.CharField(allow_null=True)
    households_benefitted = serializers.IntegerField()
    water_structure_type = serializers.CharField(allow_null=True)
    usage = serializers.CharField(allow_null=True)
    need_maintenance = serializers.CharField(allow_null=True)
    repair_activities = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()


class MaintenanceSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    demand_type = serializers.CharField(allow_null=True)
    beneficiary_settlement = serializers.CharField(allow_null=True)
    beneficiary_name = serializers.CharField(allow_null=True)
    beneficiary_father_name = serializers.CharField(allow_null=True)
    structure_type = serializers.CharField(allow_null=True)
    repair_activities = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()


class NRMWorkSerializer(serializers.Serializer):
    work_category = serializers.CharField()
    demand_type = serializers.CharField(allow_null=True)
    work_demand = serializers.CharField(allow_null=True)
    beneficiary_settlement = serializers.CharField(allow_null=True)
    beneficiary_name = serializers.CharField(allow_null=True)
    gender = serializers.CharField(allow_null=True)
    beneficiary_father_name = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField()
    longitude = serializers.FloatField()


class LivelihoodSerializer(serializers.Serializer):
    livelihood_work = serializers.CharField()
    demand_type = serializers.CharField(allow_null=True)
    work_demand = serializers.CharField(allow_null=True)
    beneficiary_settlement = serializers.CharField(allow_null=True)
    beneficiary_name = serializers.CharField(allow_null=True)
    gender = serializers.CharField(allow_null=True)
    beneficiary_father_name = serializers.CharField(allow_null=True)
    total_acres = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField(allow_null=True)
    longitude = serializers.FloatField(allow_null=True)
