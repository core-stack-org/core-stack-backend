# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AuthGroup(models.Model):
    id = models.BigIntegerField(primary_key=True)
    name = models.TextField(unique=True, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    id = models.BigIntegerField(primary_key=True)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING, blank=True, null=True)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField(blank=True, null=True)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    codename = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    id = models.BigAutoField(primary_key=True)
    password = models.TextField(blank=True, null=True)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.SmallIntegerField(blank=True, null=True)
    username = models.TextField(unique=True, blank=True, null=True)
    first_name = models.TextField(blank=True, null=True)
    last_name = models.TextField(blank=True, null=True)
    email = models.TextField(blank=True, null=True)
    is_staff = models.SmallIntegerField(blank=True, null=True)
    is_active = models.SmallIntegerField(blank=True, null=True)
    date_joined = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    id = models.BigIntegerField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING, blank=True, null=True)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    id = models.BigIntegerField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING, blank=True, null=True)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class DjangoAdminLog(models.Model):
    id = models.BigAutoField(primary_key=True)
    action_time = models.DateTimeField(blank=True, null=True)
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.TextField(blank=True, null=True)
    action_flag = models.SmallIntegerField(blank=True, null=True)
    change_message = models.TextField(blank=True, null=True)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    id = models.BigAutoField(primary_key=True)
    app_label = models.TextField(blank=True, null=True)
    model = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    id = models.BigAutoField(primary_key=True)
    app = models.TextField(blank=True, null=True)
    name = models.TextField(blank=True, null=True)
    applied = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.TextField(primary_key=True)
    session_data = models.TextField(blank=True, null=True)
    expire_date = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'django_session'


class DprAgriMaintenance(models.Model):
    agri_maintenance_id = models.BigAutoField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    work_id = models.TextField(blank=True, null=True)
    corresponding_work_id = models.TextField(blank=True, null=True)
    data_agri_maintenance = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_agri_maintenance'


class DprGwMaintenance(models.Model):
    gw_maintenance_id = models.BigAutoField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    work_id = models.TextField(blank=True, null=True)
    corresponding_work_id = models.TextField(blank=True, null=True)
    data_gw_maintenance = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_gw_maintenance'


class DprOdkAgri(models.Model):
    irrigation_work_id = models.TextField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    work_type = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    work_dimensions = models.TextField(blank=True, null=True)
    data_agri = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_agri'


class DprOdkCrop(models.Model):
    crop_grid_id = models.TextField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    irrigation_source = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    land_classification = models.TextField(blank=True, null=True)
    cropping_patterns_kharif = models.TextField(blank=True, null=True)
    cropping_patterns_rabi = models.TextField(blank=True, null=True)
    cropping_patterns_zaid = models.TextField(blank=True, null=True)
    agri_productivity = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    data_crop = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_crop'


class DprOdkGroundwater(models.Model):
    recharge_structure_id = models.TextField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    work_type = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    work_dimensions = models.TextField(blank=True, null=True)
    data_groundwater = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_groundwater'


class DprOdkLivelihood(models.Model):
    livelihood_id = models.BigAutoField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    beneficiary_contact = models.TextField(blank=True, null=True)
    livestock_development = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    fisheries = models.TextField(blank=True, null=True)
    common_asset = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    data_livelihood = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_livelihood'


class DprOdkSettlement(models.Model):
    settlement_id = models.TextField(primary_key=True)
    settlement_name = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    submitted_by = models.TextField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    number_of_households = models.BigIntegerField(blank=True, null=True)
    largest_caste = models.TextField(blank=True, null=True)
    smallest_caste = models.TextField(blank=True, null=True)
    settlement_status = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    uuid = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    farmer_family = models.TextField(blank=True, null=True)
    livestock_census = models.TextField(blank=True, null=True)
    nrega_job_aware = models.BigIntegerField(blank=True, null=True)
    nrega_job_applied = models.BigIntegerField(blank=True, null=True)
    nrega_job_card = models.BigIntegerField(blank=True, null=True)
    nrega_without_job_card = models.BigIntegerField(blank=True, null=True)
    nrega_work_days = models.BigIntegerField(blank=True, null=True)
    nrega_past_work = models.TextField(blank=True, null=True)
    nrega_raise_demand = models.TextField(blank=True, null=True)
    nrega_demand = models.TextField(blank=True, null=True)
    nrega_issues = models.TextField(blank=True, null=True)
    nrega_community = models.TextField(blank=True, null=True)
    data_settlement = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_settlement'


class DprOdkWaterbody(models.Model):
    waterbody_id = models.TextField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    beneficiary_contact = models.TextField(blank=True, null=True)
    who_manages = models.TextField(blank=True, null=True)
    specify_other_manager = models.TextField(blank=True, null=True)
    owner = models.TextField(blank=True, null=True)
    caste_who_uses = models.TextField(blank=True, null=True)
    household_benefitted = models.BigIntegerField(blank=True, null=True)
    water_structure_type = models.TextField(blank=True, null=True)
    water_structure_other = models.TextField(blank=True, null=True)
    water_structure_dimension = models.TextField(blank=True, null=True)
    identified_by = models.TextField(blank=True, null=True)
    need_maintenance = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    data_waterbody = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_waterbody'


class DprOdkWell(models.Model):
    well_id = models.TextField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    submission_time = models.DateTimeField(blank=True, null=True)
    beneficiary_settlement = models.TextField(blank=True, null=True)
    block_name = models.TextField(blank=True, null=True)
    owner = models.TextField(blank=True, null=True)
    households_benefitted = models.BigIntegerField(blank=True, null=True)
    caste_uses = models.TextField(blank=True, null=True)
    is_functional = models.TextField(blank=True, null=True)
    need_maintenance = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    system = models.TextField(blank=True, null=True)
    gps_point = models.TextField(blank=True, null=True)
    data_well = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_odk_well'


class DprSwbMaintenance(models.Model):
    swb_maintenance_id = models.BigAutoField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    work_id = models.TextField(blank=True, null=True)
    corresponding_work_id = models.TextField(blank=True, null=True)
    data_swb_maintenance = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_swb_maintenance'


class DprSwbRsMaintenance(models.Model):
    swb_rs_maintenance_id = models.BigAutoField(primary_key=True)
    uuid = models.TextField(blank=True, null=True)
    plan_id = models.TextField(blank=True, null=True)
    plan_name = models.TextField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)
    status_re = models.TextField(blank=True, null=True)
    work_id = models.TextField(blank=True, null=True)
    corresponding_work_id = models.TextField(blank=True, null=True)
    data_swb_rs_maintenance = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dpr_swb_rs_maintenance'


class GeoadminBlock(models.Model):
    id = models.BigAutoField(primary_key=True)
    block_name = models.TextField(blank=True, null=True)
    block_census_code = models.TextField(blank=True, null=True)
    active_status = models.SmallIntegerField(blank=True, null=True)
    district = models.ForeignKey('GeoadminDistrict', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'geoadmin_block'


class GeoadminDistrict(models.Model):
    id = models.BigAutoField(primary_key=True)
    district_census_code = models.TextField(blank=True, null=True)
    district_name = models.TextField(blank=True, null=True)
    active_status = models.SmallIntegerField(blank=True, null=True)
    state = models.ForeignKey('GeoadminState', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'geoadmin_district'


class GeoadminState(models.Model):
    state_census_code = models.TextField(primary_key=True)
    state_name = models.TextField(blank=True, null=True)
    active_status = models.SmallIntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'geoadmin_state'


class OrganizationOrganization(models.Model):
    id = models.UUIDField(primary_key=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    created_by = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField()
    updated_by = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'organization_organization'


class PlansPlan(models.Model):
    plan_id = models.BigAutoField(primary_key=True)
    facilitator_name = models.TextField(blank=True, null=True)
    plan = models.TextField(blank=True, null=True)
    village_name = models.TextField(blank=True, null=True)
    gram_panchayat = models.TextField(blank=True, null=True)
    block = models.ForeignKey(GeoadminBlock, models.DO_NOTHING, blank=True, null=True)
    district = models.ForeignKey(GeoadminDistrict, models.DO_NOTHING, blank=True, null=True)
    state = models.ForeignKey(GeoadminState, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'plans_plan'


class PlantationsKmlfile(models.Model):
    name = models.CharField(max_length=255)
    file = models.CharField(max_length=100)
    kml_hash = models.CharField(unique=True, max_length=64)
    created_at = models.DateTimeField()
    uploaded_by = models.ForeignKey('UsersUser', models.DO_NOTHING, blank=True, null=True)
    project = models.ForeignKey('ProjectsProject', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'plantations_kmlfile'


class ProjectsPlantationprofile(models.Model):
    profile_id = models.AutoField(primary_key=True)
    config = models.JSONField()
    created_at = models.DateTimeField()
    modified_at = models.DateTimeField()
    project_app = models.ForeignKey('ProjectsProjectapp', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'projects_plantationprofile'


class ProjectsProject(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    geojson_path = models.CharField(max_length=512, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    created_by = models.ForeignKey('UsersUser', models.DO_NOTHING)
    organization = models.ForeignKey(OrganizationOrganization, models.DO_NOTHING)
    updated_by = models.ForeignKey('UsersUser', models.DO_NOTHING, related_name='projectsproject_updated_by_set')

    class Meta:
        managed = False
        db_table = 'projects_project'


class ProjectsProjectapp(models.Model):
    app_type = models.CharField(max_length=50)
    enabled = models.BooleanField()
    project = models.ForeignKey(ProjectsProject, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'projects_projectapp'
        unique_together = (('project', 'app_type'),)


class UsersUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150)
    first_name = models.CharField(max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()
    contact_number = models.CharField(max_length=20, blank=True, null=True)
    is_superadmin = models.BooleanField()
    organization = models.ForeignKey(OrganizationOrganization, models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'users_user'


class UsersUserGroups(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(UsersUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'users_user_groups'
        unique_together = (('user', 'group'),)


class UsersUserUserPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(UsersUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'users_user_user_permissions'
        unique_together = (('user', 'permission'),)


class UsersUserprojectgroup(models.Model):
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    project = models.ForeignKey(ProjectsProject, models.DO_NOTHING)
    user = models.ForeignKey(UsersUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'users_userprojectgroup'
        unique_together = (('user', 'project'),)
