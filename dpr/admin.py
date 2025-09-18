from django.contrib import admin

from .models import (
    Agri_maintenance,
    GW_maintenance,
    ODK_agri,
    ODK_crop,
    ODK_groundwater,
    ODK_livelihood,
    ODK_settlement,
    ODK_waterbody,
    ODK_well,
    SWB_maintenance,
    SWB_RS_maintenance,
)


@admin.register(ODK_settlement)
class ODKSettlementAdmin(admin.ModelAdmin):
    list_display = [
        "settlement_id",
        "settlement_name",
        "block_name",
        "number_of_households",
        "settlement_status",
        "submission_time",
    ]
    list_filter = [
        "block_name",
        "settlement_status",
        "status_re",
        "largest_caste",
        "plan_id",
        "plan_name",
    ]
    search_fields = ["settlement_name", "settlement_id", "block_name", "submitted_by"]
    readonly_fields = [
        "uuid",
        "system",
        "gps_point",
        "farmer_family",
        "livestock_census",
    ]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "settlement_id",
                    "settlement_name",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Demographics",
            {
                "fields": (
                    "number_of_households",
                    "largest_caste",
                    "smallest_caste",
                    "settlement_status",
                )
            },
        ),
        (
            "NREGA Information",
            {
                "fields": (
                    "nrega_job_aware",
                    "nrega_job_applied",
                    "nrega_job_card",
                    "nrega_without_job_card",
                    "nrega_work_days",
                    "nrega_past_work",
                    "nrega_raise_demand",
                    "nrega_demand",
                    "nrega_issues",
                    "nrega_community",
                ),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": (
                    "submission_time",
                    "submitted_by",
                    "status_re",
                    "uuid",
                    "system",
                ),
                "classes": ("collapse",),
            },
        ),
        (
            "Data",
            {
                "fields": ("farmer_family", "livestock_census", "data_settlement"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_well)
class ODKWellAdmin(admin.ModelAdmin):
    list_display = [
        "well_id",
        "beneficiary_settlement",
        "block_name",
        "owner",
        "households_benefitted",
        "is_functional",
        "submission_time",
    ]
    list_filter = [
        "block_name",
        "is_functional",
        "need_maintenance",
        "caste_uses",
        "status_re",
        "plan_id",
        "plan_name",
    ]
    search_fields = ["well_id", "beneficiary_settlement", "owner", "block_name"]
    readonly_fields = ["uuid", "system", "gps_point"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "well_id",
                    "beneficiary_settlement",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        (
            "Owner and Usage",
            {"fields": ("owner", "households_benefitted", "caste_uses")},
        ),
        ("Status", {"fields": ("is_functional", "need_maintenance", "status_re")}),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_well"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_waterbody)
class ODKWaterbodyAdmin(admin.ModelAdmin):
    list_display = [
        "waterbody_id",
        "beneficiary_settlement",
        "block_name",
        "water_structure_type",
        "household_benefitted",
        "who_manages",
        "submission_time",
    ]
    list_filter = [
        "block_name",
        "water_structure_type",
        "who_manages",
        "need_maintenance",
        "status_re",
        "plan_id",
        "plan_name",
    ]
    search_fields = [
        "waterbody_id",
        "beneficiary_settlement",
        "block_name",
        "beneficiary_contact",
    ]
    readonly_fields = ["uuid", "system", "gps_point", "water_structure_dimension"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "waterbody_id",
                    "beneficiary_settlement",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        (
            "Structure Details",
            {
                "fields": (
                    "water_structure_type",
                    "water_structure_other",
                    "water_structure_dimension",
                )
            },
        ),
        (
            "Management",
            {
                "fields": (
                    "who_manages",
                    "specify_other_manager",
                    "owner",
                    "identified_by",
                )
            },
        ),
        (
            "Usage",
            {
                "fields": (
                    "household_benefitted",
                    "caste_who_uses",
                    "beneficiary_contact",
                )
            },
        ),
        ("Status", {"fields": ("need_maintenance", "status_re")}),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_waterbody"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_groundwater)
class ODKGroundwaterAdmin(admin.ModelAdmin):
    list_display = [
        "recharge_structure_id",
        "beneficiary_settlement",
        "block_name",
        "work_type",
        "submission_time",
    ]
    list_filter = ["block_name", "work_type", "status_re", "plan_id", "plan_name"]
    search_fields = ["recharge_structure_id", "beneficiary_settlement", "block_name"]
    readonly_fields = ["uuid", "system", "gps_point", "work_dimensions"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "recharge_structure_id",
                    "beneficiary_settlement",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Work Details", {"fields": ("work_type", "work_dimensions")}),
        ("Status", {"fields": ("status_re",)}),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_groundwater"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_agri)
class ODKAgriAdmin(admin.ModelAdmin):
    list_display = [
        "irrigation_work_id",
        "beneficiary_settlement",
        "block_name",
        "work_type",
        "submission_time",
    ]
    list_filter = ["block_name", "work_type", "status_re", "plan_id", "plan_name"]
    search_fields = ["irrigation_work_id", "beneficiary_settlement", "block_name"]
    readonly_fields = ["uuid", "system", "gps_point", "work_dimensions"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "irrigation_work_id",
                    "beneficiary_settlement",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Work Details", {"fields": ("work_type", "work_dimensions")}),
        ("Status", {"fields": ("status_re",)}),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_agri"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_crop)
class ODKCropAdmin(admin.ModelAdmin):
    list_display = [
        "crop_grid_id",
        "beneficiary_settlement",
        "land_classification",
        "irrigation_source",
        "submission_time",
    ]
    list_filter = [
        "land_classification",
        "irrigation_source",
        "status_re",
        "plan_id",
        "plan_name",
    ]
    search_fields = ["crop_grid_id", "beneficiary_settlement"]
    readonly_fields = ["uuid", "system"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "crop_grid_id",
                    "beneficiary_settlement",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        (
            "Land and Irrigation",
            {
                "fields": (
                    "land_classification",
                    "irrigation_source",
                    "agri_productivity",
                )
            },
        ),
        (
            "Cropping Patterns",
            {
                "fields": (
                    "cropping_patterns_kharif",
                    "cropping_patterns_rabi",
                    "cropping_patterns_zaid",
                )
            },
        ),
        ("Status", {"fields": ("status_re",)}),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_crop"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(ODK_livelihood)
class ODKLivelihoodAdmin(admin.ModelAdmin):
    list_display = [
        "livelihood_id",
        "beneficiary_settlement",
        "block_name",
        "livestock_development",
        "fisheries",
        "submission_time",
    ]
    list_filter = [
        "block_name",
        "livestock_development",
        "fisheries",
        "common_asset",
        "status_re",
        "plan_id",
        "plan_name",
    ]
    search_fields = ["beneficiary_settlement", "block_name", "beneficiary_contact"]
    readonly_fields = ["uuid", "system", "gps_point"]
    ordering = ["-submission_time"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "livelihood_id",
                    "beneficiary_settlement",
                    "block_name",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        (
            "Livelihood Activities",
            {"fields": ("livestock_development", "fisheries", "common_asset")},
        ),
        ("Contact", {"fields": ("beneficiary_contact",)}),
        ("Status", {"fields": ("status_re",)}),
        (
            "Location",
            {
                "fields": ("latitude", "longitude", "gps_point"),
                "classes": ("collapse",),
            },
        ),
        (
            "Metadata",
            {
                "fields": ("submission_time", "uuid", "system", "data_livelihood"),
                "classes": ("collapse",),
            },
        ),
    )


@admin.register(GW_maintenance)
class GWMaintenanceAdmin(admin.ModelAdmin):
    list_display = [
        "gw_maintenance_id",
        "work_id",
        "corresponding_work_id",
        "plan_name",
        "status_re",
    ]
    list_filter = ["status_re", "plan_id", "plan_name"]
    search_fields = ["work_id", "corresponding_work_id", "plan_name", "uuid"]
    readonly_fields = ["uuid"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "gw_maintenance_id",
                    "work_id",
                    "corresponding_work_id",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Status", {"fields": ("status_re",)}),
        ("Location", {"fields": ("latitude", "longitude")}),
        (
            "Metadata",
            {"fields": ("uuid", "data_gw_maintenance"), "classes": ("collapse",)},
        ),
    )


@admin.register(SWB_RS_maintenance)
class SWBRSMaintenanceAdmin(admin.ModelAdmin):
    list_display = [
        "swb_rs_maintenance_id",
        "work_id",
        "corresponding_work_id",
        "plan_name",
        "status_re",
    ]
    list_filter = ["status_re", "plan_id", "plan_name"]
    search_fields = ["work_id", "corresponding_work_id", "plan_name", "uuid"]
    readonly_fields = ["uuid"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "swb_rs_maintenance_id",
                    "work_id",
                    "corresponding_work_id",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Status", {"fields": ("status_re",)}),
        ("Location", {"fields": ("latitude", "longitude")}),
        (
            "Metadata",
            {"fields": ("uuid", "data_swb_rs_maintenance"), "classes": ("collapse",)},
        ),
    )


@admin.register(SWB_maintenance)
class SWBMaintenanceAdmin(admin.ModelAdmin):
    list_display = [
        "swb_maintenance_id",
        "work_id",
        "corresponding_work_id",
        "plan_name",
        "status_re",
    ]
    list_filter = ["status_re", "plan_id", "plan_name"]
    search_fields = ["work_id", "corresponding_work_id", "plan_name", "uuid"]
    readonly_fields = ["uuid"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "swb_maintenance_id",
                    "work_id",
                    "corresponding_work_id",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Status", {"fields": ("status_re",)}),
        ("Location", {"fields": ("latitude", "longitude")}),
        (
            "Metadata",
            {"fields": ("uuid", "data_swb_maintenance"), "classes": ("collapse",)},
        ),
    )


@admin.register(Agri_maintenance)
class AgriMaintenanceAdmin(admin.ModelAdmin):
    list_display = [
        "agri_maintenance_id",
        "work_id",
        "corresponding_work_id",
        "plan_name",
        "status_re",
    ]
    list_filter = ["status_re", "plan_id", "plan_name"]
    search_fields = ["work_id", "corresponding_work_id", "plan_name", "uuid"]
    readonly_fields = ["uuid"]

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "agri_maintenance_id",
                    "work_id",
                    "corresponding_work_id",
                    "plan_id",
                    "plan_name",
                )
            },
        ),
        ("Status", {"fields": ("status_re",)}),
        ("Location", {"fields": ("latitude", "longitude")}),
        (
            "Metadata",
            {"fields": ("uuid", "data_agri_maintenance"), "classes": ("collapse",)},
        ),
    )
