from dpr.models import *

corestack = {
    "Settlement Form": "Add_Settlements_form%20_V1.0.1",
    "Well Form": "Add_well_form_V1.0.1",
    "water body form": "Add_Waterbodies_Form_V1.0.3",
    "agri analysis feedback form": "nrm_agri_analysis_feedback_form_V1.0.0",
    "cropping pattern form": "crop_form_V1.0.0",
    "groundwater analysis feedback form": "nrm_groundwater_analysis_feedback_form_V1.0.0",
    "livelihood form": "NRM%20Livelihood%20Form",
    "propose maintenance of remotely sensed water structure form": "PM_Remote_Sensed_Surface_Water_structure_V1.0.0",
    "propose maintenance on existing irrigation form": "Propose_Maintenance_on_Existing_Irrigation_Structures_V1.1.1",
    "propose maintenance on existing water recharge form": "Propose_Maintenance_on_Existing_Water_Recharge_Structures_V1.1.1",
    "propose maintenance on water structure form": "NRM_form_NRM_form_Waterbody_Screen_V1.0.0",
    "new irrigation form": "NRM_form_Agri_Screen_V1.0.0",
    "new recharge structure form": "NRM_form_propose_new_recharge_structure_V1.0.0",
    "water body analysis feedback form": "rm_waterbody_analysis_feedback_form_V1.0.0",
}

model_map = {
    "settlement": ODK_settlement,
    "well": ODK_well,
    "waterbody": ODK_waterbody,
    "groundwater": ODK_groundwater,
    "agri": ODK_agri,
    "livelihood": ODK_livelihood,
    "crop": ODK_crop,
    "agri-maint": Agri_maintenance,
    "gw-maint": GW_maintenance,
    "swb-maint": SWB_maintenance,
    "swb-rs-maint": SWB_RS_maintenance,
}
