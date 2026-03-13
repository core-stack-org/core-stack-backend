from computing.zoi_layers.zoi1 import generate_zoi1
from computing.zoi_layers.zoi2 import generate_zoi_ci
from computing.zoi_layers.zoi3 import get_ndvi_for_zoi
from projects.models import Project
from utilities.gee_utils import ee_initialize, valid_gee_text, check_task_status
from waterrejuvenation.utils import wait_for_task_completion, delete_asset_on_GEE
from nrm_app.celery import app


@app.task()
def generate_zoi(
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
):
    print(f"gee account id {gee_account_id}")
    ee_initialize(gee_account_id)
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
    else:
        proj_obj = Project.objects.get(pk=proj_id)
        asset_folder_list = [proj_obj.name.lower()]
        asset_suffix = f"{proj_obj.name}_{proj_obj.id}".lower()

    generate_zoi1(
        state,
        district,
        block,
        roi,
        asset_suffix,
        asset_folder_list,
        app_type,
        gee_account_id,
        proj_id,
    )

    generate_zoi_ci(
        state,
        district,
        block,
        asset_suffix,
        asset_folder_list,
        app_type,
        gee_account_id,
        proj_id,
    )

    if proj_id:

        get_ndvi_for_zoi(
            state=state,
            district=district,
            block=block,
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            app_type=app_type,
            gee_account_id=gee_account_id,
            proj_id=proj_id,
        )
