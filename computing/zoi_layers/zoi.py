import ee

from geoadmin.models import District
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import ee_initialize, valid_gee_text, get_gee_dir_path


@app.task(bind=True)
def generate_zoi(
    self,
    state=None,
    district=None,
    block=None,
    project_wb_asset=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    ee_initialize(gee_account_id)
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        description = "swb3_" + asset_suffix
        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

        wb_fc = ee.FeatureCollection(asset_id)
        description_zoi = "zoi_" + asset_suffix
        asset_id_zoi = (
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + description
        )

        zoi_fc = wb_fc.map(compute_zoi)


def compute_zoi(feature):

    area_of_wb = ee.Number(feature.get("area_ored"))  # assumes area field exists

    # logistic_weight
    def logistic_weight(x, x0=0.2, k=50):
        return ee.Number(1).divide(
            ee.Number(1).add((ee.Number(-k).multiply(x.subtract(x0))).exp())
        )

    # y_small_bodies
    def y_small_bodies(area):
        return ee.Number(126.84).multiply(area.add(0.05).log()).add(383.57)

    # y_large_bodies
    def y_large_bodies(area):
        return ee.Number(140).multiply(area.add(0.05).log()).add(500)

    s = logistic_weight(area_of_wb)

    zoi = (
        (ee.Number(1).subtract(s))
        .multiply(y_small_bodies(area_of_wb))
        .add(s.multiply(y_large_bodies(area_of_wb)).round())
    )

    return feature.set("zoi_wb", zoi)
