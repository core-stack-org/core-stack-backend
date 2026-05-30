import geopandas as gpd
from .gen_dpr import (
    get_settlement_coordinates_for_plan,
    get_mws_uid_for_settlement_gdf,
    get_data_for_settlement,
    get_crops_data,
    get_livestock_data,
)
from .utils import to_utf8


def get_section_b_data(plan, total_settlements, mws_fortnight):

    mws_gdf = gpd.GeoDataFrame.from_features(mws_fortnight["features"])

    settlement_mws_ids = []
    settlement_coordinates = get_settlement_coordinates_for_plan(plan.id)

    for settlement_name, latitude, longitude in settlement_coordinates:
        mws_uid = get_mws_uid_for_settlement_gdf(mws_gdf, latitude, longitude)

        if mws_uid:
            settlement_mws_ids.append(
                {
                    "settlement": settlement_name,
                    "mws_id": mws_uid,
                }
            )

    centroid = None

    if settlement_mws_ids:
        intersecting_mws = mws_gdf[
            mws_gdf["uid"].isin([item["mws_id"] for item in settlement_mws_ids])
        ]

        if not intersecting_mws.empty:
            centroid = intersecting_mws.geometry.unary_union.centroid

    return (
        {
            "village_name": to_utf8(plan.village_name),
            "gram_panchayat": to_utf8(plan.gram_panchayat),
            "tehsil": to_utf8(plan.tehsil_soi.tehsil_name),
            "district": to_utf8(plan.district_soi.district_name),
            "state": to_utf8(plan.state_soi.state_name),
            "total_settlements": total_settlements,
            "settlement_mws_pairs": settlement_mws_ids,
            "village_coordinates": (
                f"{centroid.y:.8f}, {centroid.x:.8f}" if centroid else "Not available"
            ),
        },
        settlement_mws_ids,
        mws_gdf,
    )


def get_section_c_data(plan):
    settlement_data = get_data_for_settlement(plan.id)

    return {
        "socio_eco": settlement_data,
        "mgnrega": settlement_data,
        "crop_info": get_crops_data(plan.id),
        "livestock_info": get_livestock_data(plan.id),
    }
