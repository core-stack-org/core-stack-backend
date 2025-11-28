import pandas as pd
import json
import matplotlib.ticker as ticker
import geopandas as gpd
import geojson
from datetime import datetime
import ee
import sys
import numpy as np

from gee_computing.models import GEEAccount
from nrm_app.celery import app
from computing.utils import sync_layer_to_geoserver, save_layer_info_to_db
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
)
from utilities.constants import GEE_HELPER_PATH, GEE_PATHS


def calculation_df(year, df, gdf):
    vcibins = [-1.0, 40.0, 60.0, 100.0]
    vcilabels = ["poor", "fair", "good"]

    maibins = [-1.0, 25.0, 50.0, 100.0]
    mailabels = ["poor", "fair", "good"]

    spibins = [-1000.0, -2.0, -1.5, -1.0, 0.0, 1.0, 1.5, 2.0, 1000.0]
    spilabels = [
        "extremelyDry",
        "severelyDry",
        "moderatelyDry",
        "mildlyDry",
        "mildlyWet",
        "moderatelyWet",
        "severelyWet",
        "extremelyWet",
    ]

    mDrought = 0
    mDroughtCols = [col for col in df.columns if "meteorological_drought" in col]
    df[f"mDrought_{year}"] = df[mDroughtCols].sum(axis=1)

    ##DrySPells START
    drySpells = 0
    drySpellsCols = [col for col in df.columns if "dryspell" in col]
    df[f"drySpells_{year}"] = df[drySpellsCols].sum(axis=1)

    ##Cropping Area Sown in Kharif
    df[f"cas_{year}_mode"] = 0
    for index, row in df.iterrows():
        val = df.at[index, f"percent_of_area_cropped_kharif_{year}"]
        if val <= 33.3:
            df.at[index, f"cas_{year}_mode"] = 3
        elif val <= 50.0:
            df.at[index, f"cas_{year}_mode"] = 2
        else:
            df.at[index, f"cas_{year}_mode"] = 1

    ##VCI START
    # The code processes VCI data from several columns in the DataFrame.
    # It categorizes each VCI value into bins (based on vcibins and vcilabels)
    # and counts how many values fall into each category for each row.
    # It then determines the most frequent VCI category (the mode) for each row and stores this information in a new column.
    # VCI START
    vciCols = [col for col in df.columns if "vci" in col]
    for l in vcilabels:
        df[f"vci_{year}_{l}"] = 0

    for index, row in df.iterrows():
        for col in vciCols:
            vci_value = row[col]
            # Check if the value is NaN
            if pd.isna(vci_value):
                continue
            else:
                category = pd.cut([vci_value], bins=vcibins, labels=vcilabels)[0]
                if pd.notna(category):
                    df.at[index, f"vci_{year}_{category}"] += 1

    df[f"vci_{year}_mode"] = 0
    for index, row in df.iterrows():
        templ = []
        i = 1
        for l in vcilabels[::-1]:
            templ.append([df.at[index, f"vci_{year}_{l}"], i])
            i += 1
        templ.sort()
        df.at[index, f"vci_{year}_mode"] = templ[-1][1]

    # MAI START
    maiCols = [col for col in df.columns if "mai" in col]
    for l in mailabels:
        df[f"mai_{year}_{l}"] = 0

    for index, row in df.iterrows():
        for col in maiCols:
            mai_value = row[col]
            # Check if the value is NaN
            if pd.isna(mai_value):
                continue
            else:
                category = pd.cut([mai_value], bins=maibins, labels=mailabels)[0]
                if pd.notna(category):
                    df.at[index, f"mai_{year}_{category}"] += 1

    df[f"mai_{year}_mode"] = 0
    for index, row in df.iterrows():
        templ = []
        i = 1
        for l in mailabels[::-1]:
            templ.append([df.at[index, f"mai_{year}_{l}"], i])
            i += 1
        templ.sort()
        df.at[index, f"mai_{year}_mode"] = templ[-1][1]

    ##SPI START
    spiCols = [col for col in df.columns if "spi" in col]
    for l in spilabels:
        df[f"spi_{year}_{l}"] = 0

    for index, row in df.iterrows():
        for col in spiCols:
            spi_value = row[col]
            if pd.isna(spi_value):
                continue
            category = pd.cut(
                [spi_value], bins=spibins, labels=spilabels, include_lowest=True
            )[0]
            if pd.isna(category):
                continue
            df.at[index, f"spi_{year}_{category}"] += 1

    df[f"spi_{year}_mode"] = 0
    for index, row in df.iterrows():
        templ = []
        i = 1
        for l in spilabels[::-1]:
            templ.append([df.at[index, f"spi_{year}_{l}"], i])
            i += 1
        templ.sort()
        df.at[index, f"spi_{year}_mode"] = templ[-1][1]

    # Option to drop columns from df before merging
    columns_to_drop = [col for col in df.columns if col in gdf.columns and col != "uid"]
    df_cleaned = df.drop(columns=columns_to_drop)
    gdf = pd.merge(gdf, df_cleaned, on="uid", how="left")
    return gdf


def getWeekVector(fin_df, wkCt, year):
    map = {"severe": 3, "moderate": 2, "mild": 1}
    for index, row in fin_df.iterrows():
        ds = row[f"dryspell_{year}_week_{wkCt}"]
        rfdev = row[f"monthly_rainfall_deviation_{year}_week_{wkCt}"]
        spi = row[f"spi_{year}_week_{wkCt}"]
        vci = row[f"vci_{year}_week_{wkCt}"]
        mai = row[f"mai_{year}_week_{wkCt}"]
        cas = row[f"kharif_cropped_sqkm_{year}"]

        vci_class = "mild"
        if 60 < vci and vci <= 100:
            vci_class = "severe"
        elif 40 < vci and vci <= 60:
            vci_class = "moderate"
        else:
            vci_class = "mild"

        mai_class = "mild"
        if mai <= 25:
            mai_class = "severe"
        elif mai <= 50:
            mai_class = "moderate"
        else:
            mai_class = "mild"

        cas_class = "mild"
        if cas <= 33.3:
            cas_class = "severe"
        elif cas <= 50:
            cas_class = "moderate"
        else:
            cas_class = "mild"

        mD = 0
        if ds == 1 or rfdev == "scanty" or spi < -1.5:
            mD = 1
        if mD == 1:
            if (
                vci_class == "severe"
                and mai_class == "severe"
                and cas_class == "severe"
            ):
                if ds == 1:
                    fin_df.at[index, f"severe_drought_path1_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"severe_drought_path2_{year}"] += 1
                else:
                    fin_df.at[index, f"severe_drought_path3_{year}"] += 1
            elif (
                vci_class == "mild" and mai_class == "severe" and cas_class == "severe"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path1_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path2_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path3_{year}"] += 1
            elif (
                vci_class == "moderate"
                and mai_class == "severe"
                and cas_class == "severe"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path4_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path5_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path6_{year}"] += 1
            elif (
                vci_class == "severe" and mai_class == "mild" and cas_class == "severe"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path7_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path8_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path9_{year}"] += 1
            elif (
                vci_class == "severe"
                and mai_class == "moderate"
                and cas_class == "severe"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path10_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path11_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path12_{year}"] += 1
            elif (
                vci_class == "severe" and mai_class == "severe" and cas_class == "mild"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path13_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path14_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path15_{year}"] += 1
            elif (
                vci_class == "severe"
                and mai_class == "severe"
                and cas_class == "moderate"
            ):
                if ds == 1:
                    fin_df.at[index, f"moderate_drought_path16_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[index, f"moderate_drought_path17_{year}"] += 1
                else:
                    fin_df.at[index, f"moderate_drought_path18_{year}"] += 1
            else:
                if ds == 1:
                    fin_df.at[index, f"mild_drought_dryspell_score_{year}"] += 1
                elif rfdev == "scanty":
                    fin_df.at[
                        index, f"mild_drought_rainfall_deviation_score_{year}"
                    ] += 1
                elif spi < -1.5:
                    fin_df.at[index, f"mild_drought_spi_score_{year}"] += 1
                fin_df.at[index, f"mild_drought_vci_score_{year}"] += map[vci_class]
                fin_df.at[index, f"mild_drought_mai_score_{year}"] += map[mai_class]
                fin_df.at[
                    index, f"mild_drought_cropping_area_sown_score_{year}"
                ] += map[cas_class]


"""
VCI Value (%) Vegetation Condition
60-100 Good
40-60 Fair
0-40 Poor


MAI (%) Agricultural Drought Class
51-100 Mild drought
26-50 Moderate drought
0-25 Severe drought

Area Sown(%) Drought Condition
0-33.3 Severe drought
33.3-50 Moderate drought
50-100 Mild drought

"""


def data_with_column(year, gdf, fin_df):
    create_cols = [
        "frequency_of_no_drought",
        "frequency_of_mild_drought",
        "frequency_of_moderate_drought",
        "frequency_of_severe_drought",
        "intensity_of_no_drought",
        "intensity_of_mild_drought",
        "intensity_of_moderate_drought",
        "intensity_of_severe_drought",
        "severe_drought_path1",
        "severe_drought_path2",
        "severe_drought_path3",
        "moderate_drought_path1",
        "moderate_drought_path2",
        "moderate_drought_path3",
        "moderate_drought_path4",
        "moderate_drought_path5",
        "moderate_drought_path6",
        "moderate_drought_path7",
        "moderate_drought_path8",
        "moderate_drought_path9",
        "moderate_drought_path10",
        "moderate_drought_path11",
        "moderate_drought_path12",
        "moderate_drought_path13",
        "moderate_drought_path14",
        "moderate_drought_path15",
        "moderate_drought_path16",
        "moderate_drought_path17",
        "moderate_drought_path18",
        "mild_drought_dryspell_score",
        "mild_drought_rainfall_deviation_score",
        "mild_drought_spi_score",
        "mild_drought_vci_score",
        "mild_drought_mai_score",
        "mild_drought_cropping_area_sown_score",
    ]
    for col in create_cols:
        fin_df[f"{col}_{year}"] = 0
    fin_df[f"frequency_of_no_drought_{year}"] = round(
        gdf[f"freq_of_drought_{year}_at_threshold_0"], 2
    )
    fin_df[f"frequency_of_mild_drought_{year}"] = round(
        gdf[f"freq_of_drought_{year}_at_threshold_1"], 2
    )
    fin_df[f"frequency_of_moderate_drought_{year}"] = round(
        gdf[f"freq_of_drought_{year}_at_threshold_2"], 2
    )
    fin_df[f"frequency_of_severe_drought_{year}"] = round(
        gdf[f"freq_of_drought_{year}_at_threshold_3"], 2
    )
    fin_df[f"intensity_of_no_drought_{year}"] = round(
        gdf[f"intensity_of_drought_{year}_at_threshold_0"], 2
    )
    fin_df[f"intensity_of_mild_drought_{year}"] = round(
        gdf[f"intensity_of_drought_{year}_at_threshold_1"], 2
    )
    fin_df[f"intensity_of_moderate_drought_{year}"] = round(
        gdf[f"intensity_of_drought_{year}_at_threshold_2"], 2
    )
    fin_df[f"intensity_of_severe_drought_{year}"] = round(
        gdf[f"intensity_of_drought_{year}_at_threshold_3"], 2
    )

    prefixName = f"weekly_label_{year}"
    dates = [col[13:] for col in gdf.columns if (prefixName in col)]
    date_objects = [[datetime.strptime(date, "%Y-%m-%d"), date] for date in dates]
    sorted_dates = sorted(date_objects)
    sorted_dates_strings = [date[1] for date in sorted_dates]

    new_cols = []
    singleLabels = ["monsoon_onset", "total_weeks", "kharif_cropped_sqkm"]
    for label in singleLabels:
        colname = f"{label}_{year}"
        fin_df[colname] = gdf[colname]
        new_cols.append(colname)

    labels = [
        "dryspell",
        "mai",
        "meteorological_drought",
        "monthly_rainfall_deviation",
        "spi",
        "rfdev_class",
        "vci",
    ]
    for label in labels:
        for i in range(len(sorted_dates_strings)):
            s = sorted_dates_strings[i]
            col = f"{label}_{s}"
            colnew = f"{label}_{year}_week_{i + 1}"
            fin_df[colnew] = gdf[col]
            new_cols.append(colnew)

    for i in range(len(sorted_dates_strings)):
        weekCt = i + 1
        getWeekVector(fin_df, weekCt, year)

    fin_df.drop(new_cols, axis=1, inplace=True)
    fin_df[f"mild_drought_vci_score_{year}"] = (
        fin_df[f"mild_drought_vci_score_{year}"] / 6
    ).round(2)
    fin_df[f"mild_drought_mai_score_{year}"] = (
        fin_df[f"mild_drought_mai_score_{year}"] / 6
    ).round(2)
    fin_df[f"mild_drought_cropping_area_sown_score_{year}"] = (
        fin_df[f"mild_drought_cropping_area_sown_score_{year}"] / 6
    ).round(2)
    return fin_df


def count1(row, year):
    index = row.name
    paths = [
        ["severe_drought_path1", 0],
        ["severe_drought_path2", 0],
        ["severe_drought_path3", 0],
        ["moderate_drought_path1", 0],
        ["moderate_drought_path2", 0],
        ["moderate_drought_path3", 0],
        ["moderate_drought_path4", 0],
        ["moderate_drought_path5", 0],
        ["moderate_drought_path6", 0],
        ["moderate_drought_path7", 0],
        ["moderate_drought_path8", 0],
        ["moderate_drought_path9", 0],
        ["moderate_drought_path10", 0],
        ["moderate_drought_path11", 0],
        ["moderate_drought_path12", 0],
        ["moderate_drought_path13", 0],
        ["moderate_drought_path14", 0],
        ["moderate_drought_path15", 0],
        ["moderate_drought_path16", 0],
        ["moderate_drought_path17", 0],
        ["moderate_drought_path18", 0],
    ]
    for index, path in enumerate(paths):
        prefix, count = path
        col = f"{prefix}_{year}"
        count += row[col]
        paths[index][1] = count
    paths = sorted(paths, key=lambda x: x[1], reverse=True)[:3]
    output = ""
    for prefix, count in paths:
        if count == 0:
            continue
        output = output + " | " + prefix + f"({count})"
    return output[2:]


def count2(row, year):
    index = row.name
    nodes = [
        ["mild_drought_dryspell_score", row[f"mild_drought_dryspell_score_{year}"]],
        [
            "mild_drought_rainfall_deviation_score",
            row[f"mild_drought_rainfall_deviation_score_{year}"],
        ],
        ["mild_drought_spi_score", row[f"mild_drought_spi_score_{year}"]],
        ["mild_drought_vci_score", row[f"mild_drought_vci_score_{year}"]],
        ["mild_drought_mai_score", row[f"mild_drought_mai_score_{year}"]],
        [
            "mild_drought_cropping_area_sown_score",
            row[f"mild_drought_cropping_area_sown_score_{year}"],
        ],
    ]
    nodes = sorted(nodes, key=lambda x: x[1], reverse=True)[:3]
    output = ""
    for prefix, count in nodes:
        if count == 0:
            continue
        output = output + " | " + prefix + f"({count})"
    return output[2:]


def data_to_geojson(data):
    gdf = gpd.GeoDataFrame(
        data, geometry=gpd.points_from_xy(data["longitude"], data["latitude"])
    )
    return geojson.loads(gdf.to_json())


def convert_to_dict(causality_str):
    if " | " in causality_str:
        items = causality_str.split(" | ")
    else:
        items = [causality_str]

    causality_dict = {}

    for item in items:
        if "(" in item and ")" in item:
            try:
                key, value = item.split("(")
                value = value.replace(")", "")
                causality_dict[key.strip()] = float(value)
            except ValueError:
                print(f"Warning: Could not process item: '{item}'")
        else:
            print(f"Warning: Invalid format in item: '{item}'")

    return causality_dict


@app.task(bind=True)
def drought_causality(
    self, state, district, block, start_year, end_year, gee_account_id, app_type="MWS"
):
    ee_initialize(gee_account_id)
    mws_feature_collection = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    gee_obj = GEEAccount.objects.get(pk=gee_account_id)
    mws_info = mws_feature_collection.getInfo()
    mws_features = mws_info["features"]
    mws_data = [feature["properties"] for feature in mws_features]
    gdf = pd.DataFrame(mws_data)

    # Initialize a dictionary to store aggregated data for all years
    combined_uid_data = {}

    for year in range(start_year, end_year + 1):
        asset_path = ee.FeatureCollection(
            get_gee_asset_path(
                state, district, block, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "drought_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + str(year)
        )

        feature_collection = ee.FeatureCollection(asset_path)
        asset_info = feature_collection.getInfo()
        features = asset_info["features"]
        data = [feature["properties"] for feature in features]
        df = pd.DataFrame(data)

        gdf = calculation_df(year, df, gdf)
        fin_df = pd.DataFrame()
        fin_df["uid"] = gdf["uid"]
        fin_df = data_with_column(year, gdf, fin_df)
        data = fin_df

        data = data.applymap(lambda x: int(x) if isinstance(x, np.int64) else x)

        data["severe_moderate_drought_causality_" + str(year)] = data.apply(
            lambda row: count1(row, year), axis=1
        )
        data["mild_drought_causality_" + str(year)] = data.apply(
            lambda row: count2(row, year), axis=1
        )

        data = data[data["frequency_of_no_drought_" + str(year)].notna()]

        # Update combined_uid_data with data from current year
        for feature in features:
            uid = feature["properties"]["uid"]
            matching_row = data[data["uid"] == uid]

            if not matching_row.empty:
                if uid not in combined_uid_data:
                    # Find the matching mws feature to get area_in_ha
                    mws_feature = next(
                        f for f in mws_features if f["properties"]["uid"] == uid
                    )
                    combined_uid_data[uid] = {
                        "uid": uid,
                        "area_in_ha": mws_feature["properties"].get(
                            "area_in_ha", 0
                        ),  # Include area_in_ha
                        "geometry": mws_feature["geometry"],
                    }

                combined_uid_data[uid][f"se_mo_{year}"] = convert_to_dict(
                    str(
                        matching_row[f"severe_moderate_drought_causality_{year}"].iloc[
                            0
                        ]
                    )
                )
                combined_uid_data[uid][f"mild_{year}"] = convert_to_dict(
                    str(matching_row[f"mild_drought_causality_{year}"].iloc[0])
                )

        # Export individual year data if needed
        features = []
        for _, row in data.iterrows():
            feature = next(
                f for f in mws_features if f["properties"]["uid"] == row["uid"]
            )

            feature_with_geometry = {
                "type": "Feature",
                "geometry": feature["geometry"],
                "properties": row.to_dict(),
            }

            features.append(feature_with_geometry)

    # Create final features from combined data
    final_features = []
    for uid, data in combined_uid_data.items():
        feature = {
            "type": "Feature",
            "geometry": data["geometry"],
            "properties": {k: v for k, v in data.items() if k != "geometry"},
        }
        final_features.append(feature)

    layer_at_geoserver = False
    try:
        geo_filename = (
            valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_drought_causality"
        )
        # aggregated_feature_collection = ee.FeatureCollection(final_features)
        # asset_id = get_gee_asset_path(state, district, block) + geo_filename
        # task = export_vector_asset_to_gee(
        #     aggregated_feature_collection, geo_filename, asset_id
        # )
        # task_id_list = check_task_status([task])
        # print(
        #     f"drought cusality task completed for year {start_year}_{end_year} - task_id_list: {task_id_list}"
        # )
        # if is_gee_asset_exists(asset_id):
        #     save_layer_info_to_db(
        #         state,
        #         district,
        #         block,
        #         layer_name=geo_filename,
        #         asset_id=asset_id,
        #         dataset_name="Drought Causality",
        #     )
        aggregated_feature_collection = {
            "type": "FeatureCollection",
            "features": final_features,
        }
        sync_res = sync_layer_to_geoserver(
            state, aggregated_feature_collection, geo_filename, "drought_causality"
        )
        print(f"Synced aggregated data to GeoServer: {sync_res}")
        if sync_res["status_code"] == 201:
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=geo_filename,
                asset_id="",
                dataset_name="Drought Causality",
                sync_to_geoserver=True,
            )
            layer_at_geoserver = True
    except Exception as e:
        print(f"Error syncing aggregated data to GeoServer: {e}")

    return layer_at_geoserver
