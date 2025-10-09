import re
import pandas as pd
import numpy as np
import pymannkendall as mk

from datetime import datetime
from collections import defaultdict
from utilities.logger import setup_logger

logger = setup_logger(__name__)

BASE_DIR = "data/stats_excel_files/"


# ? Helpers
def format_date_monsoon_onset(date_list):
    standardized_dates = []
    for date_str in date_list:
        parts = date_str.split("-")
        if len(parts) == 3:
            year, month, day = parts
            # Add leading zeros if needed
            standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            standardized_dates.append(standardized_date)

    # Parse string dates into datetime objects
    dates = []
    for date_str in standardized_dates:
        # Parse each date string into a datetime object
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        dates.append(date_obj)

    # Find min and max dates
    min_date = min(dates)
    max_date = max(dates)

    # Format to only show month and day (MM-DD)
    min_date_formatted = min_date.strftime("%m-%d")
    max_date_formatted = max_date.strftime("%m-%d")

    return min_date_formatted, max_date_formatted


def format_years(year_list):
    if not year_list:
        return ""
    if len(year_list) == 1:
        return year_list[0]
    return "{} and {}".format(", ".join(year_list[:-1]), year_list[-1])


# ? Main Section
def get_mws_data(state, district, block, mwsList, filters):
    try:
        df = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="mws_intersect_villages")

        selected_columns = [col for col in df.columns if col.startswith('Village IDs')]

        # String to store the description
        mws_desc = f"The map on the left shows {len(mwsList)} micro-watersheds and"

        # Create a dictionary to store dataframes for each UID
        result_dfs = {}

        # Filter for each UID in the list and store the resulting dataframe
        for uid in mwsList:
            filtered_df = df.loc[df['MWS UID'] == uid, selected_columns]
            if not filtered_df.empty:
                filtered_df = eval(filtered_df.values[0].tolist()[0])
                for ids in filtered_df:
                    result_dfs[ids] = 1

        mws_desc += f" {len(result_dfs)} corresponding villages based on selected filters. "

        for i, item in enumerate(filters):
            # First item starts with "The"
            if i == 0:
                mws_desc += f"The {item['filterName']} is {item['value']}"
            else:
                mws_desc += f", {item['filterName']} is {item['value']}"

            # Last item ends with a period
            if i == len(filters) - 1:
                mws_desc += "."

        return mws_desc

    except Exception as e:
        logger.info("Could not generate Overview !", e)


def get_terrain_mws_data(state, district, block, mwsList):
    try:
        df = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="terrain")

        selected_columns = [col for col in df.columns if col.startswith('terrainCluster_ID')]
        df[selected_columns] = df[selected_columns].apply(pd.to_numeric, errors='coerce')

        selected_columns_area = [col for col in df.columns if col.startswith('area_in_hac')]
        df[selected_columns_area] = df[selected_columns_area].apply(pd.to_numeric, errors='coerce')

        terrain_parameter = f""
        total_mws_area = 0

        # Create a dictionary to store dataframes for each UID
        result_dfs = defaultdict(int)

        for uid in mwsList:
            # ? Terrain Type
            filtered_df = df.loc[df['UID'] == uid, selected_columns].values[0].tolist()
            result_dfs[filtered_df[0]] += 1

            # ? Sum of Area
            filtered_df_area = df.loc[df['UID'] == uid, selected_columns_area].values[0].tolist()
            total_mws_area += filtered_df_area[0]

        # Calculate total occurrences
        total = sum(result_dfs.values())

        # Calculate percentage for each key
        percentages = {}
        for key, value in result_dfs.items():
            if total > 0:  # Avoid division by zero
                percentages[key] = (value / total) * 100
            else:
                percentages[key] = 0

        max_key = max(percentages, key=percentages.get)

        match max_key:
            case 0:
                df["% of area hill_slope"] = pd.to_numeric(df["% of area hill_slope"], errors='coerce')
                df["% of area slopy_area"] = pd.to_numeric(df["% of area slopy_area"], errors='coerce')

                df_filtered_uid = df[df["UID"].isin(mwsList)]

                total_hill_area = df_filtered_uid["% of area hill_slope"].sum()
                total_slope_area = df_filtered_uid["% of area hill_slope"].sum()

                terrain_parameter += f"The micro-watersheds are spread across {round(total_mws_area, 2)} hectares. The terrain of these micro-watersheds consists of gently sloping land and rolling hills with {round(total_slope_area, 2)} % area under broad slopes and {round(total_hill_area, 2)} % area under hills."

            case 1:
                df["% of area plain_area"] = pd.to_numeric(df["% of area plain_area"], errors='coerce')

                df_filtered_uid = df[df["UID"].isin(mwsList)]

                total_plane_area = df_filtered_uid["% of area plain_area"].sum()

                terrain_parameter += f"The micro-watersheds are spread across {round(total_mws_area, 2)} hectares.The micro-watersheds mainly consist of flat plains covering {round(total_plane_area, 2)} % micro-watersheds area."

            case 3:
                df["% of area hill_slope"] = pd.to_numeric(df["% of area hill_slope"], errors='coerce')
                df["% of area valley_area"] = pd.to_numeric(df["% of area valley_area"], errors='coerce')

                df_filtered_uid = df[df["UID"].isin(mwsList)]

                total_hill_area = df_filtered_uid["% of area hill_slope"].sum()
                total_valley_area = df_filtered_uid["% of area valley_area"].sum()

                terrain_parameter += f"The micro-watersheds are spread across {round(total_mws_area, 2)} hectares. The micro-watersheds terrain is mainly hills and valleys with {round(total_hill_area, 2)} % under hills and {round(total_valley_area, 2)} % under valleys."

            case 4:
                df["% of area plain_area"] = pd.to_numeric(df["% of area plain_area"], errors='coerce')
                df["% of area valley_area"] = pd.to_numeric(df["% of area valley_area"], errors='coerce')
                df_filtered_uid = df[df["UID"].isin(mwsList)]

                total_plain_area = df_filtered_uid["% of area plain_area"].sum()
                total_valley_area = df_filtered_uid["% of area valley_area"].sum()

                terrain_parameter += f"The micro-watersheds are spread across {round(total_mws_area, 2)} hectares.The micro-watersheds include flat plains and gentle slopes with {round(total_plain_area, 2)} % area as plains and {round(total_valley_area, 2)} % area under broad slopes."

        return terrain_parameter

    except Exception as e:
        logger.info("Could not generate Terrain Data !", e)


def get_lulc_mws_data(state, district, block, mwsList):
    try:
        excel_file = pd.ExcelFile(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")

        lulc_parameter = f""
        slope_parameter = f""
        plain_parameter = f""

        if "terrain_lulc_slope" in excel_file.sheet_names:
            df_slope = pd.read_excel(
                BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
                sheet_name="terrain_lulc_slope")

            df_slope["area_in_hac"] = pd.to_numeric(df_slope["area_in_hac"], errors='coerce')
            df_slope["% of area forests"] = pd.to_numeric(df_slope["% of area forests"], errors='coerce')
            df_slope["% of area shrub_scrubs"] = pd.to_numeric(df_slope["% of area shrub_scrubs"], errors='coerce')
            df_slope["% of area barren"] = pd.to_numeric(df_slope["% of area barren"], errors='coerce')

            df_filtered_uid = df_slope[df_slope["UID"].isin(mwsList)]

            df_filtered_uid["area_of_forest"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area forests"]) / 100
            df_filtered_uid["area_of_shrub_scrubs"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area shrub_scrubs"]) / 100
            df_filtered_uid["area_of_barren"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area barren"]) / 100

            area_of_forest = df_filtered_uid["area_of_forest"].sum()
            area_of_shrub_scrubs = df_filtered_uid["area_of_shrub_scrubs"].sum()
            area_of_barren = df_filtered_uid["area_of_barren"].sum()

            total_mws_area = df_filtered_uid["area_in_hac"].sum()

            temp_map = {
                area_of_forest: f"{round((area_of_forest / total_mws_area) * 100, 2)} % trees",
                area_of_shrub_scrubs: f"{round((area_of_shrub_scrubs / total_mws_area) * 100, 2)} % shrubs",
                area_of_barren: f"{round((area_of_barren / total_mws_area) * 100, 2)} % barren"
            }

            # Sort by key
            sorted_map = dict(sorted(temp_map.items()))

            slope_parameter += f"On the slopes, land use is predominantly characterized by "

            values = list(sorted_map.values())

            # Join with comma + space, skipping the trailing comma
            slope_parameter += ", ".join(values)

        if "terrain_lulc_plain" in excel_file.sheet_names:
            df_plain = pd.read_excel(
                BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
                sheet_name="terrain_lulc_plain")

            df_plain["% of area single_non_kharif"] = pd.to_numeric(df_plain["% of area single_non_kharif"],
                                                                    errors='coerce')
            df_plain["% of area single_cropping"] = pd.to_numeric(df_plain["% of area single_cropping"],
                                                                  errors='coerce')
            df_plain["% of area double cropping"] = pd.to_numeric(df_plain["% of area double cropping"],
                                                                  errors='coerce')
            df_plain["% of area triple cropping"] = pd.to_numeric(df_plain["% of area triple cropping"],
                                                                  errors='coerce')
            df_plain["% of area shrub_scrubs"] = pd.to_numeric(df_plain["% of area shrub_scrubs"], errors='coerce')
            df_plain["% of area barren"] = pd.to_numeric(df_plain["% of area barren"], errors='coerce')

            df_filtered_uid = df_plain[df_plain["UID"].isin(mwsList)]

            df_filtered_uid["area_of_single_non_kharif"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area single_non_kharif"]) / 100
            df_filtered_uid["area_of_single_cropping"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area single_cropping"]) / 100
            df_filtered_uid["area_of_double_cropping"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area double cropping"]) / 100
            df_filtered_uid["area_of_triple_cropping"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area triple cropping"]) / 100

            df_filtered_uid["area_of_farmlands"] = (df_filtered_uid["area_of_single_non_kharif"] +
                                                    df_filtered_uid["area_of_single_cropping"] +
                                                    df_filtered_uid["area_of_double_cropping"] +
                                                    df_filtered_uid["area_of_triple_cropping"]
                                                    )
            df_filtered_uid["area_of_shrub_scrubs"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area shrub_scrubs"]) / 100
            df_filtered_uid["area_of_barren"] = (df_filtered_uid["area_in_hac"] * df_filtered_uid[
                "% of area barren"]) / 100

            area_of_farmlands = df_filtered_uid["area_of_farmlands"].sum()
            area_of_shrub_scrubs = df_filtered_uid["area_of_shrub_scrubs"].sum()
            area_of_barren = df_filtered_uid["area_of_barren"].sum()

            total_mws_area = df_filtered_uid["area_in_hac"].sum()

            temp_map = {
                area_of_farmlands: f"{round((area_of_farmlands / total_mws_area) * 100, 2)} % farmlands",
                area_of_shrub_scrubs: f"{round((area_of_shrub_scrubs / total_mws_area) * 100, 2)} % shrubs",
                area_of_barren: f"{round((area_of_barren / total_mws_area) * 100, 2)} % barren"
            }

            # Sort by key
            sorted_map = dict(sorted(temp_map.items()))

            plain_parameter += f"On the plains, land use has predominance of  "

            values = list(sorted_map.values())

            # Join with comma + space, skipping the trailing comma
            plain_parameter += ", ".join(values)

        if len(slope_parameter) or len(plain_parameter):
            lulc_parameter += f"During  2017- 22, the slopes and plains of these micro watersheds have exhibited distinct land-use patterns.  "
            lulc_parameter += slope_parameter
            lulc_parameter += plain_parameter
            lulc_parameter += "."

        return lulc_parameter

    except Exception as e:
        logger.info("Could not generate Terrain Data !", e)


def get_degrad_mws_data(state, district, block, mwsList):
    try:
        excel_file = pd.ExcelFile(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")

        degrad_parameters = f""

        if "change_detection_degradation" in excel_file.sheet_names:

            df_degrad = pd.read_excel(
                BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
                sheet_name="change_detection_degradation")

            df_degrad["Total_degradation"] = pd.to_numeric(df_degrad["Total_degradation"], errors='coerce')

            df_filtered_uid = df_degrad[df_degrad["UID"].isin(mwsList)]

            total_land_degrad = round(df_filtered_uid["Total_degradation"].sum(), 2)
            total_block_degrad = round(df_degrad["Total_degradation"].sum(), 2)

            if total_land_degrad >= 50:
                degrad_parameters += f"There has been a considerate level of degradation of farmlands in these micro watersheds over the years 2017-2022, about {total_land_degrad} hectares, as compared to {total_block_degrad} hectares in the entire block."

        return degrad_parameters

    except Exception as e:
        logger.info("Could not generate Land Degradation Data !", e)


def get_reduction_mws_data(state, district, block, mwsList):
    try:
        excel_file = pd.ExcelFile(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")

        reduce_parameters = f""

        if "change_detection_deforestation" in excel_file.sheet_names:

            df_reduce = pd.read_excel(
                BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
                sheet_name="change_detection_deforestation")

            df_reduce["total_deforestation"] = pd.to_numeric(df_reduce["total_deforestation"], errors='coerce')

            df_filtered_uid = df_reduce[df_reduce["UID"].isin(mwsList)]

            total_forest_reduce = df_filtered_uid["total_deforestation"].sum()
            total_block_reduce = df_reduce["total_deforestation"].sum()

            if total_forest_reduce >= 100:
                reduce_parameters += f"There has been a considerate level of reduction in tree cover in these micro watersheds over the years 2017-2022, about {round(total_forest_reduce, 2)} hectares, as compared to {round(total_block_reduce, 2)} hectares in the entire block."

        return reduce_parameters

    except Exception as e:
        logger.info("Could not generate Tree Cover Reduction Data !", e)


def get_urban_mws_data(state, district, block, mwsList):
    try:
        excel_file = pd.ExcelFile(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")

        urban_parameters = f""

        if "change_detection_urbanization" in excel_file.sheet_names:

            df_urban = pd.read_excel(
                BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
                sheet_name="change_detection_urbanization")

            df_urban["Total_urbanization"] = pd.to_numeric(df_urban["Total_urbanization"], errors='coerce')

            df_filtered_uid = df_urban[df_urban["UID"].isin(mwsList)]

            total_urban = df_filtered_uid["Total_urbanization"].sum()

            if total_urban >= 100:
                urban_parameters += f"There has been a considerate level of urbanization in these micro watersheds with about {round(total_urban, 2)} hectares of land covered with settlements"

        return urban_parameters

    except Exception as e:
        logger.info("Could not generate Tree Cover Reduction Data !", e)


def get_cropping_mws_data(state, district, block, mwsList):
    try:
        df_crop = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="croppingIntensity_annual")

        df_drought = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="croppingDrought_kharif")

        selected_columns_intensity = [col for col in df_crop.columns if col.startswith("cropping_intensity_")]
        df_crop[selected_columns_intensity] = df_crop[selected_columns_intensity].apply(pd.to_numeric, errors="coerce")

        df_filtered_uid = df_crop[df_crop["UID"].isin(mwsList)]

        # ? Parameters
        inten_para_1 = ""
        inten_para_2 = ""
        inten_para_3 = ""

        # ? Trend Calculation
        averages_inten = [df_filtered_uid[col].mean() for col in selected_columns_intensity]

        result = mk.original_test(averages_inten)
        min_res = min(averages_inten)
        max_res = max(averages_inten)

        avg_inten = sum(averages_inten) / len(averages_inten)

        if result.trend == "increasing":
            inten_para_1 += f"The cropping intensity of the selected micro-watershed regions has increased on average over the last eight years from {round(min_res, 2)} to {round(max_res, 2)}."

        elif result.trend == "decreasing":
            inten_para_1 += f"The cropping intensity of the selected micro-watershed regions has reduced over time from {round(max_res, 2)} to {round(min_res, 2)}."
            if avg_inten < 1.5:
                inten_para_1 += f"It might be possible to improve cropping intensity through more strategic placement, while keeping equity in mind, of rainwater harvesting or groundwater recharge structures. "

        else:
            inten_para_1 += f"The cropping intensity of the selected micro-watershed regions stayed steady at {round(min_res, 2)} "

        # ? Threshold Calculation

        # ? Drought Years

        selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
        selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]

        df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric,
                                                                                            errors="coerce")
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

        years_count = defaultdict(int)

        for uid in mwsList:
            filtered_df_mod = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            filtered_df_sev = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            for index, item in enumerate(filtered_df_mod):
                drought_check = filtered_df_mod[index] + filtered_df_sev[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if drought_check > 5:
                    if match_exp:
                        years_count[match_exp.group(0)] += 1
                else:
                    if match_exp:
                        years_count[match_exp.group(0)] += 0

        drought_year_mws = []
        nd_year_mws = []

        threshold = len(mwsList) * 0.4

        for year, occurrence in years_count.items():
            if occurrence >= threshold:
                drought_year_mws.append(year)
            else:
                nd_year_mws.append(year)

        if len(drought_year_mws):

            # * inten_d = intensity in drought year, nd = non Drought

            inten_d = []
            inten_nd = []

            for year in drought_year_mws:
                col_name = [col for col in df_filtered_uid.columns if col.startswith("cropping_intensity_" + year)][0]

                cropping_values = df_filtered_uid[col_name].values.mean()

                inten_d.append(cropping_values)

            for year in nd_year_mws:
                col_name = [col for col in df_filtered_uid.columns if col.startswith("cropping_intensity_" + year)][0]

                cropping_values = df_filtered_uid[col_name].values.mean()

                inten_nd.append(cropping_values)

            combined_years = []
            avg_inten_nd = sum(inten_nd) / len(inten_nd)

            for x in inten_nd:
                for y in inten_d:
                    temp_inten = (x - y) / avg_inten_nd
                    combined_years.append(temp_inten)

            inten_threshold = sum(combined_years) / len(combined_years)

            formatted_years = format_years(drought_year_mws)

            if inten_threshold > 0.2:
                inten_para_2 += f"The observed {round(inten_threshold, 3)} reduction in the average cropping intensity during drought years (AAA and BBB), compared to non-drought years, reveals a marked sensitivity of agricultural productivity to water scarcity. This decline underscores the critical need for farmers to adopt drought-resilient practices, such as constructing water harvesting structures. By capturing and storing rainwater, these structures can provide a crucial buffer against drought periods, helping to stabilize cropping intensity and sustain productivity even in water-stressed conditions."

                inten_parameter_2 = inten_parameter_2.replace("AAA and BBB", formatted_years)

        # ? Cropping Areas Graphs
        selected_columns_single = [col for col in df_filtered_uid.columns if col.startswith("single_cropped_area_")]
        selected_columns_double = [col for col in df_filtered_uid.columns if col.startswith("doubly_cropped_area_")]
        selected_columns_triple = [col for col in df_filtered_uid.columns if col.startswith("triply_cropped_area_")]
        selected_columns_sum = [col for col in df_filtered_uid.columns if col.startswith("sum")]

        df_filtered_uid[selected_columns_single] = df_filtered_uid[selected_columns_single].apply(pd.to_numeric,
                                                                                                  errors="coerce")
        df_filtered_uid[selected_columns_double] = df_filtered_uid[selected_columns_double].apply(pd.to_numeric,
                                                                                                  errors="coerce")
        df_filtered_uid[selected_columns_triple] = df_filtered_uid[selected_columns_triple].apply(pd.to_numeric,
                                                                                                  errors="coerce")
        df_filtered_uid[selected_columns_sum] = df_filtered_uid[selected_columns_sum].apply(pd.to_numeric,
                                                                                            errors="coerce")

        avg_single = [df_filtered_uid[col].mean() for col in selected_columns_single]
        avg_double = [df_filtered_uid[col].mean() for col in selected_columns_double]
        avg_triple = [df_filtered_uid[col].mean() for col in selected_columns_triple]
        avg_sum = [df_filtered_uid[col].mean() for col in selected_columns_sum]

        final_single_percent = []
        final_double_percent = []
        final_triple_percent = []
        final_non_cropped = []

        if len(avg_single) and len(avg_double) and len(avg_triple):

            for single, double, triple in zip(avg_single, avg_double, avg_triple):
                if avg_sum[0] != 0:
                    p1 = (float(single) / float(avg_sum[0])) * 100
                    p2 = (float(double) / float(avg_sum[0])) * 100
                    p3 = (float(triple) / float(avg_sum[0])) * 100
                else:
                    p1 = 0
                    p2 = 0
                    p3 = 0
                final_single_percent.append(round(p1, 2))
                final_double_percent.append(round(p2, 2))
                final_triple_percent.append(round(p3, 2))
                final_non_cropped.append(100 - round(p1 + p2 + p3, 2))

        # ? Avg Double Cropping

        # Calculate row-wise average for those columns
        df_filtered_uid["avg_doubly_cropped_area"] = df_filtered_uid[selected_columns_double].mean(axis=1)

        df_filtered_uid["avg_doubly_cropped_area_percent"] = np.where(df_filtered_uid["sum"] != 0, (
                    df_filtered_uid["avg_doubly_cropped_area"] / df_filtered_uid["sum"]) * 100, 0)

        total = len(df_filtered_uid["avg_doubly_cropped_area_percent"].values)

        # Category counts
        low = sum(val <= 30 for val in df_filtered_uid["avg_doubly_cropped_area_percent"].values)
        mid = sum(30 < val < 60 for val in df_filtered_uid["avg_doubly_cropped_area_percent"].values)
        high = sum(val >= 60 for val in df_filtered_uid["avg_doubly_cropped_area_percent"].values)

        # Calculate percentages
        low_pct = (low / total) * 100
        mid_pct = (mid / total) * 100
        high_pct = (high / total) * 100

        if low_pct >= 80:
            inten_para_3 += f"Each of these micro watersheds have a low percentage of double-cropped land, which is less than 30% of the total agricultural land being cultivated twice a year."
        elif mid_pct >= 80:
            inten_para_3 += f"Each of these micro watersheds have a moderate percentage of double-cropped land, which is about <y%> of the total agricultural land being cultivated twice a year."
        else:
            inten_para_3 += f"Each of these micro watersheds have a high percentage of double-cropped land, which is more than 60% of the total agricultural land being cultivated twice a year."

        return inten_para_1, inten_para_2, inten_para_3, final_single_percent, final_double_percent, final_triple_percent, final_non_cropped

    except Exception as e:
        logger.info("Could not generate Cropping Intensity Data !", e)


def get_surface_wb_mws_data(state, district, block, mwsList):
    try:

        df_swb = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="surfaceWaterBodies_annual")

        df_terrain = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="terrain")

        df_drought = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="croppingDrought_kharif")

        selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
        selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]

        df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric,
                                                                                            errors="coerce")
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

        # ? Parameters Desc
        swb_parameter = f""
        rabi_parameter = f""
        kharif_para_1 = f""
        kharif_para_2 = f""
        kharif_para_3 = f""

        # ? Total Area of Selected MWS
        df_terrain["area_in_hac"] = pd.to_numeric(df_terrain["area_in_hac"], errors='coerce')
        df_filtered_uid_terrain = df_terrain[df_terrain["UID"].isin(mwsList)]

        total_mws_area = df_filtered_uid_terrain["area_in_hac"].sum()

        # ? total area of SWB
        selected_columns = [col for col in df_swb.columns if col.startswith("total_area_")]
        df_swb[selected_columns] = df_swb[selected_columns].apply(pd.to_numeric, errors="coerce")

        df_filtered_uid_swb = df_swb[df_swb["UID"].isin(mwsList)]
        df_filtered_uid_swb[selected_columns] = df_filtered_uid_swb[selected_columns].apply(pd.to_numeric,
                                                                                            errors="coerce")

        total_swb_area = 0
        for uid in mwsList:
            filtered_df_total = df_filtered_uid_swb.loc[df_filtered_uid_swb["UID"] == uid, selected_columns]
            if not filtered_df_total.empty:
                total_swb_area += (sum(filtered_df_total.values[0]) / len(selected_columns))

        percent_coverage = (total_swb_area / total_mws_area) * 100

        swb_parameter += f"{round(percent_coverage, 2)} % of the area under these micro watersheds, equivalent to {round(total_swb_area, 2)} hectares, is covered by surface water bodies."

        # ? total Water in Rabi
        selected_columns_rabi = [col for col in df_swb.columns if col.startswith("rabi_area_")]
        df_swb[selected_columns_rabi] = df_swb[selected_columns_rabi].apply(pd.to_numeric, errors="coerce")

        df_filtered_uid_swb[selected_columns_rabi] = df_filtered_uid_swb[selected_columns_rabi].apply(pd.to_numeric,
                                                                                                      errors="coerce")

        percent_rabi = 0
        mws_count = 0
        for uid in mwsList:
            filtered_df_total = df_filtered_uid_swb.loc[df_filtered_uid_swb["UID"] == uid, selected_columns]
            filtered_df_rabi = df_filtered_uid_swb.loc[df_filtered_uid_swb["UID"] == uid, selected_columns_rabi]

            if not filtered_df_total.empty and not filtered_df_rabi.empty:
                avg_rabi = (sum(filtered_df_rabi.values[0]) / len(selected_columns_rabi))
                avg_swb = (sum(filtered_df_total.values[0]) / len(selected_columns))

                percent_comp = (avg_rabi / avg_swb) * 100

                percent_rabi += percent_comp

                if percent_comp <= 25:
                    mws_count += 1

        if (mws_count / len(mwsList)) >= 80:
            rabi_parameter += f"The average surface water availability for cropping during the Rabi season is low ({round(percent_rabi / len(mwsList), 2)}%), indicating dependency on Kharif crop yield for livelihoods. Investigation into irrigation infrastructure used for cropping during the Rabi season and its impact on groundwater depletion should also be done. Additionally, investigation into funds allocated for alternate livelihood activities and community’s dependency on forest resources might also lead to important insights for planning interventions."

        # ? total water in Kharif
        selected_columns_kh = [col for col in df_swb.columns if col.startswith("kharif_area_")]
        df_filtered_uid = df_swb[df_swb["UID"].isin(mwsList)]

        averages_kh = [df_filtered_uid[col].mean() for col in selected_columns_kh]

        result = mk.original_test(averages_kh)

        # ? Drought Years
        drought_years = defaultdict(int)

        for uid in mwsList:
            filtered_df_mod = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            filtered_df_sev = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            for index, item in enumerate(filtered_df_mod):
                drought_check = filtered_df_mod[index] + filtered_df_sev[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if drought_check > 5:
                    if match_exp:
                        drought_years[match_exp.group(0)] += 1
                else:
                    if match_exp:
                        drought_years[match_exp.group(0)] += 0

        drought_year_mws = []
        nd_year_mws = []

        threshold = len(mwsList) * 0.5

        for year, occurrence in drought_years.items():
            if occurrence >= threshold:
                drought_year_mws.append(year)
            else:
                nd_year_mws.append(year)

        total_area_d = 0
        total_area_nd = 0
        total_kh_d = 0
        total_kh_nd = 0
        total_rb_d = 0
        total_rb_nd = 0

        for year in drought_year_mws:
            prefix_total = f"total_area_{year}"
            prefix_kh = f"kharif_area_{year}"
            prefix_rb = f"rabi_area_{year}"

            # For total_area
            matching_cols_total = [col for col in df_filtered_uid.columns if col.startswith(prefix_total)]
            for col in matching_cols_total:
                avg = df_filtered_uid[col].mean()
                total_area_d += avg

            # For kharif_area
            matching_cols_kh = [col for col in df_filtered_uid.columns if col.startswith(prefix_kh)]
            for col in matching_cols_kh:
                avg = df_filtered_uid[col].mean()
                total_kh_d += avg

            # For rabi_area
            matching_cols_rb = [col for col in df_filtered_uid.columns if col.startswith(prefix_rb)]
            for col in matching_cols_rb:
                avg = df_filtered_uid[col].mean()
                total_rb_d += avg

        for year in nd_year_mws:
            prefix_total = f"total_area_{year}"
            prefix_kh = f"kharif_area_{year}"
            prefix_rb = f"rabi_area_{year}"

            # For total_area
            matching_cols = [col for col in df_filtered_uid.columns if col.startswith(prefix_total)]
            for col in matching_cols:
                avg = df_filtered_uid[col].mean()
                total_area_nd += avg

            # For kharif_area
            matching_cols_kh = [col for col in df_filtered_uid.columns if col.startswith(prefix_kh)]
            for col in matching_cols_kh:
                avg = df_filtered_uid[col].mean()
                total_kh_nd += avg

            # For rabi_area
            matching_cols_rb = [col for col in df_filtered_uid.columns if col.startswith(prefix_rb)]
            for col in matching_cols_rb:
                avg = df_filtered_uid[col].mean()
                total_rb_nd += avg

        if len(drought_year_mws) > 1:
            year_str_d = ", ".join(map(str, drought_year_mws[:-1])) + f", and {drought_year_mws[-1]}"
        elif len(drought_year_mws) == 1:
            year_str_d = str(drought_year_mws[0])
        else:
            year_str_d = "N/A"

        if total_area_nd != 0:
            d_to_nd_percent = round((abs(total_area_nd - total_area_d) / total_area_nd) * 100, 2)

        if total_kh_nd != 0:
            kh_to_rb_percent = round((abs(total_kh_nd - total_rb_nd) / total_kh_nd) * 100, 2)

        if total_kh_d != 0:
            kh_to_rb_percent_d = round((abs(total_kh_d - total_rb_d) / total_kh_d) * 100, 2)

        if result.trend == "increasing":
            kharif_para_1 += f"Surface water presence has increased by {round(result.slope, 2)} hectares across these micro watersheds per year during 2017-22."

            kharif_para_2 += f"During the monsoon, on average we observe that the area under surface water during drought years {year_str_d} is {d_to_nd_percent}% less than during non-drought years. This decline highlights a significant impact of drought on surface water availability during the primary crop-growing season, and indicates sensitivity of the cropping to droughts."

            kharif_para_3 += f"In non-drought years, surface water typically decreases by {kh_to_rb_percent}% from the Kharif to the Rabi season. However, during drought years, this reduction is significantly higher, and reaches YYY% from Kharif to Rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

        elif result.trend == "decreasing":
            kharif_para_1 += f"Surface water presence has decreased by {round(result.slope, 2)} hectares across these micro watersheds per year during 2017-22. Siltation could be a cause for decrease in surface water presence and therefore may require repair and maintenance of surface water bodies. Waterbody analysis can help identify waterbodies that may need such treatment."

            kharif_para_2 += f"During the monsoon, we observed a {d_to_nd_percent}% decrease in surface water area during drought years {year_str_d}, as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."

            kharif_para_3 += f"In non-drought years, surface water in kharif typically decreases by {kh_to_rb_percent}% in rabi. However, during drought years, this seasonal reduction is significantly higher, reaching YYY % from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

        else:
            kharif_para_1 += f"The surface water presence has remained steady during 2017-22."

            kharif_para_2 += f"During the monsoon, we observed a {d_to_nd_percent}% decrease in surface water area during drought years {year_str_d}, as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."

            kharif_para_3 += f"In non-drought years, surface water in kharif typically decreases by {kh_to_rb_percent}% in rabi. However, during drought years, this seasonal reduction is significantly higher, reaching {kh_to_rb_percent_d}% from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

        return swb_parameter, rabi_parameter, kharif_para_1, kharif_para_2, kharif_para_3

    except Exception as e:
        logger.info("Could not generate Surface Water Data !", e)


def get_water_balance_mws_data(state, district, block, mwsList):
    try:

        df_hydro = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="hydrological_annual")

        df_drought = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="croppingDrought_kharif")

        df_fortnight = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="hydrological_seasonal")

        # ? Parameter Desc
        deltag_parameter = f""
        good_rainfall_param = f""
        bad_rainfall_param = f""

        selected_columns = [col for col in df_hydro.columns if col.startswith("DeltaG_")]
        df_hydro[selected_columns] = df_hydro[selected_columns].apply(pd.to_numeric, errors="coerce")

        selected_columns_g = [col for col in df_hydro.columns if col.startswith("G_")]
        df_hydro[selected_columns_g] = df_hydro[selected_columns_g].apply(pd.to_numeric, errors="coerce")

        selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
        df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric,
                                                                                            errors="coerce")

        selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

        total_deltaG = 0
        trend_calc = {
            "increasing": 0,
            "decreasing": 0,
            "no trend": 0
        }

        for uid in mwsList:
            filtered_df = df_hydro.loc[df_hydro["UID"] == uid, selected_columns].values[0]
            total_deltaG += sum(filtered_df)

            filtered_df_g = df_hydro.loc[df_hydro["UID"] == uid, selected_columns_g].values[0]
            # ? Mann Kendal Slope Calculation
            result = mk.original_test(filtered_df_g)
            trend_calc[result.trend] += 1

        max_key = max(trend_calc, key=trend_calc.get)

        if total_deltaG > 0:
            deltag_parameter += f"The water balance is positive and indicates that the groundwater situation in these microwatersheds may be stable."

            if max_key == "increasing":
                deltag_parameter += f"Year on year, the groundwater situation seems to be improving."

            else:
                deltag_parameter += f"This however should not be a cause for complacency - over-extraction should be reduced, because over the years it seems that the rate of extraction of groundwater has increased."

        if total_deltaG < 0:
            deltag_parameter += f"The water balance is negative and indicates that the groundwater situation in these microwatersheds may be deteriorating."

            if max_key == "increasing":
                deltag_parameter += f"There may be efforts of recharge which seems to improve groundwater despite extraction of groundwater."

            else:
                deltag_parameter += f"This is a matter of worry. Year on year, the groundwater seems to be depleting due to persistent over-extraction over the years."

        # ? Drought Calculation

        drought_years = defaultdict(int)
        non_drought_years = defaultdict(int)

        for uid in mwsList:
            filtered_df_mod = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            filtered_df_sev = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            for index, item in enumerate(filtered_df_mod):
                drought_check = filtered_df_mod[index] + filtered_df_sev[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if drought_check > 5:
                    if match_exp:
                        drought_years[match_exp.group(0)] += 1
                else:
                    if match_exp:
                        # drought_years[match_exp.group(0)] += 0
                        non_drought_years[match_exp.group(0)] += 1

        # * Get 3 most recurring years by count
        top_years_d = sorted(drought_years.items(), key=lambda x: x[1], reverse=True)[:3]
        top_years_nd = sorted(non_drought_years.items(), key=lambda x: x[1], reverse=True)[:3]

        top_years_d = [year for year, count in top_years_d]
        top_years_nd = [year for year, count in top_years_nd]

        # * Get Year by choosing deciding a threshold
        # top_years_d = []
        # top_years_nd = []

        # threshold = len(mwsList) * 0.5

        # for year, occurrence in drought_years.items():
        #     if occurrence >= threshold:
        #         top_years_d.append(year)
        #     else:
        #         top_years_nd.append(year)

        precipitation_nd = 0
        precipitation_d = 0

        for uid in mwsList:
            rainfall_nd = 0
            rainfall_d = 0
            for year in top_years_nd:
                selected_columns_nd = [col for col in df_hydro.columns if col.startswith("Precipitation_" + year)]
                df_hydro[selected_columns_nd] = df_hydro[selected_columns_nd].apply(pd.to_numeric, errors="coerce")

                rainfall_data = df_hydro.loc[df_hydro["UID"] == uid, selected_columns_nd].values[0]
                rainfall_nd += rainfall_data[0]

            if len(top_years_nd):
                precipitation_nd += (rainfall_nd / len(top_years_nd))

            for year in top_years_d:
                selected_columns_nd = [col for col in df_hydro.columns if col.startswith("Precipitation_" + year)]
                df_hydro[selected_columns_nd] = df_hydro[selected_columns_nd].apply(pd.to_numeric, errors="coerce")

                rainfall_data = df_hydro.loc[df_hydro["UID"] == uid, selected_columns_nd].values[0]
                rainfall_d += rainfall_data[0]

            if len(top_years_d):
                precipitation_d += (rainfall_d / len(top_years_d))

        precipitation_d = precipitation_d / len(mwsList)
        precipitation_nd = precipitation_nd / len(mwsList)

        # ? Fortnight Calculation
        avg_deltaG_nd = 0
        avg_deltaG_d = 0

        for uid in mwsList:
            delG_nd = 0
            delG_d = 0

            for year in top_years_nd:
                selected_columns_dg_kh = [col for col in df_fortnight.columns if
                                          col.startswith("delta g_kharif_" + year)]
                selected_columns_dg_rb = [col for col in df_fortnight.columns if col.startswith("delta g_rabi_" + year)]
                selected_columns_dg_zd = [col for col in df_fortnight.columns if col.startswith("delta g_zaid_" + year)]

                df_fortnight[selected_columns_dg_kh] = df_fortnight[selected_columns_dg_kh].apply(pd.to_numeric,
                                                                                                  errors="coerce")
                df_fortnight[selected_columns_dg_rb] = df_fortnight[selected_columns_dg_rb].apply(pd.to_numeric,
                                                                                                  errors="coerce")
                df_fortnight[selected_columns_dg_zd] = df_fortnight[selected_columns_dg_zd].apply(pd.to_numeric,
                                                                                                  errors="coerce")

                deltaG_data_kh = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]
                deltaG_data_rb = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]
                deltaG_data_zd = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]

                fortnight_delg = (deltaG_data_kh + deltaG_data_rb + deltaG_data_zd)
                delG_nd += fortnight_delg

            if len(top_years_nd) > 0:
                avg_deltaG_nd += (delG_nd) / len(top_years_nd)

            for year in top_years_d:
                selected_columns_dg_kh = [col for col in df_fortnight.columns if
                                          col.startswith("delta g_kharif_" + year)]
                selected_columns_dg_rb = [col for col in df_fortnight.columns if col.startswith("delta g_rabi_" + year)]
                selected_columns_dg_zd = [col for col in df_fortnight.columns if col.startswith("delta g_zaid_" + year)]

                df_fortnight[selected_columns_dg_kh] = df_fortnight[selected_columns_dg_kh].apply(pd.to_numeric,
                                                                                                  errors="coerce")
                df_fortnight[selected_columns_dg_rb] = df_fortnight[selected_columns_dg_rb].apply(pd.to_numeric,
                                                                                                  errors="coerce")
                df_fortnight[selected_columns_dg_zd] = df_fortnight[selected_columns_dg_zd].apply(pd.to_numeric,
                                                                                                  errors="coerce")

                deltaG_data_kh = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]
                deltaG_data_rb = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]
                deltaG_data_zd = df_fortnight.loc[df_fortnight["UID"] == uid, selected_columns_dg_kh].values[0]

                fortnight_delg = (deltaG_data_kh + deltaG_data_rb + deltaG_data_zd)
                delG_d += fortnight_delg

            if len(top_years_d) > 0:
                avg_deltaG_d += delG_d / len(top_years_d)

        avg_deltaG_nd = avg_deltaG_nd / len(mwsList)
        avg_deltaG_d = avg_deltaG_d / len(mwsList)

        # ? Surface Runoff
        df_filtered_uid = df_hydro[df_hydro["UID"].isin(mwsList)]
        df_filtered_drought = df_drought[df_drought["UID"].isin(mwsList)]

        total_runoff_d = 0
        total_precp_d = 0
        total_runoff_nd = 0
        total_precp_nd = 0
        monsoon_onset_nd = []

        for year in top_years_d:
            prefix_precp = f"Precipitation_{year}"
            prefix_runoff = f"RunOff_{year}"
            prefix_onset = f"monsoon_onset_{year}"

            # Match and process all Precipitation columns starting with the prefix
            matching_precp_cols = [col for col in df_filtered_uid.columns if col.startswith(prefix_precp)]
            for col in matching_precp_cols:
                avg = df_filtered_uid[col].mean()
                total_precp_d += avg

            # Match and process all RunOff columns starting with the prefix
            matching_runoff_cols = [col for col in df_filtered_uid.columns if col.startswith(prefix_runoff)]
            for col in matching_runoff_cols:
                avg = df_filtered_uid[col].mean()
                total_runoff_d += avg

        for year in top_years_nd:
            prefix_precp = f"Precipitation_{year}"
            prefix_runoff = f"RunOff_{year}"
            prefix_onset = f"monsoon_onset_{year}"

            # Match and process all Precipitation columns starting with the prefix
            matching_precp_cols = [col for col in df_filtered_uid.columns if col.startswith(prefix_precp)]
            for col in matching_precp_cols:
                avg = df_filtered_uid[col].mean()
                total_precp_nd += avg

            # Match and process all RunOff columns starting with the prefix
            matching_runoff_cols = [col for col in df_filtered_uid.columns if col.startswith(prefix_runoff)]
            for col in matching_runoff_cols:
                avg = df_filtered_uid[col].mean()
                total_runoff_nd += avg

            matching_onset_cols = [col for col in df_filtered_drought.columns if col.startswith(prefix_onset)]
            for col in matching_onset_cols:
                monsoon_date = df_filtered_drought[col]
                monsoon_onset_nd.append(monsoon_date[0])

        total_precp_d = total_precp_d / len(mwsList)
        total_precp_nd = total_precp_nd / len(mwsList)
        total_runoff_d = total_runoff_d / len(mwsList)
        total_runoff_nd = total_runoff_nd / len(mwsList)

        if total_precp_d != 0:
            precp_to_runoff_d = round((total_runoff_d / total_precp_d) * 100, 2)
        else:
            precp_to_runoff_d = 0

        if total_precp_nd != 0:
            precp_to_runoff_nd = round(((total_runoff_nd / total_precp_nd) * 100), 2)
        else:
            precp_to_runoff_nd = 0

        if len(top_years_nd) > 1:
            year_str_nd = ", ".join(top_years_nd[:-1]) + f" and {top_years_nd[-1]}"
        else:
            year_str_nd = top_years_nd[0]

        if len(top_years_d) > 1:
            year_str_d = ", ".join(top_years_d[:-1]) + f" and {top_years_d[-1]}"
        else:
            year_str_d = top_years_d[0]

        min_date_nd, max_date_nd = format_date_monsoon_onset(monsoon_onset_nd)

        if avg_deltaG_nd > 0:
            good_rainfall_param += f"In these micro-watersheds, {year_str_nd} were good rainfall years, bringing an average annual rainfall of approximately {round(total_precp_nd, 2)} mm with monsoon onset between [{min_date_nd}, {max_date_nd}]. This favorable rainfall pattern resulted in positive groundwater recharge, with average groundwater change of {avg_deltaG_nd} mm, indicating replenishment of groundwater resources. During these years, around {precp_to_runoff_nd} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds. "
        else:
            good_rainfall_param += f"In these micro-watersheds, {year_str_nd} were good rainfall years, bringing an average annual rainfall of approximately {round(total_precp_nd, 2)} mm with monsoon onset between [{min_date_nd}, {max_date_nd}]. This favorable rainfall pattern resulted in negative groundwater recharge, with average groundwater change of QQQ mm, indicating depletion of groundwater resources. During these years, around {precp_to_runoff_nd} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds."

        if avg_deltaG_d > 0:
            bad_rainfall_param += f"In contrast, {year_str_d} were bad rainfall years, leading to reduced annual rainfall averaging around {round(total_precp_d, 2)} mm. Limited water availability in these years resulted in positive groundwater changes, with an average replenishment of {round(avg_deltaG_d[0], 2)} mm. Runoff in these years is reduced to {precp_to_runoff_d} % of total rainfall, diminishing the harvestable water."

        else:
            bad_rainfall_param += f"In contrast, {year_str_d} were bad rainfall years, leading to reduced annual rainfall averaging around {round(total_precp_d, 2)} mm. Limited water availability in these years resulted in negative groundwater changes, with an average depletion of {round(avg_deltaG_d[0], 2)} mm. Runoff in these years is reduced to {precp_to_runoff_d} % of total rainfall, diminishing the harvestable water. "

        return deltag_parameter, good_rainfall_param, bad_rainfall_param

    except Exception as e:
        logger.info("Could not generate Water Balance Data !", e)


def get_drought_mws_data(state, district, block, mwsList):
    try:
        df_drought = pd.read_excel(
            BASE_DIR + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            sheet_name="croppingDrought_kharif")

        selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
        df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric,
                                                                                            errors="coerce")
        selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

        # ? Drought Years
        drought_years = defaultdict(int)
        non_drought_years = defaultdict(int)

        for uid in mwsList:
            filtered_df_mod = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            filtered_df_sev = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            for index, item in enumerate(filtered_df_mod):
                drought_check = filtered_df_mod[index] + filtered_df_sev[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if drought_check > 5:
                    if match_exp:
                        drought_years[match_exp.group(0)] += 1
                else:
                    if match_exp:
                        non_drought_years[match_exp.group(0)] += 1

        drought_year_mws = []

        # for key, value in drought_years.items():
        #     if value == len(mwsList):
        #         drought_year_mws.append(key)

        threshold = len(mwsList) * 0.5

        for year, occurrence in drought_years.items():
            if occurrence >= threshold:
                drought_year_mws.append(year)
            # else:
            #     drought_year_mws.append(year)

        # ? DrySpell Calculation
        selected_columns_drysp = [col for col in df_drought.columns if col.startswith("drysp_")]
        df_drought[selected_columns_drysp] = df_drought[selected_columns_drysp].apply(pd.to_numeric, errors="coerce")

        drySp_weeks = []

        for year in drought_year_mws:
            week_count = 0

            selected_columns_drysp = [col for col in df_drought.columns if col.startswith("drysp_" + year)]
            df_drought[selected_columns_drysp] = df_drought[selected_columns_drysp].apply(pd.to_numeric,
                                                                                          errors="coerce")

            for uid in mwsList:
                weeks = df_drought.loc[df_drought["UID"] == uid, selected_columns_drysp].values[0]
                week_count += weeks[0]

            week_count = week_count / len(mwsList)
            drySp_weeks.append(round(week_count))



    except Exception as e:
        logger.info("Could not generate Drought Data !", e)
