import os
import subprocess
from nrm_app.settings import USERNAME_GESDISC, PASSWORD_GESDISC
import datetime


def upload_assets_gee():
    """Upload Vector Layers and Raster Layers in the GEE Assets

    Vector layers are directly uploaded to the GEE
    Raster layers first needs to be uploaded to the Google Bucket --> GEE Assets
    """
    pass


def download_et_data(asset_path):
    """
    Download files required for Evapotranspiration from NASA
    """

    # Global Dataset
    globalnames = []

    date = datetime.date(2023, 1, 1)
    year = str(date.year)
    month = str(date.month).zfill(2)
    day = str(date.day).zfill(2)
    full_date = date.strftime("%Y%m")

    base_url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"
    params = {
        "FILENAME": f"/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/{year}/FLDAS_NOAH01_C_GL_M.A{year}{month}.001.nc",
        "VARIABLES": "Evap_tavg",
        "FORMAT": "Y29nLw",
        "LABEL": f"FLDAS_NOAH01_C_GL_M.A{year}{month}.001.nc.SUB.tif",
        "SERVICE": "L34RS_LDAS",
        "DATASET_VERSION": "001",
        "VERSION": "1.02",
        "SHORTNAME": "FLDAS_NOAH01_C_GL_M",
        "BBOX": "-60,-180,90,180",
    }

    filename = f'{base_url}?{"&".join(f"{k}={v}" for k, v in params.items())}'

    output_name = f"{full_date}.tif"
    print(date.strftime("%Y%m%d"), year, month, day, filename)
    subprocess.call(
        [
            "wget",
            "-O",
            output_name,
            "--user",
            USERNAME_GESDISC,
            "--password",
            PASSWORD_GESDISC,
            filename,
        ]
    )
    date -= datetime.timedelta(days=1)

    final_output_filename = output_name
    final_output_assetid = asset_path + full_date
    globalnames.append(output_name)

    # Central Asia
    central_asia_dataset = []


def download_sm_data():

    pass


def extract_ndmis(feature):
    # Extract coordinates
    coordinates = feature.geometry().coordinates()
    lon = coordinates.get(0)  # Extract longitude
    lat = coordinates.get(1)
    waterbody_name = feature.get("waterbody_name")
    uid = feature.get("UID")

    # List of NDMI properties from 100m to 1500m
    ndmi_properties = [
        "100m_NDMI",
        "150m_NDMI",
        "200m_NDMI",
        "250m_NDMI",
        "300m_NDMI",
        "350m_NDMI",
        "400m_NDMI",
        "450m_NDMI",
        "500m_NDMI",
        "550m_NDMI",
        "600m_NDMI",
        "650m_NDMI",
        "700m_NDMI",
        "750m_NDMI",
        "800m_NDMI",
        "850m_NDMI",
        "900m_NDMI",
        "950m_NDMI",
        "1000m_NDMI",
        "1050m_NDMI",
        "1100m_NDMI",
        "1150m_NDMI",
        "1200m_NDMI",
        "1250m_NDMI",
        "1300m_NDMI",
        "1350m_NDMI",
        "1400m_NDMI",
        "1450m_NDMI",
        "1500m_NDMI",
    ]

    # Extract NDMI values
    ndmis = [feature.get(prop) for prop in ndmi_properties]

    return ee.Feature(
        None,
        {
            "lat": lat,
            "lon": lon,
            "ndmis": ee.List(ndmis),
            "waterbody_name": waterbody_name,
            "uid": uid,
        },
    )


def mask_landsat_clouds(image):

    cloud_mask = image.select("QA_PIXEL").bitwiseAnd(31).eq(0)
    saturation_mask = image.select("QA_RADSAT").eq(0)
    return image.updateMask(cloud_mask).updateMask(saturation_mask)


def calculate_slopes_with_smoothing(ndmi_values, poly_degree=2):
    import numpy as np
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import PolynomialFeatures

    slopes = []
    ndmi_values = np.array(ndmi_values, dtype=np.float64)

    # Remove NaN entries and corresponding indices
    valid_mask = ~np.isnan(ndmi_values)
    ndmi_values = ndmi_values[valid_mask]

    n = len(ndmi_values)
    if n < 4:  # Too few data points to process
        return [], [], []

    x_full = np.arange(n).reshape(-1, 1)  # Indexes as independent variable
    y_full = ndmi_values.reshape(-1, 1)

    poly = PolynomialFeatures(degree=poly_degree)
    x_poly_full = poly.fit_transform(x_full)

    print("-----------Track from here ---------")
    print(x_poly_full, y_full)

    if x_poly_full.size > 0 and y_full.size > 0:
        poly_model = LinearRegression()
        poly_model.fit(x_poly_full, y_full)
        smoothened_y_full = poly_model.predict(x_poly_full).flatten()

        # Create intervals with overlap
        step = 3
        intervals = []
        for start in range(0, n, step):
            end = min(start + 4, n)
            if end - start >= 2:  # Only use intervals with at least 2 points
                intervals.append(smoothened_y_full[start:end])
            if end == n:
                break

        # Perform linear regression for each interval
        for idx, interval in enumerate(intervals):
            x = np.arange(len(interval)).reshape(-1, 1)
            y = np.array(interval).reshape(-1, 1)
            linear_model = LinearRegression()
            linear_model.fit(x, y)
            slope = linear_model.coef_[0][0]
            slopes.append((slope, x, linear_model))  # Include model and x for plotting
    else:
        smoothened_y_full = []
        intervals = []

    return smoothened_y_full, intervals, slopes


def plot_intervals_with_smoothing(ndmi_values, smoothened_y_full, slopes, idx):
    plt.figure(figsize=(10, 6))

    # Plot the original NDMI values
    x_full = np.arange(len(ndmi_values))
    plt.plot(x_full, ndmi_values, label="Original NDMI", marker="o", alpha=0.6)

    # Plot the polynomial smoothed NDMI values
    plt.plot(x_full, smoothened_y_full, label="Smoothed NDMI (Polynomial)", linewidth=2)

    # Plot linear regression for each interval
    for i, (slope, x_interval, model) in enumerate(slopes):
        start = i * 3  # Starting index of the interval
        end = start + len(x_interval)  # Ending index of the interval
        y_interval = model.predict(x_interval)
        # , label=f"Interval {i+1} (Linear Fit)"
        plt.plot(np.arange(start, end), y_interval)

    plt.title(f"Checkdam NDMI Analysis (Index {idx})")
    plt.xlabel("Time")
    plt.ylabel("NDMI")
    plt.legend()
    plt.grid(True)
    plt.show()


def classify_checkdams(ndmi_list, poly_degree=2, threshold=0):
    total_impactful = 0
    total_non_impactful = 0
    impactful_indices = []
    non_impactful_indices = []
    zoi_results = {}
    for idx, ndmi_values in enumerate(
        ndmi_list
    ):  # Loop through each list of NDMI values (each checkdam)
        # Get smoothed data, intervals, and slopes
        smoothened_y_full, intervals, slopes = calculate_slopes_with_smoothing(
            ndmi_values, poly_degree
        )
        if idx == 2:
            print("slopes: ")
            slope_arr = []
            for slope in slopes:
                slope_arr.append(slope[0])
            print(slope_arr)

        impactful = False

        for start in range(3):  # We will consider up to 3 starting points (0, 1, 2)
            score = 0
            cum_scores = []
            zoi_dist = None
            max_score = None
            # cum_scores = []
            for j in range(start, len(slopes)):
                slope = slopes[j][0]  # Extract slope
                if slope < 0:  # Descent
                    score += abs(slope)
                else:  # Ascent
                    score -= abs(slope)
                # cum_scores.append(score)

                if score >= threshold and (j - start >= 2 and j - start <= 5):
                    impactful = True
                    # break  # Once classified as impactful, stop checking further intervals
                cum_scores.append(score)
            if idx == 2:
                print("cum_scores : ", cum_scores)

            for j in range(len(slopes) - 2, start - 1, -1):
                if cum_scores[j - start] >= threshold:

                    if zoi_dist == None:
                        if slopes[j][0] > 0:
                            zoi_dist = 100 + 150 * (j)
                        else:
                            zoi_dist = 100 + 150 * (j + 1)
                        max_score = cum_scores[j - start]
                    else:
                        if max_score < cum_scores[j - start]:
                            max_score = cum_scores[j - start]
                            if slopes[j][0] > 0:
                                zoi_dist = 100 + 150 * (j)
                            else:
                                zoi_dist = 100 + 150 * (j + 1)

            res_dict = {}
            if zoi_dist:
                res_dict["zoi"] = zoi_dist
            else:
                res_dict["zoi"] = 400
            res_dict["impactful"] = impactful
            cum_score_dict = {f"score_{i}": val for i, val in enumerate(cum_scores)}

            res_dict["cumlative_score_array"] = cum_score_dict
            zoi_results[idx] = res_dict
        if impactful:
            total_impactful += 1

            impactful_indices.append(idx)
        else:
            total_non_impactful += 1
            non_impactful_indices.append(idx)
        # print("id: ",idx)
        # print("zoi: ",zoi_dist)

    total = total_impactful + total_non_impactful
    if total > 0:
        impactful_percentage = (total_impactful / total) * 100
        non_impactful_percentage = (total_non_impactful / total) * 100
    else:
        impactful_percentage = 0
        non_impactful_percentage = 0

    print(f"Impactful: {impactful_percentage:.2f}%")
    print(f"Non-Impactful: {non_impactful_percentage:.2f}%")

    print("Indices of impactful checkdams:", impactful_indices)
    print("Indices of non-impactful checkdams:", non_impactful_indices)
    print("Zone of Influence (meters) for impactful checkdams:", zoi_results)
    print("impactful")
    print(impactful_indices)
    print(non_impactful_percentage)
    return impactful_indices, non_impactful_indices, zoi_results


def classify_checkdams_updated(ndmi_list, poly_degree=2, threshold=0):
    impactful_indices = []
    non_impactful_indices = []
    zoi_results = {}

    for idx, ndmi_values in enumerate(ndmi_list):
        smooth_y, intervals, slopes = calculate_slopes_with_smoothing(
            ndmi_values, poly_degree
        )

        # Debug log for a specific index
        if idx == 2:
            print("Slopes:", [round(s[0], 4) for s in slopes])

        impactful = False
        best_zoi_dist = None
        best_score = float("-inf")

        for start in range(3):
            score = 0
            cum_scores = []

            for j in range(start, len(slopes)):
                slope_val = slopes[j][0]
                score += abs(slope_val) if slope_val < 0 else -abs(slope_val)
                cum_scores.append(score)

                if score >= threshold and 2 <= j - start <= 5:
                    impactful = True

            # Debug log for cumulative scores
            if idx == 2:
                print("Cumulative scores:", [round(s, 4) for s in cum_scores])

            for j in range(len(slopes) - 2, start - 1, -1):
                if cum_scores[j - start] >= threshold:
                    score_at_j = cum_scores[j - start]
                    zoi_candidate = 100 + 150 * (j if slopes[j][0] > 0 else j + 1)

                    if score_at_j > best_score:
                        best_score = score_at_j
                        best_zoi_dist = zoi_candidate

        if impactful and best_zoi_dist:
            zoi_results[idx] = best_zoi_dist
            impactful_indices.append(idx)
        else:
            non_impactful_indices.append(idx)

    total = len(impactful_indices) + len(non_impactful_indices)
    impactful_pct = (len(impactful_indices) / total) * 100 if total else 0
    non_impactful_pct = (len(non_impactful_indices) / total) * 100 if total else 0

    print(f"Impactful: {impactful_pct:.2f}%")
    print(f"Non-Impactful: {non_impactful_pct:.2f}%")
    print("Indices of impactful checkdams:", impactful_indices)
    print("Indices of non-impactful checkdams:", non_impactful_indices)
    print("Zone of Influence (meters):", zoi_results)

    return impactful_indices, non_impactful_indices, zoi_results
