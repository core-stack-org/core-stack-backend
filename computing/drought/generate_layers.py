import ee

from nrm_app.settings import GEE_HELPER_ACCOUNT_ID
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    is_gee_asset_exists,
    ee_initialize,
    check_task_status,
    make_asset_public,
    create_gee_dir,
    get_gee_dir_path,
    export_vector_asset_to_gee,
)


def get_day_of_year(date):
    # date is ee.Date object
    date = ee.Date(date)  # ensure the type
    return date.difference(ee.Date.fromYMD(date.get("year"), 1, 1), "day").add(1)


def concat(str1, str2):
    # str1 and str2 can be js string or ee.String
    str1 = ee.String(str1)
    str2 = ee.String(str2)
    return str1.cat(str2)


def date2str(date):
    # date is ee.Date object
    date = ee.Date(date)  # ensure the type
    year = concat(ee.String.encodeJSON(date.get("year")), "-")
    month = concat(ee.String.encodeJSON(date.get("month")), "-")
    day = ee.String.encodeJSON(date.get("day"))
    string = concat(concat(year, month), day)
    return string


def rename_column(old_name, new_name, fc):
    # old_name -> ee.String
    # new_name -> ee.String
    # fc -> ee.FeatureColleciton
    old_name = ee.String(old_name)
    new_name = ee.String(new_name)
    fc = ee.FeatureCollection(fc)
    fc = fc.map(lambda f: f.set(new_name, f.get(old_name)))
    return fc


def rename_column_with_transformation(old_name, new_name, fc, transform, args):
    # old_name -> ee.String
    # new_name -> ee.String
    # fc -> ee.FeatureColleciton
    # transformation:function -> function to apply on old_value
    # args:ee.Dictionary -> arguments to transformation, args will be modified and will contain a field 'value'
    # which is the carrier of data
    old_name = ee.String(old_name)
    new_name = ee.String(new_name)
    fc = ee.FeatureCollection(fc)

    def inner(f):
        old_value = f.get(old_name)
        new_args = args
        new_args = new_args.set("value", old_value)
        new_value = transform(new_args)
        return f.set(new_name, new_value)

    fc = fc.map(inner)
    return fc


def sqm2sqkm(args):
    value = args.get("value")
    scale = args.get("scale")
    value = ee.Number(value)
    scale = ee.Number(scale)
    value = value.multiply((scale.multiply(scale)).divide(1000000))
    return value


def generate_drought_layers(
    aoi,
    asset_suffix,
    asset_folder_list,
    app_type,
    current_year,
    start_year,
    end_year,
    chunk_size,
):
    task_ids = []
    asset_ids = []

    size = aoi.size().getInfo()
    print("size=", size)
    parts = size // chunk_size
    print("parts=", parts)
    ee_initialize(GEE_HELPER_ACCOUNT_ID)
    create_gee_dir(
        asset_folder_list, gee_project_path=GEE_PATHS[app_type]["GEE_HELPER_PATH"]
    )
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = (
            asset_suffix
            + "_drought_"
            + str(start)
            + "-"
            + str(end)
            + "_"
            + str(current_year)
        )
        chunk = ee.FeatureCollection(aoi.toList(aoi.size()).slice(start, end))
        if chunk.size().getInfo() > 0:
            drought_chunk(
                chunk,
                block_name_for_parts,
                current_year,
                end_year,
                start_year,
                task_ids,
                asset_ids,
                asset_suffix,
                asset_folder_list,
                app_type,
            )

    print("Done iterating")
    task_id_list = check_task_status(task_ids)
    print("All chunks' asset generated, task id: ", task_id_list)
    for asset_id in asset_ids:
        make_asset_public(asset_id)
    print("All chunks' asset made public.")


def drought_chunk(
    aoi,
    block_name_for_parts,
    current_year,
    end_year,
    start_year,
    task_ids,
    asset_ids,
    asset_suffix,
    asset_folder_list,
    app_type,
):
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_HELPER_PATH"]
        )
        + block_name_for_parts
    )

    if is_gee_asset_exists(asset_id):
        return

    chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").select("precipitation")
    chirps_available_from_year = 1981  # it is available from 1981-01-01
    # chirps_scale = 5566
    modis_ndvi = ee.ImageCollection("MODIS/MOD09GA_006_NDVI").select("NDVI")
    modis_ndvi_scale = 464
    modis_ndvi_available_from_year = 2000  # it is available from 2000-02-24
    modis_ndwi = ee.ImageCollection("MODIS/MOD09GA_006_NDWI").select("NDWI")
    # modis_ndwi_scale = 464
    modis_ndwi_available_from_year = 2000  # it is available from 2000-02-24
    modis = ee.ImageCollection("MODIS/061/MOD16A2GF").select(["ET", "PET"])
    modis_scale = 500
    # modis_available_from_year = 2000  # it is available from 2000-01-01
    lulc_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + asset_suffix
        + "_"
        + str(current_year)
        + "-07-01_"
        + str(current_year + 1)
        + "-06-30_LULCmap_10m"
    )
    cur_year_crop_img = ee.Image(lulc_path)
    lulc_scale = 10
    lulc_available_from_year = start_year
    lulc_y = start_year
    lulc_images = []
    while lulc_y <= end_year:
        lulc_images.append(
            ee.Image(
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + asset_suffix
                + "_"
                + str(lulc_y)
                + "-07-01_"
                + str(lulc_y + 1)
                + "-06-30_LULCmap_10m"
            )
        )
        lulc_y += 1
    lulc = ee.List(lulc_images)

    def get_monsoon_on_set_date(year, main_roi):
        A = ee.Geometry.Polygon(
            [
                [
                    [69.17606600131256, 37.50688033728282],
                    [69.17606600131256, 26.132493256032653],
                    [82.09598787631256, 26.132493256032653],
                    [82.09598787631256, 37.50688033728282],
                ]
            ],
            None,
            False,
        )
        B = ee.Geometry.Polygon(
            [
                [
                    [67.68192537631256, 26.148475498372616],
                    [67.68192537631256, 18.36801791110178],
                    [76.00956209506256, 18.36801791110178],
                    [76.00956209506256, 26.148475498372616],
                ]
            ],
            None,
            False,
        )
        E = ee.Geometry.Polygon(
            [
                [
                    [72.28204341674216, 18.348999397274717],
                    [72.28204341674216, 7.596336766662568],
                    [84.43292232299216, 7.596336766662568],
                    [84.43292232299216, 18.348999397274717],
                ]
            ],
            None,
            False,
        )
        C = ee.Geometry.Polygon(
            [
                [
                    [76.0413365622055, 26.089074629747927],
                    [76.0413365622055, 18.346936996159776],
                    [82.0398717184555, 18.346936996159776],
                    [82.0398717184555, 26.089074629747927],
                ]
            ],
            None,
            False,
        )
        D = ee.Geometry.Polygon(
            [
                [
                    [82.15028471689531, 30.784246603238188],
                    [82.15028471689531, 18.390939094394867],
                    [99.33290190439531, 18.390939094394867],
                    [99.33290190439531, 30.784246603238188],
                ]
            ],
            None,
            False,
        )

        HMZs = ee.FeatureCollection(
            [
                ee.Feature(A),
                ee.Feature(B),
                ee.Feature(C),
                ee.Feature(D),
                ee.Feature(E),
            ]
        )

        dictionary = ee.Dictionary(
            {
                "western": ee.Feature(B),
                "northern": ee.Feature(A),
                "central": ee.Feature(C),
                "eastern": ee.Feature(D),
                "southern": ee.Feature(E),
            }
        )
        thresholds = ee.Dictionary(
            {
                "western": 75,
                "northern": 75,
                "central": 69,
                "eastern": 65,
                "southern": 50,
            }
        )

        def inner_intersection_area(hmz):
            dummy_feature = ee.Feature(None)
            area = (
                ee.Feature(hmz)
                .geometry()
                .intersection(main_roi.geometry(), ee.ErrorMargin(1))
                .area()
            )
            dummy_feature = dummy_feature.set("area", area)
            return dummy_feature

        intersection_area = HMZs.map(inner_intersection_area)
        region_index = ee.Number(
            ee.Array(intersection_area.aggregate_array("area")).gt(0).argmax().get(0)
        )

        mapping = ee.Dictionary(
            {
                "0": "northern",
                "1": "western",
                "2": "central",
                "3": "eastern",
                "4": "southern",
            }
        )
        region = mapping.get(region_index.format())

        roi = ee.Feature(dictionary.get(region)).geometry()
        threshold = thresholds.get(region)

        current_year = year
        startYear = 1981

        def on_set(roi_, dataset, duration, s_year, band_name, threshold_):
            start_days = ee.List.sequence(0, 365, duration)
            end_days = ee.List.sequence(duration, 365, duration)
            weeks = start_days.zip(end_days)

            s_year = ee.Number(s_year)
            e_year = current_year
            dataset = dataset.filterDate(
                ee.Date.fromYMD(s_year, 1, 1), ee.Date.fromYMD(e_year, 12, 31)
            )

            def inner_weekly_totals(week):
                filtered = dataset.filter(
                    ee.Filter.calendarRange(
                        ee.List(week).get(0), ee.List(week).get(1), "day_of_year"
                    )
                )
                weekly_total = filtered.sum()
                return weekly_total.set({"day": ee.List(week).get(0)})

            weekly_totals = weeks.map(inner_weekly_totals)
            weekly_totals = ee.ImageCollection.fromImages(weekly_totals)

            def inner_value(image):
                value = image.reduceRegion(ee.Reducer.sum(), roi_, 5566).get(band_name)
                value = ee.Number(value).divide(
                    ee.Number(e_year).subtract(ee.Number(s_year)).add(ee.Number(1))
                )
                f = ee.Feature(None)
                f = f.set(band_name, value)
                return f

            weekly_totals = weekly_totals.map(inner_value)
            weekly_totals = weekly_totals.aggregate_array(band_name)

            threshold_org_values = weekly_totals.reduce(
                ee.Reducer.percentile([threshold_])
            )

            def find_date(fort_night, curr_year, band_name):
                # month = fortNight.divide(ee.Number(4)).floor()
                # i = (fortNight.subtract(1)).mod(4)
                s = ee.Number(ee.List(weeks.get(fort_night)).get(0))
                start_date = ee.Date.fromYMD(year, 1, 1).advance(s, "day")
                end_date = start_date.advance(6, "day")

                def inner_filtered(image):
                    value = image.reduceRegion(ee.Reducer.sum(), roi_, 5566).get(
                        band_name
                    )
                    value = ee.Number(value)
                    f = ee.Feature(None)
                    f = f.set(band_name, value)
                    return f

                filtered = chirps.filterDate(start_date, end_date)
                filtered = filtered.map(inner_filtered)
                filtered = filtered.aggregate_array(band_name)

                first_derivative = (
                    filtered.slice(1)
                    .zip(filtered.slice(0, -1))
                    .map(
                        lambda pair: ee.Number(ee.List(pair).get(0)).subtract(
                            ee.List(pair).get(1)
                        )
                    )
                )

                onset_date = ee.Number(ee.Array(first_derivative).gt(0).argmax().get(0))
                return start_date.advance(onset_date, "day")

            high_changes_in_weekly_totals = weekly_totals.map(
                lambda element: ee.Number(element).gte(threshold_org_values)
            )
            onset_month = ee.Number(
                ee.Array(high_changes_in_weekly_totals.slice(18))
                .gt(0.100396432)
                .argmax()
                .get(0)
            ).add(ee.Number(18))
            return find_date(onset_month, e_year, band_name)

        return on_set(roi, chirps, 7, startYear, "precipitation", threshold)

    def get_monsoon_cessation_date():
        return ee.Date.fromYMD(current_year, 10, 31)

    def get_week_start_dates(start_date, end_date):
        delta = end_date.difference(start_date, "day")
        sequence = ee.List.sequence(0, delta, 7)

        def inner_dates(delta):
            return start_date.advance(delta, "day")

        dates = sequence.map(inner_dates)
        return dates

    def get_rainfall_deviation(start_date, roi, delta):
        # start_date is ee.Date object
        # roi is ee.FeatureCollection
        # delta is js integer, either 7 or 28
        start_date = ee.Date(start_date)
        roi = ee.FeatureCollection(roi)

        starting_day_of_year = get_day_of_year(start_date)
        days = ee.List.sequence(starting_day_of_year, starting_day_of_year.add(delta))
        years = ee.List.sequence(chirps_available_from_year, current_year - 1)
        long_term_mean = ee.ImageCollection(
            years.map(
                lambda year: ee.ImageCollection(
                    days.map(
                        lambda day: chirps.filter(
                            ee.Filter.calendarRange(year, year, "year")
                        )
                        .filter(ee.Filter.calendarRange(day))
                        .sum()
                    )
                ).sum()
            )
        ).mean()
        current_year_value = ee.ImageCollection(
            days.map(
                lambda day: chirps.filter(
                    ee.Filter.calendarRange(current_year, current_year, "year")
                )
                .filter(ee.Filter.calendarRange(day, day, "day_of_year"))
                .sum()
            )
        ).sum()

        rainfall_deviation = (current_year_value.subtract(long_term_mean)).divide(
            long_term_mean
        )
        roi = rainfall_deviation.reduceRegions(roi, ee.Reducer.mean(), 5566)
        roi = roi.map(
            lambda feature: feature.set(
                "mean",
                ee.Algorithms.If(
                    feature.get("mean"), ee.Number(feature.get("mean")).multiply(100), 0
                ),
            )
        )

        if delta == 7:
            roi = rename_column(
                "mean",
                concat("weekly_rainfall_deviation_", date2str(start_date)),
                roi,
            )
        elif delta == 28:
            roi = rename_column(
                "mean",
                concat("monthly_rainfall_deviation_", date2str(start_date)),
                roi,
            )

        return roi

    def get_weekly_deviation_for_dry_spell(start, end, roi):
        days = ee.List.sequence(start, end)
        years = ee.List.sequence(chirps_available_from_year, current_year - 1)
        long_term_mean = ee.ImageCollection(
            years.map(
                lambda year: ee.ImageCollection(
                    days.map(
                        lambda day: chirps.filter(
                            ee.Filter.calendarRange(year, year, "year")
                        )
                        .filter(ee.Filter.calendarRange(day))
                        .sum()
                    )
                ).sum()
            )
        ).mean()
        current_year_value = ee.ImageCollection(
            days.map(
                lambda day: chirps.filter(
                    ee.Filter.calendarRange(current_year, current_year, "year")
                )
                .filter(ee.Filter.calendarRange(day, day, "day_of_year"))
                .sum()
            )
        ).sum()

        rainfall_deviation = (current_year_value.subtract(long_term_mean)).divide(
            long_term_mean
        )

        roi = rainfall_deviation.reduceRegions(roi, ee.Reducer.mean(), 5566)
        roi = roi.map(
            lambda feature: feature.set(
                "mean",
                ee.Algorithms.If(
                    feature.get("mean"), ee.Number(feature.get("mean")).multiply(100), 0
                ),
            )
        )

        return roi

    def dry_spell_label(value):
        value = ee.Number(value)
        return ee.Algorithms.If(value.lte(-50), 1, 0)

    def get_monthly_dry_spell(start_date, roi):
        w1s = get_day_of_year(start_date)
        w2s = w1s.add(7)
        w3s = w1s.add(14)
        w4s = w1s.add(21)

        w1e = w1s.add(6)
        w2e = w2s.add(6)
        w3e = w3s.add(6)
        w4e = w4s.add(6)

        w1dev = (
            get_weekly_deviation_for_dry_spell(w1s, w1e, roi)
            .aggregate_array("mean")
            .map(dry_spell_label)
        )
        w2dev = (
            get_weekly_deviation_for_dry_spell(w2s, w2e, roi)
            .aggregate_array("mean")
            .map(dry_spell_label)
        )
        w3dev = (
            get_weekly_deviation_for_dry_spell(w3s, w3e, roi)
            .aggregate_array("mean")
            .map(dry_spell_label)
        )
        w4dev = (
            get_weekly_deviation_for_dry_spell(w4s, w4e, roi)
            .aggregate_array("mean")
            .map(dry_spell_label)
        )

        w1_ee = ee.List(w1dev)
        w2_ee = ee.List(w2dev)
        w3_ee = ee.List(w3dev)
        w4_ee = ee.List(w4dev)

        zipped_list = w1_ee.zip(w2_ee).zip(w3_ee).zip(w4_ee)
        dryspell = zipped_list.map(
            lambda item: ee.Number(ee.List(ee.List(ee.List(item).get(0)).get(0)).get(0))
            .And(ee.Number(ee.List(ee.List(ee.List(item).get(0)).get(0)).get(1)))
            .And(ee.Number(ee.List(ee.List(item).get(0)).get(1)))
            .And(ee.Number(ee.List(item).get(1)))
        )

        roi_dryspell_zip = roi.toList(roi.size()).zip(dryspell)

        roi = roi_dryspell_zip.map(
            lambda zlist: ee.Feature(ee.List(zlist).get(0)).set(
                concat("dryspell_", date2str(start_date)),
                ee.Number(ee.List(zlist).get(1)),
            )
        )

        roi = ee.FeatureCollection(roi)

        return roi

    def get_max_dryspell_length(roi):

        first_feature = roi.first()

        all_properties = ee.Feature(first_feature).propertyNames()
        dryspell_properties = all_properties.filter(
            ee.Filter.stringStartsWith("item", "dryspell")
        )

        # Function to calculate max consecutive 1s for each feature
        def dry_spell_length(feature):
            # Function to update max consecutive count
            def update_max(prop, prev):
                value = ee.Number(feature.get(prop))
                prev_dict = ee.Dictionary(prev)
                current = ee.Algorithms.If(
                    value.eq(1),
                    ee.Number(prev_dict.get("current")).add(1),
                    ee.Number(0),
                )
                max_val = ee.Number(prev_dict.get("max")).max(current)
                return ee.Dictionary({"max": max_val, "current": current})

            # Calculate max consecutive 1s
            result = dryspell_properties.iterate(
                update_max, ee.Dictionary({"max": 0, "current": 0})
            )

            # Get the max value
            max_dry_spell = ee.Number(ee.Dictionary(result).get("max"))
            max_dry_spell = ee.Algorithms.If(
                max_dry_spell.gt(0),
                max_dry_spell.add(3),
                ee.Number(0),
            )

            # Add the result to a new property
            return feature.set("dryspell_length_" + str(current_year), max_dry_spell)

        roi = roi.map(dry_spell_length)
        roi = ee.FeatureCollection(roi)
        return roi

    def get_spi(start_date, roi):
        # start_date is ee.Date object
        # roi is ee.FeatureCollection
        start_date = ee.Date(start_date)
        roi = ee.FeatureCollection(roi)
        delta = 28  # Computing SPI-1

        starting_day_of_year = get_day_of_year(start_date)
        days = ee.List.sequence(starting_day_of_year, starting_day_of_year.add(delta))
        years = ee.List.sequence(chirps_available_from_year, current_year - 1)

        longterm_mean = ee.ImageCollection(
            years.map(
                lambda year: ee.ImageCollection(
                    days.map(
                        lambda day: chirps.filter(
                            ee.Filter.calendarRange(year, year, "year")
                        )
                        .filter(ee.Filter.calendarRange(day))
                        .mean()
                    )
                ).sum()
            )
        ).reduce(ee.Reducer.mean())
        longterm_stddev = ee.ImageCollection(
            years.map(
                lambda year: ee.ImageCollection(
                    days.map(
                        lambda day: chirps.filter(
                            ee.Filter.calendarRange(year, year, "year")
                        )
                        .filter(ee.Filter.calendarRange(day))
                        .mean()
                    )
                ).sum()
            )
        ).reduce(ee.Reducer.stdDev())

        current_year_value = ee.ImageCollection(
            days.map(
                lambda day: chirps.filter(
                    ee.Filter.calendarRange(current_year, current_year, "year")
                )
                .filter(ee.Filter.calendarRange(day, day, "day_of_year"))
                .sum()
            )
        ).reduce(ee.Reducer.mean())

        spi = current_year_value.select(["precipitation_mean"]).subtract(
            longterm_mean.select(["precipitation_mean"])
        )
        spi = spi.divide(longterm_stddev.select(["precipitation_stdDev"])).rename("spi")

        roi = spi.reduceRegions(roi, ee.Reducer.mean(), 5566)
        roi = rename_column("mean", concat("spi_", date2str(start_date)), roi)

        return roi

    def get_kharif_cropping_pixel_mask(aez):
        single_kharif = ee.Image.constant(0)
        single_non_kharif = ee.Image.constant(0)
        double = ee.Image.constant(0)
        triple = ee.Image.constant(0)

        year = current_year
        single_kharif = single_kharif.Or(
            ee.Image(lulc.get(year - lulc_available_from_year))
            .select(["predicted_label"])
            .eq(8)
        )
        single_non_kharif = single_non_kharif.Or(
            ee.Image(lulc.get(year - lulc_available_from_year))
            .select(["predicted_label"])
            .eq(9)
        )
        double = double.Or(
            ee.Image(lulc.get(year - lulc_available_from_year))
            .select(["predicted_label"])
            .eq(10)
        )
        triple = triple.Or(
            ee.Image(lulc.get(year - lulc_available_from_year))
            .select(["predicted_label"])
            .eq(11)
        )

        kharif = single_kharif.Or(double).Or(triple)
        mask = kharif.clip(aez.geometry())

        return mask

    def get_percentage_of_area_cropped(roi):
        single_kharif = ee.Image.constant(0)
        single_non_kharif = ee.Image.constant(0)
        double = ee.Image.constant(0)
        triple = ee.Image.constant(0)

        for year in range(lulc_available_from_year, current_year + 1):
            single_kharif = single_kharif.Or(
                ee.Image(lulc.get(year - lulc_available_from_year))
                .select(["predicted_label"])
                .eq(8)
            )
            single_non_kharif = single_non_kharif.Or(
                ee.Image(lulc.get(year - lulc_available_from_year))
                .select(["predicted_label"])
                .eq(9)
            )
            double = double.Or(
                ee.Image(lulc.get(year - lulc_available_from_year))
                .select(["predicted_label"])
                .eq(10)
            )
            triple = triple.Or(
                ee.Image(lulc.get(year - lulc_available_from_year))
                .select(["predicted_label"])
                .eq(11)
            )

        kharif_cropable = single_kharif.Or(double).Or(triple)

        current_year_single_kharif = cur_year_crop_img.select(["predicted_label"]).eq(8)
        current_year_single_non_kharif = cur_year_crop_img.select(
            ["predicted_label"]
        ).eq(9)
        current_year_double = cur_year_crop_img.select(["predicted_label"]).eq(10)
        current_year_triple = cur_year_crop_img.select(["predicted_label"]).eq(11)

        kharif_cropped = current_year_single_kharif.Or(current_year_double).Or(
            current_year_triple
        )

        roi = kharif_cropable.reduceRegions(roi, ee.Reducer.sum(), lulc_scale)
        args = ee.Dictionary({"scale": lulc_scale})
        roi = rename_column_with_transformation(
            "sum", "kharif_croppable_sqkm", roi, sqm2sqkm, args
        )

        roi = kharif_cropped.reduceRegions(roi, ee.Reducer.sum(), lulc_scale)
        roi = rename_column_with_transformation(
            "sum", "kharif_cropped_sqkm_" + str(current_year), roi, sqm2sqkm, args
        )

        roi = roi.map(
            lambda feature: feature.set(
                "percent_of_area_cropped_kharif_" + str(current_year),
                ee.Number(feature.get("kharif_cropped_sqkm_" + str(current_year)))
                .divide(ee.Number(feature.get("kharif_croppable_sqkm")))
                .multiply(100),
            )
        )

        return roi

    def get_monthly_vci(start_date, roi, cropping_mask):
        # start_date is ee.Date object
        # roi is ee.FeatureCollection
        start_date = ee.Date(start_date)
        roi = ee.FeatureCollection(roi)

        # delta is js integer, adjust delta according to ndvi availability
        delta = 28

        starting_day_of_year = get_day_of_year(start_date)

        years_ndvi = ee.List.sequence(modis_ndvi_available_from_year, current_year)
        years_ndwi = ee.List.sequence(modis_ndwi_available_from_year, current_year)

        start = starting_day_of_year
        end = start.add(delta)

        ndvis = years_ndvi.map(
            lambda year: modis_ndvi.filter(ee.Filter.calendarRange(year, year, "year"))
            .filter(ee.Filter.calendarRange(start, end, "day_of_year"))
            .select("NDVI")
            .mean()
        )

        ndvis = ee.ImageCollection(ndvis)
        ndvi_cur = (
            modis_ndvi.filter(
                ee.Filter.calendarRange(current_year, current_year, "year")
            )
            .filter(ee.Filter.calendarRange(start, end, "day_of_year"))
            .select("NDVI")
            .mean()
        )

        ndwis = years_ndwi.map(
            lambda year: modis_ndwi.filter(ee.Filter.calendarRange(year, year, "year"))
            .filter(ee.Filter.calendarRange(start, end, "day_of_year"))
            .select("NDWI")
            .mean()
        )

        ndwis = ee.ImageCollection(ndwis)
        ndwi_cur = (
            modis_ndwi.filter(
                ee.Filter.calendarRange(current_year, current_year, "year")
            )
            .filter(ee.Filter.calendarRange(start, end, "day_of_year"))
            .select("NDWI")
            .mean()
        )

        ndvi_min = ndvis.reduce(ee.Reducer.min())
        ndvi_max = ndvis.reduce(ee.Reducer.max())

        vci_ndvi_numerator = ndvi_cur.select("NDVI").subtract(
            ndvi_min.select(["NDVI_min"])
        )
        vci_ndvi_denomenator = ndvi_max.select(["NDVI_max"]).subtract(
            ndvi_min.select(["NDVI_min"])
        )
        vci_ndvi = (vci_ndvi_numerator.select("NDVI")).divide(
            vci_ndvi_denomenator.select(["NDVI_max"])
        )

        ndwi_min = ndwis.reduce(ee.Reducer.min())
        ndwi_max = ndwis.reduce(ee.Reducer.max())
        vci_ndwi_numerator = ndwi_cur.select("NDWI").subtract(
            ndwi_min.select(["NDWI_min"])
        )
        vci_ndwi_denomenator = ndwi_max.select(["NDWI_max"]).subtract(
            ndwi_min.select(["NDWI_min"])
        )
        vci_ndwi = (vci_ndwi_numerator.select("NDWI")).divide(
            vci_ndwi_denomenator.select(["NDWI_max"])
        )

        vci = vci_ndvi.min(vci_ndwi)

        vci = ee.Image(vci).multiply(cropping_mask)
        vci = vci.multiply(100)

        roi = vci.reduceRegions(roi, ee.Reducer.sum(), modis_ndvi_scale)
        pc = cropping_mask.reduceRegions(roi, ee.Reducer.sum(), modis_ndvi_scale)

        def inner(feature):
            pkid = feature.get("uid")
            f1 = ee.Feature(pc.filter(ee.Filter.eq("uid", pkid)).first())
            nume = feature.get("sum")
            deno = f1.get("sum")
            nume = ee.Number(nume)
            deno = ee.Number(deno)
            vci_value = nume.divide(deno)
            feature = feature.set(concat("vci_", date2str(start_date)), vci_value)
            return feature

        roi = roi.map(inner)
        return roi

    def calculate_weight(image_date, start, end):
        image_date = ee.Date(image_date)
        start = ee.Date.fromYMD(current_year, 1, 1).advance(start.subtract(1), "day")
        end = ee.Date.fromYMD(current_year, 1, 1).advance(end.subtract(1), "day")

        s = image_date.advance(-8, "day")
        e = image_date

        # Find the start and end of the overlapping region
        s1 = s
        e1 = e
        s2 = start
        e2 = end
        s1millis = s1.millis()
        e1millis = e1.millis()
        s2millis = s2.millis()
        e2millis = e2.millis()

        # Find the start and end of the overlapping region
        start_overlap_millis = s1millis.max(s2millis)
        end_overlap_millis = e1millis.min(e2millis)

        start_overlap = ee.Date(
            ee.Algorithms.If(start_overlap_millis.eq(s1millis), s1, s2)
        )
        end_overlap = ee.Date(ee.Algorithms.If(end_overlap_millis.eq(e1millis), e1, e2))

        # Calculate the length of the overlapping region in days
        overlap_length = end_overlap.difference(start_overlap, "day").add(1)
        overlap_length = overlap_length.min(8)

        weight = overlap_length.divide(8)
        return weight

    def get_monthly_mai(start_date, roi, cropping_mask):
        # start_date is ee.Date object
        # roi is ee.FeatureCollection
        start_date = ee.Date(start_date)
        # end_date = ee.Date.fromYMD(start_date.get('year'), 1, 1).advance(end.subtract(1), 'day')
        roi = ee.FeatureCollection(roi)

        # delta is js integer, adjust delta according to ndvi availability
        delta = 28

        start = get_day_of_year(start_date)
        end = start.add(delta)
        take_images_till = start.add(
            delta + 7
        )  # take one more images because, for the last interval,
        # the data will come from the image which is out of the interval

        data = modis.filter(
            ee.Filter.calendarRange(current_year, current_year, "year")
        ).filter(ee.Filter.calendarRange(start, take_images_till, "day_of_year"))

        et = data.select("ET")
        pet = data.select("PET")
        # get the dates of the images of modis
        dates = et.aggregate_array("system:index").map(
            lambda date: ee.Date.parse("Y_M_d", ee.String(date))
        )

        # compute the weight of each image
        # but, for example if start is 2nd july and end is 26th july and modis image is
        # available at 4th, 12th, 20th and 28th then multiply the image obtained at 4thjuly
        # by 2 / 8 as 2days of 8days have to be taken for the first image and multiply by 6 / 8
        # the last image i.e.the image obtained by 28th july as 6days out of 8days are
        # falling in the start and end (both inclusive).
        weights = dates.map(lambda date: calculate_weight(date, start, end))

        et_w = et.toList(et.size()).zip(weights)
        pet_w = pet.toList(pet.size()).zip(weights)

        def inner1(et_list):
            et_list = ee.List(et_list)
            img = ee.Image(et_list.get(0))
            weight = ee.Number(et_list.get(1))
            return ((img.multiply(weight)).multiply(0.1)).toDouble()

        new_et = et_w.map(inner1)
        new_et = ee.ImageCollection(new_et).sum()

        def inner2(pet_list):
            pet_list = ee.List(pet_list)
            img = ee.Image(pet_list.get(0))
            weight = ee.Number(pet_list.get(1))
            return ((img.multiply(weight)).multiply(0.1)).toDouble()

        new_pet = pet_w.map(inner2)
        new_pet = ee.ImageCollection(new_pet).sum()

        new_et = ee.Image(new_et).multiply(cropping_mask)
        new_pet = ee.Image(new_pet).multiply(cropping_mask)

        roi = new_et.reduceRegions(roi, ee.Reducer.sum(), modis_scale)
        roi = rename_column("sum", concat("et_", date2str(start_date)), roi)

        roi = new_pet.reduceRegions(roi, ee.Reducer.sum(), modis_scale)
        roi = rename_column("sum", concat("pet_", date2str(start_date)), roi)

        # pc = croppingMask.reduceRegions(roi, ee.Reducer.sum(), modis_scale)

        def inner3(feature):
            et_ = ee.Feature(feature).get(concat("et_", date2str(start_date)))
            pet_ = ee.Feature(feature).get(concat("pet_", date2str(start_date)))

            mai_ = ee.Number(et_).divide(ee.Number(pet_))
            mai_ = mai_.multiply(100)
            feature = feature.set(concat("mai_", date2str(start_date)), mai_)
            return feature

        roi = roi.map(inner3)

        # if we do mai=et.divide(pet) and then do reduceregions on mai then
        return roi

    def join(roi, to_pick, weekly, fc_of_fc, start_dates, index):
        # roi = roi.toList(roi.size())
        # roi: ee.List -> result will be written over this
        # to_pick: js string -> pick to_pick from the source
        # fc_of_fc: ee.FeatureCollection of ee.FeatureCollection
        # start_dates: ee.List of ee.Date
        # index: pick fc_of_fc.get(index) and start_dates.get(index)
        # if weekly is true then use start_dates otherwise there is a single value of the entire season

        if weekly:
            roi = ee.List(roi)

            fc = ee.FeatureCollection(
                ee.FeatureCollection(fc_of_fc).toList(fc_of_fc.size()).get(index)
            )
            fc = fc.toList(fc.size())

            date = ee.Date(start_dates.get(index))
            zipped = roi.zip(fc)

            def inner(zlist):
                zlist = ee.List(zlist)
                f1 = ee.Feature(zlist.get(0))
                f2 = ee.Feature(zlist.get(1))
                value = f2.get(concat(to_pick, date2str(date)))
                f1 = f1.set(concat(to_pick, date2str(date)), value)
                return f1

            result = zipped.map(inner)
            return result
        else:
            roi = ee.List(roi)
            fc = fc_of_fc.toList(fc_of_fc.size())

            zipped = roi.zip(fc)

            def inner(zlist):
                zlist = ee.List(zlist)
                f1 = ee.Feature(zlist.get(0))
                f2 = ee.Feature(zlist.get(1))
                value = f2.get(to_pick)
                f1 = f1.set(to_pick, value)
                return f1

            result = zipped.map(inner)
            return result

    # Rainfall Deviation Table
    # ------------------------
    # if rainfalldev '+19 to -19' then 'Normal rf'
    # if rainfalldev '-20 to -59' then 'Deficit rf'
    # if rainfalldev '-60 to -100' then 'Scanty rf'
    def rf_dev_table(rfdev):
        rfdev = ee.Number(rfdev)
        return ee.Algorithms.If(
            rfdev.gte(-19),
            "Normal rf",
            ee.Algorithms.If(rfdev.gte(-59), "Deficit rf", "Scanty rf"),
        )

    # VCI Value( %) Vegetation Condition
    # 60 - 100 Good
    # 40 - 60 Fair
    # 0 - 40 Poor
    def vci_table(vci):
        # returns 3 if severe
        #         2 if moderate
        #         1 otherwise
        # vci is not available at all places, in that case vci is returned as 1

        vci = ee.Number(vci)
        return ee.Algorithms.If(
            vci,
            ee.Algorithms.If(vci.lte(40), 3, ee.Algorithms.If(vci.lte(60), 2, 1)),
            1,
        )

    # MAI (%) Agricultural Drought Class
    # 76 – 100 No drought
    # 51-75 Mild drought
    # 26-50 Moderate drought
    # 0-25 Severe drought
    def mai_table(mai):
        # returns 3 if severe
        #         2 if moderate
        #         1 otherwise
        mai = ee.Number(mai)
        return ee.Algorithms.If(mai.lte(25), 3, ee.Algorithms.If(mai.lte(50), 2, 1))

    #     Area Sown(%) Drought Condition
    #     0-33.3 Severe drought
    #     33.3-50 Moderate drought
    #     50-100 Mild or No drought
    def pas_table(pas):
        # returns 3 if severe
        #         2 if moderate
        #         1 otherwise
        pas = ee.Number(pas)
        return ee.Algorithms.If(pas.lte(33.3), 3, ee.Algorithms.If(pas.lte(50), 2, 1))

    # *************************************************
    #     ('Rf Dev/SPI', 'Dry spell', 'Drought trigger')
    #     -----------------------------------------------
    #     if 'Deficit or scanty rf/SPI<-1' and 'Yes' then 'Yes'
    #     if 'Deficit or scanty rf/SPI<-1' and 'No' then 'Yes if rainfall is scanty or SPI<-1.5, else No'
    #     if 'Normal rf/SPI>-1' and 'Yes' then 'Yes'
    #     if 'Normal rf/SPI>-1' and 'No' then 'No'
    #     *************************************************
    def get_meteorological_drought(start_date, roi):
        # start_date: ee.Date
        # roi: ee.FeatureCollection
        # roi must contain the following columns
        #     dryspell_<start_date>
        #     spi_<start_date>
        #     monthly_rainfall_deviation_<start_date>

        roi = ee.FeatureCollection(roi)
        start_date = ee.Date(start_date)
        roi = roi.map(
            lambda feat: feat.set(
                concat("rfdev_class_", date2str(start_date)),
                rf_dev_table(
                    feat.get(
                        concat("monthly_rainfall_deviation_", date2str(start_date))
                    )
                ),
            )
        )

        def inner(feature):
            dryspell_col = concat("dry_spell_", date2str(start_date))
            rfdev_col = concat("rfdev_class_", date2str(start_date))
            spi_col = concat("spi_", date2str(start_date))

            dryspell_value = feature.get(dryspell_col)
            rfdev_value = feature.get(rfdev_col)
            spi_value = ee.Number(feature.get(spi_col))
            spi_value = ee.Algorithms.If(spi_value, spi_value, 0)
            spi_value = ee.Number(spi_value)

            # if normal rf:
            #     if dryspell:
            #         md = 1
            #     otherwise:
            #         md = 0
            # otherwise:
            #     if dryspell:
            #         md = 1
            #     otherwise:
            #         if scanty rf:
            #             md = 1
            #         else if spi<-1.5:
            #             md = 1
            #         otherwise:
            #             md = 0

            # return feature

            # normal_rf = ee.Algorithms.IsEqual(rfdev_value, "Normal rf")
            dryspell_condition = ee.Algorithms.IsEqual(dryspell_value, 1)
            scanty_rf = ee.Algorithms.IsEqual(rfdev_value, "Scanty rf")
            spi_condition = spi_value.lt(-1.5)

            md = ee.Algorithms.If(
                dryspell_condition,
                1,
                ee.Algorithms.If(scanty_rf, 1, ee.Algorithms.If(spi_condition, 1, 0)),
            )

            # Add the 'md' property to the feature
            feature = feature.set(
                concat("meteorological_drought_", date2str(start_date)), md
            )
            return feature

        roi = roi.map(inner)
        return roi

    def get_drought(start_date, roi):
        # start_date: ee.Date
        # roi: ee.FeatureCollection
        # roi must contain the following columns
        #     meteorological_drought_<start_date>
        #     mai_<start_date>
        #     vci_<start_date>
        #     percent_of_area_cropped_kharif_<current_year>
        start_date = ee.Date(start_date)

        def inner(feature):
            # severe_value = 0
            # moderate_value = 0
            # if mai is severe:
            #     severe_value++
            # else if mai is moderate:
            #     moderate_value++
            #
            # if vci is severe:
            #     severe_value++
            # else if vci is moderate:
            #     moderate_value++
            #
            # if pas is severe:
            #     severe_value++
            # else if pas is moderate:
            #     moderate_value++
            #
            # if md == 1:
            #     if severe_value==3:
            #         d = 3
            #     else if moderate_value>=2:
            #         d = 2
            #     else:
            #         d = 1
            # else:
            #     d = 0

            vci_value = ee.Number(feature.get(concat("vci_", date2str(start_date))))
            mai_value = ee.Number(feature.get(concat("mai_", date2str(start_date))))
            pas_value = ee.Number(
                feature.get("percent_of_area_cropped_kharif_" + str(current_year))
            )
            md = ee.Number(
                feature.get(concat("meteorological_drought_", date2str(start_date)))
            )

            vci_class = ee.Number(vci_table(vci_value))
            mai_class = ee.Number(mai_table(mai_value))
            pas_class = ee.Number(pas_table(pas_value))

            num_of_indicators_denoting_severe = ee.Number(0)
            num_of_indicators_denoting_moderate = ee.Number(0)

            num_of_indicators_denoting_severe = num_of_indicators_denoting_severe.add(
                ee.Algorithms.If(vci_class.eq(3), 1, 0)
            )
            num_of_indicators_denoting_severe = num_of_indicators_denoting_severe.add(
                ee.Algorithms.If(mai_class.eq(3), 1, 0)
            )
            num_of_indicators_denoting_severe = num_of_indicators_denoting_severe.add(
                ee.Algorithms.If(pas_class.eq(3), 1, 0)
            )

            num_of_indicators_denoting_moderate = (
                num_of_indicators_denoting_moderate.add(
                    ee.Algorithms.If(vci_class.eq(2), 1, 0)
                )
            )
            num_of_indicators_denoting_moderate = (
                num_of_indicators_denoting_moderate.add(
                    ee.Algorithms.If(mai_class.eq(2), 1, 0)
                )
            )
            num_of_indicators_denoting_moderate = (
                num_of_indicators_denoting_moderate.add(
                    ee.Algorithms.If(pas_class.eq(2), 1, 0)
                )
            )

            drought = ee.Algorithms.If(
                md.eq(0),
                0,
                ee.Algorithms.If(
                    num_of_indicators_denoting_severe.eq(3),
                    3,
                    ee.Algorithms.If(num_of_indicators_denoting_moderate.gte(2), 2, 1),
                ),
            )

            feature = feature.set(concat("drought_", date2str(start_date)), drought)
            return feature

        roi = roi.map(inner)
        return roi

    def get_weekly_labels(date, start_date, roi):
        date = ee.Date(date)
        start_date = ee.Date(start_date)
        roi = ee.FeatureCollection(roi)

        # print(date)
        # print(start)
        start_millis = start_date.millis()
        date1 = date
        date2 = date.advance(-7, "day")
        date3 = date.advance(-14, "day")
        date4 = date.advance(-21, "day")

        date1_millis = date1.millis()
        date2_millis = date2.millis()
        date3_millis = date3.millis()
        date4_millis = date4.millis()
        # print(date1, date2, date3, date4)

        # print(date1Millis, date2_millis, date3_millis, date4_millis)

        date1 = ee.Algorithms.If(start_millis.lt(date1_millis), date1, start_date)
        date2 = ee.Algorithms.If(start_millis.lt(date2_millis), date2, start_date)
        date3 = ee.Algorithms.If(start_millis.lt(date3_millis), date3, start_date)
        date4 = ee.Algorithms.If(start_millis.lt(date4_millis), date4, start_date)

        # print(date1, date2, date3, date4)

        def inner(feature):
            value1 = ee.Number(feature.get(concat("drought_", date2str(date1))))
            value2 = ee.Number(feature.get(concat("drought_", date2str(date2))))
            value3 = ee.Number(feature.get(concat("drought_", date2str(date3))))
            value4 = ee.Number(feature.get(concat("drought_", date2str(date4))))

            weekly_label = value1.max(value2).max(value3).max(value4)

            feature = feature.set(concat("weekly_label_", date2str(date)), weekly_label)
            return feature

        roi = roi.map(inner)
        return roi

    def get_weekly_freq_of_diff_types_of_drought(s_dates, roi):
        def inner(feature):
            drought_labels = s_dates.iterate(
                lambda date, prev: ee.List(prev).add(
                    feature.get(concat("weekly_label_", date2str(date)))
                ),
                ee.List([]),
            )
            drought_labels = ee.List(drought_labels)
            total_weeks = drought_labels.size()

            number_of_weeks_in_no_drought = drought_labels.filter(
                ee.Filter.eq("item", 0)
            ).size()
            number_of_weeks_in_mild_drought = drought_labels.filter(
                ee.Filter.eq("item", 1)
            ).size()
            number_of_weeks_in_moderate_drought = drought_labels.filter(
                ee.Filter.eq("item", 2)
            ).size()
            number_of_weeks_in_severe_drought = drought_labels.filter(
                ee.Filter.eq("item", 3)
            ).size()

            feature = feature.set("total_weeks_" + str(current_year), total_weeks)
            feature = feature.set(
                "number_of_weeks_in_no_drought_" + str(current_year),
                number_of_weeks_in_no_drought,
            )
            feature = feature.set(
                "number_of_weeks_in_mild_drought_" + str(current_year),
                number_of_weeks_in_mild_drought,
            )
            feature = feature.set(
                "number_of_weeks_in_moderate_drought_" + str(current_year),
                number_of_weeks_in_moderate_drought,
            )
            feature = feature.set(
                "number_of_weeks_in_severe_drought_" + str(current_year),
                number_of_weeks_in_severe_drought,
            )
            feature = feature.set(
                "drought_labels_" + str(current_year),
                ee.String.encodeJSON(drought_labels),
            )
            return feature

        roi = roi.map(inner)
        return roi

    def get_drought_freq_intensity(roi, threshold):
        th = threshold
        threshold = ee.Number(threshold)

        def inner(feature):
            weekly_labels = feature.get("drought_labels_" + str(current_year))
            weekly_labels = ee.String(weekly_labels).decodeJSON()
            weekly_labels = ee.List(weekly_labels)

            freq = weekly_labels.filter(ee.Filter.gte("item", threshold)).size()
            intensity = weekly_labels.filter(ee.Filter.gte("item", threshold))
            intensity = ee.List(intensity)
            intensity = intensity.reduce(ee.Reducer.sum())
            intensity = ee.Number(intensity).divide(freq)

            feature = feature.set(
                "freq_of_drought_" + str(current_year) + "_at_threshold_" + str(th),
                freq,
            )
            feature = feature.set(
                "intensity_of_drought_"
                + str(current_year)
                + "_at_threshold_"
                + str(th),
                intensity,
            )
            return feature

        roi = roi.map(inner)
        return roi

    # ************ COMPUTATION START *************
    monsoon_onset = get_monsoon_on_set_date(current_year, aoi)
    monsoon_cessation = get_monsoon_cessation_date()  # 30th oct
    start_dates = get_week_start_dates(monsoon_onset, monsoon_cessation)
    # weekly_rainfall_deviations = start_dates.map(
    #     lambda date: get_rainfall_deviation(date, aez, 7)
    # )
    monthly_dryspells = start_dates.map(lambda date: get_monthly_dry_spell(date, aoi))
    monthly_rainfall_deviations = start_dates.map(
        lambda date: get_rainfall_deviation(date, aoi, 28)
    )
    monthly_spis = start_dates.map(lambda date: get_spi(date, aoi))
    cropping_pixel_mask = get_kharif_cropping_pixel_mask(aoi)
    percentage_of_area_sown = get_percentage_of_area_cropped(aoi)
    monthly_vcis = start_dates.map(
        lambda date: get_monthly_vci(date, aoi, cropping_pixel_mask)
    )
    monthly_mais = start_dates.map(
        lambda date: get_monthly_mai(date, aoi, cropping_pixel_mask)
    )
    aez_copy = aoi
    aoi = aoi.map(
        lambda feature: feature.set(
            "monsoon_onset_" + str(current_year), date2str(monsoon_onset)
        )
    )
    aoi = aoi.toList(aoi.size())
    index_list = ee.List.sequence(0, start_dates.size().subtract(1))
    aoi = index_list.iterate(
        lambda cur_index, prev: join(
            prev, "dryspell_", True, monthly_dryspells, start_dates, cur_index
        ),
        aoi,
    )
    aoi = index_list.iterate(
        lambda cur_index, prev: join(
            prev,
            "monthly_rainfall_deviation_",
            True,
            monthly_rainfall_deviations,
            start_dates,
            cur_index,
        ),
        aoi,
    )
    aoi = index_list.iterate(
        lambda cur_index, prev: join(
            prev, "spi_", True, monthly_spis, start_dates, cur_index
        ),
        aoi,
    )
    aoi = index_list.iterate(
        lambda cur_index, prev: join(
            prev, "vci_", True, monthly_vcis, start_dates, cur_index
        ),
        aoi,
    )
    aoi = join(aoi, "kharif_croppable_sqkm", False, percentage_of_area_sown, [], -1)
    aoi = join(
        aoi,
        "kharif_cropped_sqkm_" + str(current_year),
        False,
        percentage_of_area_sown,
        [],
        -1,
    )
    aoi = join(
        aoi,
        "percent_of_area_cropped_kharif_" + str(current_year),
        False,
        percentage_of_area_sown,
        [],
        -1,
    )
    aoi = index_list.iterate(
        lambda cur_index, prev: join(
            prev, "mai_", True, monthly_mais, start_dates, cur_index
        ),
        aoi,
    )
    aoi = aez_copy.map(
        lambda feature: ee.Feature(
            (ee.List(aoi).filter(ee.Filter.eq("uid", feature.get("uid")))).get(0)
        )
    )

    def aez_iterate(date, prev):
        prev = get_meteorological_drought(date, prev)
        prev = get_drought(date, prev)
        return prev

    aoi = start_dates.iterate(lambda date, prev: aez_iterate(date, prev), aoi)
    aoi = start_dates.iterate(
        lambda date, prev: get_weekly_labels(date, start_dates.get(0), prev), aoi
    )
    aoi = ee.FeatureCollection(aoi)
    aoi = get_weekly_freq_of_diff_types_of_drought(start_dates, aoi)
    aoi = ee.FeatureCollection(aoi)
    aoi = get_max_dryspell_length(aoi)
    aoi = get_drought_freq_intensity(aoi, 0)
    aoi = get_drought_freq_intensity(aoi, 1)
    aoi = get_drought_freq_intensity(aoi, 2)
    aoi = get_drought_freq_intensity(aoi, 3)

    # Export fecture collection to GEE
    task_id = export_vector_asset_to_gee(aoi, block_name_for_parts, asset_id)
    if task_id:
        task_ids.append(task_id)
        asset_ids.append(asset_id)
