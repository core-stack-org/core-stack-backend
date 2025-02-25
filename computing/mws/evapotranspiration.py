import ee
import datetime

from dateutil.relativedelta import relativedelta
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists
import calendar


def evapotranspiration(state, district, block, start_date, end_date, is_annual):
    description = (
        ("ET_annual_" if is_annual else "ET_fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    asset_id = get_gee_asset_path(state, district, block) + description
    if is_gee_asset_exists(asset_id):
        return

    bounding_box = ee.Image("projects/ee-dharmisha-siddharth/assets/Hydro_2020_2021_4")
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    bbox_geometry = bounding_box.geometry()
    is_within = bbox_geometry.contains(roi.geometry(), ee.ErrorMargin(1))

    if is_within.getInfo():
        return et_fldas(
            roi,
            start_date,
            end_date,
            is_annual,
            description,
            asset_id,
        )
    else:
        return et_global_fldas(
            roi,
            start_date,
            end_date,
            is_annual,
            description,
            asset_id,
        )


def et_fldas(
    shape,
    start_date,
    end_date,
    is_annual,
    description,
    asset_id,
):
    print("In FLDAS")
    size = shape.size()
    size1 = ee.Number(size).subtract(ee.Number(1))

    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    fn_index = 0  # fortnight index
    s_year = f_start_date.date().year
    while f_start_date < end_date:
        if is_annual:
            f_end_date = f_start_date + relativedelta(years=1)
            image_path = (
                "projects/ee-dharmisha-siddharth/assets/ET_Hydroyear/ET_"
                + str(s_year)
                + "_"
                + str(s_year + 1)
                + "_Hydroyear"
            )
            s_year += 1
        else:
            image_path = (
                "projects/ee-dharmisha-siddharth/assets/Hydro_"
                + str(s_year)
                + "_"
                + str(s_year + 1)
                + "_"
                + str(fn_index)
            )
            if fn_index == 25:
                # Setting date to 1st July if index==25
                f_end_date = f_start_date + relativedelta(months=1, day=1)
                fn_index = 0
                s_year += 1
            else:
                f_end_date = f_start_date + datetime.timedelta(days=14)
                fn_index += 1

        total = ee.Image(image_path)  # downloaded image for ET Hydro_2017_2018_25
        mws = ee.List.sequence(0, size1)
        total = total.select("b1")

        # Total pixels
        pixel_count = total.reduceRegions(
            reducer=ee.Reducer.count(), collection=shape, scale=1113.1949079327357
        )
        pixel_count = pixel_count.toList(size)

        # Total Negative Pixels
        negative_pixels = total.lt(0)

        negative_pixel_count = negative_pixels.reduceRegions(
            reducer=ee.Reducer.sum(), collection=shape, scale=1113.1949079327357
        )

        def ll(k):
            pc = ee.Feature(pixel_count.get(k))
            id = pc.get("uid")
            nc = negative_pixel_count.filter(ee.Filter.eq("uid", id)).first()
            p = ee.Number(pc.get("count"))
            nc = ee.Number(nc.get("sum"))
            val = p.subtract(nc)
            return pc.set("tot", val)

        total_pix = ee.FeatureCollection(mws.map(ll))

        total = total.expression("ET>0?86400*ET:0", {"ET": total.select("b1")})

        stats2 = total.reduceRegions(
            reducer=ee.Reducer.sum(),
            collection=shape,
            scale=1113.1949079327357,
        )

        statsl = stats2.toList(size)

        def res(m):
            f = ee.Feature(statsl.get(m))
            s = ee.Number(f.get("sum"))
            uid = f.get("uid")
            feat = shape.filter(ee.Filter.eq("uid", uid)).first()
            pix = total_pix.filter(ee.Filter.eq("uid", uid)).first()
            pix = ee.Number(pix.get("tot"))
            val = s.divide(pix)
            return feat.set(start_date, val)

        shape = ee.FeatureCollection(mws.map(res))
        f_start_date = f_end_date
        start_date = str(f_start_date.date())
    try:
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": shape,
                "description": description,
                "assetId": asset_id,
            }
        )
        task.start()
        print("Successfully started the task evapotranspiration", task.status())
        return task.status()
    except Exception as e:
        print(f"Error occurred in running evapotranspiration task: {e}")


def et_global_fldas(
    shape,
    start_date,
    end_date,
    is_annual,
    description,
    asset_id,
):
    print("In Global FLDAS")
    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    fn_index = 0  # fortnight index
    s_year = f_start_date.date().year

    size = shape.size()
    size1 = ee.Number(size).subtract(ee.Number(1))

    fldas_dataset = ee.ImageCollection("NASA/FLDAS/NOAH01/C/GL/M/V001")

    while f_start_date < end_date:
        if is_annual:
            f_end_date = f_start_date + relativedelta(years=1)
            s_year += 1
            img = ee.Image.constant(0).clip(shape)
            annual_start_date = f_start_date
            # Loop over all months and individually calculate the ET for each month. Add them later to get ET for an entire year.
            for n in range(12):
                s = annual_start_date
                e = s + relativedelta(months=1) - relativedelta(days=1)
                numberOfDaysInMonth = ee.Date(str(e.date())).get("day")
                image = filter_dataset(
                    annual_start_date, numberOfDaysInMonth, fldas_dataset
                )

                img = img.add(ee.Image(image))
                annual_start_date = annual_start_date + relativedelta(months=1)
            # for n in range(12):
            #     s = annual_start_date  # start_dates.get(n)
            #     e = (
            #         s + relativedelta(months=1) - relativedelta(days=1)
            #     )  # end_dates.get(n)
            #     startDateObj = ee.Date(str(s.date()))
            #     endDateObj = ee.Date(str(e.date()))
            #
            #     dataset = ee.ImageCollection("NASA/FLDAS/NOAH01/C/GL/M/V001").filter(
            #         ee.Filter.date(startDateObj, endDateObj)
            #     )
            #
            #     image = ee.Image(dataset.select("Evap_tavg").first())
            #
            #     numberOfDaysInMonth = endDateObj.get("day")
            #
            #     image = ee.Image(image.multiply(numberOfDaysInMonth.getInfo()))
            #
            #     image = image.expression(
            #         "ET>0?86400*ET:0", {"ET": image.select("Evap_tavg")}
            #     )
            #
            #     img = img.add(ee.Image(image))
            #     annual_start_date = annual_start_date + relativedelta(months=1)

            sd = str(f_start_date.year) + "-07-01"
        else:
            if fn_index == 25:
                """Setting date to 1st July if index==25"""
                f_end_date = f_start_date + relativedelta(months=1, day=1)
                fn_index = 0
                s_year += 1
            else:
                f_end_date = f_start_date + datetime.timedelta(days=14)
                fn_index += 1

            """ Checking if fortnight falls in two months """
            if f_start_date.month != f_end_date.month:
                """If fortnight falls in two months, we have to do separate calculation as FLDAS will give different
                image for both months. So here we are getting number of days that falls in both months in that
                fortnight and passing that to 'filter_dataset' function to do further calculations.
                """
                res = calendar.monthrange(f_start_date.year, f_start_date.month)
                number_of_days = res[1] - f_start_date.day
                img1 = filter_dataset(f_start_date, number_of_days, fldas_dataset)

                number_of_days = f_end_date.day
                img2 = filter_dataset(f_end_date, number_of_days, fldas_dataset)
                img = img1.add(img2)
            else:
                """If fortnight falls in single month"""
                number_of_days = f_end_date.day - f_start_date.day
                img = filter_dataset(f_start_date, number_of_days, fldas_dataset)

            sd = str(f_start_date.date())

        mws = ee.List.sequence(0, size1)

        total = ee.Image(img)
        stats2 = total.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=shape,
            scale=11132,
        )

        statsl = stats2.toList(size)

        def res(m):
            f = ee.Feature(statsl.get(m))
            mean = ee.Number(f.get("mean"))
            uid = f.get("uid")
            feat = shape.filter(ee.Filter.eq("uid", uid)).first()
            return feat.set(sd, mean)

        shape = ee.FeatureCollection(mws.map(res))
        f_start_date = f_end_date

    try:
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": shape,
                "description": description,
                "assetId": asset_id,
            }
        )
        task.start()
        print("Successfully started the task evapotranspiration", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running evapotranspiration task: {e}")


def filter_dataset(f_start_date, number_of_days, fldas_dataset):
    """Extracting first and last date of the month, as FLDAS gives only one output per month by filtering on them
    and contains monthly average values of ET. So we multiply filtered dataset with number of days to get the ET value
    for that month for the required number of days.
    """
    s = str(f_start_date.year) + "-" + str(f_start_date.month) + "-01"
    res = calendar.monthrange(f_start_date.year, f_start_date.month)
    e = str(f_start_date.year) + "-" + str(f_start_date.month) + "-" + str(res[1])

    dataset = fldas_dataset.filter(ee.Filter.date(ee.Date(s), ee.Date(e)))
    image = ee.Image(dataset.select("Evap_tavg").first())
    image = ee.Image(image.multiply(number_of_days))
    image = image.expression("ET>0?86400*ET:0", {"ET": image.select("Evap_tavg")})

    return ee.Image(image)
