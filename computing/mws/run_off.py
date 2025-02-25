import ee
import datetime
from dateutil.relativedelta import relativedelta
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def run_off(state, district, block, start_date, end_date, is_annual):
    description = (
        ("Runoff_annual_" if is_annual else "Runoff_fortnight_")
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return

    soil = ee.Image("projects/ee-dharmisha-siddharth/assets/HYSOGs250m")
    # srtm = ee.Image("CGIAR/SRTM90_V4")
    srtm2 = ee.Image("USGS/SRTMGL1_003")
    lulc_img = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")

    shape = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )  # feature collection
    geometry = shape.geometry()
    DEM = srtm2.clip(geometry)

    # Calculating Slope
    slope = ee.Terrain.slope(DEM)

    # fc = ee.FeatureCollection(shape)
    size = shape.size()
    size1 = ee.Number(size).subtract(ee.Number(1))

    soil = soil.expression(
        "(b('b1') == 14) ? 4"
        + ": (b('b1') == 13) ? 3"
        + ": (b('b1') == 12) ? 2"
        + ": (b('b1') == 11) ? 1"
        + ": b('b1')"
    ).rename("soil")

    soil = soil.clip(shape).rename("soil").reproject(crs="EPSG:4326", scale=30)

    mws = ee.List.sequence(0, size1)

    f_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    fn_index = 0
    while f_start_date <= end_date:
        if is_annual:
            f_end_date = f_start_date + relativedelta(years=1)
        else:
            if fn_index == 25:
                # Setting date to 1st July if index==25
                f_end_date = f_start_date + relativedelta(months=1, day=1)
                fn_index = 0
            else:
                f_end_date = f_start_date + datetime.timedelta(days=14)
                fn_index += 1

        lulc = lulc_img.filterDate(f_start_date, f_end_date)
        classification = lulc.select("label")

        dwComposite = classification.reduce(ee.Reducer.mode())

        dwComposite = (
            dwComposite.clip(shape).rename("label").reproject(crs="EPSG:4326", scale=30)
        )

        lulc = dwComposite.rename(["lulc"])

        lulc_soil = lulc.addBands(soil)
        lulc_soil = lulc_soil.unmask(0)

        CN2 = lulc_soil.expression(
            "(b('soil') == 1) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 1) and(b('lulc')==1) ? 30"
            + ": (b('soil') == 1) and(b('lulc')==2) ? 39"
            + ": (b('soil') == 1) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 1) and(b('lulc')==4) ? 64"
            + ": (b('soil') == 1) and(b('lulc')==5) ? 39"
            + ": (b('soil') == 1) and(b('lulc')==6) ? 82"
            + ": (b('soil') == 1) and(b('lulc')==7) ? 49"
            +
            # // ": (b('soil') == 1) and(b('lulc')==8) ? 45" +
            ": (b('soil') == 2) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 2) and(b('lulc')==1) ? 55"
            + ": (b('soil') == 2) and(b('lulc')==2) ? 61"
            + ": (b('soil') == 2) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 2) and(b('lulc')==4) ? 75"
            + ": (b('soil') == 2) and(b('lulc')==5) ? 61"
            + ": (b('soil') == 2) and(b('lulc')==6) ? 88"
            + ": (b('soil') == 2) and(b('lulc')==7) ? 69"
            +
            # // ": (b('soil') == 2) and(b('lulc')==8) ? 66" +
            ": (b('soil') == 3) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 3) and(b('lulc')==1) ? 70"
            + ": (b('soil') == 3) and(b('lulc')==2) ? 74"
            + ": (b('soil') == 3) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 3) and(b('lulc')==4) ? 82"
            + ": (b('soil') == 3) and(b('lulc')==5) ? 74"
            + ": (b('soil') == 3) and(b('lulc')==6) ? 91"
            + ": (b('soil') == 3) and(b('lulc')==7) ? 79"
            +
            # // ": (b('soil') == 3) and(b('lulc')==8) ? 77" +
            "  : (b('soil') == 4) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 4) and(b('lulc')==1) ? 77"
            + ": (b('soil') == 4) and(b('lulc')==2) ? 80"
            + ": (b('soil') == 4) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 4) and(b('lulc')==4) ? 85"
            + ": (b('soil') == 4) and(b('lulc')==5) ? 80"
            + ": (b('soil') == 4) and(b('lulc')==6) ? 93"
            + ": (b('soil') == 4) and(b('lulc')==7) ? 84"
            +
            # // ": (b('soil') == 4) and(b('lulc')==8) ? 83" +
            ": (b('soil') == 0) ? 0"
            + ": 0"
        ).rename("CN2")

        CN2 = CN2.clip(shape).rename("CN2").reproject(crs="EPSG:4326", scale=30)

        # CN1 = CN2.expression("-75*CN2/(CN2-175)", {"CN2": CN2.select("CN2")}).rename(
        #     "CN1"
        # )

        # CN1 = CN1.clip(shape).rename("CN1").reproject(crs="EPSG:4326", scale=30)

        CN3 = CN2.expression(
            "CN2*((2.718)**(0.00673*(100-CN2)))", {"CN2": CN2.select("CN2")}
        ).rename("CN3")

        CN3 = CN3.clip(shape).rename("CN3").reproject(crs="EPSG:4326", scale=30)

        slope = slope.rename("slope")

        slope = slope.clip(shape).rename("slope").reproject(crs="EPSG:4326", scale=30)

        part1 = (
            CN3.select("CN3")
            .subtract(CN2.select("CN2"))
            .divide(ee.Number(3))
            .rename("p1")
        )

        part1 = part1.clip(shape).rename("p1").reproject(crs="EPSG:4326", scale=30)

        part2 = slope.expression(
            "1-(2*(2.718)**(-13.86*slope))", {"slope": slope.select("slope")}
        ).rename("p2")

        part2 = part2.clip(shape).rename("p2").reproject(crs="EPSG:4326", scale=30)

        CN2a = slope.expression(
            "p1*p2+CN2",
            {
                "p1": part1.select("p1"),
                "p2": part2.select("p2"),
                "CN2": CN2.select("CN2"),
            },
        ).rename("CN2a")

        CN2a = CN2a.clip(shape).rename("CN2a").reproject(crs="EPSG:4326", scale=30)

        CN1a = CN2a.expression(
            "4.2*CN2a/(10-0.058*CN2a)", {"CN2a": CN2a.select("CN2a")}
        ).rename("CN1a")

        CN1a = CN1a.clip(shape).rename("CN1a").reproject(crs="EPSG:4326", scale=30)

        CN3a = CN2a.expression(
            "23*CN2a/(10+0.13*CN2a)", {"CN2a": CN2a.select("CN2a")}
        ).rename("CN3a")

        CN3a = CN3a.clip(shape).rename("CN3a").reproject(crs="EPSG:4326", scale=30)

        sr1 = CN1a.expression("(25400/CN1a)-254", {"CN1a": CN1a.select("CN1a")}).rename(
            "sr1"
        )

        sr1 = sr1.clip(shape).rename("sr1").reproject(crs="EPSG:4326", scale=30)

        sr2 = CN2a.expression("(25400/CN2a)-254", {"CN2a": CN2a.select("CN2a")}).rename(
            "sr2"
        )

        sr2 = sr2.clip(shape).rename("sr2").reproject(crs="EPSG:4326", scale=30)

        sr3 = CN3a.expression("(25400/CN3a)-254", {"CN3a": CN3a.select("CN3a")}).rename(
            "sr3"
        )

        sr3 = sr3.clip(shape).rename("sr3").reproject(crs="EPSG:4326", scale=30)

        base = ee.Date(f_end_date)

        def ant(i):
            a = ee.Number(i).multiply(ee.Number(-1))
            b = (ee.Number(i).multiply(ee.Number(-1))).subtract(ee.Number(1))
            c = (ee.Number(i).multiply(ee.Number(-1))).subtract(ee.Number(4))
            dtTo = base.advance(a, "day").format("YYYY-MM-dd")
            dtMid = base.advance(b, "day").format("YYYY-MM-dd")
            dtFrom = base.advance(c, "day").format("YYYY-MM-dd")

            dataset = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").filter(
                ee.Filter.date(dtFrom, dtTo)
            )

            antecedent = dataset.reduce(ee.Reducer.sum())
            antecedent = (
                antecedent.clip(shape)
                .select("hourlyPrecipRate_sum")
                .reproject(crs="EPSG:4326", scale=30)
            )

            M2 = CN2a.expression(
                "0.5*(-sr+sqrt(sr**2+4*p*sr))",
                {
                    "sr": sr2.select("sr2"),
                    "p": antecedent.select("hourlyPrecipRate_sum"),
                },
            ).rename("m2")

            M2 = M2.clip(shape).rename("m2").reproject(crs="EPSG:4326", scale=30)

            M1 = CN2a.expression(
                "0.5*(-sr+sqrt(sr**2+4*p*sr))",
                {
                    "sr": sr1.select("sr1"),
                    "p": antecedent.select("hourlyPrecipRate_sum"),
                },
            ).rename("m1")

            M1 = M1.clip(shape).rename("m1").reproject(crs="EPSG:4326", scale=30)

            M3 = CN2a.expression(
                "0.5*(-sr+sqrt(sr**2+4*p*sr))",
                {
                    "sr": sr3.select("sr3"),
                    "p": antecedent.select("hourlyPrecipRate_sum"),
                },
            ).rename("m3")

            M3 = M3.clip(shape).rename("m3").reproject(crs="EPSG:4326", scale=30)

            dataset = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").filter(
                ee.Filter.date(dtMid, dtTo)
            )
            total = dataset.reduce(ee.Reducer.sum())

            total = (
                total.clip(shape)
                .select("hourlyPrecipRate_sum")
                .reproject(crs="EPSG:4326", scale=30)
            )

            runoff = total.expression(
                "(P>=0.2*sr1) and (P5>=0) and (P5<=35) and (((P-0.2*sr1)*(P-0.2*sr1+m1))/(P+0.2*sr1+sr1+m1))>=0? ((P-0.2*sr1)*(P-0.2*sr1+m1))/(P+0.2*sr1+sr1+m1)"
                + ": (P>=0.2*sr2) and (P5>=0) and (P5>35) and (((P-0.2*sr2)*(P-0.2*sr2+m2))/(P+0.2*sr2+sr2+m2))>=0 ? ((P-0.2*sr2)*(P-0.2*sr2+m2))/(P+0.2*sr2+sr2+m2)"
                + ": (P>=0.2*sr3) and (P5>=0) and (P5>52.5) and (((P-0.2*sr3)*(P-0.2*sr3+m3))/(P+0.2*sr3+sr3+m3))>=0 ? ((P-0.2*sr3)*(P-0.2*sr3+m3))/(P+0.2*sr3+sr3+m3)"
                + ":0",
                {
                    "P": total.select("hourlyPrecipRate_sum"),
                    "m1": M1.select("m1"),
                    "m2": M2.select("m2"),
                    "m3": M3.select("m3"),
                    "P5": antecedent.select("hourlyPrecipRate_sum"),
                    "sr2": sr2.select("sr2"),
                    "sr1": sr1.select("sr1"),
                    "sr3": sr3.select("sr3"),
                },
            ).rename("runoff")

            # // runoffs=runoffs.add(runoff);
            return runoff

        ll = ee.List.sequence(0, 364 if is_annual else 14)
        runoffs = ll.map(ant)
        runoffs = ee.ImageCollection(runoffs)
        runoffTotal = runoffs.reduce(ee.Reducer.sum())

        runoffTotal = (
            runoffTotal.clip(shape)
            .select("runoff_sum")
            .reproject(crs="EPSG:4326", scale=30)
        )
        total = runoffTotal
        total = total.clip(shape)
        total = total.select("runoff_sum")
        total = total.expression("p*30*30", {"p": total.select("runoff_sum")}).rename(
            "p"
        )
        stats2 = total.reduceRegions(
            reducer=ee.Reducer.sum(), collection=shape, scale=30
        )

        statsl = ee.List(stats2.toList(size))
        # l = ee.List([])

        def res(m):
            f = ee.Feature(statsl.get(m))
            uid = f.get("uid")
            feat = ee.Feature(shape.filter(ee.Filter.eq("uid", uid)).first())
            val = ee.Number(f.get("sum"))
            a = ee.Number(feat.area())
            val = val.divide(a)
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
                "scale": 30,
                "maxPixels": 1e13,
            }
        )
        task.start()
        print("Successfully started the task run_off", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running run-off task: {e}")
