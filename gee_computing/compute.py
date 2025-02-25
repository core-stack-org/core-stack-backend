import ee


class ComputeOnGEE:
    def __init__(self):
        ee.Initialize()

    def precipitation(self, data_path, mws_path, start_date, end_date, out_path):
        """
        Computes Precipitation on Google Earth Engine
        """
        # TODO: change this to block specific path and mws layer [comes from one time activity]
        active_district = ee.FeatureCollection(mws_path)
        dataset = ee.ImageCollection(data_path).filter(
            ee.Filter.date(start_date, end_date)
        )

        area = dataset.reduce(ee.Reducer.sum())
        clipped_area = area.clip(active_district)

        clipped_area.expression(
            "(hourlyPrecipRate_sum * 11132 * 11132)/1000",
            {"hourlyPrecipRate_sum": clipped_area.select("hourlyPrecipRate_sum")},
        ).rename("hourlyPrecipRate_sum")

        stats = clipped_area.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=active_district,
            scale=11132,
        )

        # export to gee assets
        try:
            task = ee.batch.Export.table.toAsset(
                **{"collection": stats, "description": "xyz_data", "assetId": out_path}
            )

            task.start()
        except Exception as e:
            print(f"Error occured in running precipitation task: {e}")

        # export to google drive
        # TODO

    def run_off(self, slope_path, soil_path, mws_path, lulc_path):
        slope = ee.Image(slope_path)
        soil = ee.Image(soil_path)
        # TODO: change this
        active_district = ee.FeatureCollection(
            mws_path
        )  # block specific mws layer and path
        lulc = ee.ImageCollection(lulc_path)
        classification = lulc.select("label")
        dw_composite = classification.reduce(ee.Reducer.mode())
        dw_composite = (
            dw_composite.clip(active_district)
            .rename("label")
            .reproject(crs="EPSG:4326", scale=30)
        )
        lulc = dw_composite.rename(["lulc"])
        soil = soil.expression(
            "(b('b1') == 14) ? 4"
            + ": (b('b1') == 13) ? 3"
            + ": (b('b1') == 12) ? 2"
            + ": (b('b1') == 11) ? 1"
            + ": b('b1')"
        )

        soil = (
            soil.clip(active_district)
            .rename("soil")
            .reproject(crs="EPSG:4326", scale=30)
        )

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
            + ": (b('soil') == 2) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 2) and(b('lulc')==1) ? 55"
            + ": (b('soil') == 2) and(b('lulc')==2) ? 61"
            + ": (b('soil') == 2) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 2) and(b('lulc')==4) ? 75"
            + ": (b('soil') == 2) and(b('lulc')==5) ? 61"
            + ": (b('soil') == 2) and(b('lulc')==6) ? 88"
            + ": (b('soil') == 2) and(b('lulc')==7) ? 69"
            + ": (b('soil') == 3) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 3) and(b('lulc')==1) ? 70"
            + ": (b('soil') == 3) and(b('lulc')==2) ? 74"
            + ": (b('soil') == 3) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 3) and(b('lulc')==4) ? 82"
            + ": (b('soil') == 3) and(b('lulc')==5) ? 74"
            + ": (b('soil') == 3) and(b('lulc')==6) ? 91"
            + ": (b('soil') == 3) and(b('lulc')==7) ? 79"
            + "  : (b('soil') == 4) and(b('lulc')==0) ? 0"
            + ": (b('soil') == 4) and(b('lulc')==1) ? 77"
            + ": (b('soil') == 4) and(b('lulc')==2) ? 80"
            + ": (b('soil') == 4) and(b('lulc')==3) ? 0"
            + ": (b('soil') == 4) and(b('lulc')==4) ? 85"
            + ": (b('soil') == 4) and(b('lulc')==5) ? 80"
            + ": (b('soil') == 4) and(b('lulc')==6) ? 93"
            + ": (b('soil') == 4) and(b('lulc')==7) ? 84"
            + ": (b('soil') == 0) ? 0"
            + ": 0"
        ).rename("CN2")

        CN2 = (
            CN2.clip(active_district)
            .rename("CN2")
            .reproject(crs="EPSG: 4326", scale=30)
        )

        CN1 = CN2.expression("-75*CN2/(CN2-175)", {"CN2": CN2.select("CN2")}).rename(
            "CN1"
        )

        CN1 = (
            CN1.clip(active_district).rename("CN1").reproject(crs="EPSG:4326", scale=30)
        )

        CN3 = CN2.expression(
            "CN2*((2.718)**(0.00673*(100-CN2)))", {"CN2": CN2.select("CN2")}
        ).rename("CN3")

        CN3 = (
            CN3.clip(active_district).rename("CN3").reproject(crs="EPSG:4326", scale=30)
        )

        slope = slope.rename("slope")

        slope = (
            slope.clip(active_district)
            .rename("slope")
            .reproject(crs="EPSG:4326", scale=30)
        )

        part1 = (
            CN3.select("CN3")
            .subtract(CN2.select("CN2"))
            .divide(ee.Number(3))
            .rename("p1")
        )

        part1 = (
            part1.clip(active_district)
            .rename("p1")
            .reproject(crs="EPSG:4326", scale=30)
        )

        part2 = slope.expression(
            "1-(2*(2.718)**(-13.86*slope))", {"slope": slope.select("slope")}
        ).rename("p2")

        part2 = (
            part2.clip(active_district)
            .rename("p2")
            .reproject(crs="EPSG:4326", scale=30)
        )

        CN2a = slope.expression(
            "p1*p2+CN2",
            {
                "p1": part1.select("p1"),
                "p2": part2.select("p2"),
                "CN2": CN2.select("CN2"),
            },
        ).rename("CN2a")

        CN2a = (
            CN2a.clip(active_district)
            .rename("CN2a")
            .reproject(crs="EPSG:4326", scale=30)
        )

        CN1a = CN2a.expression(
            "4.2*CN2a/(10-0.058*CN2a)", {"CN2a": CN2a.select("CN2a")}
        ).rename("CN1a")

        CN1a = (
            CN1a.clip(active_district)
            .rename("CN1a")
            .reproject(crs="EPSG:4326", scale=30)
        )

        CN3a = CN2a.expression(
            "23*CN2a/(10+0.13*CN2a)", {"CN2a": CN2a.select("CN2a")}
        ).rename("CN3a")

        CN3a = (
            CN3a.clip(active_district)
            .rename("CN3a")
            .reproject(crs="EPSG:4326", scale=30)
        )

        sr1 = CN1a.expression("(25400/CN1a)-254", {"CN1a": CN1a.select("CN1a")}).rename(
            "sr1"
        )

        sr1 = (
            sr1.clip(active_district).rename("sr1").reproject(crs="EPSG:4326", scale=30)
        )

        sr2 = CN2a.expression("(25400/CN2a)-254", {"CN2a": CN2a.select("CN2a")}).rename(
            "sr2"
        )

        sr2 = (
            sr2.clip(active_district).rename("sr2").reproject(crs="EPSG:4326", scale=30)
        )

        sr3 = CN3a.expression("(25400/CN3a)-254", {"CN3a": CN3a.select("CN3a")}).rename(
            "sr3"
        )

        sr3 = (
            sr3.clip(active_district).rename("sr3").reproject(crs="EPSG:4326", scale=30)
        )

        # Calculating the precipitation
        runoffs = ee.List([])
        precip = ee.List([])

        for i in range(14):
            # TODO: ask what date is this?
            # this would be the current date (use ee.datetime) check docs
            base = ee.Date("2016-07-28")
            dtTo = ee.Date(base).advance(-i, "day").format("YYYY-MM-dd").getInfo()
            dtMid = ee.Date(base).advance(-i - 1, "day").format("YYYY-MM-dd").getInfo()
            dtFrom = ee.Date(base).advance(-i - 4, "day").format("YYYY-MM-dd").getInfo()
            # jamui=ee.FeatureCollection(shape)
            dataset = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").filter(
                ee.Filter.date(dtFrom, dtTo)
            )
            antecedent = dataset.reduce(ee.Reducer.sum())
            antecedent = (
                antecedent.clip(active_district)
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

            M2 = (
                M2.clip(active_district)
                .rename("m2")
                .reproject(crs="EPSG:4326", scale=30)
            )
            M1 = CN2a.expression(
                "0.5*(-sr+sqrt(sr**2+4*p*sr))",
                {
                    "sr": sr1.select("sr1"),
                    "p": antecedent.select("hourlyPrecipRate_sum"),
                },
            ).rename("m1")

            M1 = (
                M1.clip(active_district)
                .rename("m1")
                .reproject(crs="EPSG:4326", scale=30)
            )

            M3 = CN2a.expression(
                "0.5*(-sr+sqrt(sr**2+4*p*sr))",
                {
                    "sr": sr3.select("sr3"),
                    "p": antecedent.select("hourlyPrecipRate_sum"),
                },
            ).rename("m3")

            M3 = (
                M3.clip(active_district)
                .rename("m3")
                .reproject(crs="EPSG:4326", scale=30)
            )

            dataset = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").filter(
                ee.Filter.date(dtMid, dtTo)
            )
            total = dataset.reduce(ee.Reducer.sum())

            total = (
                total.clip(active_district)
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

            runoffs = runoffs.add(runoff)
            precip = precip.add(total)

        runoffs = ee.ImageCollection(runoffs)
        runoffTotal = runoffs.reduce(ee.Reducer.sum())
        precip = ee.ImageCollection(precip)
        precipTotal = precip.reduce(ee.Reducer.sum())

        runoffTotal = (
            runoffTotal.clip(active_district)
            .select("runoff_sum")
            .reproject(crs="EPSG:4326", scale=30)
        )

        precipTotal = (
            precipTotal.clip(active_district)
            .select("hourlyPrecipRate_sum_sum")
            .reproject(crs="EPSG:4326", scale=30)
        )

        stats = runoffTotal.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=active_district, scale=30
        )
        runoffTotal = runoffTotal.expression(
            "(p*30*30)/1000", {"p": runoffTotal.select("runoff_sum")}
        ).rename("runoff_sum")

        runoffTotal = runoffTotal.reduceRegions(
            reducer=ee.Reducer.sum(), collection=active_district, scale=30
        )

        # export to gee assets

        # export to google drive

    def evapotranspiration(self, *args, **kwargs):
        pass

    def soil_moisture(self, *args, **kwargs):
        pass

    def delta_g(self, *args, **kwargs):
        pass
