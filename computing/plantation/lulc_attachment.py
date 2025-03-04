import ee


def get_lulc_data(suitability_vector, start_year, end_year):
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year+1}-06-30"

    # Get Dynamic World collection
    dynamic_world = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1").filterDate(
        start_date, end_date
    )
    years = ee.List.sequence(start_year, end_year)

    def get_lulc(feature):

        def process_year(year):
            s_date = ee.Date.fromYMD(year, 7, 1)
            e_date = s_date.advance(1, "year")

            mode_lulc = (
                dynamic_world.filterDate(s_date, e_date)
                .filterBounds(feature.geometry())
                .mode()
                .select("label")
            )

            lulc_histogram = mode_lulc.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=feature.geometry(),
                scale=10,
                bestEffort=True,
            )
            temp_dict = ee.Dictionary(lulc_histogram.get("label"))

            # Converts histogram values from pixels to hectares
            temp_dict = temp_dict.map(
                lambda key, value: ee.Number(value)
                .multiply(0.01)
                .multiply(1000)
                .round()
                .divide(1000)
            )

            lulc_dict = ee.Dictionary({"year": year})
            return lulc_dict.combine(temp_dict)

        lulc_by_year = years.map(process_year)

        return feature.set(
            "LULC", ee.String.encodeJSON(lulc_by_year)
        )  # TODO It can be done in separate columns, year-wise

    return suitability_vector.map(get_lulc)

    #     result = {}
    #     year_str = str(year)
    #
    #     def process_key(key):
    #         new_key = f"LULC_{year}_{key}"
    #         result[new_key] = (
    #             temp_dict.get(key).multiply(0.01).multiply(1000).round().divide(1000)
    #         )
    #         return new_key
    #
    #     keys = temp_dict.keys()
    #     new_keys = keys.map(process_key)
    #
    #     return ee.Dictionary(result)
    #
    # # Process all years and combine results
    # all_years = years.map(process_year)
    # combined_dict = ee.Dictionary({}).combine(all_years, True)
    # return feature.set(combined_dict)


# def export_results():
#     result = get_lulc(ee.Feature(roi.first()))
#     task = ee.batch.Export.table.toDrive(
#         collection=ee.FeatureCollection([result]),
#         description="LULC_Analysis",
#         fileFormat="CSV",
#     )
#     task.start()
