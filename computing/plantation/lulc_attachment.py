import ee


def get_lulc_data(suitability_vector, start_year, end_year):
    """
    Retrieve Land Use/Land Cover (LULC) data for a given set of features
    across a specified time range.

    Args:
        suitability_vector: Earth Engine FeatureCollection to annotate with LULC data
        start_year: Beginning of the temporal analysis range
        end_year: End of the temporal analysis range

    Returns:
        FeatureCollection with LULC data added as encoded JSON
    """

    lulc_years = {}
    # for (year = start year <= end year++) {
    for year in range(start_year, end_year + 1):
        asset_id = (
            "projects/ee-corestackdev/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_"
            + str(year)
            + "_"
            + str(year + 1)
        )

        lulc_years[year] = (
            ee.Image(asset_id)
            .select(["predicted_label"])
            .clip(suitability_vector.geometry())
        )

    lulc_years_dict = ee.Dictionary(lulc_years)

    def get_lulc(feature):
        """
        Extract Land Use/Land Cover data for a single feature.

        Args:
            feature: A single geographic feature to analyze

        Returns:
            Feature with LULC data added as a property
        """

        def process_year(year):
            """
            Process LULC data for a specific year.

            Args:
                year: Year to analyze

            Returns:
                Dictionary with LULC data for the given year
            """

            annual_lulc = (
                ee.Image(lulc_years_dict.get(year))
                .select(["predicted_label"])
                .clip(feature.geometry())
            )

            # Compute histogram of LULC classes within the feature's geometry
            lulc_histogram = annual_lulc.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=feature.geometry(),
                scale=10,  # 10-meter resolution
                bestEffort=True,  # Helps with processing large or complex geometries
            )

            # Extract the label histogram
            temp_dict = ee.Dictionary(lulc_histogram.get("predicted_label"))

            # Convert pixel counts to hectares
            # Conversion assumes 10m resolution:
            # 1. Multiply by 0.01 (10m x 10m = 100 sq meters)
            # 2. Multiply by 1000 to convert to hectares
            # 3. Round to 3 decimal places for precision
            temp_dict = temp_dict.map(
                lambda key, value: ee.Number(value)
                .multiply(0.01)  # Convert sq meters
                .multiply(1000)  # Convert to hectares
                .round()
                .divide(1000)
            )

            # Create a dictionary with the year and LULC data
            lulc_dict = ee.Dictionary({"year": year})
            return lulc_dict.combine(temp_dict)

        # Process LULC data for all years in the analysis period
        lulc_by_year = lulc_years_dict.keys().map(process_year)

        # Set LULC data as an encoded JSON string in the feature's properties
        return feature.set("LULC", ee.String.encodeJSON(lulc_by_year))

    # Apply LULC data retrieval to each feature in the suitability vector
    return suitability_vector.map(get_lulc)
