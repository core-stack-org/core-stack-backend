import ee


class GEETerrainClassifier:
    def __init__(self):
        """Initialize classifier with predefined cluster centroids"""
        self.centroids = [
            [0.36255426, 0.21039965, 0.12161905, 0.17393119, 0.13149585],
            [0.09171062, 0.84299211, 0.035222, 0.02172654, 0.00834873],
            [0.08497599, 0.01051893, 0.23763531, 0.37992855, 0.28694122],
            [0.22301813, 0.5611825, 0.08511123, 0.07314189, 0.05754624],
        ]
        self.dem = ee.Image("USGS/SRTMGL1_003")

    def create_kernels(self):
        """Create annular kernels for TPI calculation"""
        # Small kernel parameters
        small_inner = ee.Number(5)
        small_outer = ee.Number(10)
        small_inner_circle = ee.Kernel.circle(small_inner, "pixels", False, -1)
        small_outer_circle = ee.Kernel.circle(small_outer, "pixels", False, 1)
        small_kernel = small_outer_circle.add(small_inner_circle, True)

        # Large kernel parameters
        large_inner = ee.Number(62)
        large_outer = ee.Number(67)
        large_inner_circle = ee.Kernel.circle(large_inner, "pixels", False, -1)
        large_outer_circle = ee.Kernel.circle(large_outer, "pixels", False, 1)
        large_kernel = large_outer_circle.add(large_inner_circle, True)

        return small_kernel, large_kernel

    def calculate_tpi(self, dem_clipped, kernel):
        """Calculate and standardize TPI"""
        focal_mean = dem_clipped.reduceNeighborhood(ee.Reducer.mean(), kernel)
        tpi = dem_clipped.subtract(focal_mean)

        # Standardize TPI
        mean = tpi.reduceRegion(reducer=ee.Reducer.mean(), bestEffort=True).get(
            "elevation"
        )
        tpi = tpi.subtract(ee.Number(mean))
        std_dev = tpi.reduceRegion(reducer=ee.Reducer.stdDev(), bestEffort=True).get(
            "elevation"
        )

        return (
            tpi.divide(ee.Number(std_dev)).multiply(ee.Number(100)).add(ee.Number(0.5))
        )

    def classify_landforms(self, feature):
        """Classify landforms based on TPI and slope"""
        geometry = feature.geometry()
        dem_clipped = self.dem.clip(geometry)

        # Calculate TPI at two scales
        small_kernel, large_kernel = self.create_kernels()
        tpi_small = self.calculate_tpi(dem_clipped, small_kernel)
        tpi_large = self.calculate_tpi(dem_clipped, large_kernel)

        # Calculate slope
        slope = ee.Terrain.slope(self.dem)
        slope_clipped = slope.clip(geometry)

        # Calculate classification thresholds
        dem_std = dem_clipped.reduceRegion(
            reducer=ee.Reducer.stdDev(), bestEffort=True
        ).get("elevation")
        dem_std = ee.Number(dem_std).add(1)

        factor = ee.Number(3).subtract(ee.Number(dem_std).log10())
        factor = factor.max(0.3)

        right_limit = ee.Number(100).multiply(factor)
        left_limit = ee.Number(-100).multiply(factor)

        # Initialize landform classification
        landforms = ee.Image.constant(0).clip(geometry)

        # Apply classification rules
        landforms = landforms.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(slope_clipped.lt(5)),
            5,  # Plains
        )

        landforms = landforms.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(slope_clipped.gte(5))
            .And(slope_clipped.lt(20)),
            6,  # Broad slopes
        )

        landforms = landforms.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gte(right_limit))
            .And(slope_clipped.lt(6)),
            7,  # Flat ridge tops
        )

        # Add remaining classification rules
        landforms = self.add_remaining_classifications(
            landforms, tpi_small, tpi_large, slope_clipped, left_limit, right_limit
        )

        return landforms

    def add_remaining_classifications(
        self, landforms, tpi_small, tpi_large, slope_clipped, left_limit, right_limit
    ):
        """Add remaining landform classifications"""
        # Upper slopes
        landforms = landforms.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(slope_clipped.gte(20)),
            8,
        )

        # Additional classifications
        classifications = [
            # Valley classifications
            {
                "value": 4,
                "condition": tpi_small.gt(left_limit)
                .And(tpi_small.lt(right_limit))
                .And(tpi_large.lte(left_limit)),
            },
            {
                "value": 2,
                "condition": tpi_small.lte(left_limit)
                .And(tpi_large.gt(left_limit))
                .And(tpi_large.lt(right_limit)),
            },
            {
                "value": 3,
                "condition": tpi_small.lte(left_limit).And(tpi_large.gte(right_limit)),
            },
            {
                "value": 1,
                "condition": tpi_small.lte(left_limit).And(tpi_large.lte(left_limit)),
            },
            # Ridge classifications
            {
                "value": 10,
                "condition": tpi_small.gte(right_limit)
                .And(tpi_large.gt(left_limit))
                .And(tpi_large.lt(right_limit)),
            },
            {
                "value": 11,
                "condition": tpi_small.gte(right_limit).And(tpi_large.gte(right_limit)),
            },
            {
                "value": 9,
                "condition": tpi_small.gte(right_limit).And(tpi_large.lte(left_limit)),
            },
        ]

        for classification in classifications:
            landforms = landforms.where(
                classification["condition"], classification["value"]
            )

        return landforms

    def calculate_terrain_proportions(self, landforms, geometry):
        """Calculate proportions of different terrain types"""
        # Calculate total area
        mwshed_area = (
            landforms.neq(0)
            .multiply(ee.Image.pixelArea())
            .reduceRegion(
                reducer=ee.Reducer.sum(), geometry=geometry, scale=30, maxPixels=1e10
            )
            .get("constant")
        )
        mwshed_area = ee.Number(mwshed_area).divide(1e6)

        # # Define terrain types
        # slopy = landforms.eq(6)
        # plains = landforms.eq(5)
        # steep_slopes = landforms.eq(8)
        # ridge = landforms.gte(9).Or(landforms.eq(7))
        # valleys = landforms.gte(1).And(landforms.lte(4))

        # Define terrain types
        slopy = landforms.eq(6)
        plains = landforms.eq(5)
        steep_slopes = landforms.eq(8)
        ridge = (
            landforms.eq(3)
            .Or(landforms.eq(7))
            .Or(landforms.eq(10))
            .Or(landforms.eq(11))
        )
        valleys = (
            landforms.eq(1).Or(landforms.eq(2)).Or(landforms.eq(4)).Or(landforms.eq(9))
        )

        # Calculate proportions
        calc_proportion = (
            lambda terrain: ee.Number(
                terrain.eq(1)
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=geometry,
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )

        return [
            calc_proportion(slopy),
            calc_proportion(plains),
            calc_proportion(ridge),
            calc_proportion(valleys),
            calc_proportion(steep_slopes),
        ]

    def assign_cluster(self, feature):
        """Assign terrain cluster to a feature"""
        # Classify landforms
        landforms = self.classify_landforms(feature)

        # Calculate terrain proportions
        proportions = self.calculate_terrain_proportions(landforms, feature.geometry())

        # Calculate distances to centroids
        distances = [
            ee.List(centroid)
            .zip(ee.List(proportions))
            .map(
                lambda pair: ee.Number(ee.List(pair).get(0))
                .subtract(ee.Number(ee.List(pair).get(1)))
                .pow(2)
            )
            .reduce(ee.Reducer.sum())
            for centroid in self.centroids
        ]

        # Find closest cluster
        min_distance = ee.List(distances).reduce(ee.Reducer.min())
        cluster_index = ee.List(distances).indexOf(ee.Number(min_distance))

        return feature.set("terrain_cluster", cluster_index)
