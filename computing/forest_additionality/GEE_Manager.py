import ee

# from google.colab import drive
from typing import List
import os
import time
import folium
import geemap
import rasterio
from rasterio.warp import reproject, Resampling, calculate_default_transform
from rasterio.transform import Affine
from rasterio.windows import Window
from osgeo import gdal

import numpy as np
from scipy.ndimage import distance_transform_edt

from utilities.gee_utils import (
    sync_raster_to_gcs,
    check_task_status,
    download_tif_from_gcs,
    gcs_file_exists,
)


class GEEManager:
    def __init__(self):
        print(
            "Before Initializing this class, please make sure you have authenticated your Google Earth Engine account and mounted your Google Drive."
        )
        # self.initialize_gee_and_drive("core-stack-dev-2")

    def initialize_gee_and_drive(self, project_name: str):
        """
        Initialize Google Earth Engine (GEE) and authenticate the user.
        This function is called in the constructor of the class.

        Args:
            project_name (str): The name of the GEE project to initialize.
        """
        try:
            ee.Authenticate()
            ee.Initialize(project=project_name)
            print("Google Earth Engine initialized successfully.")
        except Exception as e:
            print(f"Error initializing GEE: {e}")
            print("Please authenticate your Google Earth Engine account.")
        if False:
            try:
                drive.mount("/content/drive")
                print("Google Drive mounted successfully.")
            except Exception as e:
                print(
                    "Google Drive is not available. Please run this code in Google Colab."
                )

    def get_state_image(self, state_name: str):
        """
        Get the image of a state from the FAO/GAUL dataset.

        Args:
            state_name (str): The name of the state to get the image for.

        Returns:
            ee.Image: The image of the state.
        """
        return ee.FeatureCollection("FAO/GAUL/2015/level2").filter(
            ee.Filter.eq("ADM2_NAME", state_name)
        )

    def get_aoi_from_kmz(self, asset_name):
        return

    def create_forest_cover_map(self, state_name: str, year_of_interest: int):
        """
        Create a forest/non-forest cover map for a given state and year.

        Args:
            state_name (str): The name of the state to create the map for.
            year_of_interest (int): The year of interest for the map.

        Returns:
            ee.Image: The forest/non-forest cover map for the state and year.

        Note:
            This function currently uses the data provided by the GLC_FCS30D dataset.
            It has data for the years 1985, 1990, 1995, 2000,..., 2022.
            For recent years you might have to change this function to use other datasets such as dynamic world.
        """
        # Print a caution message to the user.
        print(
            "Note: \nThis function currently uses the data provided by the GLC_FCS30D dataset.\nIt has data for the years 1985, 1990, 1995, 2000,..., 2022.\nFor recent years you might have to change this function to use other datasets such as dynamic world."
        )

        # Load the pre-processed annual mosaics from GLC_FCS30D
        annual = ee.ImageCollection("projects/sat-io/open-datasets/GLC-FCS30D/annual")

        # Classification scheme: 36 classes (35 landcover classes + 1 fill value)
        classValues = [
            10,
            11,
            12,
            20,
            51,
            52,
            61,
            62,
            71,
            72,
            81,
            82,
            91,
            92,
            120,
            121,
            122,
            130,
            140,
            150,
            152,
            153,
            181,
            182,
            183,
            184,
            185,
            186,
            187,
            190,
            200,
            201,
            202,
            210,
            220,
            0,
        ]
        newClassValues = ee.List.sequence(1, ee.List(classValues).length())

        # Mosaic and rename annual images
        annualMosaic = annual.mosaic()

        yearsList = ee.List.sequence(2000, 2022).map(
            lambda year: ee.Number(year).format("%04d")
        )
        annualMosaicRenamed = annualMosaic.rename(yearsList)

        yearlyMosaics = yearsList.map(
            lambda year: annualMosaicRenamed.select([year]).set(
                {
                    "system:time_start": ee.Date.fromYMD(
                        ee.Number.parse(year), 1, 1
                    ).millis(),
                    "system:index": year,
                    "year": ee.Number.parse(year),
                }
            )
        )

        mosaicsCol = ee.ImageCollection.fromImages(yearlyMosaics)

        # Recode the original classes to sequential values (1 to 36)
        def renameClasses(image):
            reclassified = image.remap(classValues, newClassValues).rename(
                "classification"
            )
            return reclassified

        landcoverCol = mosaicsCol.map(renameClasses)
        # print("Pre-processed Landcover Collection", landcoverCol.getInfo())

        # -----------------------------------------------------------------------------
        # Define the forest classes (in the recoded image)
        # From the original mapping:
        # 51->5, 52->6, 61->7, 62->8, 71->9, 72->10, 81->11, 82->12, 91->13, 92->14, 185->27
        forestClassRecoded = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 27]

        yearStr = ee.Number(year_of_interest).format("%04d")
        selectedLandcover = landcoverCol.filter(
            ee.Filter.eq("year", year_of_interest)
        ).first()

        # Create a binary (forest/non-forest) image:
        # Pixels whose 'classification' value is in forestClassRecoded become 1 (forest),
        # others become 0 (non-forest).
        forestNonForest = selectedLandcover.remap(
            forestClassRecoded, ee.List.repeat(1, len(forestClassRecoded)), 0
        ).rename("forest_binary")

        # -----------------------------------------------------------------------------
        # Load FAO/GAUL Level-1 boundaries to get Indian state boundaries.
        # (FAO/GAUL Level-1 provides sub-national admin boundaries.)
        countries = ee.FeatureCollection("FAO/GAUL/2015/level2")
        state = countries.filter(ee.Filter.eq("ADM2_NAME", state_name))

        # Clip the binary forest image to the selected state.
        stateForest = forestNonForest.clip(state)

        return stateForest

    def export_forest_cover_to_drive(
        self, forest_non_forest_cover_maps, years, state_name
    ) -> None:
        """
        Export the forest/non-forest cover map to Google Drive.
        This function will wait for the export tasks to complete before returning.
        It is recommended to run this function in a separate thread or process to avoid blocking the main thread.
        The export tasks will be started in the background, and you can check their status in the Google Earth Engine Task Manager.


        Args:
            forest_non_forest_cover_maps : The forest/non-forest cover maps to export.
            years (List[int]): The years of the maps.
            state_name (str): The name of the state to export the maps for.
        """
        # Load FAO/GAUL Level-1 boundaries to get Indian state boundaries.
        state = self.get_state_image(state_name=state_name)
        state_geometry = state.geometry()

        tasks = []

        # print(state.getInfo())  # or just print(state) if debugging in GEE Python API

        for year in years:
            if not gcs_file_exists(f"nrm_raster/{state_name}_{year}"):
                image = forest_non_forest_cover_maps[year]

                task_id = sync_raster_to_gcs(
                    image, 30, f"{state_name}_{year}", state_geometry
                )
                tasks.append(task_id)

        check_task_status(tasks)

        #     # Export the final deforestation image as a GeoTIFF file to Google Drive
        #     task = ee.batch.Export.image.toDrive(
        #         image=image,
        #         description='GLC_FSC30D',
        #         folder=f'GEE_exports_{state_name}',
        #         fileNamePrefix=f'{state_name}_{year}',
        #         region=state_geometry,
        #         scale=30,
        #         crs='EPSG:4326',
        #         fileFormat='GeoTIFF',
        #         # formatOptions={'cloudOptimized': True},
        #         maxPixels=1e13
        #     )
        #     task.start()
        #     tasks.append(task)
        #     print(f"Export task for year {year} started")
        #
        # # Wait for all tasks to complete
        # all_completed = False
        # while not all_completed:
        #     all_completed = True
        #     for task in tasks:
        #         if task.active():
        #             all_completed = False
        #             print("Export tasks still running...")
        #             time.sleep(30)
        #             break
        #     if all_completed:
        #         print("All export tasks completed.")
        for year in years:
            download_tif_from_gcs(
                source_blob_name=f"nrm_raster/{state_name}_{year}.tif",
                destination_file_name=f"data/forest_additionality/{state_name}/{state_name}_{year}.tif",
            )
        return None

    # Function to visualize forest cover map
    def visualize_forest_cover(
        self, forest_cover: ee.image.Image, year_of_interest: int
    ) -> None:
        """
        Visualize the forest cover map using folium.

        Args:
            forest_cover (ee.Image): The forest cover map to visualize.
            year_of_interest (int): The year of interest for the map.

        Returns:
            None
        """
        # Visualization parameters for binary classification:
        # We assign 0: non-forest (e.g., light gray) and 1: forest (e.g., dark green).
        binaryVis = {"min": 0, "max": 1, "palette": ["lightgray", "darkgreen"]}

        # Create a folium map.
        map_id_dict = forest_cover.getMapId(binaryVis)
        map_folium = folium.Map(
            location=[23.83, 91.28], zoom_start=9
        )  # approximate center of tripura
        folium.TileLayer(
            tiles=map_id_dict["tile_fetcher"].url_format,
            attr="Map Data &copy; Google Earth Engine",
            overlay=True,
            name="Forest/NonForest " + str(year_of_interest),
        ).add_to(map_folium)
        # display(map_folium)  # Display function works in a jupyter notebook environment

        return None

    def create_forest_maps_and_export(self, state_name: str, years: List[int]) -> None:
        """
        Create and export forest/non-forest cover maps for a given state and years.

        Args:
            state_name (str): The name of the state to create the maps for.
            years (List[int]): The years of interest for the maps.

        Returns:
            None
        """
        # Create a dictionary to store the forest/non-forest cover maps
        forest_non_forest_cover_maps = {}

        # Loop through the years and create the forest/non-forest cover map for each year
        for year in years:
            print(f"Creating forest/non-forest cover map for {state_name} in {year}...")
            forest_non_forest_cover_map = self.create_forest_cover_map(
                state_name=state_name, year_of_interest=year
            )
            forest_non_forest_cover_maps[year] = forest_non_forest_cover_map

        # Export the forest/non-forest cover maps to Google Drive
        self.export_forest_cover_to_drive(
            forest_non_forest_cover_maps=forest_non_forest_cover_maps,
            years=years,
            state_name=state_name,
        )

        return None

    # Functions to export administrative divisions
    def export_districts(self, state_name: str):
        """
        We are treating the districts as the administrative divisions, here.
        This function needs to be changed according to the requirements.

        Args:
            state_name (str): The name of the state to export the districts for.

        Returns:
            None
        """
        # Load FAO/GAUL level-2 boundaries (districts)
        districts = ee.FeatureCollection("FAO/GAUL/2015/level2")

        # Filter to Tripura state
        state_districts = districts.filter(ee.Filter.eq("ADM2_NAME", state_name))

        # Rasterize the feature collection
        rasterized_districts = state_districts.reduceToImage(
            properties=["ADM2_CODE"], reducer=ee.Reducer.first()
        ).rename("district_codes")

        # # Export the rasterized districts as a GeoTIFF to Google Drive
        # task = ee.batch.Export.image.toDrive(
        #     image=rasterized_districts,
        #     description=f"{state_name}Districts_GeoTIFF",
        #     folder=f"GEE_exports_{state_name}",
        #     fileNamePrefix=f"{state_name}_districts",
        #     region=state_districts.geometry(),
        #     scale=30,
        #     crs="EPSG:4326",
        #     fileFormat="GeoTIFF",
        #     maxPixels=1e13,
        # )
        # task.start()
        #
        # while task.active():
        #     print("Task is running....")
        #     time.sleep(60)
        if not gcs_file_exists(f"nrm_raster/{state_name}_districts"):
            task_id = sync_raster_to_gcs(
                rasterized_districts,
                30,
                f"{state_name}_districts",
                state_districts.geometry(),
            )

            check_task_status([task_id])

        print("Task completed")

        download_tif_from_gcs(
            source_blob_name=f"nrm_raster/{state_name}_districts.tif",
            destination_file_name=f"data/forest_additionality/{state_name}/{state_name}_districts.tif",
        )

        return None

    # Jurisdiction mask creation
    def create_jurisdiction_mask(self, state_name: str):
        """
        Creates a binary mask for the jurisdiction, where 1 indicates areas within the jurisdiction

        Args:
            state_name (str): The name of the state to create the mask for.

        Returns:
            ee.Image: A binary mask image where 1 indicates areas within the jurisdiction and 0 indicates areas outside the jurisdiction.
        """
        state = ee.FeatureCollection("FAO/GAUL/2015/level2").filter(
            ee.Filter.eq("ADM2_NAME", state_name)
        )

        # Rasterize the state boundary to create a mask
        mask = (
            state.reduceToImage(
                properties=["ADM2_CODE"],  # Use any property to rasterize
                reducer=ee.Reducer.first(),
            )
            .gt(0)
            .rename("jurisdiction_mask")
        )

        return mask

    def display_jurisdiction_mask(self, jurisdiction_mask: ee.image.Image):
        """
        Displays the created jurisdiction mask.
        """
        if jurisdiction_mask is None:
            return

        # Define visualization parameters for the binary mask
        mask_vis = {
            "min": 0,
            "max": 1,
            "palette": ["white", "blue"],  # White: outside, Blue: inside
        }

        # Get map ID and tile URL
        map_id_dict = jurisdiction_mask.getMapId(mask_vis)

        map_folium = folium.Map(location=[23.83, 91.28], zoom_start=9)

        # Add the mask as a tile layer
        folium.TileLayer(
            tiles=map_id_dict["tile_fetcher"].url_format,
            attr="Map Data &copy; Google Earth Engine",
            overlay=True,
            name="Jurisdiction Mask",
        ).add_to(map_folium)

        # Display the map
        # display(map_folium)  # This function works only in Jupyter nootebook environment

    def export_jurisdiction_mask(
        self, jurisdiction_mask: ee.image.Image, state_name: str
    ):
        """
        Exports the created jusisdiction mask to the drive.
        """
        state = ee.FeatureCollection("FAO/GAUL/2015/level2").filter(
            ee.Filter.eq("ADM2_NAME", state_name)
        )

        if jurisdiction_mask == None:
            return

        # # Export the mask as a GeoTIFF to Google Drive
        # task = ee.batch.Export.image.toDrive(
        #     image=jurisdiction_mask,
        #     description=f"{state_name}JurisdictionMask_GeoTIFF",
        #     folder=f"GEE_exports_{state_name}",
        #     fileNamePrefix=f"{state_name}_jurisidiction_mask",
        #     region=state.geometry(),
        #     scale=30,
        #     crs="EPSG:4326",
        #     fileFormat="GeoTIFF",
        #     maxPixels=1e13,
        # )
        # task.start()
        #
        # while task.active():
        #     print("Task is running....")
        #     time.sleep(60)
        if not gcs_file_exists(f"nrm_raster/{state_name}_jurisidiction_mask"):
            task_id = sync_raster_to_gcs(
                jurisdiction_mask,
                30,
                f"{state_name}_jurisidiction_mask",
                state.geometry(),
            )

            check_task_status([task_id])
        print("Jurisdiction Mask Export completed")

        download_tif_from_gcs(
            source_blob_name=f"nrm_raster/{state_name}_jurisidiction_mask.tif",
            destination_file_name=f"data/forest_additionality/{state_name}/{state_name}_jurisidiction_mask.tif",
        )

        return None

    def create_and_export_jurisdiction_mask(self, state_name: str):
        """
        This function will create and export the jurisdiction mask for the required state.

        Args:
            state_name (str): The name of the state we intend to create a jurisdiction mask for.
        """
        # Create the jurisdiction mask
        jurisdiction_mask = self.create_jurisdiction_mask(state_name=state_name)

        # Export the jurisdiction mask
        self.export_jurisdiction_mask(
            jurisdiction_mask=jurisdiction_mask, state_name=state_name
        )

        return None

    def export_settlement_map(
        self,
        state_name: str,
        settlement_id="projects/ee-mtpictd-dev/assets/settlement_tripura",
    ):
        """
        Creates a binary raster map for settlements in Tripura where:
        - 0 = Settlement
        - 1 = Non-settlement

        The function exports this binary raster to Google Drive using 30m Land
        """
        print(
            "Note:\nHere I have used settlement_id as 'projects/ee-mtpictd-dev/assets/settlement_tipura'\nIn order to do this for some other state you need to download the shapefiles from this website and add it to gee assets\nhttps://indiawris.gov.in/wris/#/geoSpatialData"
        )
        # 1. Load FAO GAUL and Get Tripura Boundary
        countries = ee.FeatureCollection("FAO/GAUL/2015/level2")
        tripura = countries.filter(ee.Filter.eq("ADM2_NAME", "Tripura"))

        # 2. Load Settlement Vector (replace asset path if needed)
        settlements = ee.FeatureCollection(settlement_id)

        # 3. Create Reference Image (30m constant image over Tripura)
        reference_image = (
            ee.Image.constant(1).clip(tripura).reproject(crs="EPSG:4326", scale=30)
        )

        # 4. Create Binary Raster: 0 = Settlement 1 = Non-settlement
        base = ee.Image.constant(1).clip(tripura).rename("settlement")
        settlement_raster = base.paint(featureCollection=settlements, color=0).rename(
            "settlement"
        )

        # Match projection
        settlement_raster = settlement_raster.reproject(
            crs=reference_image.projection()
        )

        # 5. Export to Google Drive
        task = ee.batch.Export.image.toDrive(
            image=settlement_raster,
            description="Tripura_NonSettlement_Binary_30m",
            folder=f"GEE_exports_{state_name}",
            fileNamePrefix=f"settlement_binary_{state_name}",
            region=tripura.geometry(),
            scale=30,
            crs="EPSG:4326",
            maxPixels=1e13,
        )

        task.start()

        import time

        while task.active():
            print("Waiting for export to finish")
            time.sleep(30)

    def extract_dem(self, state_name: str):
        """
        Extracts the DEM for the specified state and exports it to Google Drive.

        Args:
            state_name (str): The name of the state to extract the DEM for.
            export_folder (str): The folder in Google Drive to export the DEM to.
        """
        # Load SRTM DEM
        dem = ee.Image("USGS/SRTMGL1_003")

        # Get state boundary
        countries = ee.FeatureCollection("FAO/GAUL/2015/level2")
        state_boundary = countries.filter(ee.Filter.eq("ADM2_NAME", state_name))

        # Clip DEM to state boundary
        dem_clipped = dem.clip(state_boundary)

        # Export DEM to Google Drive
        task = ee.batch.Export.image.toDrive(
            image=dem_clipped,
            description="SRTM_DEM",
            folder=f"GEE_exports_{state_name}",
            fileNamePrefix=f"{state_name}_DEM",
            region=state_boundary.geometry(),
            scale=30,
            crs="EPSG:4326",
            maxPixels=1e13,
        )

        task.start()

        while task.active():
            print("Waiting for export to finish")
            time.sleep(30)

    def resample_raster(self, input_tif, output_tif, target_resolution=(25, 25)):
        """
        Resamples a raster to a target resolution.

        Args:
            input_tif (str): Path to the input raster.
            output_tif (str): Path to save the resampled raster.
            target_resolution (tuple): Target resolution in meters (x_res, y_res).
        """
        with rasterio.open(input_tif) as src:
            src_crs = src.crs
            print(src_crs)
            src_transform = src.transform
            src_width = src.width
            src_height = src.height
            num_bands = src.count

            if src_crs.is_geographic:
                dst_crs = "EPSG:32644"  # 32633 Adjust for correct UTM zone
            else:
                dst_crs = src_crs

            transform, width, height = calculate_default_transform(
                src_crs,
                dst_crs,
                src_width,
                src_height,
                *src.bounds,
                resolution=target_resolution,
            )

            resampled_data = np.zeros((num_bands, height, width), dtype=np.uint8)

            for i in range(1, num_bands + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=resampled_data[i - 1],
                    src_transform=src.transform,
                    src_crs=src_crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )

            profile = src.profile
            profile.update(
                dtype=np.uint8,
                count=num_bands,
                crs=dst_crs,
                transform=transform,
                height=height,
                width=width,
            )

        with rasterio.open(output_tif, "w", **profile) as dst:
            dst.write(resampled_data)

        print(f"Resampled TIFF saved to {output_tif}")

        return None

    def euclidean_dist_calc(
        self, input_tif, output_tif, block_size=1024, buffer_size=100, pixel_size=25
    ):
        """
        Calculates a Euclidean distance transform from non-zero pixels in a binary raster,
        writing the result to a new raster in blocks for memory efficiency.

        Args:
            input_tif (str): Path to the input binary raster (e.g., 1 for feature presence like settlement, 0 for absence).
            output_tif (str): Path to the output distance raster (will contain Euclidean distances in meters).
            block_size (int, optional): Size (in pixels) of each tile/block to read/write. Defaults to 1024.
            buffer_size (int, optional): Number of pixels to buffer around each block. Buffers reduce edge effects in distance calculation. Defaults to 100.
            pixel_size (float, optional): Pixel size in meters (used to convert distance from pixels to meters). Defaults to 25.

        Returns:
            None. Writes an output GeoTIFF raster where each pixel contains the distance (in meters) to the nearest feature pixel (value > 0 in input).

        Notes:
            - The function uses a block-wise strategy to avoid memory overload on large rasters.
            - Applies a buffer around each block to compute correct distances near block edges.
            - Output is saved as compressed, tiled GeoTIFF with LZW compression.
            - Useful for proximity analysis or as an input feature for machine learning models.
        """
        with rasterio.open(input_tif) as src:
            meta = src.meta.copy()

            # Update the metadata for the output raster
            meta.update(
                dtype=rasterio.int32,  # Use int32 for integer distance values
                count=1,  # Single Band
                compress="LZW",  # Use LZW compression to reduce file size
                tiled=True,  # Use tiled storage for better performance
                blockxsize=256,  # Tile size
                blockysize=256,  # Tile size
            )

            # Open the output raster for writing
            with rasterio.open(output_tif, "w", **meta) as dst:
                # Loop through the raster in blocks
                for i in range(0, src.height, block_size):
                    for j in range(0, src.width, block_size):
                        # Define the window (block) to read, including a buffer
                        window_height = min(
                            block_size + 2 * buffer_size,
                            src.width - max(j - buffer_size, 0),
                        )
                        window_width = min(
                            block_size + 2 * buffer_size,
                            src.height - max(i - buffer_size, 0),
                        )

                        window = Window(
                            max(j - buffer_size, 0),
                            max(i - buffer_size, 0),
                            window_width,
                            window_height,
                        )

                        # Read the data for the current block (including buffer)
                        data = src.read(1, window=window)

                        # Convert to binary (1=forest, 0=non-forest)
                        binary_data = (data > 0).astype(np.uint8)

                        # Skip computation if block has no forest pixels
                        if np.all(binary_data == 0):
                            distances_meters = np.full(
                                binary_data.shape, 0, dtype=np.int32
                            )
                        else:
                            distances = distance_transform_edt(binary_data)
                            distances_meters = (distances * pixel_size).astype(np.int32)

                        # Dynamically crop buffer region
                        crop_top = buffer_size if i >= buffer_size else 0
                        crop_left = buffer_size if j >= buffer_size else 0
                        crop_bottom = distances_meters.shape[0] - (
                            buffer_size
                            if i + block_size + buffer_size < src.height
                            else 0
                        )
                        crop_right = distances_meters.shape[1] - (
                            buffer_size
                            if j + block_size + buffer_size < src.width
                            else 0
                        )

                        distances_meters = distances_meters[
                            crop_top:crop_bottom, crop_left:crop_right
                        ]

                        # Define the window for writing (without buffer)
                        write_window = Window(
                            j,
                            i,
                            min(block_size, src.width - j),
                            min(block_size, src.height - i),
                        )

                        # Write the processed block to the output raster
                        dst.write(distances_meters, window=write_window, indexes=1)

        print(f"Distance map saved to {output_tif}")
        return None

    def generate_deforestation_map(
        self, forest_cover_path_earlier, forest_cover_path_later, output_path
    ):
        """
        Generates a binary deforestation map by comparing two binary forest cover rasters
        from consecutive years. The map highlights areas that changed from forest (1) to
        non-forest (0) as deforested pixels.

        Args:
            forest_cover_path_earlier (str): File path to the earlier year's forest cover GeoTIFF.
            forest_cover_path_later (str): File path to the later year's forest cover GeoTIFF.
            output_path (str): Path where the deforestation map GeoTIFF will be saved.

        Returns:
            None. Saves a single-band GeoTIFF with 1 indicating deforestation, 0 elsewhere.

        Raises:
            ValueError: If the input rasters do not have matching dimensions.

        Notes:
            - Assumes input rasters are binary (1 = forest, 0 = non-forest).
            - Deforestation is defined as a transition from 1 (forest) to 0 (non-forest).
        """
        with rasterio.open(forest_cover_path_earlier) as src1:
            earlier_data = src1.read(1)  # Read first band
            profile = src1.profile

        with rasterio.open(forest_cover_path_later) as src2:
            later_data = src2.read(1)  # Read first band

        # Ensure the rasters have the same dimensions
        if earlier_data.shape != later_data.shape:
            raise ValueError("Both rasters must have the same dimensions")

        # Calculate deforestation: forest (1) to non-forest (0)
        deforestation_map = (earlier_data == 1) & (later_data == 0)
        deforestation_map = deforestation_map.astype(np.uint8)

        # Update metadata profile
        profile.update(dtype=rasterio.uint8, count=1)

        # Save the deforestation map
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(deforestation_map, 1)

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(deforestation_map, 1)

        print(f"Deforestation map saved to: {output_path}")

    def generate_forest_change_maps(self, folder_pth, out_dir, state_name, years):
        """
        Generate forest change maps (deforestation as well as afforestation) for each pair of consecutive years.

        Args:
            folder_path (str): The path to the folder wher all the forest-nonforest cover maps are present.
            state_name (str): Area of interest
            years (List[int]): Years for which we want to create forest change maps.
        """
        for year1, year2 in zip(years[:-1], years[1:]):
            # Check if forest cover files exists
            file1 = os.path.join(folder_pth, f"{state_name}_{year1}.tif")
            file2 = os.path.join(folder_pth, f"{state_name}_{year2}.tif")

            if not os.path.exists(file1) or not os.path.exists(file2):
                raise FileNotFoundError(
                    f"Forest cover files for {year1} or {year2} not found"
                )

            with rasterio.open(file1) as src1:
                earlier_data = src1.read(1)
                profile = src1.profile

            with rasterio.open(file2) as src2:
                later_data = src2.read(1)

            if earlier_data.shape != later_data.shape:
                raise ValueError("Both rasters must have the same dimensions")

            deforested = (earlier_data == 1) & (later_data == 0)
            afforested = (earlier_data == 0) & (later_data == 1)

            forest_change_map = np.zeros_like(earlier_data, dtype=np.int8)
            forest_change_map[deforested] = 1  # Deforestation
            forest_change_map[afforested] = -1  # Afforestation

            profile.update(dtype=rasterio.int8, count=1)
            output_path = os.path.join(
                out_dir, f"forest_change_map_{year1}_{year2}.tif"
            )

            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(forest_change_map, 1)

            print(f"Forest change map saved to {output_path}")

    def compute_slope(self, dem_path, output_path):
        """
        Computes the slope from a DEM raster and saves it as a GeoTIFF.

        Args:
            dem_path (str): Path to the input DEM raster.
            output_path (str): Path to save the slope raster.

        Returns:
            None
        """
        with rasterio.open(dem_path) as src:
            dem = src.read(1)
            profile = src.profile
            transform = src.transform
            pixel_size_x = transform[0]
            pixel_size_y = -transform[4]  # Negative for correct orientation

        # Compute gradients in x and y directions
        gradient_x, gradient_y = np.gradient(dem, pixel_size_x, pixel_size_y)
        slope = np.degrees(np.arctan(np.sqrt(gradient_x**2 + gradient_y**2)))

        # Save the slope raster
        profile.update(dtype=rasterio.float32, count=1)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(slope.astype(rasterio.float32), 1)

        print(f"Slope map saved to: {output_path}")
        return None

    def compute_distance(self, raster_path, output_path, target_value=0):
        """
        Compute distance to nearest target value (e.g., non-forest or settlement)

        Args:
            raster_path (str): Path to input raster
            output_path (str): Path to save output raster
            target_value (float): Value to compute distance to (default: 0)

        Returns:
            None
        """
        with rasterio.open(raster_path) as src:
            data = src.read(1)
            profile = src.profile
            transform = src.transform
            pixel_size = transform[0]  # Given we have square pixels

        # Create binary mask (1 where target value, 0 elsewhere)
        mask = np.where(data == target_value, 1, 0)

        # Compute distance to nearest target pixel (in pixels)
        distance = scipy.ndimage.distance_transform_edt(mask == 0)
        # Convert to meters
        distance_meters = distance * pixel_size

        profile.update(dtype=rasterio.float32, count=1)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(distance_meters.astype(rasterio.float32), 1)

        print(f"Distance map saved to {output_path}")
        return

    # Utilities
    def get_image_dimension(self, image_path):
        """
        Returns the dimensions (width, height) of a raster image.

        Args:
            image_path (str): File path to the input raster image.

        Returns:
            tuple: (width, height) in pixels
        """
        dataset = gdal.Open(image_path)
        if dataset is None:
            raise FileNotFoundError(f"Unable to open image: {image_path}")

        width = dataset.RasterXSize  # Number of columns
        height = dataset.RasterYSize  # Number of rows
        return width, height

    def get_image_resolution(self, image_path):
        """
        Returns the spatial resolution (pixel size) of a raster image.

        Args:
            image_path (str): File path to the input raster image.

        Returns:
            float: Pixel size in units of the spatial reference system (usually meters).
        """
        dataset = gdal.Open(image_path)
        if dataset is None:
            raise FileNotFoundError(f"Unable to open image: {image_path}")

        pixel_size = dataset.GetGeoTransform()[
            1
        ]  # Resolution in X-direction (assumes square pixels)
        return pixel_size

    def reproject(self, input_file, output_file):
        with rasterio.open(input_file) as src:
            src_crs = src.crs
            print(src_crs)
            # Get the target CRS as pyproj CRS object
            target_crs = "EPSG:4326"

            # Calculate the transform and new dimensions of the reprojected raster
            transform, width, height = calculate_default_transform(
                src_crs, target_crs, src.width, src.height, *src.bounds
            )

            # Define the metadata for the reprojected raster
            kwargs = src.meta.copy()
            kwargs.update(
                {
                    "crs": target_crs,
                    "transform": transform,
                    "width": width,
                    "height": height,
                }
            )

            # Reproject the raster and save to new file
            with rasterio.open(output_file, "w", **kwargs) as dst:
                for i in range(1, src.count + 1):
                    # Read each band
                    band = src.read(i)
                    # Reproject the band to the new CRS
                    reproject(
                        source=band,
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src_crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.nearest,
                    )

            print(
                f"Raster successfully reprojected to EPSG:4326 and saved to {output_file}"
            )
