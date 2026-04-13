import os

# import pandas as pd
# import geemap
# import folium
# from typing import List
import rasterio

# import time

import numpy as np
from .GEE_Manager import GEEManager
from .RMT_FIT_CAL import RMT_FIT_CAL
from .RMT_PRE_CNF import RMT_PRE_CNF
from .RMT_FIT_HRP import RMT_FIT_HRP
from .RMT_PRE_VP import RMT_PRE_VP
from .AT_FIT_CAL import AT_FIT_CAL
from .AT_FIT_HRP import AT_FIT_HRP
from .AT_PRE_CNF import AT_PRE_CNF
from .AT_PRE_VP import AT_PRE_VP

# from MapChecker import MapChecker
# from AllocationTool import AllocationTool
# from MCT_FIT_CAL import MCT_FIT_CAL
# from MCT_PRE_CNF import MCT_PRE_CNF
from .util import get_image_resolution  # , get_image_dimension


class RiskMaps:
    def __init__(
        self,
        base_file_path,
        working_directory,
        start_year,
        mid_year,
        end_year,
        state_name,
    ):
        """
        Initialize the RiskMaps class for generating final risk maps.

        Args:
            working_directory (str): Directory to store intermediate and output files.
            start_year (int): Beginning year of the historical reference period.
            mid_year (int): Middle year, used for calibration period.
            end_year (int): Ending year of the historical reference period.
            state_name (str): Name of the state/region for analysis.
        """
        self.working_directory = working_directory
        self.start_year = start_year
        self.mid_year = mid_year
        self.end_year = end_year
        self.state_name = state_name

        # Initialize Google Earth Engine (GEE) and Drive manager
        self.gee_manager = GEEManager()

        # Years of interest for forest maps
        self.years = [self.start_year, self.mid_year, self.end_year]

        # Define key filenames
        self.forest_edge_distance_start = os.path.join(
            self.working_directory, f"forest_edge_distance_{self.start_year}.tif"
        )
        self.forest_edge_distance_cnf = os.path.join(
            self.working_directory, f"forest_edge_distance_{self.mid_year}.tif"
        )
        self.forest_edge_distance_vp = os.path.join(
            self.working_directory, f"forest_edge_distance_{self.end_year}.tif"
        )
        self.deforestation_hrp = os.path.join(
            self.working_directory,
            f"deforestation_map_{self.start_year}_{self.end_year}.tif",
        )
        self.deforestation_cal = os.path.join(
            self.working_directory,
            f"deforestation_map_{self.start_year}_{self.mid_year}.tif",
        )
        self.deforestation_cnf = os.path.join(
            self.working_directory,
            f"deforestation_map_{self.mid_year}_{self.end_year}.tif",
        )
        self.vulnerability_map_cal = os.path.join(
            self.working_directory, f"Vulnerability_Map_CAL.tif"
        )
        self.vulnerability_map_cnf = os.path.join(
            self.working_directory, f"Vulnerability_Map_CNF.tif"
        )
        self.vulnerability_map_hrp = os.path.join(
            self.working_directory, f"Vulnerability_Map_HRP.tif"
        )
        self.jurisdiction_mask = os.path.join(
            self.working_directory, f"{self.state_name}_jurisidiction_mask.tif"
        )
        self.administrative_divisions = os.path.join(
            self.working_directory, f"{self.state_name}_districts.tif"
        )
        self.forest_nonforest_cal = os.path.join(
            self.working_directory, f"{self.state_name}_{self.start_year}.tif"
        )

        # List of files to validate for consistency
        self.files = [
            self.forest_edge_distance_start,
            self.deforestation_cal,
            self.deforestation_hrp,
            self.jurisdiction_mask,
            self.administrative_divisions,
            self.forest_nonforest_cal,
        ]

        self.relative_freq_CAL = os.path.join(
            self.working_directory, "Relative_Frequency_Table_CAL.csv"
        )
        self.relative_freq_HRP = os.path.join(
            self.working_directory, "Relative_Frequency_Table_HRP.csv"
        )
        self.fitted_density_CAL = os.path.join(
            self.working_directory, "Acre_Fitted_Density_Map_CAL.tif"
        )
        self.fitted_density_HRP = os.path.join(
            self.working_directory, "Acre_Fitted_Density_Map_HRP.tif"
        )
        self.modeling_region_CAL = os.path.join(
            self.working_directory, "Acre_Modeling_Region_CAL.tif"
        )
        self.modeling_region_HRP = os.path.join(
            self.working_directory, "Acre_Modeling_Region_HRP.tif"
        )
        self.modeling_region_CNF = os.path.join(
            self.working_directory, "Acre_Prediction_Modeling_Region_CNF.tif"
        )
        self.prediction_density_CNF = os.path.join(
            self.working_directory, "Acre_Adjucted_Density_Map_CNF.tif"
        )
        self.def_review_cnf = os.path.join(
            self.working_directory, "Acre_Def_Review_CNF.tif"
        )
        self.residuals_cnf = os.path.join(
            self.working_directory, "Acre_Residuals_CNF.tif"
        )
        self.vulnerability_map_vp = os.path.join(
            self.working_directory, "Acre_Vulnerability_VP.tif"
        )
        self.modeling_region_VP = os.path.join(
            self.working_directory, "Acre_Prediction_Modeling_Region_VP.tif"
        )
        self.adjucted_density_VP = os.path.join(
            self.working_directory, "Acre_Adjucted_Density_Map_VP.tif"
        )

        # List of expected outputs
        self.output_files = [
            "Relative_Frequency_Table_CAL.csv",
            "Relative_Frequency_Table_HRP.csv",
            "Vulnerability_Map_CAL.tif",
            "Vulnerability_Map_CNF.tif",
            "Vulnerability_Map_HRP.tif",
            "Acre_Fitted_Density_Map_CAL.tif",
            "Acre_Fitted_Density_Map_HRP.tif",
            "Acre_Modeling_Region_CAL.tif",
            "Acre_Modeling_Region_HRP.tif",
            "Acre_Prediction_Modeling_Region_CNF.tif",
            "Acre_Adjucted_Density_Map_CNF.tif",
            "Acre_Def_Review_CNF.tif",
            "Acre_Residuals_CNF.tif",
            "Acre_Prediction_Modeling_Region_VP.tif",
            "Acre_Adjucted_Density_Map_VP.tif",
        ]

        # Drive export path
        self.drive_folder_path = base_file_path  # f'/mnt/d/workspaces/BECC/data/GEE_exports_{self.state_name}'

        # NRT (Normalized Risk Threshold) placeholder
        self.nrt = None

    def perform_gee_operations(self):
        """
        Run all required GEE operations:
        - Generate forest cover maps for specified years.
        - Create jurisdiction and administrative boundary masks.

        Note: This function is time-intensive (~30-45 minutes).
        """
        self.gee_manager.create_forest_maps_and_export(self.state_name, self.years)
        self.gee_manager.create_and_export_jurisdiction_mask(self.state_name)
        self.gee_manager.export_districts(self.state_name)

    def prepare_data(self):
        """
        Process GEE output data:
        - Resample rasters to standard resolution.
        - Generate edge distance and deforestation maps.
        - Verify dimensions of files for consistency.
        """
        print("================= Preparing Data ===================")
        for year in self.years:
            file_pth = os.path.join(
                self.drive_folder_path, f"{self.state_name}_{year}.tif"
            )
            output_pth = os.path.join(
                self.working_directory, f"{self.state_name}_{year}.tif"
            )
            self.gee_manager.resample_raster(file_pth, output_pth)

        # Resample jurisdiction mask
        self.gee_manager.resample_raster(
            os.path.join(
                self.drive_folder_path, f"{self.state_name}_jurisidiction_mask.tif"
            ),
            self.jurisdiction_mask,
        )

        # Resample administrative divisions
        self.gee_manager.resample_raster(
            os.path.join(self.drive_folder_path, f"{self.state_name}_districts.tif"),
            self.administrative_divisions,
        )

        # Calculate Euclidean distance from forest edge at the start of the calibration period
        self.gee_manager.euclidean_dist_calc(
            os.path.join(
                self.working_directory, f"{self.state_name}_{self.start_year}.tif"
            ),
            self.forest_edge_distance_start,
        )

        # Calcuate Euclidean distance from forest edge at the start of the confirmation period
        self.gee_manager.euclidean_dist_calc(
            os.path.join(
                self.working_directory, f"{self.state_name}_{self.mid_year}.tif"
            ),
            self.forest_edge_distance_cnf,
        )

        # Calculate Euclidean distance from forest edge at the start of the validity period
        self.gee_manager.euclidean_dist_calc(
            os.path.join(
                self.working_directory, f"{self.state_name}_{self.end_year}.tif"
            ),
            self.forest_edge_distance_vp,
        )

        # Generate deforestation maps
        file1 = os.path.join(
            self.working_directory, f"{self.state_name}_{self.start_year}.tif"
        )
        file2 = os.path.join(
            self.working_directory, f"{self.state_name}_{self.mid_year}.tif"
        )
        file3 = os.path.join(
            self.working_directory, f"{self.state_name}_{self.end_year}.tif"
        )

        self.gee_manager.generate_deforestation_map(
            file1, file2, self.deforestation_cal
        )
        self.gee_manager.generate_deforestation_map(
            file1, file3, self.deforestation_hrp
        )
        self.gee_manager.generate_deforestation_map(
            file2, file3, self.deforestation_cnf
        )

        # Validate all key input files for size and resolution
        for file in self.files:
            rows, cols = self.gee_manager.get_image_dimension(file)
            resolution = self.gee_manager.get_image_resolution(file)
            print(f"{file} dimension: {rows} x {cols} | Resolution: {resolution}")

    def final_check(self):
        """
        Validate that all input rasters have identical resolution and dimensions.
        """
        print("================= Final Check ===================")
        resolutions = [self.gee_manager.get_image_resolution(img) for img in self.files]
        if len(set(resolutions)) != 1:
            raise ValueError("All images must have the same resolution.")

        dimensions = [self.gee_manager.get_image_dimension(img) for img in self.files]
        if len(set(dimensions)) != 1:
            raise ValueError(
                "All images must have the same number of rows and columns."
            )

        print("All files are OK. We can create risk maps")

    def total_deforestation(self, hrp_deforestation, hrp_start, hrp_end):
        pixel_size = get_image_resolution(hrp_deforestation)

        with rasterio.open(hrp_deforestation) as src:
            data = src.read(1)  # Read the first (and only) band

        # Count pixels where deforestation (value=1) has occurred
        deforested_pixels = np.sum(data == 1)

        # Convert pixel count to area
        pixel_area_m2 = (
            pixel_size * pixel_size
        )  # Each pixel represents (pixel_size)² m² area
        # total_deforestation_m2 = deforested_pixels * pixel_area_m2
        total_deforestation_m2 = (
            np.float64(deforested_pixels) * pixel_area_m2
        )  # Avoid overflow
        total_deforestation_ha = total_deforestation_m2 / 10000  # Convert to hectares
        total_annual_deforestation_ha = total_deforestation_ha / (hrp_end - hrp_start)

        print(f"Total deforested area: {total_deforestation_ha:.2f} hectares")
        print(
            f"Total Annual Deforestation: {total_annual_deforestation_ha:.2f} hectares"
        )

        return total_annual_deforestation_ha

    def testing_stage_fitting_phase_CAL(self):
        # 1. Vulnerability Mapping
        ## 1.1 NRT Calculation
        rmt_fit_cal = RMT_FIT_CAL(
            self.working_directory,
            self.forest_edge_distance_start,
            self.deforestation_hrp,
            self.jurisdiction_mask,
        )
        self.nrt = rmt_fit_cal.calculate_nrt()
        print("NRT:", self.nrt)
        ## 1.2 Vulnerability Mapping (CAL)
        rmt_fit_cal.prepare_vulnerability_map()
        print("Vulnerability map created for Calibration Period")

        # 2. Allocated Risk Mapping
        at_fit_cal = AT_FIT_CAL(
            self.working_directory,
            self.administrative_divisions,
            self.vulnerability_map_cal,
            self.deforestation_cal,
        )
        at_fit_cal.process_data()
        print("Fitted Density created for Calibration period")

        # 3. Model Fit Assessment
        # Note to the reader: Uncomment the following lines if you require model assessment
        # mct_fit_cal = MCT_FIT_CAL(self.working_density, self.fitted_density_CAL, self.jurisdiction_mask, self.deforestation_cal, "Title")
        # mct_fit_cal.process_data()
        # print('Model Fit Assessment created for Calibration period')

    def testing_stage_prediction_phase_cnf(self):
        # 1. Vulnerability Mapping
        rmt_pre_cnf = RMT_PRE_CNF(
            self.working_directory,
            self.forest_edge_distance_cnf,
            self.jurisdiction_mask,
            self.nrt,
        )
        rmt_pre_cnf.process_data()

        # 2. Allocated Risk Mapping
        at_pre_cnf = AT_PRE_CNF(
            self.working_directory,
            self.administrative_divisions,
            self.relative_freq_CAL,
            self.vulnerability_map_cnf,
            self.deforestation_cnf,
        )
        at_pre_cnf.process_data()

        # 3. Model Prediction Assessment
        # Note to the reader: Uncomment the following lines if you require model assessment. To get idea about results look at UDefARP documentation
        # mct_pre_cnf = MCT_PRE_CNF(self.working_directory, self.prediction_density_CNF, self.jurisdiction_mask, self.deforestation_cnf, self.deforestation_cal, self.forest_nonforest_cal)
        # mct_pre_cnf.process_data()

    def application_stage_fitting_phase_HRP(self):
        # 1. Vulnerability Mapping
        rmt_fit_hrp = RMT_FIT_HRP(
            self.working_directory,
            self.forest_edge_distance_start,
            self.jurisdiction_mask,
            self.nrt,
        )
        rmt_fit_hrp.prepare_vul_map()
        print("Vulnerability map created for Historical Reference Period")

        # 2. Allocated Risk Mapping
        at_fit_hrp = AT_FIT_HRP(
            self.working_directory,
            self.administrative_divisions,
            self.vulnerability_map_hrp,
            self.deforestation_hrp,
        )
        at_fit_hrp.prepare_risk_map()
        print("Fitted Density Map for Historical Reference Period")

    def application_stage_prediction_phase_VP(self):
        # 1. Vulnerability Mapping
        rmt_pre_vp = RMT_PRE_VP(
            self.working_directory,
            self.forest_edge_distance_vp,
            self.jurisdiction_mask,
            self.nrt,
        )
        rmt_pre_vp.process_data()

        # 2. Allocated Risk Mapping
        expected_deforestation = self.total_deforestation(
            self.deforestation_hrp, self.start_year, self.end_year
        )
        at_pre_vp = AT_PRE_VP(
            self.working_directory,
            self.administrative_divisions,
            self.relative_freq_HRP,
            self.vulnerability_map_vp,
            expected_deforestation,
        )
        at_pre_vp.process_data()

    def create_risk_map(self):
        """
        Generate the risk maps:
        - Calculate Normalized Risk Threshold (NRT).
        - Create vulnerability and risk maps for both calibration and HRP periods.
        """
        # Step 1: Compute NRT and vulnerability for CAL
        rmt_fit_cal = RMT_FIT_CAL(
            self.working_directory,
            self.forest_edge_distance_start,
            self.deforestation_hrp,
            self.jurisdiction_mask,
        )
        self.nrt = rmt_fit_cal.calculate_nrt()
        print("NRT:", self.nrt)
        rmt_fit_cal.prepare_vulnerability_map()
        print("Vulnerability map created for Calibration Period")

        # Step 2: Generate risk map using AT model for CAL
        at_fit_cal = AT_FIT_CAL(
            self.working_directory,
            self.administrative_divisions,
            self.vulnerability_map_cal,
            self.deforestation_cal,
        )
        at_fit_cal.process_data()
        print("Risk Map created for Calibration period")

        # Step 3: Prepare vulnerability map using HRP and NRT
        rmt_fit_hrp = RMT_FIT_HRP(
            self.working_directory,
            self.forest_edge_distance_start,
            self.jurisdiction_mask,
            self.nrt,
        )
        rmt_fit_hrp.prepare_vul_map()
        print("Vulnerability map created for Historical Reference Period")

        # Step 4: Generate risk map using AT model for HRP
        at_fit_hrp = AT_FIT_HRP(
            self.working_directory,
            self.administrative_divisions,
            self.vulnerability_map_hrp,
            self.deforestation_hrp,
        )
        at_fit_hrp.prepare_risk_map()
        print("Risk Map created for Historical Reference Period")

    def run_udefarp(self):
        """
        Run the udefarp pipeline.
        """
        print("================= Running UDefARP ===================")
        # self.perform_gee_operations()
        # self.prepare_data()
        # self.final_check()
        self.testing_stage_fitting_phase_CAL()
        self.testing_stage_prediction_phase_cnf()
        self.application_stage_fitting_phase_HRP()
        self.application_stage_prediction_phase_VP()

    def run_w_gee(self):
        """
        Full pipeline execution with Google Earth Engine steps.
        """
        self.perform_gee_operations()
        self.prepare_data()
        self.final_check()
        # self.create_risk_map()
        self.run_udefarp()

    def run_wo_gee(self):
        """
        Run pipeline assuming GEE outputs are already available.
        """
        self.prepare_data()
        self.final_check()
        # self.create_risk_map()
        self.run_udefarp()

    def get_nrt(self):
        """
        Returns:
            float: Normalized Risk Threshold (NRT) calculated during risk mapping.
        """
        return self.nrt

    def export_output_files_Drive(self):
        """
        Export all output files to user's Google Drive.
        """
        import shutil

        for file in self.output_files:
            file_path = os.path.join(self.working_directory, file)
            if os.path.exists(file_path):
                shutil.copy(file_path, self.drive_folder_path)
                print(f"Exported {file} to Google Drive.")
            else:
                print(f"{file} does not exist in the working directory.")

    # def download_output_file(self):
    #     """
    #     Download all output files to the local machine.
    #     """
    #     from google.colab import files

    #     for file in self.output_files:
    #         file_path = os.path.join(self.working_directory, file)
    #         if os.path.exists(file_path):
    #             files.download(file_path)
    #             print(f"Downloaded {file} to local machine.")
    #         else:
    #             print(f"{file} does not exist in the working directory.")
