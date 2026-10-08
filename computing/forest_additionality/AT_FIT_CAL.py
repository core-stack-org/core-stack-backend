from .AllocationTool import AllocationTool
from .MapChecker import MapChecker
import os


class AT_FIT_CAL:
    """Allocation Tool for the Calibration Period"""

    def __init__(
        self,
        working_directory: str,
        administrative_divisions: str,
        vulnerability_map: str,
        deforestation_cal: str,
    ):
        self.working_directory = working_directory
        self.administrative_divisions = administrative_divisions
        self.deforestation_cal = deforestation_cal
        self.vulnerability_map_hrp = vulnerability_map

        self.allocation_tool = AllocationTool()
        self.map_checker = MapChecker()

        self.output_modeling_regions_name = os.path.join(
            self.working_directory, "Acre_Modeling_Region_CAL.tif"
        )
        self.output_relative_frequency_table_name = os.path.join(
            self.working_directory, "Relative_Frequency_Table_CAL.csv"
        )
        self.output_fitted_density_map_cal = os.path.join(
            self.working_directory, "Acre_Fitted_Density_Map_CAL.tif"
        )

    def process_data(self):
        images = [
            self.administrative_divisions,
            self.deforestation_cal,
            self.vulnerability_map_hrp,
        ]

        # Check if all images have the same resolution
        resolutions = [self.map_checker.get_image_resolution(img) for img in images]
        if len(set(resolutions)) != 1:
            raise ValueError("All images must have the same resolution.")

        # Check if all images have the same number of rows and columns
        dimensions = [self.map_checker.get_image_dimensions(img) for img in images]
        if len(set(dimensions)) != 1:
            raise ValueError(
                "All images must have the same number of rows and columns."
            )

        self.allocation_tool.execute_workflow_fit(
            self.working_directory,
            self.vulnerability_map_hrp,
            self.administrative_divisions,
            self.deforestation_cal,
            self.output_relative_frequency_table_name,
            self.output_modeling_regions_name,
            self.output_fitted_density_map_cal,
        )
