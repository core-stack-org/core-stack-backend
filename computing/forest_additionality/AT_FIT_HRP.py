from .AllocationTool import AllocationTool
from .MapChecker import MapChecker
import os


class AT_FIT_HRP:
    """Allocation Tool for the Historical Reference Period"""

    def __init__(
        self,
        working_directory,
        administrative_divisions,
        vul_map_hrp,
        deforestation_hrp,
    ):
        self.working_directory = working_directory
        self.administrative_divisions = administrative_divisions
        self.vul_map_hrp = vul_map_hrp
        self.deforestation_hrp = deforestation_hrp

        self.allocation_tool = AllocationTool()
        self.map_checker = MapChecker()

        self.output_modeling_regions_name = os.path.join(
            self.working_directory, "Acre_Modeling_Region_HRP.tif"
        )
        self.output_relative_frequency_table_name = os.path.join(
            self.working_directory, "Relative_Frequency_Table_HRP.csv"
        )
        self.output_fitted_density_map_hrp = os.path.join(
            self.working_directory, "Acre_Fitted_Density_Map_HRP.tif"
        )

    def prepare_risk_map(self):
        images = [
            self.administrative_divisions,
            self.deforestation_hrp,
            self.vul_map_hrp,
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
            self.vul_map_hrp,
            self.administrative_divisions,
            self.deforestation_hrp,
            self.output_relative_frequency_table_name,
            self.output_modeling_regions_name,
            self.output_fitted_density_map_hrp,
        )
