from .AllocationTool import AllocationTool
from .MapChecker import MapChecker
import os


class AT_PRE_VP:
    def __init__(
        self,
        working_directory,
        administrative_divisions,
        rel_freq_table_hrp,
        vul_map_vp,
        expected_deforestation,
        max_iter=3,
    ):
        self.working_directory = working_directory
        self.administrative_divisions = administrative_divisions
        self.rel_freq_table_hrp = rel_freq_table_hrp
        self.vul_map_vp = vul_map_vp
        self.expected_deforestation = expected_deforestation
        self.max_iter = max_iter

        self.allocation_tool = AllocationTool()
        self.map_checker = MapChecker()

        self.image1 = "Acre_Prediction_Modeling_Region_VP.tif"
        self.image2 = "Acre_Adjucted_Density_Map_VP.tif"

    def process_data(self):
        images = [self.administrative_divisions, self.vul_map_vp]

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

        id_difference, iteration_count = self.allocation_tool.execute_workflow_vp(
            self.working_directory,
            self.max_iter,
            self.rel_freq_table_hrp,
            self.administrative_divisions,
            self.expected_deforestation,
            self.vul_map_vp,
            self.image1,
            self.image2,
        )
