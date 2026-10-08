from .AllocationTool import AllocationTool
from .MapChecker import MapChecker
import os


class AT_PRE_CNF:
    def __init__(
        self,
        working_directory,
        administrative_divisions,
        cal_rel_freq_table,
        vulnerabilit_cnf,
        deforestation_cnf,
        max_iter=3,
    ):
        self.working_directory = working_directory
        self.administrative_divisions = administrative_divisions
        self.cal_rel_freq_table = cal_rel_freq_table
        self.vulnerability_cnf = vulnerabilit_cnf
        self.deforestation_cnf = deforestation_cnf
        self.max_iter = max_iter
        self.allocation_tool = AllocationTool()
        self.map_checker = MapChecker()

        self.modeling_regions_cnf = "Acre_Prediction_Modeling_Region_CNF.tif"
        self.prediction_density_cnf = "Acre_Adjucted_Density_Map_CNF.tif"

    def process_data(self):
        images = [
            self.administrative_divisions,
            self.deforestation_cnf,
            self.vulnerability_cnf,
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

        id_difference, iteration_count = self.allocation_tool.execute_workflow_cnf(
            self.working_directory,
            self.max_iter,
            self.cal_rel_freq_table,
            self.administrative_divisions,
            self.deforestation_cnf,
            self.vulnerability_cnf,
            self.modeling_regions_cnf,
            self.prediction_density_cnf,
        )
