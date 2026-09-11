from .VulnerabilityMap import VulnerabilityMap
from .MapChecker import MapChecker
import os
from osgeo import gdal


class RMT_FIT_HRP:
    """Risk Mapping tool for the Historical Reference Period"""

    def __init__(self, working_directory, forest_edge_distance, mask, nrt):
        """
        Args:
        working_directory (str): The working directory where each the vulnerability map will be stored.
        forest_edge_distance (str): The path to the forest edge distance raster.""
        mask (str): The path to the jurisdiction mask raster.
        nrt (int): The NRT value for the historical reference period.
        """
        self.working_directory = working_directory
        self.forest_edge_distance = forest_edge_distance
        self.mask = mask
        self.nrt = nrt

        self.vulnerability_map = VulnerabilityMap()
        self.map_checker = MapChecker()

    def prepare_vul_map(self):
        try:
            self.nrt = int(self.nrt)
            if self.nrt <= 0:
                raise ValueError("NRT must be a positive integer.")
        except ValueError:
            raise ValueError("NRT must be a valid integer.")

        n_classes = int(29)
        out_file_name = "Vulnerability_Map_HRP.tif"
        out_file_path = os.path.join(self.working_directory, out_file_name)

        mask_arr = self.vulnerability_map.geometric_classification(
            self.forest_edge_distance, self.nrt, n_classes, self.mask
        )
        self.vulnerability_map.array_to_image(
            self.forest_edge_distance, out_file_name, mask_arr, gdal.GDT_Int16, -1
        )
        self.vulnerability_map.replace_ref_system(
            self.forest_edge_distance, out_file_name
        )

        print("Vulnerability map processing completed successfully.")
