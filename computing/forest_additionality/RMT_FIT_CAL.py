from .VulnerabilityMap import VulnerabilityMap
from .MapChecker import MapChecker
import os
from osgeo import gdal


class RMT_FIT_CAL:
    """Risk Mapping Tool for the Calibration period"""

    def __init__(
        self,
        working_directory: str,
        forest_edge_distance: str,
        deforestation_hrp: str,
        jurisdiction_mask: str,
    ):
        self.vulnerability_map = VulnerabilityMap()
        self.map_checker = MapChecker()
        self.directory = working_directory
        self.forest_edge_distance = forest_edge_distance
        self.deforestation_hrp = deforestation_hrp
        self.mask = jurisdiction_mask
        self.NRT = None
        self.n_classes = None
        self.out_fn = None
        self.directory_2 = None
        self.mask_2 = None
        self.fmask_2 = None
        self.in_fn_2 = None
        self.out_fn_2 = None
        self.n_classes_2 = None
        self.MRT = None

    def calculate_nrt(self):
        images = [self.forest_edge_distance, self.deforestation_hrp, self.mask]

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

        # Check if the deforestation map is a binary image
        if not self.map_checker.check_binary_map(self.deforestation_hrp):
            raise ValueError("The deforestation map is not a binary image.")

        # Check if the jurisdiction mask is a binary image
        if not self.map_checker.check_binary_map(self.mask):
            raise ValueError("The jurisdiction mask is not a binary image.")

        NRT = self.vulnerability_map.nrt_calculation(
            self.forest_edge_distance, self.deforestation_hrp, self.mask
        )
        self.NRT = NRT

        return NRT

    def calculate_mrt(self):
        images = [self.forest_edge_distance, self.deforestation_hrp, self.mask]

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

        # Check if the deforestation map is a binary image
        if not self.map_checker.check_binary_map(self.deforestation_hrp):
            raise ValueError("The deforestation map is not a binary image.")

        # Check if the jurisdiction mask is a binary image
        if not self.map_checker.check_binary_map(self.mask):
            raise ValueError("The jurisdiction mask is not a binary image.")

        MRT = self.vulnerability_map.mrt_calculation(
            self.forest_edge_distance, self.deforestation_hrp, self.mask
        )
        self.MRT = MRT

        return MRT

    def prepare_vulnerability_map(self):
        NRT = self.NRT

        try:
            self.NRT = int(NRT)
            if self.NRT <= 0:
                raise ValueError("NRT must be a positive integer.")
        except ValueError:
            raise ValueError("NRT must be a valid integer.")

        n_classes = int(29)

        out_file_name = "Vulnerability_Map_CAL.tif"

        out_file_path = os.path.join(self.directory, out_file_name)
        self.out_fn = out_file_path

        mask_arr = self.vulnerability_map.geometric_classification(
            self.forest_edge_distance, NRT, n_classes, self.mask
        )
        print(1)
        self.vulnerability_map.array_to_image(
            self.forest_edge_distance, out_file_path, mask_arr, gdal.GDT_Int16, -1
        )
        self.vulnerability_map.replace_ref_system(
            self.forest_edge_distance, out_file_name
        )

    def prepare_maximum_risk_map(self):
        MRT = self.MRT
