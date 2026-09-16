from .VulnerabilityMap import VulnerabilityMap

# from MapChecker import MapChecker
# import os
from osgeo import gdal


class RMT_PRE_VP:
    def __init__(self, working_directory, forest_edge_distance_vp, mask, nrt):
        self.working_directory = working_directory
        self.in_fn = forest_edge_distance_vp
        self.mask = mask
        self.nrt = nrt

        self.vulnerability_map = VulnerabilityMap()

        self.out_fn = "Acre_Vulnerability_VP.tif"

    def process_data(self):
        n_classes = int(29)

        self.vulnerability_map.set_working_directory(self.working_directory)
        mask_arr = self.vulnerability_map.geometric_classification(
            self.in_fn, self.nrt, n_classes, self.mask
        )
        self.vulnerability_map.array_to_image(
            self.in_fn, self.out_fn, mask_arr, gdal.GDT_Int16, -1
        )
        self.vulnerability_map.replace_ref_system(self.in_fn, self.out_fn)
