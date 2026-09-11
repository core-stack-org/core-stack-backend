from .VulnerabilityMap import VulnerabilityMap

# from MapChecker import MapChecker
import os
from osgeo import gdal


class RMT_PRE_CNF:
    def __init__(self, working_directory, forest_edge_distance_cnf, mask, nrt):
        self.working_directory = working_directory
        self.forest_edge_distance = forest_edge_distance_cnf
        self.mask = mask
        self.nrt = nrt

        self.vulnerability_map = VulnerabilityMap()

        self.vulnerability_map_cnf = "Vulnerability_Map_CNF.tif"

        self.working_directory_2 = None
        self.mask2 = None
        self.forest_edge_distance_2 = None
        self.nrt_2 = None
        self.vulnerability_map_cnf_2 = "Acre_Vunerability_CNF_2.tif"

    def process_data(self):
        NRT = self.nrt
        n_classes = int(29)

        self.vulnerability_map.set_working_directory(self.working_directory)
        mask_arr = self.vulnerability_map.geometric_classification(
            self.forest_edge_distance, self.nrt, n_classes, self.mask
        )
        self.vulnerability_map.array_to_image(
            self.forest_edge_distance,
            self.vulnerability_map_cnf,
            mask_arr,
            gdal.GDT_Int16,
            -1,
        )
        self.vulnerability_map.replace_ref_system(
            self.forest_edge_distance, self.vulnerability_map_cnf
        )
