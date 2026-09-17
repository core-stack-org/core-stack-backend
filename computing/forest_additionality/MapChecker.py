import os
from osgeo import gdal
import numpy as np


class MapChecker:
    def __init__(self):
        self.image = None
        self.arr = None
        self.in_fn = None

    def get_image_resolution(self, image):
        in_ds = gdal.Open(image)
        P = in_ds.GetGeoTransform()[1]
        return P

    def get_image_dimensions(self, image):
        dataset = gdal.Open(image)
        cols = dataset.RasterXSize
        rows = dataset.RasterYSize
        return rows, cols

    def get_image_datatype(self, image):
        in_ds = gdal.Open(image)
        in_band = in_ds.GetRasterBand(1)
        datatype = gdal.GetDataTypeName(in_band.DataType)
        return datatype

    def get_image_max_min(self, image):
        in_ds = gdal.Open(image)
        in_band = in_ds.GetRasterBand(1)
        min, max = in_band.ComputeRasterMinMax()
        return min, max

    def find_unique_values(self, arr, limit=2):
        unique_values = set()
        for value in np.nditer(arr):
            unique_values.add(value.item())
            if len(unique_values) > limit:
                return False
        return True

    def check_binary_map(self, in_fn):
        """
        Check if input image is binary map
        :param in_fn: input image
        :return: True if the file is a binary map, False otherwise
        """
        file_extension = in_fn.split(".")[-1].lower()
        file_name, _ = os.path.splitext(in_fn)
        if file_extension == "rst":
            with open(file_name + ".rdc", "r") as read_file:
                rdc_content = read_file.read().lower()
                byte_or_integer_binary = "data type   : byte" in rdc_content or (
                    "data type   : integer" in rdc_content
                    and "min. value  : 0" in rdc_content
                    and "max. value  : 1" in rdc_content
                )
                float_binary = (
                    "data type   : real" in rdc_content
                    and "min. value  : 0.0000000" in rdc_content
                    and "max. value  : 1.0000000" in rdc_content
                )
        elif file_extension == "tif":
            datatype = self.get_image_datatype(in_fn)
            min_val, max_val = self.get_image_max_min(in_fn)
            byte_or_integer_binary = (
                datatype
                in ["Byte", "CInt16", "CInt32", "Int16", "Int32", "UInt16", "UInt32"]
                and max_val == 1
                and min_val == 0
            )
            float_binary = (
                datatype in ["Float32", "Float64", "CFloat32", "CFloat64"]
                and max_val == 1.0000000
                and min_val == 0.0000000
            )

        if byte_or_integer_binary or (float_binary):
            # For float_binary, use find_unique_values function to check if data only have two unique values [0.0000000, 1.0000000].
            if float_binary:
                in_ds = gdal.Open(in_fn)
                in_band = in_ds.GetRasterBand(1)
                arr = in_band.ReadAsArray()
                # If more than two unique values are found, it's not a binary map, return False.
                if not self.find_unique_values(arr, 2):
                    return False
            # Binary map: byte_or_integer_binary or float_binary with two unique values [0.0000000, 1.0000000], it returns True.
            return True
        # For any other scenario, it returns False.
        return False
