from osgeo import gdal

def get_image_dimension(image):
    dataset = gdal.Open(image)
    rows = dataset.RasterXSize
    cols = dataset.RasterYSize
    return rows, cols

def get_image_resolution(image):
    in_ds = gdal.Open(image)
    P = in_ds.GetGeoTransform()[1]
    return P