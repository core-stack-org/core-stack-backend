import json
import hashlib
from shapely.geometry import Polygon, Point, MultiPolygon, MultiPoint
import geopandas as gpd
import pandas as pd
import fiona

dataset_paths = {
    "AWC": "projects/ee-plantationsitescores/assets/Raster-AWC_CLASS",
    "annualPrecipitation": "projects/ee-plantationsitescores/assets/AnnualPrecipitation",
    "meanAnnualTemperature": "projects/ee-plantationsitescores/assets/MeanAnnualTemp",
    "aridityIndex": "projects/ee-plantationsitescores/assets/India-AridityIndex",
    "referenceEvapoTranspiration": "projects/ee-plantationsitescores/assets/ReferenceEvapotranspiration",
    "topsoilPH": "projects/ee-plantationsitescores/assets/Raster-T_PH_H2O",
    "topsoilOC": "projects/ee-plantationsitescores/assets/Raster-T_OC",
    "topsoilCEC": "projects/ee-plantationsitescores/assets/Raster-T_CEC_SOIL",
    "topsoilTexture": "projects/ee-plantationsitescores/assets/Raster-T_TEXTURE",
    "subsoilPH": "projects/ee-plantationsitescores/assets/Raster-S_PH_H2O",
    "subsoilOC": "projects/ee-plantationsitescores/assets/Raster-S_OC",
    "subsoilCEC": "projects/ee-plantationsitescores/assets/Raster-S_CEC_SOIL",
    "subsoilTexture": "projects/ee-plantationsitescores/assets/Raster-S_USDA_TEX_CLASS",
    "topsoilBD": "projects/ee-plantationsitescores/assets/Raster-T_BULK_DEN",
    "subsoilBD": "projects/ee-plantationsitescores/assets/Raster-S_BULK_DEN",
    "drainage": "projects/ee-plantationsitescores/assets/Raster-Drainage",
    "elevation": "USGS/SRTMGL1_003",
    "slope": "USGS/SRTMGL1_003",
    "aspect": "USGS/SRTMGL1_003",
    "distToDrainage": "projects/ee-plantationsitescores/assets/so_thinned2",
}


def fix_invalid_geometries(gdf):
    def clean_geometry(geom):
        if geom is None:
            return None
        if not geom.is_valid:
            # buffer(0) tries to clean the geometry (fixes self-intersections)
            fixed = geom.buffer(0)
            abc = fixed if fixed.is_valid else None
            return abc
        return geom

    # Apply cleaning
    gdf["geometry"] = gdf["geometry"].apply(clean_geometry)

    # Drop any geometries that could not be fixed
    gdf = gdf.dropna(subset=["geometry"])
    gdf = gdf[gdf.is_valid]
    gdf = gdf[~gdf.is_empty]

    return gdf


def combine_kmls(kml_files_obj):
    # Enable KML driver
    fiona.drvsupport.supported_drivers["KML"] = "rw"

    # Read and combine all KML files
    gdfs = []
    print(kml_files_obj)
    for kml_file in kml_files_obj:
        try:
            gdf = gpd.read_file(kml_file.file, driver="KML")
            # Convert geometries to 2D
            gdf["geometry"] = gdf["geometry"].apply(convert_to_2d)
            # kml_hash = create_hash_using_geometry(gdf["geometry"])
            # Add filename as source column
            gdf["source"] = kml_file.name
            gdf["uid"] = kml_file.kml_hash
            gdfs.append(gdf)
        except Exception as e:
            print(f"Error reading {kml_file}: {e}")

    # Combine all geodataframes
    combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    combined_gdf = combined_gdf.dropna(axis=1, how="any")
    return combined_gdf


def convert_to_2d(geometry):
    """
    Convert a 3D geometry to 2D by removing Z coordinates.

    Parameters:
    geometry: Shapely geometry object

    Returns:
    Shapely geometry object with only X,Y coordinates
    """
    if geometry.has_z:
        if isinstance(geometry, Polygon):
            exterior_2d = [(x, y) for x, y, z in geometry.exterior.coords]
            interiors_2d = [
                [(x, y) for x, y, z in interior.coords]
                for interior in geometry.interiors
            ]
            return Polygon(exterior_2d, interiors_2d)


def geometry_to_dict(geom):
    """Convert geometry to a dictionary representation."""
    if geom is None:
        return None

    if isinstance(geom, gpd.GeoSeries):
        geom = geom.iloc[0]

    if isinstance(geom, (Point, MultiPoint)):
        coords = (
            list(geom.coords)
            if isinstance(geom, Point)
            else [list(p.coords) for p in geom.geoms]
        )
        return {"type": geom.geom_type, "coordinates": coords}

    elif isinstance(geom, (Polygon, MultiPolygon)):
        if isinstance(geom, Polygon):
            exterior = list(geom.exterior.coords)
            interiors = [list(interior.coords) for interior in geom.interiors]
            coords = [exterior] + interiors
        else:  # MultiPolygon
            coords = [
                [list(poly.exterior.coords)]
                + [list(interior.coords) for interior in poly.interiors]
                for poly in geom.geoms
            ]
        return {"type": geom.geom_type, "coordinates": coords}

    else:
        raise ValueError(f"Unsupported geometry type: {type(geom)}")


def create_hash_using_geometry(geometry):
    # Convert geometry to dictionary
    geom_dict = geometry_to_dict(geometry)
    if geom_dict:
        # Convert to JSON string with sorted keys for consistency
        geom_json = json.dumps(geom_dict, sort_keys=True)
        hash_object = hashlib.md5(geom_json.encode())
        geom_hash = hash_object.hexdigest()  # 206d4fb2e47585cab5c2ecd39a077e42
        return geom_hash
    return None
