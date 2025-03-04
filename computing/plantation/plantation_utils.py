import json
import hashlib
from shapely.geometry import Polygon, Point, MultiPolygon, MultiPoint
import geopandas as gpd
import pandas as pd
import fiona
from pathlib import Path

dataset_paths = {
    "AWC": "projects/ee-plantationsitescores/assets/Raster-AWC_CLASS",  # (Pan India)
    # "LULC": "projects/ee-indiasat/assets/LULC_Version2_Outputs_NewHierarchy/Anantapur_2019-07-01_2020-06-30_LULCmap_30m",
    # "NDVI": "",
    "annualPrecipitation": "projects/ee-plantationsitescores/assets/AnnualPrecipitation",  # (Global)
    "aridityIndex": "projects/ee-plantationsitescores/assets/India-AridityIndex",  # (Pan India)
    "aspect": "USGS/SRTMGL1_003",
    "distToDrainage": "projects/ee-plantationsitescores/assets/so_thinned2",  # (Pan India)
    # "distToRoad": "projects/ee-mtpictd/assets/shiva/Road_DRRP3_TN",  # (Only available for AP and TN)
    # "distToSettlements": "projects/ee-indiasat/assets/LULC_Version2_Outputs_NewHierarchy/Anantapur_2019-07-01_2020-06-30_LULCmap_30m",
    "drainage": "projects/ee-plantationsitescores/assets/Raster-AWC_CLASS",  # (Pan India)
    "elevation": "USGS/SRTMGL1_003",
    "meanAnnualTemperature": "projects/ee-plantationsitescores/assets/MeanAnnualTemp",  # (Global)
    "referenceEvapoTranspiration": "projects/ee-plantationsitescores/assets/ReferenceEvapotranspiration",  # (Global)
    # "roi": "projects/ee-mtpictd/assets/shiva/ATREE/Plantations_TN",  # (TN only, are this converted from KMLs?)
    "slope": "USGS/SRTMGL1_003",
    "subsoilBD": "projects/ee-plantationsitescores/assets/Raster-S_BULK_DEN",  # (Pan India)
    "subsoilCEC": "projects/ee-plantationsitescores/assets/Raster-S_CEC_SOIL",  # (Pan India)
    "subsoilOC": "projects/ee-plantationsitescores/assets/Raster-S_OC",  # (Pan India)
    "subsoilPH": "projects/ee-plantationsitescores/assets/Raster-S_PH_H2O",  # (Pan India)
    "subsoilTexture": "projects/ee-plantationsitescores/assets/Raster-S_USDA_TEX_CLASS",  # (Pan India)
    "topsoilBD": "projects/ee-plantationsitescores/assets/Raster-T_BULK_DEN",  # (Pan India)
    "topsoilCEC": "projects/ee-plantationsitescores/assets/Raster-T_CEC_SOIL",  # (Pan India)
    "topsoilOC": "projects/ee-plantationsitescores/assets/Raster-T_OC",  # (Pan India)
    "topsoilPH": "projects/ee-plantationsitescores/assets/Raster-T_PH_H2O",  # (Pan India)
    "topsoilTexture": "projects/ee-plantationsitescores/assets/Raster-T_TEXTURE",  # (Pan India)
}

saytrees_weights = {
    "AWC": 0.20000000298023224,
    "Climate": 0.10000000149011612,
    "Ecology": 0.3499999940395355,
    "LULC": 0.5,
    "NDVI": 0.5,
    "Socioeconomic": 0,
    "Soil": 0.3499999940395355,
    "Topography": 0.20000000298023224,
    "annualPrecipitation": 0.3499999940395355,
    "aridityIndex": 0.15000000596046448,
    "aspect": 0.20000000298023224,
    "distToDrainage": 0.33000001311302185,
    "distToRoad": 0.33000001311302185,
    "distToSettlements": 0.3400000035762787,
    "drainage": 0.20000000298023224,
    "elevation": 0.4000000059604645,
    "meanAnnualTemperature": 0.3499999940395355,
    "rcSubsoilBD": 0.25,
    "rcSubsoilPH": 0.25,
    "rcTopsoilBD": 0.25,
    "rcTopsoilPH": 0.25,
    "referenceEvapoTranspiration": 0.15000000596046448,
    "rootingCondition": 0.20000000298023224,
    "slope": 0.4000000059604645,
    "snSubsoilCEC": 0.25,
    "snSubsoilOC": 0.25,
    "snSubsoilPH": 0.25,
    "snSubsoilTexture": 0.25,
    "subsoilNutrient": 0.20000000298023224,
    "tnTopsoilCEC": 0.25,
    "tnTopsoilOC": 0.25,
    "tnTopsoilPH": 0.25,
    "tnTopsoilTexture": 0.25,
    "topsoilNutrient": 0.20000000298023224,
}

saytrees_intervals = {
    "AWC": {"labels": "1,1,1,0,0,0,0", "thresholds": "1,3,5,2,4,6,7"},
    "LULC": {"labels": "0,0,1,0,0,1,0,1,0,0", "thresholds": "0,1,2,3,4,5,6,7,8,9"},
    "NDVI": {"labels": "1,0,1", "thresholds": "negInf-0.4,0.4-1,1-posInf"},
    "annualPrecipitation": {
        "labels": "0,1,0",
        "thresholds": "negInf-500,500-2000,2000-posInf",
    },
    "aridityIndex": {
        "labels": "0,1,0",
        "thresholds": "negInf-15000,15000-50000,50000-posInf",
    },
    "aspect": {"labels": "1,0", "thresholds": "67.5-292.5,292.5-67.5"},
    "distToDrainage": {"labels": "1,0", "thresholds": "0-180,180-posInf"},
    "distToRoad": {"labels": "1,0", "thresholds": "0-180,180-posInf"},
    "distToSettlements": {"labels": "1,0", "thresholds": "0-180,180-posInf"},
    "drainage": {"labels": "1,0,0,0,0,0", "thresholds": "3,1,2,4,5,6"},
    "elevation": {"labels": "1,0", "thresholds": "negInf-1500,1500-posInf"},
    "meanAnnualTemperature": {
        "labels": "0,1,0",
        "thresholds": "negInf-22,22-30,30-posInf",
    },
    "referenceEvapoTranspiration": {
        "labels": "0,1,0",
        "thresholds": "negInf-1250,1250-1663,1663-posInf",
    },
    "slope": {"labels": "1,0", "thresholds": "negInf-10,10-posInf"},
    "subsoilBD": {"labels": "0,1,0", "thresholds": "negInf-1.2,1.2-1.4,1.4-posInf"},
    "subsoilCEC": {"labels": "0,1,0", "thresholds": "negInf-10,10-25,25-posInf"},
    "subsoilOC": {"labels": "0,1,0", "thresholds": "negInf-0.5,0.5-1.5,1.5-posInf"},
    "subsoilPH": {"labels": "0,1,0", "thresholds": "7.5-posInf,5.5-7.5,negInf-5.5"},
    "subsoilTexture": {"labels": "0,1,0", "thresholds": "negInf-7,8-13,14-posInf"},
    "topsoilBD": {"labels": "0,1,0", "thresholds": "negInf-1.2,1.1-2.4,1.4-posInf"},
    "topsoilCEC": {"labels": "0,1,0", "thresholds": "negInf-10,10-25,25-posInf"},
    "topsoilOC": {"labels": "0,1,0", "thresholds": "negInf-0.5,0.5-1.5,1.5-posInf"},
    "topsoilPH": {"labels": "0,1,0", "thresholds": "7.5-posInf,5.5-7.5,negInf-5.5"},
    "topsoilTexture": {"labels": "0,1,0", "thresholds": "negInf-7,8-13,14-posInf"},
}


def combine_kmls(input_dir):
    # Enable KML driver
    fiona.drvsupport.supported_drivers["KML"] = "rw"

    # Get all KML files in directory
    kml_files = list(Path(input_dir).glob("*.kml"))

    if not kml_files:
        raise ValueError(f"No KML files found in {input_dir}")

    # Read and combine all KML files
    gdfs = []
    for kml_file in kml_files:
        try:
            gdf = gpd.read_file(kml_file, driver="KML")
            # Convert geometries to 2D
            gdf["geometry"] = gdf["geometry"].apply(convert_to_2d)
            kml_hash = create_hash_using_geometry(gdf["geometry"])
            # Add filename as source column
            gdf["source"] = kml_file.name
            gdf["uid"] = kml_hash
            gdfs.append(gdf)
        except Exception as e:
            print(f"Error reading {kml_file}: {e}")

    # Combine all geodataframes
    combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

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
