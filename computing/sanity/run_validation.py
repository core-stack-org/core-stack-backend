import geopandas as gpd
import pandas as pd

from .validator import VectorValidator
from .report import summarize


def run_layer_validation(layer_path, rule_name):
    """
    Validate a vector layer (GeoJSON/Shapefile/etc.)
    """
    gdf = gpd.read_file(layer_path)

    validator = VectorValidator(f"computing/sanity/configs/{rule_name}.yaml")

    results = validator.validate(gdf)

    report = summarize(results)
    print(report)


# run_layer_validation("data/cropping_intensity_bhavnagar.geojson", "cropping_intensity")


def run_excel_validation(excel_path, rule_name):
    """
    Validate data from an Excel workbook.

    Looks for a sheet whose name matches rule_name
    (case-insensitive).
    """

    xls = pd.ExcelFile(excel_path)

    # sheet_name = next(
    #     (s for s in xls.sheet_names if s.lower() == rule_name.lower()),
    #     None,
    # )
    sheet_name = "croppingIntensity_annual"

    if sheet_name is None:
        raise ValueError(
            f"Sheet '{rule_name}' not found. " f"Available sheets: {xls.sheet_names}"
        )

    df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
    )

    print(df.columns)

    validator = VectorValidator(f"computing/sanity/configs/{rule_name}.yaml")

    results = validator.validate(df)

    return summarize(results)
