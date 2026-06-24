import geopandas as gpd

from .validator import VectorValidator
from .report import summarize


def run_layer_validation(layer_path, rule_name):
    gdf = gpd.read_file(layer_path)

    validator = VectorValidator(f"computing/sanity/configs/{rule_name}.yaml")

    results = validator.validate(gdf)

    report = summarize(results)
    print(report)

# run_layer_validation("data/cropping_intensity_bhavnagar.geojson", "cropping_intensity")
