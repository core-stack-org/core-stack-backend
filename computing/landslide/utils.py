"""Utility functions for landslide susceptibility processing."""

import ee
from typing import Dict


def get_susceptibility_statistics(fc: ee.FeatureCollection) -> Dict:
    """Compute summary statistics for landslide susceptibility across MWS features.
    
    Args:
        fc: FeatureCollection with landslide attributes
        
    Returns:
        dict: Summary statistics including total area by class, mean slope, etc.
    """
    # This would typically be called client-side after export
    stats = {
        "total_mws": fc.size().getInfo(),
        "area_by_class": {},
        "mean_slope": None,
        "mean_curvature": None,
    }
    
    # Compute total area by susceptibility class
    classes = ["low", "moderate", "high", "very_high"]
    for cls in classes:
        total = fc.aggregate_sum(f"{cls}_area_ha").getInfo()
        stats["area_by_class"][cls] = total
    
    # Compute mean slope and curvature
    stats["mean_slope"] = fc.aggregate_mean("mean_slope_deg").getInfo()
    stats["mean_curvature"] = fc.aggregate_mean("mean_curvature").getInfo()
    
    return stats


def validate_landslide_outputs(fc: ee.FeatureCollection, expected_mws_count: int = None) -> Dict:
    """Validate landslide susceptibility outputs.
    
    Args:
        fc: FeatureCollection to validate
        expected_mws_count: Expected number of MWS features (optional)
        
    Returns:
        dict: Validation results with status and any issues found
    """
    validation = {
        "valid": True,
        "issues": [],
        "feature_count": None,
        "has_required_properties": True,
    }
    
    # Check feature count
    count = fc.size().getInfo()
    validation["feature_count"] = count
    
    if expected_mws_count and count != expected_mws_count:
        validation["valid"] = False
        validation["issues"].append(
            f"Feature count mismatch: expected {expected_mws_count}, got {count}"
        )
    
    # Check required properties
    required_props = [
        "low_area_ha",
        "moderate_area_ha", 
        "high_area_ha",
        "very_high_area_ha",
        "mean_slope_deg",
        "mean_curvature",
        "susceptibility_score",
        "susceptibility_category",
    ]
    
    first_feature = fc.first()
    props = first_feature.propertyNames().getInfo()
    
    missing_props = [p for p in required_props if p not in props]
    if missing_props:
        validation["valid"] = False
        validation["has_required_properties"] = False
        validation["issues"].append(f"Missing properties: {', '.join(missing_props)}")
    
    return validation


def create_landslide_visualization(susceptibility_img: ee.Image) -> Dict:
    """Create visualization parameters for landslide susceptibility.
    
    Args:
        susceptibility_img: Classified susceptibility image
        
    Returns:
        dict: Visualization parameters for GEE Code Editor or Map display
    """
    vis_params = {
        "min": 1,
        "max": 4,
        "palette": ["green", "yellow", "orange", "red"],
        "bands": ["susceptibility"],
    }
    
    return vis_params


def export_landslide_validation_report(
    fc: ee.FeatureCollection,
    state: str,
    district: str,
    block: str,
    output_path: str = None
) -> str:
    """Export a validation report for landslide susceptibility mapping.
    
    Args:
        fc: FeatureCollection with landslide attributes
        state: State name
        district: District name
        block: Block/Tehsil name
        output_path: Path to save report (optional)
        
    Returns:
        str: Validation report as formatted text
    """
    stats = get_susceptibility_statistics(fc)
    validation = validate_landslide_outputs(fc)
    
    report = f"""
Landslide Susceptibility Validation Report
==========================================

Location: {state} > {district} > {block}
Date: {ee.Date(ee.Number(ee.Date.now().millis())).format('YYYY-MM-dd').getInfo()}

Summary Statistics
------------------
Total MWS Features: {stats['total_mws']}
Mean Slope: {stats['mean_slope']:.2f}°
Mean Curvature: {stats['mean_curvature']:.4f}

Area by Susceptibility Class (hectares)
---------------------------------------
Low:       {stats['area_by_class']['low']:.2f} ha
Moderate:  {stats['area_by_class']['moderate']:.2f} ha
High:      {stats['area_by_class']['high']:.2f} ha
Very High: {stats['area_by_class']['very_high']:.2f} ha

Validation Results
------------------
Status: {"PASS" if validation['valid'] else "FAIL"}
Feature Count: {validation['feature_count']}
Has Required Properties: {"Yes" if validation['has_required_properties'] else "No"}

Issues:
{chr(10).join(f"  - {issue}" for issue in validation['issues']) if validation['issues'] else "  None"}

"""
    
    if output_path:
        with open(output_path, 'w') as f:
            f.write(report)
    
    return report


def clip_landslide_to_custom_geometry(
    landslide_asset: str,
    geometry: ee.Geometry,
    scale: int = 100
) -> ee.Image:
    """Clip landslide susceptibility raster to a custom geometry.
    
    Args:
        landslide_asset: GEE asset path to landslide susceptibility
        geometry: Geometry to clip to
        scale: Processing scale in meters
        
    Returns:
        ee.Image: Clipped landslide susceptibility image
    """
    landslide = ee.Image(landslide_asset)
    clipped = landslide.clip(geometry)
    
    return clipped


def compute_high_risk_percentage(fc: ee.FeatureCollection) -> float:
    """Compute percentage of area in high or very high susceptibility.
    
    Args:
        fc: FeatureCollection with landslide attributes
        
    Returns:
        float: Percentage of total area in high/very high categories
    """
    high = fc.aggregate_sum("high_area_ha").getInfo()
    very_high = fc.aggregate_sum("very_high_area_ha").getInfo()
    total = fc.aggregate_sum("total_area_ha").getInfo()
    
    if total > 0:
        percentage = ((high + very_high) / total) * 100
        return percentage
    
    return 0.0


def compare_with_historical_landslides(
    fc: ee.FeatureCollection,
    landslide_inventory: ee.FeatureCollection
) -> Dict:
    """Compare susceptibility mapping with historical landslide inventory.
    
    This performs spatial overlay to check if known landslide locations
    fall within high susceptibility zones.
    
    Args:
        fc: FeatureCollection with susceptibility attributes
        landslide_inventory: FeatureCollection of known landslide locations
        
    Returns:
        dict: Validation metrics (accuracy, precision, recall)
    """
    # Join landslide points with susceptibility polygons
    spatial_filter = ee.Filter.intersects(
        leftField=".geo",
        rightField=".geo",
        maxError=100
    )
    
    joined = ee.Join.saveFirst("susceptibility_match").apply(
        landslide_inventory,
        fc,
        spatial_filter
    )
    
    # Count how many landslides fall in high/very high zones
    def check_high_susceptibility(feature):
        match = ee.Feature(feature.get("susceptibility_match"))
        category = match.get("susceptibility_category")
        is_high = ee.Algorithms.If(
            ee.List(["high", "very_high"]).contains(category),
            1,
            0
        )
        return feature.set("in_high_zone", is_high)
    
    validated = joined.map(check_high_susceptibility)
    
    total_landslides = landslide_inventory.size().getInfo()
    correct_predictions = validated.aggregate_sum("in_high_zone").getInfo()
    
    accuracy = (correct_predictions / total_landslides * 100) if total_landslides > 0 else 0
    
    return {
        "total_landslides": total_landslides,
        "correctly_predicted": correct_predictions,
        "accuracy_percent": accuracy,
    }
