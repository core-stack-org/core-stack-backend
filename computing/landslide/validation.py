"""Validation utilities for landslide susceptibility outputs.

This module provides functions to validate landslide susceptibility
mapping results against quality criteria and historical data.
"""

import ee
from typing import Dict, Optional
import json


def validate_coverage(
    landslide_asset: str,
    aoi: ee.Geometry,
    min_coverage_percent: float = 95.0
) -> Dict:
    """Validate that landslide susceptibility covers the AOI adequately.
    
    Args:
        landslide_asset: Path to landslide susceptibility asset
        aoi: Area of interest geometry
        min_coverage_percent: Minimum required coverage percentage
        
    Returns:
        dict: Validation results with coverage percentage and status
    """
    landslide = ee.Image(landslide_asset)
    
    # Compute total AOI area
    aoi_area = aoi.area(maxError=100)
    
    # Compute area covered by landslide data
    mask = landslide.mask()
    pixel_area = ee.Image.pixelArea()
    covered_area = pixel_area.updateMask(mask).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=aoi,
        scale=100,
        maxPixels=1e13,
        bestEffort=True
    ).get('area')
    
    coverage_percent = ee.Number(covered_area).divide(aoi_area).multiply(100)
    
    result = {
        'aoi_area_m2': aoi_area.getInfo(),
        'covered_area_m2': ee.Number(covered_area).getInfo(),
        'coverage_percent': coverage_percent.getInfo(),
        'meets_threshold': coverage_percent.gte(min_coverage_percent).getInfo(),
        'threshold': min_coverage_percent
    }
    
    return result


def validate_attributes(fc: ee.FeatureCollection) -> Dict:
    """Validate that all required attributes are present in vectorized output.
    
    Args:
        fc: FeatureCollection with landslide attributes
        
    Returns:
        dict: Validation results listing missing attributes
    """
    required_attrs = [
        'low_area_ha',
        'moderate_area_ha',
        'high_area_ha',
        'very_high_area_ha',
        'mean_slope_deg',
        'mean_curvature',
        'susceptibility_score',
        'susceptibility_category',
        'total_area_ha'
    ]
    
    first = fc.first()
    props = first.propertyNames().getInfo()
    
    missing = [attr for attr in required_attrs if attr not in props]
    
    result = {
        'valid': len(missing) == 0,
        'required_attributes': required_attrs,
        'present_attributes': props,
        'missing_attributes': missing,
        'total_features': fc.size().getInfo()
    }
    
    return result


def validate_classification(
    fc: ee.FeatureCollection,
    expected_classes: list = None
) -> Dict:
    """Validate that susceptibility classifications are correct.
    
    Args:
        fc: FeatureCollection with susceptibility_category
        expected_classes: List of expected class names
        
    Returns:
        dict: Validation results with class distribution
    """
    if expected_classes is None:
        expected_classes = ['low', 'moderate', 'high', 'very_high']
    
    # Get unique categories
    categories = fc.aggregate_array('susceptibility_category').distinct().getInfo()
    
    # Count features in each category
    class_counts = {}
    for cat in expected_classes:
        count = fc.filter(ee.Filter.eq('susceptibility_category', cat)).size().getInfo()
        class_counts[cat] = count
    
    unexpected = [c for c in categories if c not in expected_classes]
    
    result = {
        'valid': len(unexpected) == 0,
        'expected_classes': expected_classes,
        'found_classes': categories,
        'unexpected_classes': unexpected,
        'class_distribution': class_counts
    }
    
    return result


def validate_against_inventory(
    fc: ee.FeatureCollection,
    inventory_asset: str,
    high_risk_threshold: float = 0.7
) -> Dict:
    """Validate landslide susceptibility against historical inventory.
    
    This checks if known landslide locations fall within high/very high
    susceptibility zones at the expected rate.
    
    Args:
        fc: FeatureCollection with susceptibility data
        inventory_asset: Path to historical landslide inventory
        high_risk_threshold: Minimum percentage of landslides in high zones
        
    Returns:
        dict: Validation metrics including accuracy
    """
    inventory = ee.FeatureCollection(inventory_asset)
    
    # Spatial join
    spatial_filter = ee.Filter.intersects(
        leftField='.geo',
        rightField='.geo',
        maxError=100
    )
    
    joined = ee.Join.saveFirst('susceptibility').apply(
        inventory,
        fc,
        spatial_filter
    )
    
    # Check how many are in high/very high zones
    def check_high_risk(feature):
        match = ee.Feature(feature.get('susceptibility'))
        category = match.get('susceptibility_category')
        is_high = ee.List(['high', 'very_high']).contains(category)
        return feature.set('in_high_zone', ee.Number(is_high))
    
    validated = joined.map(check_high_risk)
    
    total = inventory.size().getInfo()
    in_high = validated.aggregate_sum('in_high_zone').getInfo()
    
    accuracy = (in_high / total) if total > 0 else 0
    
    result = {
        'total_landslides': total,
        'in_high_zones': in_high,
        'accuracy': accuracy,
        'meets_threshold': accuracy >= high_risk_threshold,
        'threshold': high_risk_threshold
    }
    
    return result


def generate_validation_report(
    asset_id: str,
    aoi: ee.Geometry,
    inventory_asset: Optional[str] = None
) -> str:
    """Generate a comprehensive validation report.
    
    Args:
        asset_id: Path to vectorized landslide asset
        aoi: Area of interest geometry
        inventory_asset: Optional path to historical inventory
        
    Returns:
        str: Formatted validation report
    """
    fc = ee.FeatureCollection(asset_id)
    
    # Run all validations
    attr_validation = validate_attributes(fc)
    class_validation = validate_classification(fc)
    
    report_lines = [
        "=" * 70,
        "LANDSLIDE SUSCEPTIBILITY VALIDATION REPORT",
        "=" * 70,
        "",
        f"Asset: {asset_id}",
        f"Total Features: {attr_validation['total_features']}",
        "",
        "ATTRIBUTE VALIDATION",
        "-" * 70,
        f"Status: {'PASS' if attr_validation['valid'] else 'FAIL'}",
    ]
    
    if attr_validation['missing_attributes']:
        report_lines.append(f"Missing Attributes: {', '.join(attr_validation['missing_attributes'])}")
    else:
        report_lines.append("All required attributes present ✓")
    
    report_lines.extend([
        "",
        "CLASSIFICATION VALIDATION",
        "-" * 70,
        f"Status: {'PASS' if class_validation['valid'] else 'FAIL'}",
        "Class Distribution:"
    ])
    
    for cls, count in class_validation['class_distribution'].items():
        report_lines.append(f"  {cls.capitalize()}: {count} features")
    
    if class_validation['unexpected_classes']:
        report_lines.append(f"Unexpected Classes: {', '.join(class_validation['unexpected_classes'])}")
    
    if inventory_asset:
        try:
            inv_validation = validate_against_inventory(fc, inventory_asset)
            report_lines.extend([
                "",
                "HISTORICAL VALIDATION",
                "-" * 70,
                f"Status: {'PASS' if inv_validation['meets_threshold'] else 'FAIL'}",
                f"Total Landslides: {inv_validation['total_landslides']}",
                f"In High Risk Zones: {inv_validation['in_high_zones']}",
                f"Accuracy: {inv_validation['accuracy']:.2%}",
                f"Threshold: {inv_validation['threshold']:.2%}"
            ])
        except Exception as e:
            report_lines.extend([
                "",
                "HISTORICAL VALIDATION",
                "-" * 70,
                f"Could not validate against inventory: {str(e)}"
            ])
    
    report_lines.extend([
        "",
        "=" * 70,
        "END OF REPORT",
        "=" * 70
    ])
    
    return "\n".join(report_lines)


def export_validation_metrics(
    asset_id: str,
    output_path: str
) -> None:
    """Export validation metrics to JSON file.
    
    Args:
        asset_id: Path to vectorized landslide asset
        output_path: Path to save JSON file
    """
    fc = ee.FeatureCollection(asset_id)
    
    metrics = {
        'attribute_validation': validate_attributes(fc),
        'classification_validation': validate_classification(fc),
    }
    
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"Validation metrics exported to {output_path}")
