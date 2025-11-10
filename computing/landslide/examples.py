"""Example usage script for landslide susceptibility module.

This script demonstrates how to:
1. Generate landslide susceptibility for a tehsil
2. Validate the outputs
3. Generate reports
4. Visualize results
"""

import ee
from computing.landslide.landslide_vector import (
    vectorise_landslide,
    generate_demo_susceptibility,
)
from computing.landslide.utils import (
    get_susceptibility_statistics,
    compute_high_risk_percentage,
)
from computing.landslide.validation import (
    validate_attributes,
    validate_classification,
    generate_validation_report,
)
from utilities.gee_utils import ee_initialize


def example_generate_for_tehsil():
    """Example: Generate landslide susceptibility for a specific tehsil."""
    print("=== Example 1: Generate Landslide Susceptibility ===\n")
    
    # Configuration
    state = "jharkhand"
    district = "ranchi"
    block = "ranchi"
    gee_account_id = 1
    
    print(f"Generating landslide susceptibility for:")
    print(f"  State: {state}")
    print(f"  District: {district}")
    print(f"  Block: {block}\n")
    
    # Trigger async task
    result = vectorise_landslide.apply_async(
        args=[state, district, block, gee_account_id],
        queue="nrm"
    )
    
    print(f"Task ID: {result.id}")
    print(f"Task Status: {result.status}")
    print("\nMonitor progress in GEE Tasks tab or Django admin.\n")


def example_validate_outputs():
    """Example: Validate landslide susceptibility outputs."""
    print("=== Example 2: Validate Outputs ===\n")
    
    # Initialize GEE
    ee_initialize()
    
    # Asset to validate
    asset_id = "users/corestack/jharkhand_ranchi_landslide_vector"
    
    try:
        fc = ee.FeatureCollection(asset_id)
        
        print(f"Validating asset: {asset_id}\n")
        
        # Validate attributes
        print("1. Attribute Validation:")
        attr_result = validate_attributes(fc)
        print(f"   Status: {'PASS' if attr_result['valid'] else 'FAIL'}")
        if attr_result['missing_attributes']:
            print(f"   Missing: {', '.join(attr_result['missing_attributes'])}")
        else:
            print("   All required attributes present ✓")
        
        # Validate classification
        print("\n2. Classification Validation:")
        class_result = validate_classification(fc)
        print(f"   Status: {'PASS' if class_result['valid'] else 'FAIL'}")
        print("   Distribution:")
        for cls, count in class_result['class_distribution'].items():
            print(f"     {cls}: {count} MWS")
        
        print("\nValidation complete.\n")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: Make sure the asset exists and you have access.\n")


def example_generate_statistics():
    """Example: Generate statistics for landslide susceptibility."""
    print("=== Example 3: Generate Statistics ===\n")
    
    # Initialize GEE
    ee_initialize()
    
    asset_id = "users/corestack/jharkhand_ranchi_landslide_vector"
    
    try:
        fc = ee.FeatureCollection(asset_id)
        
        print(f"Computing statistics for: {asset_id}\n")
        
        # Get overall statistics
        stats = get_susceptibility_statistics(fc)
        
        print("Overall Statistics:")
        print(f"  Total MWS: {stats['total_mws']}")
        print(f"  Mean Slope: {stats['mean_slope']:.2f}°")
        print(f"  Mean Curvature: {stats['mean_curvature']:.4f}\n")
        
        print("Area by Susceptibility Class:")
        for cls, area in stats['area_by_class'].items():
            print(f"  {cls.capitalize()}: {area:.2f} ha")
        
        # Compute high risk percentage
        high_risk_pct = compute_high_risk_percentage(fc)
        print(f"\nHigh Risk Area: {high_risk_pct:.2f}% of total\n")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: Make sure the asset exists and you have access.\n")


def example_generate_report():
    """Example: Generate comprehensive validation report."""
    print("=== Example 4: Generate Validation Report ===\n")
    
    # Initialize GEE
    ee_initialize()
    
    asset_id = "users/corestack/jharkhand_ranchi_landslide_vector"
    aoi = ee.Geometry.Point([85.3, 23.3]).buffer(50000)
    
    try:
        # Generate report
        report = generate_validation_report(
            asset_id=asset_id,
            aoi=aoi,
            inventory_asset=None  # Add inventory asset if available
        )
        
        print(report)
        
        # Optionally save to file
        output_path = "/tmp/landslide_validation_report.txt"
        with open(output_path, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {output_path}\n")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: Make sure the asset exists and you have access.\n")


def example_demo_susceptibility():
    """Example: Generate demo susceptibility from slope."""
    print("=== Example 5: Generate Demo Susceptibility ===\n")
    
    # Initialize GEE
    ee_initialize()
    
    print("Generating demo susceptibility map from slope...\n")
    
    # Generate demo
    susceptibility_img = generate_demo_susceptibility()
    
    # Define a small test area
    aoi = ee.Geometry.Point([85.3, 23.3]).buffer(10000)  # 10km radius
    
    # Clip to AOI
    clipped = susceptibility_img.clip(aoi)
    
    # Compute area by class
    pixel_area = ee.Image.pixelArea()
    
    for class_val in [1, 2, 3, 4]:
        mask = clipped.eq(class_val)
        area = pixel_area.updateMask(mask).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=aoi,
            scale=100,
            maxPixels=1e13
        ).get('area')
        
        area_ha = ee.Number(area).divide(10000).getInfo()
        class_name = {1: "Low", 2: "Moderate", 3: "High", 4: "Very High"}[class_val]
        print(f"  {class_name}: {area_ha:.2f} ha")
    
    print("\nDemo generation complete.")
    print("This is a simple slope-based classification for testing.\n")


def example_api_usage():
    """Example: Use the REST API endpoint."""
    print("=== Example 6: Use REST API ===\n")
    
    import requests
    
    # API endpoint
    url = "http://localhost/computing/generate_landslide_layer/"
    
    # Request data
    data = {
        "state": "jharkhand",
        "district": "ranchi",
        "block": "ranchi",
        "gee_account_id": 1
    }
    
    print(f"POST {url}")
    print(f"Data: {data}\n")
    
    try:
        response = requests.post(url, json=data)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}\n")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: Make sure the server is running and you have authentication.\n")


def main():
    """Run all examples."""
    print("\n" + "=" * 70)
    print("LANDSLIDE SUSCEPTIBILITY MODULE - EXAMPLES")
    print("=" * 70 + "\n")
    
    examples = [
        ("Generate for Tehsil", example_generate_for_tehsil),
        ("Validate Outputs", example_validate_outputs),
        ("Generate Statistics", example_generate_statistics),
        ("Generate Report", example_generate_report),
        ("Demo Susceptibility", example_demo_susceptibility),
        ("REST API Usage", example_api_usage),
    ]
    
    print("Available examples:\n")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\nTo run all examples, continue. To run specific examples,")
    print("import this script and call the example functions directly.\n")
    
    choice = input("Run all examples? (y/n): ")
    
    if choice.lower() == 'y':
        for name, func in examples:
            try:
                func()
            except Exception as e:
                print(f"Error in {name}: {e}\n")
            
            input("Press Enter to continue...")
    else:
        print("\nTo run specific examples:")
        print("  from computing.landslide.examples import example_generate_for_tehsil")
        print("  example_generate_for_tehsil()\n")


if __name__ == "__main__":
    main()
