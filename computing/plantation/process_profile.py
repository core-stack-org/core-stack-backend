# Input data as a Python dictionary instead of reading from CSV
form_data = {
    "annualPrecipitation": "400-600, 600-740, 740-860, 860-1000",
    "meanAnnualTemperature": "24-26, 26-28",
    "aridityIndex": "0.20-0.50",
    "referenceEvapoTranspiration": "2-4, 4-6",
    "Climate": "20",
    "topsoilPH": "5.5-6, 6-6.5, 6.5-7, 7-7.5",
    "subsoilPH": "5.5-6, 6-6.5, 6.5-7, 7-7.5",
    "topsoilOC": "0.5-0.7, 0.7-1",
    "subsoilOC": "0.4-0.6, 0.6-0.8, 0.8-1",
    "topsoilCEC": "10-15, 15-20, 20-25",
    "subsoilCEC": "10-15, 15-20, 20-25",
    "topsoilTexture": "Coarse",
    "subsoilTexture": "Loam, Sandy clay loam",
    "topsoilBD": "1.1-1.3, 1.3-1.35, 1.35-1.40",
    "subsoilBD": "1.3-1.35, 1.35-1.4",
    "drainage": "Well drained",
    "AWC": "100, 75",
    "Soil": "20%",
    "elevation": "0-100, 100-200, 200-500, 500-800",
    "slope": "0-5, 5-10, 10-15",
    "aspect": "",
    "Topography": "20",
    "NDVI": "0.7-1, 0.5-0.7",
    "LULC": "Grassland, Cropland, Shrub and scrub, Bare ground",
    "Ecology": "20",
    "distToDrainage": "0-5, 5-50, 50-100",
    "distToSettlements": "0-20, 200-500",
    "distToRoad": "0-20, 100-200",
    "Socioeconomic": "20",
}

# List of environmental variables to be processed
variables = [
    "annualPrecipitation",
    "meanAnnualTemperature",
    "aridityIndex",
    "referenceEvapoTranspiration",
    "topsoilPH",
    "subsoilPH",
    "topsoilOC",
    "subsoilOC",
    "topsoilCEC",
    "subsoilCEC",
    "topsoilTexture",
    "subsoilTexture",
    "topsoilBD",
    "subsoilBD",
    "drainage",
    "AWC",
    "elevation",
    "slope",
    "aspect",
    "NDVI",
    "LULC",
    "distToDrainage",
    "distToSettlements",
    "distToRoad",
]

# Define categorical variables (those that use class mappings instead of ranges)
categorical_variables = ["topsoilTexture", "subsoilTexture", "drainage", "AWC", "LULC"]

# Define weight categories that appear in the form data
weight_categories = ["Climate", "Soil", "Topography", "Ecology", "Socioeconomic"]

# Define classification dictionaries for categorical variables
topsoilTextureClasses = {"Fine": 1, "Medium": 2, "Coarse": 3}
subsoilTextureClasses = {
    "Clay (heavy)": 1,
    "Silty clay": 2,
    "Clay": 3,
    "Silty clay loam": 4,
    "Clay loam": 5,
    "Silt": 6,
    "Silt loam": 7,
    "Sandy clay": 8,
    "Loam": 9,
    "Sandy clay loam": 10,
    "Sandy loam": 11,
    "Loamy sand": 12,
    "Sand": 13,
}
drainageClasses = {
    "Excessively drained": 1,
    "Somewhat excessively drained": 2,
    "Well drained": 3,
    "Moderately well drained": 4,
    "Imperfectly drained": 5,
    "Poorly drained": 6,
    "Very poorly drained": 7,
}
awcClasses = {"150": 1, "125": 2, "100": 3, "75": 4, "50": 5, "15": 6, "0": 7}
lulcClasses = {
    "Forest": 1,
    "Grassland": 2,
    "Flooded vegetation": 3,
    "Cropland": 4,
    "Shrub and scrub": 5,
    "Bare ground": 7,
    "Snow and ice": 8,
}

# Map variable names to their corresponding class dictionaries
class_mappings = {
    "topsoilTexture": topsoilTextureClasses,
    "subsoilTexture": subsoilTextureClasses,
    "drainage": drainageClasses,
    "AWC": awcClasses,
    "LULC": lulcClasses,
}


def generate_labels_and_ranges(input_string):
    """
    Function to parse and process numerical range inputs.
    Converts a string of ranges like "10-20, 30-40" into a structured format
    with labels (0 or 1) for each range segment.

    Args:
        input_string: A string containing comma-separated ranges (e.g., "10-20, 30-40")

    Returns:
        Tuple of (keys_string, values_string) where:
        - keys_string: Comma-separated range boundaries
        - values_string: Comma-separated binary values (0 or 1) indicating ideal ranges
    """
    # Parse input ranges and flatten the start and end points into a sorted list of unique boundaries
    input_ranges = [tuple(map(float, r.split("-"))) for r in input_string.split(", ")]
    boundaries = sorted(set([boundary for r in input_ranges for boundary in r]))

    # Add negative infinity and positive infinity boundaries
    all_boundaries = ["negInf"] + boundaries + ["posInf"]

    # Generate all possible ranges based on consecutive boundaries
    all_ranges = [
        (str(all_boundaries[i]), str(all_boundaries[i + 1]))
        for i in range(len(all_boundaries) - 1)
    ]

    # Create dictionary with all ranges initialized to 0 (not ideal)
    range_dict = {f"{start}-{end}": 0 for start, end in all_ranges}

    # Update the dictionary: mark ranges from input as 1 (ideal)
    for start, end in input_ranges:
        key = f"{str(start)}-{str(end)}"
        if key in range_dict:
            range_dict[key] = 1

    # Merge consecutive intervals with the same value (to simplify output)
    merged_dict = {}
    prev_key, prev_value, current_end = None, None, None

    for key, value in range_dict.items():
        # Split the range key to get the start and end of the current range
        start, end = key.split("-")

        if prev_key is None:
            # Initialize the first range
            prev_key, prev_value = start, value
            current_end = end
            continue

        if value == prev_value:
            # Continue extending the range if the value is the same
            current_end = end
        else:
            # Add the merged range to the dictionary
            merged_dict[f"{prev_key}-{current_end}"] = prev_value
            # Reset the start for the new range
            prev_key, prev_value = start, value
            current_end = end

    # Add the last range to the dictionary
    merged_dict[f"{prev_key}-{current_end}"] = prev_value

    # Format the output as comma-separated strings
    keys_string = ",".join(merged_dict.keys())
    values_string = ",".join(map(str, merged_dict.values()))
    return keys_string, values_string


def generate_labels_and_classes(input_string, variable):
    """
    Function to process categorical variables.
    Maps selected categories to binary values (1=ideal, 0=not ideal)

    Args:
        input_string: A string containing comma-separated category names
        variable: The name of the variable to identify which classification to use

    Returns:
        Tuple of (classes, labels) where:
        - classes: Comma-separated list of class values
        - labels: Comma-separated binary values (0 or 1) indicating ideal classes
    """
    # Get the appropriate classification dictionary for this variable
    if variable in class_mappings:
        classDict = class_mappings[variable]
    else:
        print(f"Unknown variable: {variable}")
        return None

    # Create string of all class values
    classes = ",".join(map(str, classDict.values()))

    # Create binary labels: 1 for ideal classes, 0 for others
    ideal_classes = input_string.split(", ")
    labels = ",".join(
        ["1" if key in ideal_classes else "0" for key in classDict.keys()]
    )

    return classes, labels


def clean_percentage(value):
    """Convert percentage string to float, removing % sign if present"""
    if isinstance(value, str) and value.endswith("%"):
        return float(value[:-1])
    return float(value)


def process_project_profile():
    weights = {}
    final_variables = {}
    final_weights = {}

    # Process each variable
    for var_name in variables:
        # Check if the variable exists in form_data
        if var_name not in form_data:
            print(f"Warning: No data found for variable {var_name}")
            continue

        value = form_data[var_name]

        # Skip if the value is empty
        if not value:
            print(f"{var_name} is empty")
            continue

        # Process the variable based on its type
        if var_name in categorical_variables:
            classes, labels = generate_labels_and_classes(value, var_name)
            final_variables[var_name] = {"labels": labels, "thresholds": classes}
        else:
            # Numerical variable
            thresholds, labels = generate_labels_and_ranges(value)
            final_variables[var_name] = {"labels": labels, "thresholds": thresholds}

    # Extract weights from form data
    for category in weight_categories:
        if category in form_data:
            weights[category] = form_data[category]

    # Verify weights sum to 100%
    weight_values = [clean_percentage(w) for w in weights.values()]
    weight_sum = sum(weight_values)

    if abs(weight_sum - 100) > 0.01:  # Allow small floating-point error
        print(f"Warning: Weights sum to {weight_sum}%, not 100%")

    # Process each weight (convert from percentage to decimal)
    for category, value in weights.items():
        decimal_value = clean_percentage(value) / 100
        final_weights[category] = decimal_value

    print(final_weights)
    print(final_variables)
