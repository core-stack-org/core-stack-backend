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
categorical_variables = [
    "topsoilTexture",
    "subsoilTexture",
    "drainage",
    "aspect",
    "AWC",
    "LULC",
]

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

lulcClassesIndiaSat = {
    "Forest": [6],
    "Grassland": [12],
    "Flooded vegetation": [6],
    "Cropland": [5, 8, 9, 10, 11],
    "Shrub and scrub": [12],
    "Bare ground": [7],
    "Snow and ice": [],
}

aspect = {
    "North": ["0 - 22.5", "337.5 - 360"],
    "Northeast": "22.5 - 67.5",
    "East": "67.5 - 112.5",
    "Southeast": "112.5 - 157.5",
    "South": "157.5 - 202.5",
    "Southwest": "202.5 - 247.5",
    "West": "247.5 - 292.5",
    "Northwest": "292.5 - 337.5",
}

# Map variable names to their corresponding class dictionaries
class_mappings = {
    "topsoilTexture": topsoilTextureClasses,
    "subsoilTexture": subsoilTextureClasses,
    "drainage": drainageClasses,
    "aspect": aspect,
    "AWC": awcClasses,
    # "LULC": lulcClasses,
    "LULC": lulcClassesIndiaSat,
}


def generate_labels_and_ranges(input_string):
    """
    Function to parse and process numerical range inputs.
    Converts a string of ranges like "10-20, 30-40" into a structured format
    with labels (0 or 1) for each range segment.
    Also handles inequality expressions like '<50' or '>100'.

    Args:
        input_string: A string containing comma-separated ranges (e.g., "10-20, 30-40, <50, >100")

    Returns:
        Tuple of (keys_string, values_string) where:
        - keys_string: Comma-separated range boundaries
        - values_string: Comma-separated binary values (0 or 1) indicating ideal ranges
    """
    # Initialize list to store all ranges
    input_ranges = []

    # Split input into individual range expressions
    for r in input_string.split(", "):
        r = r.strip()

        # Handle inequality expressions
        if r.startswith("<"):
            # Less than: from negative infinity to the specified value
            value = float(r[1:])
            input_ranges.append(("negInf", value))
        elif r.startswith(">"):
            # Greater than: from the specified value to positive infinity
            value = float(r[1:])
            input_ranges.append((value, "posInf"))
        elif "-" in r:
            # Regular range
            start, end = map(float, r.split("-"))
            input_ranges.append((start, end))
        else:
            # Single value (treat as a point range)
            try:
                value = float(r)
                input_ranges.append((value, value))
            except ValueError:
                print(f"Warning: Could not parse range expression: {r}")

    # Extract all boundary values, sorting them numerically
    boundaries = []
    for start, end in input_ranges:
        if start != "negInf":
            boundaries.append(float(start))
        if end != "posInf":
            boundaries.append(float(end))

    boundaries = sorted(set(boundaries))  # Remove duplicates and sort

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
        # For each defined boundary pair in all_ranges
        for boundary_start, boundary_end in all_ranges:
            # Check if this boundary pair overlaps with our input range

            # Convert to float for comparison if not "negInf"/"posInf"
            bs = float(boundary_start) if boundary_start != "negInf" else float("-inf")
            be = float(boundary_end) if boundary_end != "posInf" else float("inf")
            rs = float(start) if start != "negInf" else float("-inf")
            re = float(end) if end != "posInf" else float("inf")

            # If ranges overlap, mark as ideal (1)
            # A range overlaps if:
            # 1. The boundary start is within the input range, or
            # 2. The boundary end is within the input range, or
            # 3. The input range completely contains the boundary range
            if (rs <= bs < re) or (rs < be <= re) or (bs <= rs and be >= re):
                range_dict[f"{boundary_start}-{boundary_end}"] = 1

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

    # For aspect, we need to handle the special case differently
    if variable == "aspect":
        # Flatten the list for North aspect if needed
        # Create a flattened list of all range values
        all_values = []
        for value in classDict.values():
            if isinstance(value, list):
                all_values.extend(value)  # Add all ranges from the list
            else:
                all_values.append(value)  # Add single range

        # Join all ranges with commas
        classes = ",".join(all_values)

        # Create binary labels - need to handle North direction specially
        ideal_classes = input_string.split(", ")
        labels = []

        for key in classDict.keys():
            if key in ideal_classes:
                # If this is North, add two "1" values (one for each range)
                if key == "North":
                    labels.extend(["1", "1"])
                else:
                    labels.append("1")
            else:
                # If this is North, add two "0" values (one for each range)
                if key == "North":
                    labels.extend(["0", "0"])
                else:
                    labels.append("0")

        labels = ",".join(labels)
    elif variable == "LULC":
        ideal_classes = input_string.split(", ")
        # Build reverse mapping (numeric code -> list of classes)
        code_to_classes = {}
        for cls, codes in lulcClassesIndiaSat.items():
            for c in codes:
                code_to_classes.setdefault(c, []).append(cls)

        # Assign labels
        class_list = []
        label_list = []

        for code, mapped_classes in code_to_classes.items():
            # Label = 1 if any mapped class is in ideal_classes
            label = 1 if any(cls in ideal_classes for cls in mapped_classes) else 0
            class_list.append(str(code))
            label_list.append(str(label))

        classes = ",".join(class_list)
        labels = ",".join(label_list)

        # print("Classes:", classes)
        # print("Labels:", labels)
    else:
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


def process_project_profile(form_data):
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
        final_weights[category] = round(decimal_value, 2)

    return final_variables, final_weights
