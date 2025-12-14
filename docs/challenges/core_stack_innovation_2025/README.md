
# CoRE Stack Starter Kit

This repository provides a **hands-on starter kit** to learn and experiment with the **CoRE Stack data structure**, designed especially for participants in the **CoRE Stack Innovation Challenge 2025** - [link](https://github.com/core-stack-org/core-stack-backend/blob/main/docs/challenges/core_stack_innovation_2025/core_stack_innovation_challenge_brief_problem_set.md).

The goal is to help researchers, developers, and innovators quickly understand:

- How the CoRE Stack represents **landscape entities**
- How to **fetch, process, and analyze** CoRE stack data from underlying geospatial layers using Python
- How to generate useful insights from the data

----------

## CoRE Stack Data Structure

The CoRE Stack organizes data into **nested landscape entities**, E.g.

`Watershed or Tehsil → Micro-Watersheds (MWS) → Surface Waterbodies` 

Each entity contains **spatial geometry** and **thematic attributes** derived from geospatial layers such as:
- Terrain classes
- Cropping intensity    
- Water balance
- Drought frequency
- Seasonal surface water availability
- Drainage / stream networks
- Land-use and Land-cover   
- Precipitation & Evapotranspiration
    
A detailed narrative of this structure is available here:  
🔗 [https://core-stack.org/the-core-stack-data-structure/](https://core-stack.org/the-core-stack-data-structure/)

The **Entity-Relationship (ER) diagram** can help visualize this conceptually:

![CoRE Stack ER Diagram](https://core-stack.org/wp-content/uploads/2025/11/watershed_erd-1.jpg)

----------

## Where this fits in CoRE Stack

The starter-kit demonstrates how to:

 1. Use the CoRE stack APIs to query available geospatial layers for a location  
 2. Download their GeoJSON / GeoTIFF data  
 3. Extract relevant properties  
 4. Populate structured Python ORM objects that reflect the CoRE stack data structure

API documentation:  [https://api-doc.core-stack.org/](https://api-doc.core-stack.org/), [create an API key beforehand](https://core-stack.org/use-apis/)

----------

## Code Components

### `core_stack_orm.py`

Defines Python ORM-style classes for tehsil, micro-watersheds, and waterbodies, and utility functions to populate the properties of these classes from underlying GeoJSON layers. Includes utilities to populate fields from GeoJSON layers.

----------

### `core-stack-layer-load.py`

End-to-end workflow:
- Select **State → District → Tehsil**  
- Fetch layer URLs using the public CoRE stack APIs  
- Download layer GeoJSONs  
- Build the ORM hierarchy  
- Flatten into a Pandas dataframe  
- Plot ready-to-use insights: Cropping intensity vs. drought frequency, Surface water vs. groundwater balance, Terrain influence on agriculture
    
Outputs sample CSVs and charts into the `data/` folder.

----------

### `data/`

Contains:
- Sample cleaned data  
- Example charts generated from a real location

----------

### `layer_descriptions/`

Lists attributes present in each geospatial layer. Useful for interpreting charts and building new analytics. 

----------

## Installation

You need Python ≥ 3.9. Install dependencies:

`pip install pandas geopandas matplotlib requests tqdm` 

----------

## How to Run

Example command:

`python core-stack-layer-load.py` 

The script can be changed to specify a (State, District, Tehsil) for which data is available with the CoRE stack. Then it will fetch all necessary layers and generate outputs  in `./data/`
    

----------

## What You Can Build

Check out the [CoRE stack challenge page](https://github.com/core-stack-org/core-stack-backend/blob/main/docs/challenges/core_stack_innovation_2025/core_stack_innovation_challenge_brief_problem_set.md) for ideas on what you can do, including:
-   Develop indicators for water security
-   Visualize risk hotspots
-   Rank watersheds for restoration planning
-   Design decision tools for farming communities
    
Perfect for rapid prototyping in the CoRE Stack Innovation Challenge.

----------

## Important note

You need not always download the layers and extract data, as shown in the starter-kit. The purpose here is to convey a detailed understanding of the underlying data flow process. You can directly use the CoRE stack APIs which do this internally for you and provide ready-to-use formatted data that can be easily converted to data frames for analysis.

----------

## Contributions & Support

Feedback and PRs are welcome!  
If you build something cool for the challenge — we’d love to showcase it.
