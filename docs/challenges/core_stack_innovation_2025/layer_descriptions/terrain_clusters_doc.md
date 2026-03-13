# Terrain Clusters Layer

## Description

Terrain classification data. Classes in the terrain raster map are grouped into five broad categories: plains, broad slopes, steep slopes, valleys and ridges. Percentage area under each category is computed for each microwatershed. 

Clustering process was further applied to microwatersheds across diverse terrain blocks to come up with broad categories for the terrain type of a micro-watershed. 

**Identified categories:**
1. Broad sloppy and hilly
2. Mostly plains
3. Mostly hills and valleys
4. Broad plains and slopes

Precomputed cluster centroids are used to assign terrain clusters to new microwatersheds. Details of methodology can be found in the technical manual and the codebase on [GitHub](https://github.com/core-stack-org/core-stack-backend/tree/main/computing/terrain_descriptor).

---

## Properties

### Identification
| Property | Type | Description |
|----------|------|-------------|
| uid | object | Unique MWS identifier |

### Area Metrics
| Property | Type | Description |
|----------|------|-------------|
| area_in_ha | float64 | Area in hectares |

### Terrain Classification
| Property | Type | Description |
|----------|------|-------------|
| plain_area | float64 | Plain area percent |
| slopy_area | float64 | Slopy area percent |
| hill_slope | float64 | Hill slope area percent |
| valley_are | float64 | Valley area percent |
| ridge_area | float64 | Ridge area percent |
| terrainClu | int32 | Terrain cluster ID |

### Geometry
| Property | Type | Description |
|----------|------|-------------|
| geometry | geometry | Spatial geometry |

---