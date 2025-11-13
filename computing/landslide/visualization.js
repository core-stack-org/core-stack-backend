// Earth Engine Code Editor script for visualizing landslide susceptibility
// This script demonstrates how to visualize the pan-India landslide susceptibility
// and clip it to specific regions for inspection.

// ============================================================================
// Configuration
// ============================================================================

// Pan-India landslide susceptibility asset
// TODO: Replace with actual published asset path
var LANDSLIDE_ASSET = 'projects/ee-corestack/assets/india_landslide_susceptibility_100m';

// India boundary for context
var india = ee.FeatureCollection('FAO/GAUL/2015/level0')
  .filter(ee.Filter.eq('ADM0_NAME', 'India'));

// Example: Load a specific tehsil/district boundary
// Replace with your AOI
var aoi = india.geometry(); // Start with full India

// ============================================================================
// Load Landslide Susceptibility
// ============================================================================

var landslide;
try {
  landslide = ee.Image(LANDSLIDE_ASSET);
  print('Loaded landslide susceptibility from asset');
} catch (e) {
  print('Could not load asset. Generating demo from slope...');
  // Fallback: generate from SRTM slope
  var dem = ee.Image('USGS/SRTMGL1_003').select('elevation');
  var slope = ee.Terrain.slope(dem);
  
  // Classify slope into susceptibility classes
  landslide = slope.lt(15).multiply(1)
    .add(slope.gte(15).and(slope.lt(25)).multiply(2))
    .add(slope.gte(25).and(slope.lt(35)).multiply(3))
    .add(slope.gte(35).multiply(4))
    .rename('susceptibility');
}

// Clip to AOI
var landslideClipped = landslide.clip(aoi);

// ============================================================================
// Visualization Parameters
// ============================================================================

// Color scheme: green (low) -> yellow (moderate) -> orange (high) -> red (very high)
var visParams = {
  min: 1,
  max: 4,
  palette: ['00ff00', 'ffff00', 'ff9900', 'ff0000'],
  opacity: 0.7
};

// DEM for context
var dem = ee.Image('USGS/SRTMGL1_003').select('elevation');
var demVis = {
  min: 0,
  max: 4000,
  palette: ['blue', 'green', 'yellow', 'red', 'white']
};

// Slope for context
var slope = ee.Terrain.slope(dem);
var slopeVis = {
  min: 0,
  max: 60,
  palette: ['white', 'black']
};

// ============================================================================
// Add Layers to Map
// ============================================================================

Map.centerObject(aoi, 5);
Map.addLayer(dem, demVis, 'DEM', false);
Map.addLayer(slope, slopeVis, 'Slope', false);
Map.addLayer(landslideClipped, visParams, 'Landslide Susceptibility');
Map.addLayer(aoi, {color: '000000'}, 'AOI Boundary', false);

// ============================================================================
// Statistics and Analysis
// ============================================================================

// Compute area by susceptibility class
var pixelArea = ee.Image.pixelArea();

// Function to compute area for a class
var computeClassArea = function(classValue, className) {
  var mask = landslideClipped.eq(classValue);
  var area = pixelArea.updateMask(mask).reduceRegion({
    reducer: ee.Reducer.sum(),
    geometry: aoi,
    scale: 100,
    maxPixels: 1e13,
    bestEffort: true
  });
  
  var areaHa = ee.Number(area.get('area')).divide(10000);
  print(className + ' area (ha):', areaHa);
  return areaHa;
};

print('=== Landslide Susceptibility Statistics ===');
computeClassArea(1, 'Low');
computeClassArea(2, 'Moderate');
computeClassArea(3, 'High');
computeClassArea(4, 'Very High');

// ============================================================================
// Legend
// ============================================================================

// Create a legend panel
var legend = ui.Panel({
  style: {
    position: 'bottom-left',
    padding: '8px 15px'
  }
});

// Create legend title
var legendTitle = ui.Label({
  value: 'Landslide Susceptibility',
  style: {
    fontWeight: 'bold',
    fontSize: '16px',
    margin: '0 0 4px 0',
    padding: '0'
  }
});
legend.add(legendTitle);

// Create legend items
var makeRow = function(color, name) {
  var colorBox = ui.Label({
    style: {
      backgroundColor: '#' + color,
      padding: '8px',
      margin: '0 0 4px 0'
    }
  });
  
  var description = ui.Label({
    value: name,
    style: {margin: '0 0 4px 6px'}
  });
  
  return ui.Panel({
    widgets: [colorBox, description],
    layout: ui.Panel.Layout.Flow('horizontal')
  });
};

legend.add(makeRow('00ff00', 'Low'));
legend.add(makeRow('ffff00', 'Moderate'));
legend.add(makeRow('ff9900', 'High'));
legend.add(makeRow('ff0000', 'Very High'));

// Add legend to map
Map.add(legend);

// ============================================================================
// Export Functions
// ============================================================================

// Example: Export to asset for a specific region
var exportToAsset = function() {
  Export.image.toAsset({
    image: landslideClipped,
    description: 'landslide_susceptibility_export',
    assetId: 'users/YOUR_USERNAME/landslide_susceptibility',
    region: aoi,
    scale: 100,
    maxPixels: 1e13
  });
  print('Export task created. Check Tasks tab.');
};

// Uncomment to run export:
// exportToAsset();

// ============================================================================
// Vectorization Example
// ============================================================================

// Example: Vectorize to polygons
var vectorizeExample = function() {
  var vectors = landslideClipped.reduceToVectors({
    geometry: aoi,
    scale: 100,
    geometryType: 'polygon',
    eightConnected: true,
    labelProperty: 'susceptibility_class',
    maxPixels: 1e13
  });
  
  print('Vector features:', vectors.limit(10));
  Map.addLayer(vectors, {color: 'blue'}, 'Vectorized', false);
  
  return vectors;
};

// Uncomment to run vectorization (warning: can be slow for large areas):
// var vectors = vectorizeExample();

print('Visualization complete. Adjust AOI and re-run as needed.');
