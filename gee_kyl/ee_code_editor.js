// Earth Engine Code Editor helper script for visualizing landslide susceptibility
// Use this snippet in the Earth Engine Code Editor to preview DEM, slope and a
// computed susceptibility raster (if available as an asset).

// Example: compute a quick slope-based susceptibility preview from SRTM
var dem = ee.Image('USGS/SRTMGL1_003').select('elevation');
var slope = ee.Terrain.slope(dem).rename('slope');

// Quick normalized susceptibility: normalized slope (0..1)
var slopeStats = slope.reduceRegion({reducer: ee.Reducer.percentile([2,98]), geometry: dem.geometry(), scale: 1000, maxPixels: 1e13});
var p2 = ee.Number(slopeStats.get('slope_p2'));
var p98 = ee.Number(slopeStats.get('slope_p98'));
var slopeNorm = slope.subtract(p2).divide(p98.subtract(p2)).clamp(0,1).rename('susceptibility');

// Classify into 3 classes for visualization
var classified = slopeNorm.lt(0.33).multiply(1)
  .add(slopeNorm.gte(0.33).and(slopeNorm.lt(0.66)).multiply(2))
  .add(slopeNorm.gte(0.66).multiply(3));

var visParamsSus = {min:0, max:1, palette: ['00ff00','ffff00','ff0000']};
var visParamsSlope = {min:0, max:60, palette: ['white','black']};

Map.setCenter(0,0,2);
Map.addLayer(dem, {min:0, max:4000}, 'DEM');
Map.addLayer(slope, visParamsSlope, 'Slope');
Map.addLayer(classified, {min:1, max:3, palette:['00ff00','ffff00','ff0000']}, 'Susceptibility (quick)');

// If you have exported an asset from the Python script, replace the asset ID below
// var susAsset = ee.Image('users/you/kyl_susceptibility_100m');
// Map.addLayer(susAsset, visParamsSus, 'Published Susceptibility');
