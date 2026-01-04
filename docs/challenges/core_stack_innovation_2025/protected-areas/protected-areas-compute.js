var protected_areas = ee.FeatureCollection("projects/ee-aaditeshwar/assets/protected-areas"); // public asset
var tree_cover_change = ee.ImageCollection("projects/corestack-trees/assets/tree_characteristics/overall_change_2017_2022"); // public asset
var tcc = tree_cover_change.mosaic();

var lulc_2018 = ee.Image("projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_2018_2019"); // public asset
var lulc_2022 = ee.Image("projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_2022_2023"); // public asset

// see later for class values legend
var classValues = [-2, -1, 0, 1, 2, 3, 4, 5];

// only show for tree cover areas
var tree_mask_2018 = lulc_2018.eq(6);
var tree_mask_2022 = lulc_2022.eq(6);
var tree_mask = tree_mask_2018.or(tree_mask_2022);

var tcc_masked = tcc.updateMask(tree_mask);

var pixelAreaHa = ee.Image.pixelArea().divide(10000); // area in hectares
var classAreaImages = ee.ImageCollection(
  classValues.map(function(v) {
    return pixelAreaHa
      .updateMask(tcc_masked.eq(v))
      .rename('class_' + v);
  })
);

var classAreaImage = classAreaImages.toBands();

var processChunk = function(chunkIndex) {
  var CHUNK_SIZE = 10; // compute in chunks of 10 features at a time; can safely scale to 20 at a time though
  var startIndex = chunkIndex * CHUNK_SIZE;

  var protected_areas_chunk = ee.FeatureCollection(
    protected_areas.toList(CHUNK_SIZE, startIndex)
  );

  // filter for incomplete geometries
  var protected_areas_valid = protected_areas_chunk.filter(
    ee.Filter.notNull(['system:index'])
  ).map(function(f) {
    return f.set('valid_geom', 1);
  });
  
  // Reduce regions to get area statistics
  var protected_areas_stats = classAreaImage.reduceRegions({
    collection: protected_areas_valid,
    reducer: ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326',
    maxPixelsPerRegion: 1e13
  });

  // add columns with the right column names
  var protected_areas_data = protected_areas_stats.map(function(f) {
    return f.set({
      'area_tree_loss_ha': f.get('0_class_-2'),
      'area_degradation_ha': f.get('1_class_-1'),
      'area_no_change_ha': f.get('2_class_0'),
      'area_improvement_ha': f.get('3_class_1'),
      'area_tree_gain_ha': f.get('4_class_2'),
      'area_partially_degraded_ha': f.get('5_class_3'),
      'area_partially_degraded2_ha': f.get('6_class_4'),
      'area_missing_data_ha': f.get('7_class_5')
    });
  });

  // remove the old columns  
  var colsToRemove = [
    '0_class_-2',
    '1_class_-1',
    '2_class_0',
    '3_class_1',
    '4_class_2',
    '5_class_3',
    '6_class_4',
    '7_class_5'
  ];
  
  var protected_areas_cleaned = protected_areas_data.map(function(clean) {
    return clean.select(
      clean.propertyNames().removeAll(colsToRemove)
    );
  });
  
  return protected_areas_cleaned;
};

for (var i = 0; i < 55; i++) {
  var chunk = processChunk(i);
  
  Export.table.toAsset({
    collection: chunk,
    description: 'protected_areas_stats_chunk_' + i,
    assetId: 'projects/ee-aaditeshwar/assets/protected_areas_data_chunk_' + i // change to your projects space to create the assets
  });
}

// now run protected-areas-join to join the chunked assets
