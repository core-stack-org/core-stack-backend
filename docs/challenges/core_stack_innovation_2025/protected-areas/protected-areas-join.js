var nChunks = 55;

var collections = [];

for (var i = 0; i < nChunks; i++) {
  if (i != 46) {  // fails for a few features in chunk 46
    collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_' + i)); // change to your project space
  }
}

// add the remaining features one by one
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_4')); // change to your project space
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_5')); // change to your project space
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_6')); // change to your project space
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_7')); // change to your project space
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_8')); // change to your project space
collections.push(ee.FeatureCollection('projects/ee-aaditeshwar/assets/protected_areas_data_chunk_46_9')); // change to your project space

var mergedData = ee.FeatureCollection(collections).flatten();

Export.table.toAsset({
  collection: mergedData,
  description: 'protected_areas_stats_merged',
  assetId: 'projects/ee-aaditeshwar/assets/protected_areas_data_final' // change to your project space
});

// now run protected-areas-display and create a GEE app
