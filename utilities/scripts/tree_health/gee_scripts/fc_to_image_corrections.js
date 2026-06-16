// TODO: Set total_files according to the number of result files for a particular ACZ
// in a particular year
var total_files_ccd = 1;
var total_files_ch = 1;

// TODO: Mention the year for which you want to run this script for. (from first_year+1 to last_year-1)
var year = '2023';

// TODO: Uncomment the acz for which you want to run this script for.
// var acz = 'Eastern Plateau &amp; Hills Region';
var acz = 'Middle Gangetic Plain Region';
// var acz = 'Lower Gangetic Plain Region';
// var acz = 'Western Himalayan Region';
// var acz = 'Eastern Himalayan Region';
// var acz = 'Upper Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau &amp; Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
// var acz = 'East Coast Plains & Hills Region';

var res_list_ccd = [];
for (var i = 0; i < total_files_ccd; i++) {
  res_list_ccd.push('_result_' + String(i) + '_');
}

var agroclimaticZoneAcronymDict = {
  'Eastern Plateau &amp; Hills Region': 'EPAHR',
  'Southern Plateau and Hills Region': 'SPAHR',
  'East Coast Plains & Hills Region': 'ECPHR',
  'Western Plateau and Hills Region': 'WPAHR',
  'Central Plateau &amp; Hills Region': 'CPAHR',
  'Lower Gangetic Plain Region': 'LGPR',
  'Middle Gangetic Plain Region': 'MGPR',
  'Upper Gangetic Plain Region': 'UGPR',
  'Trans Gangetic Plain Region': 'TGPR',
  'Eastern Himalayan Region': 'EHR',
  'Western Himalayan Region': 'WHR'
};

var acronym = agroclimaticZoneAcronymDict[acz];

var india_boundary = ee.FeatureCollection("projects/ext-datasets/assets/datasets/ACZs");

var acz_boundary = ee.FeatureCollection("projects/ext-datasets/assets/datasets/Agro_Climatic_Zones").filter(ee.Filter.eq("regionname", acz));
var india_district_boundary = ee.FeatureCollection("projects/ee-indiasat/assets/india_district_boundaries");
var aoi = india_district_boundary.filterBounds(acz_boundary.geometry()).union().geometry();
Map.addLayer(aoi)
// var aoi = india_boundary.filter(ee.Filter.eq('regionname', acz)).geometry();
var project_path = "projects/corestack1-dev-alpha/assets/tree_characteristics";
var outpath_path = "projects/corestack1-dev-alpha/assets/tree_characteristics";

// correction ccd
res_list_ccd.forEach(function(res) {
  var assetPath = project_path + '/corrections_ccd_' + year + res + acronym;
  var bandName = 'cc_' + year;

  // Try to get asset metadata (client-side)
  var assetExists = false;
  try {
    var assetInfo = ee.data.getAsset(assetPath);
    assetExists = assetInfo !== null;
  } catch (err) {
    assetExists = false;
  }

  // Create image to export
  var district_img;
  if (assetExists) {
    var fc = ee.FeatureCollection(assetPath);
    var image = fc.reduceToImage({
      properties: [bandName],
      reducer: ee.Reducer.first()
    }).rename(bandName);
    district_img = ee.Image(0).addBands(image).select([bandName]).reproject('EPSG:4326', null, 25).clip(aoi);
  }
  // else {
  //   print('Asset missing: '+ assetPath);
  //   district_img = ee.Image(0).rename(bandName).reproject('EPSG:4326', null, 25).clip(aoi);
  // }

  // Map.addLayer(district_img);

  Export.image.toAsset({
    image: district_img,
    description: 'fc_to_image_corrections_ccd_' +year + res + acronym,
    assetId: outpath_path + '/corrections_ccd_' + year + '/corrections_ccd_' + year + res + acronym,
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 1e10
  });
});

// correction ch
var ch_res_list = [];
for (var i = 0; i < total_files_ch; i++) {
  ch_res_list.push('_result_' + String(i) + '_');
}

for (var k = 0; k < ch_res_list.length; k++) {

  var res = ch_res_list[k];
  // var fc = ee.FeatureCollection('projects/ee-amanverma/assets/tree_health_updated/corrections_ch_' + year + res + acronym);
  var assetPath = project_path + '/corrections_ch_' + year + res + acronym;
  var bands = ['rh50_' + year, 'rh75_' + year, 'rh98_' + year, 'ch_' + year];

  // Try to get asset metadata (client-side)
  var assetExists = false;
  try {
    var assetInfo = ee.data.getAsset(assetPath);
    assetExists = assetInfo !== null;
  } catch (err) {
    assetExists = false;
  }



  var district_img = ee.Image();  // Start with an empty image

  if (assetExists) {
    var fc = ee.FeatureCollection(assetPath);
    for (var i=0; i<bands.length; i++) {

      var image = fc.reduceToImage({
        // List of properties to convert to bands
        properties: [bands[i]],

        // Reducer function (optional, default is 'first')
        reducer: ee.Reducer.first() // Chooses the value from the first feature for each pixel
      });

      image = image.rename([bands[i]]);
      // print(image);

      district_img = district_img.addBands(image);
    }

    district_img = district_img.reproject('EPSG:4326', null, 25).clip(aoi).select(bands);
  } else {
    print('Asset missing:' + assetPath);
    // for (var i=0; i<bands.length; i++) {
    //   district_img = ee.Image(0).rename(bands[i]).reproject('EPSG:4326', null, 25).clip(aoi);
    // }
    // var blankBands = bands.map(function(b) {
    //   return ee.Image(0).rename(b).reproject('EPSG:4326', null, 25).clip(aoi);
    // });
    // district_img = ee.ImageCollection.fromImages(blankBands).toBands().select(bands);

    var blankBands = bands.map(function(band) {
      return ee.Image.constant(0).rename(band);
    });

    // Merge into a single image with correct band names
    district_img = ee.ImageCollection.fromImages(blankBands)
      .toBands()
      // Remove auto-numbering like '0_', '1_'
      .rename(bands)
      .reproject('EPSG:4326', null, 25)
      .clip(aoi);
  }


  // // Final formatting
  // district_img = district_img
  //   .select(bands)
  //   .reproject('EPSG:4326', null, 25)
  //   .clip(aoi);

  // print('district_img for ' + res + " - " + district_img);

Map.addLayer(district_img);
  Export.image.toAsset({
    image: district_img,
    description: 'fc_to_image_corrections_ch_' + year + '_' + acronym,
    assetId: outpath_path + '/corrections_ch_' + year + '/corrections_ch_' + year + res + acronym,
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });
}


