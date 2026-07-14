// This script is for doing a modal analysis while analysing forest cover change over the years between any 2 given years.
// It also gives the change quantification

// Set year_1 and year_2. These are the 2 years between which comparison has to be made
// Note that year_2 must be > year_1 and both must be of Integer type
var year_1 = 2017;
var year_2 = 2023;

var india_boundary = ee.FeatureCollection("projects/ext-datasets/assets/datasets/ACZs");

var agroclimaticZoneAcronymDict = {
  'Eastern Plateau & Hills Region': 'EPAHR',
  'Southern Plateau and Hills Region': 'SPAHR',
  'East Coast Plains & Hills Region': 'ECPHR',
  'Western Plateau and Hills Region': 'WPAHR',
  'Central Plateau & Hills Region': 'CPAHR',
  'Lower Gangetic Plain Region': 'LGPR',
  'Middle Gangetic Plain Region': 'MGPR',
  'Upper Gangetic Plain Region': 'UGPR',
  'Trans Gangetic Plain Region': 'TGPR',
  'Eastern Himalayan Region': 'EHR',
  'Western Himalayan Region': 'WHR'
};

// var acz = 'Eastern Plateau & Hills Region';
// var acz = 'Lower Gangetic Plain Region';
// var acz = 'Western Himalayan Region';
// var acz = 'Eastern Himalayan Region';
// var acz = 'Upper Gangetic Plain Region';
var acz = 'Middle Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau & Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
// var acz = 'East Coast Plains & Hills Region';

// Set the AOI
var aoi = india_boundary.filter(ee.Filter.eq('regionname', acz)).geometry();

var project_path = 'projects/corestack-trees/assets/tree_characteristics';

var initial_image = ee.Image(project_path+'/modal_ccd_' + year_1 + '/' + agroclimaticZoneAcronymDict[acz]);
var final_image = ee.Image(project_path+'/modal_ccd_' + year_2 + '/' + agroclimaticZoneAcronymDict[acz]);

var change = initial_image.addBands(final_image);
change = change.unmask(-9999);

print(change);

// 3 denotes missing data
change = change.expression(
        "((b('cc')==0) and (b('cc_1')==0)) ? (0)"+
        ":((b('cc')==1) and (b('cc_1')==1)) ? (0)"+
        ":((b('cc')==0) and (b('cc_1')==1)) ? (1)"+
        ":((b('cc')==1) and (b('cc_1')==0)) ? (-1)"+
        ":((b('cc')==-9999) and (b('cc_1')!=-9999)) ? (2)"+
        ":((b('cc')!=-9999) and (b('cc_1')==-9999)) ? (-2)"+
        ":((b('cc')==2) or (b('cc_1')==2)) ? (3)"+
        ":-9999"
      )
      .clip(aoi);
change = change.updateMask(change.neq(-9999));
// var palette = ['red', 'orange', 'white', 'lightgreen', 'darkgreen', 'black'];
var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];
Map.addLayer(change, {min: -2, max: 3, palette: palette}, 'change layer');

// Export CCD change image
Export.image.toAsset({
    image: change,
    description: 'ccd_change_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/ccd_change_' + year_1 + '_' + year_2 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });