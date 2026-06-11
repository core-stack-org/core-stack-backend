// modal analysis for overall health change between 2017 and 2021 using latest modal rules where we want least missing data

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

var acz = 'Eastern Plateau & Hills Region';
// var acz = 'Lower Gangetic Plain Region';
// var acz = 'Western Himalayan Region';
// var acz = 'Eastern Himalayan Region';
// var acz = 'Upper Gangetic Plain Region';
// var acz = 'Middle Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau & Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
// var acz = 'East Coast Plains & Hills Region';

// var fc = ee.FeatureCollection('users/mtpictd/world_boundary');
// fc = fc.filter(ee.Filter.eq('Name', 'India'));
// var aoi = fc.geometry();

var aoi = india_boundary.filter(ee.Filter.eq('regionname', acz)).geometry();
var project_path = 'projects/corestack-trees/assets/tree_characteristics';

var year_in_1 = String(year_1);
var year_fi_1 = String(year_2);


var change_ccd = ee.ImageCollection(project_path+'/ccd_change_' + year_in_1 + '_' + year_fi_1)
  .filterBounds(aoi)
  .mean()
  .clip(aoi);

var change_chm = ee.ImageCollection(project_path+'/ch_change_' + year_in_1 + '_' + year_fi_1)
  .filterBounds(aoi)
  .mean()
  .clip(aoi);


var palette = ['red', 'orange', 'white', 'lightgreen', 'green', 'black'];
Map.addLayer(change_ccd, {min: -2, max: 3, palette: palette}, 'ccd change');
Map.addLayer(change_chm, {min: -2, max: 3, palette: palette}, 'ch change');


var overall_change = change_ccd.addBands(change_chm);

overall_change = overall_change.unmask(-9999);
overall_change = overall_change.expression(
  "((b('constant') == 3) or (b('constant_1') == 3)) ? (5)"+
  ": ((b('constant') == -2) or (b('constant') == 2)) ? (b('constant'))"+
  ": ((b('constant') == 1) and (b('constant_1') == 1)) ? (1)"+
  ": ((b('constant') == 1) and (b('constant_1') == -1)) ? (3)"+
  ": ((b('constant') == 1) and (b('constant_1') == 0)) ? (1)"+
  ": ((b('constant') == -1) and (b('constant_1') == 1)) ? (4)"+
  ": ((b('constant') == -1) and (b('constant_1') == -1)) ? (-1)"+
  ": ((b('constant') == -1) and (b('constant_1') == 0)) ? (-1)"+
  ": ((b('constant') == 0) and (b('constant_1') == 1)) ? (1)"+
  ": ((b('constant') == 0) and (b('constant_1') == -1)) ? (-1)"+
  ": ((b('constant') == 0) and (b('constant_1') == 0)) ? (0)"+
  ": -9999"
).clip(aoi);
overall_change = overall_change.updateMask(overall_change.neq(-9999));
// print(overall_change);
var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', 'DEE64C', 'DEE64C', '000000'];
Map.addLayer(overall_change, {min: -2, max: 5, palette: palette}, 'overall change');

Export.image.toAsset({
    image: overall_change,
    description: 'overall_change_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/overall_change_' + year_in_1 + '_' + year_fi_1 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });


// // Set the change list according to the following:
// // -2 -> Deforestation
// // -1 -> Degradation
// // 0 -> No Change
// // 1 -> Improvement
// // 2 -> Afforestation
// // 3 denotes partial improvement and degradation where ccd improves and ch degrades
// // 4 denotes partial improvement and degradation where ccd degrades and ch improves
// // 5 -> Missing Data

// // var change_list = [2, -2, 1, -1, 3, 4, 0, 5];
// var change_list = [-1, 3, 4];

// for (var c=0; c<change_list.length; c++) {

//   var change_num = change_list[c];

//   var img = overall_change.updateMask(overall_change.eq(change_num));

//   var proj = ee.Projection('EPSG:4326');
//   var grid = aoi.coveringGrid(proj, 100000);
//   var total_count = ee.Number(0);
//   var features = grid.getInfo().features;
//   for (var i = 0; i < features.length; i++) {
//     var feature = features[i];
//     var coord = feature.geometry.coordinates[0];
//     var poly = ee.Geometry.Polygon(coord);
//     poly = poly.intersection(aoi);
//     var count = img.reduceRegion({
//       reducer: ee.Reducer.count(),
//       geometry: poly,
//       scale: 25,
//       maxPixels: 1000000000
//     });
//     total_count = total_count.add(count.get('constant'));
//   }
//   print(total_count.multiply(625).divide(10000));

// }

// // Adding Legend on GEE map

// // set position of panel
// var legend = ui.Panel({
//   style: {
//     position: 'bottom-left',
//     padding: '8px 15px'
//   }
// });

// // Create legend title
// var legendTitle = ui.Label({
//   value: 'Legend',
//   style: {
//     fontWeight: 'bold',
//     fontSize: '18px',
//     margin: '0 0 4px 0',
//     padding: '0'
//     }
// });

// // Add the title to the panel
// legend.add(legendTitle);

// // Creates and styles 1 row of the legend.
// var makeRow = function(color, name) {

//       // Create the label that is actually the colored box.
//       var colorBox = ui.Label({
//         style: {
//           backgroundColor: '#' + color,
//           // Use padding to give the box height and width.
//           padding: '8px',
//           margin: '0 0 4px 0',
//           border: '1px solid black'
//         }
//       });

//       // Create the label filled with the description text.
//       var description = ui.Label({
//         value: name,
//         style: {margin: '0 0 4px 6px'}
//       });

//       // return the panel
//       return ui.Panel({
//         widgets: [colorBox, description],
//         layout: ui.Panel.Layout.Flow('horizontal')
//       });
// };

// //  Palette with the colors
// // var palette =['FFA500', '007500', '000000'];
// // var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];
// // var palette =['FFA500', 'DEE64C', '007500', '000000'];
// var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', 'DEE64C', '000000'];



// // name of the legend
// // var names = ['Low Density','High Density','Missing Data'];
// // var names = ['Deforestation','Degradation','No Change','Improvement','Afforestation','Missing Data'];
// // var names = ['Short Trees', 'Medium Height Trees', 'Tall Trees', 'Missing Data'];
// var names = ['Deforestation', 'Degradation', 'No Change', 'Improvement', 'Afforestation', 'Partially Degraded', 'Missing Data']

// // Add color and and names
// for (var i = 0; i < names.length; i++) {
//   legend.add(makeRow(palette[i], names[i]));
//   }

// // add legend to map (alternatively you can also print the legend to the console)
// // Map.add(legend);
// print(legend);