var tree_cover = require('users/corestackdev/core-stack:tree_health/tree_cover');
// This script is for doing a modal analysis while analysing forest cover change over the years between any 2 given years.
// This is the updated modal computation, where we won't have missing data apart from the area where data is missing for all 3 years.
// It also gives the change quantification

// Set year_1 and year_2. These are the 2 years between which comparison has to be made
// Note that year_2 must be > year_1 and both must be of Integer type
var year_1 = 2017;
var year_2 = 2018;

// var india_district_boundary = ee.FeatureCollection("projects/ee-indiasat/assets/india_district_boundaries");
// var aoi = india_district_boundary.filter(ee.Filter.eq('Name', "Baloda Bazar")).geometry();

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
// var acz = 'Middle Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau & Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
var acz = 'East Coast Plains & Hills Region';

// Set the AOI
var aoi = india_boundary.filter(ee.Filter.eq('regionname', acz)).geometry();

// Set the change list according to the following:
// -2 -> Deforestation
// -1 -> Degradation
// 0 -> No Change
// 1 -> Improvement
// 2 -> Afforestation
// 3 -> Missing Data
var change_list = [-1, 1];

Map.addLayer(aoi, {'color': 'black'}, 'AOI');
Map.centerObject(aoi, 7);

var year_in_0 = String(year_1-1);
var year_in_1 = String(year_1);
var year_in_2 = String(year_1+1);

var year_fi_0 = String(year_2-1);
var year_fi_1 = String(year_2);
var year_fi_2 = String(year_2+1);

var project_path = 'projects/corestack-trees/assets/tree_characteristics';

var ccd_in_0 = ee.ImageCollection(project_path+'/ccd_' + year_in_0)
                .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });


ccd_in_0 = ee.Algorithms.If(
  ccd_in_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_in_0.mean().clip(aoi).select('cc')
);

ccd_in_0 = ee.Image(ccd_in_0);


// var tree_cover_in_0 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_0)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_0 = tree_cover.get_tree_cover(aoi, year_in_0);
tree_cover_in_0 = tree_cover_in_0.rename(['label_' + year_in_0.slice(-2)]);


var ccd_in_1 = ee.ImageCollection(project_path+'/ccd_' + year_in_1)
                .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });

ccd_in_1 = ee.Algorithms.If(
  ccd_in_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_in_1.mean().clip(aoi).select('cc')
);
ccd_in_1 = ee.Image(ccd_in_1);

// var tree_cover_in_1 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_1)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_1 = tree_cover.get_tree_cover(aoi, year_in_1);
tree_cover_in_1 = tree_cover_in_1.rename(['label_' + year_in_1.slice(-2)]);

var ccd_in_2 = ee.ImageCollection(project_path+'/ccd_' + year_in_2)
                .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });

ccd_in_2 = ee.Algorithms.If(
  ccd_in_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_in_2.mean().clip(aoi).select('cc')
);
ccd_in_2 = ee.Image(ccd_in_2);

// var tree_cover_in_2 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_2)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_2 = tree_cover.get_tree_cover(aoi, year_in_2);
tree_cover_in_2 = tree_cover_in_2.rename(['label_' + year_in_2.slice(-2)]);

var ccd_fi_0 = ee.ImageCollection(project_path+'/ccd_' + year_fi_0)
                .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });

ccd_fi_0 = ee.Algorithms.If(
  ccd_fi_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_fi_0.mean().clip(aoi).select('cc')
);
ccd_fi_0 = ee.Image(ccd_fi_0);

// var tree_cover_fi_0 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_0)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_0 = tree_cover.get_tree_cover(aoi, year_fi_0);
tree_cover_fi_0 = tree_cover_fi_0.rename(['label_' + year_fi_0.slice(-2)]);

var ccd_fi_1 = ee.ImageCollection(project_path+'/ccd_' + year_fi_1)
                .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });

ccd_fi_1 = ee.Algorithms.If(
  ccd_fi_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_fi_1.mean().clip(aoi).select('cc')
);
ccd_fi_1 = ee.Image(ccd_fi_1);

// var tree_cover_fi_1 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_1)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_1 = tree_cover.get_tree_cover(aoi, year_fi_1);
tree_cover_fi_1 = tree_cover_fi_1.rename(['label_' + year_fi_1.slice(-2)]);

var ccd_fi_2 = ee.ImageCollection(project_path+'/ccd_' + year_fi_2)
              .filterBounds(aoi)
                .map(function(img) {
                  return img.select('cc').toInt32();
                });

ccd_fi_2 = ee.Algorithms.If(
  ccd_fi_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc')
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ccd_fi_2.mean().clip(aoi).select('cc')
);
ccd_fi_2 = ee.Image(ccd_fi_2);

// var tree_cover_fi_2 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_2)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_2 = tree_cover.get_tree_cover(aoi, year_fi_2);
tree_cover_fi_2 = tree_cover_fi_2.rename(['label_' + year_fi_2.slice(-2)]);

var correction_in_0 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_in_0)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_in_0).toInt32();
                      });

correction_in_0 = ee.Algorithms.If(
  correction_in_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_in_0)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_0.mode().clip(aoi).select('cc_' + year_in_0)
);
correction_in_0 = ee.Image(correction_in_0);

var correction_in_1 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_in_1)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_in_1).toInt32();
                      });

correction_in_1 = ee.Algorithms.If(
  correction_in_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_in_1)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_1.mode().clip(aoi).select('cc_' + year_in_1)
);
correction_in_1 = ee.Image(correction_in_1);

var correction_in_2 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_in_2)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_in_2).toInt32();
                      });

correction_in_2 = ee.Algorithms.If(
  correction_in_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_in_2)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_2.mode().clip(aoi).select('cc_' + year_in_2)
);
correction_in_2 = ee.Image(correction_in_2);

var correction_fi_0 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_fi_0)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_fi_0).toInt32();
                      });

correction_fi_0 = ee.Algorithms.If(
  correction_fi_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_fi_0)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_0.mode().clip(aoi).select('cc_' + year_fi_0)
);
correction_fi_0 = ee.Image(correction_fi_0);

var correction_fi_1 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_fi_1)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_fi_1).toInt32();
                      });

correction_fi_1 = ee.Algorithms.If(
  correction_fi_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_fi_1)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_1.mode().clip(aoi).select('cc_' + year_fi_1)
);
correction_fi_1 = ee.Image(correction_fi_1);

var correction_fi_2 = ee.ImageCollection(project_path+'/corrections_ccd_' + year_fi_2)
                      .filterBounds(aoi)
                      .map(function(img) {
                        return img.select('cc_' + year_fi_2).toInt32();
                      });

correction_fi_2 = ee.Algorithms.If(
  correction_fi_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant(-9999)
    .rename('cc_' + year_fi_2)
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_2.mode().clip(aoi).select('cc_' + year_fi_2)
);
correction_fi_2 = ee.Image(correction_fi_2);


var palette2 =['FFA500', '007500', '000000'];
Map.addLayer(correction_fi_2, {bands:['cc_'+year_fi_2], min: 0, max: 2, palette: palette2}, 'Empty');

correction_in_0 = correction_in_0.rename(['cc_in_0']);
correction_in_1 = correction_in_1.rename(['cc_in_1']);
correction_in_2 = correction_in_2.rename(['cc_in_2']);
correction_fi_0 = correction_fi_0.rename(['cc_fi_0']);
correction_fi_1 = correction_fi_1.rename(['cc_fi_1']);
correction_fi_2 = correction_fi_2.rename(['cc_fi_2']);




ccd_in_0 = ccd_in_0.addBands(correction_in_0);
ccd_in_0 = ccd_in_0.unmask(-9999);
ccd_in_0 = ccd_in_0.expression(
    "((b('cc_in_0')!=b('cc')) and (b('cc_in_0')!=-9999)) ? (b('cc_in_0'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_in_0 = ccd_in_0.updateMask(ccd_in_0.neq(-9999));
ccd_in_0 = ccd_in_0.rename(['cc_in_0']);


ccd_in_1 = ccd_in_1.addBands(correction_in_1);
ccd_in_1 = ccd_in_1.unmask(-9999);
ccd_in_1 = ccd_in_1.expression(
    "((b('cc_in_1')!=b('cc')) and (b('cc_in_1')!=-9999)) ? (b('cc_in_1'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_in_1 = ccd_in_1.updateMask(ccd_in_1.neq(-9999));
ccd_in_1 = ccd_in_1.rename(['cc_in_1']);


ccd_in_2 = ccd_in_2.addBands(correction_in_2);
ccd_in_2 = ccd_in_2.unmask(-9999);
ccd_in_2 = ccd_in_2.expression(
    "((b('cc_in_2')!=b('cc')) and (b('cc_in_2')!=-9999)) ? (b('cc_in_2'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_in_2 = ccd_in_2.updateMask(ccd_in_2.neq(-9999));
ccd_in_2 = ccd_in_2.rename(['cc_in_2']);


ccd_fi_0 = ccd_fi_0.addBands(correction_fi_0);
ccd_fi_0 = ccd_fi_0.unmask(-9999);
ccd_fi_0 = ccd_fi_0.expression(
    "((b('cc_fi_0')!=b('cc')) and (b('cc_fi_0')!=-9999)) ? (b('cc_fi_0'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_fi_0 = ccd_fi_0.updateMask(ccd_fi_0.neq(-9999));
ccd_fi_0 = ccd_fi_0.rename(['cc_fi_0']);


ccd_fi_1 = ccd_fi_1.addBands(correction_fi_1);
ccd_fi_1 = ccd_fi_1.unmask(-9999);
ccd_fi_1 = ccd_fi_1.expression(
    "((b('cc_fi_1')!=b('cc')) and (b('cc_fi_1')!=-9999)) ? (b('cc_fi_1'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_fi_1 = ccd_fi_1.updateMask(ccd_fi_1.neq(-9999));
ccd_fi_1 = ccd_fi_1.rename(['cc_fi_1']);


ccd_fi_2 = ccd_fi_2.addBands(correction_fi_2);
ccd_fi_2 = ccd_fi_2.unmask(-9999);
ccd_fi_2 = ccd_fi_2.expression(
    "((b('cc_fi_2')!=b('cc')) and (b('cc_fi_2')!=-9999)) ? (b('cc_fi_2'))"+
    ":b('cc')"
  ).clip(aoi);
ccd_fi_2 = ccd_fi_2.updateMask(ccd_fi_2.neq(-9999));
ccd_fi_2 = ccd_fi_2.rename(['cc_fi_2']);


tree_cover_in_0 = tree_cover_in_0.rename(['label_in_0']);
tree_cover_in_1 = tree_cover_in_1.rename(['label_in_1']);
tree_cover_in_2 = tree_cover_in_2.rename(['label_in_2']);
tree_cover_fi_0 = tree_cover_fi_0.rename(['label_fi_0']);
tree_cover_fi_1 = tree_cover_fi_1.rename(['label_fi_1']);
tree_cover_fi_2 = tree_cover_fi_2.rename(['label_fi_2']);

var tree_cover_initial = tree_cover_in_0.addBands(tree_cover_in_1).addBands(tree_cover_in_2);
tree_cover_initial = tree_cover_initial.unmask(-9999);
tree_cover_initial = tree_cover_initial.expression(
    "((b('label_in_1')!=-9999)) ? (b('label_in_1'))"+
    ":((b('label_in_0')!=-9999) and (b('label_in_1')==-9999) and (b('label_in_2')!=-9999)) ? (b('label_in_0'))"+
    ":(-9999)"
  ).clip(aoi);
tree_cover_initial = tree_cover_initial.rename(['label']);
tree_cover_initial = tree_cover_initial.updateMask(tree_cover_initial.neq(-9999));
// print(tree_cover_initial);


var tree_cover_final = tree_cover_fi_0.addBands(tree_cover_fi_1).addBands(tree_cover_fi_2);
tree_cover_final = tree_cover_final.unmask(-9999);
tree_cover_final = tree_cover_final.expression(
    "((b('label_fi_1')!=-9999)) ? (b('label_fi_1'))"+
    ":((b('label_fi_0')!=-9999) and (b('label_fi_1')==-9999) and (b('label_fi_2')!=-9999)) ? (b('label_fi_0'))"+
    ":(-9999)"
  ).clip(aoi);
tree_cover_final = tree_cover_final.rename(['label']);
tree_cover_final = tree_cover_final.updateMask(tree_cover_final.neq(-9999));

Map.addLayer(tree_cover_initial, {palette: ['white']}, 'tree cover initial');
Map.addLayer(tree_cover_final, {palette: ['white']}, 'tree cover final');

// 2 denotes missing data in modal images

var initial_image = ccd_in_0.addBands(ccd_in_1).addBands(ccd_in_2);
initial_image = initial_image.unmask(-9999);
// 0 denotes Low; 1 denotes High; 2 denotes missing data
initial_image = initial_image.expression(
    "((b('cc_in_0')==0) and (b('cc_in_1')==0) and (b('cc_in_2')==0)) ? (0)"+
    ":((b('cc_in_0')==0) and (b('cc_in_1')==0) and (b('cc_in_2')==1)) ? (0)"+
    ":((b('cc_in_0')==0) and (b('cc_in_1')==1) and (b('cc_in_2')==0)) ? (0)"+
    ":((b('cc_in_0')==0) and (b('cc_in_1')==1) and (b('cc_in_2')==1)) ? (1)"+
    ":((b('cc_in_0')==1) and (b('cc_in_1')==0) and (b('cc_in_2')==0)) ? (0)"+
    ":((b('cc_in_0')==1) and (b('cc_in_1')==0) and (b('cc_in_2')==1)) ? (1)"+
    ":((b('cc_in_0')==1) and (b('cc_in_1')==1) and (b('cc_in_2')==0)) ? (1)"+
    ":((b('cc_in_0')==1) and (b('cc_in_1')==1) and (b('cc_in_2')==1)) ? (1)"+
    ":((b('cc_in_0')==-9999 or b('cc_in_2')==-9999) and (b('cc_in_1')!=-9999)) ? (b('cc_in_1'))"+
    ":((b('cc_in_0')!=-9999 and b('cc_in_1')==-9999 and b('cc_in_2')!=-9999) and (b('cc_in_0')==b('cc_in_2'))) ? (b('cc_in_0'))"+
    ":((b('cc_in_0')!=-9999 and b('cc_in_1')==-9999 and b('cc_in_2')==-9999)) ? (b('cc_in_0'))"+
    ":((b('cc_in_1')==-9999 and b('cc_in_2')!=-9999) and (b('cc_in_0')!=b('cc_in_2'))) ? (b('cc_in_2'))"+
    ":(-9999)"
  ).clip(aoi);
initial_image = initial_image.rename(['cc']);
initial_image = initial_image.updateMask(initial_image.neq(-9999));

initial_image = initial_image.addBands(tree_cover_initial);
initial_image = initial_image.unmask(-9999);
// 2 denotes missing data
initial_image = initial_image.expression(
  "((b('cc') == -9999) and (b('label') != -9999)) ? (2)"+
  ":b('cc')"
  ).clip(aoi);
initial_image = initial_image.rename(['cc']);
initial_image = initial_image.updateMask(initial_image.neq(-9999));
print("initial_image", initial_image)

var final_image = ccd_fi_0.addBands(ccd_fi_1).addBands(ccd_fi_2);
final_image = final_image.unmask(-9999);
final_image = final_image.expression(
    "((b('cc_fi_0')==0) and (b('cc_fi_1')==0) and (b('cc_fi_2')==0)) ? (0)"+
    ":((b('cc_fi_0')==0) and (b('cc_fi_1')==0) and (b('cc_fi_2')==1)) ? (0)"+
    ":((b('cc_fi_0')==0) and (b('cc_fi_1')==1) and (b('cc_fi_2')==0)) ? (0)"+
    ":((b('cc_fi_0')==0) and (b('cc_fi_1')==1) and (b('cc_fi_2')==1)) ? (1)"+
    ":((b('cc_fi_0')==1) and (b('cc_fi_1')==0) and (b('cc_fi_2')==0)) ? (0)"+
    ":((b('cc_fi_0')==1) and (b('cc_fi_1')==0) and (b('cc_fi_2')==1)) ? (1)"+
    ":((b('cc_fi_0')==1) and (b('cc_fi_1')==1) and (b('cc_fi_2')==0)) ? (1)"+
    ":((b('cc_fi_0')==1) and (b('cc_fi_1')==1) and (b('cc_fi_2')==1)) ? (1)"+
    ":((b('cc_fi_0')==-9999 or b('cc_fi_2')==-9999) and (b('cc_fi_1')!=-9999)) ? (b('cc_fi_1'))"+
    ":((b('cc_fi_0')!=-9999 and b('cc_fi_1')==-9999 and b('cc_fi_2')!=-9999) and (b('cc_fi_0')==b('cc_fi_2'))) ? (b('cc_fi_0'))"+
    ":((b('cc_fi_0')!=-9999 and b('cc_fi_1')==-9999 and b('cc_fi_2')==-9999)) ? (b('cc_fi_0'))"+
    ":((b('cc_fi_1')==-9999 and b('cc_fi_2')!=-9999) and (b('cc_fi_0')!=b('cc_fi_2'))) ? (b('cc_fi_2'))"+
    ":(-9999)"
  ).clip(aoi);
final_image = final_image.rename(['cc']);
final_image = final_image.updateMask(final_image.neq(-9999));

final_image = final_image.addBands(tree_cover_final);
final_image = final_image.unmask(-9999);
// 2 denotes missing data
final_image = final_image.expression(
  "((b('cc') == -9999) and (b('label') != -9999)) ? (2)"+
  ":b('cc')"
  ).clip(aoi);
final_image = final_image.rename(['cc']);
final_image = final_image.updateMask(final_image.neq(-9999));

// var palette = ['orange', 'green', 'black'];
var palette =['FFA500', '007500', '000000'];
Map.addLayer(initial_image, {bands:['cc'], min: 0, max: 2, palette: palette}, 'initial modal image');
Map.addLayer(final_image, {bands:['cc'], min: 0, max: 2, palette: palette}, 'final modal image');

// var change = initial_image.addBands(final_image);
// change = change.unmask(-9999);

// print(change);

// // 3 denotes missing data
// change = change.expression(
//         "((b('cc')==0) and (b('cc_1')==0)) ? (0)"+
//         ":((b('cc')==1) and (b('cc_1')==1)) ? (0)"+
//         ":((b('cc')==0) and (b('cc_1')==1)) ? (1)"+
//         ":((b('cc')==1) and (b('cc_1')==0)) ? (-1)"+
//         ":((b('cc')==-9999) and (b('cc_1')!=-9999)) ? (2)"+
//         ":((b('cc')!=-9999) and (b('cc_1')==-9999)) ? (-2)"+
//         ":((b('cc')==2) or (b('cc_1')==2)) ? (3)"+
//         ":-9999"
//       )
//       .clip(aoi);
// change = change.updateMask(change.neq(-9999));
// // var palette = ['red', 'orange', 'white', 'lightgreen', 'darkgreen', 'black'];
// var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];
// Map.addLayer(change, {min: -2, max: 3, palette: palette}, 'change layer');

// Export modal initial image
Export.image.toAsset({
    image: initial_image,
    description: 'ccd_' + year_in_1 + '_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/modal_ccd_' + year_in_1 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });

// Export modal final image
Export.image.toAsset({
    image: final_image,
    description: 'ccd_' + year_fi_1 + '_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/modal_ccd_' + year_fi_1 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });

// // Export CCD change image
// Export.image.toAsset({
//     image: change,
//     description: 'ccd_change_' + agroclimaticZoneAcronymDict[acz],
//     assetId: project_path+'/ccd_change_' + year_in_1 + '_' + year_fi_1 + '/' + agroclimaticZoneAcronymDict[acz],
//     region: aoi,
//     scale: 25,
//     crs: 'EPSG:4326',
//     maxPixels: 10000000000
//   });

//===========================================
// var change_ccd = change;

// for (var c=0; c<change_list.length; c++) {

//   var change_num = change_list[c];
//   var img = change.updateMask(change.eq(change_num));

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
// var palette =['FFA500', '007500', '000000'];
// // var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];



// // name of the legend
// var names = ['Low Density','High Density','Missing Data'];
// // var names = ['Deforestation','Degradation','No Change','Improvement','Afforestation','Missing Data'];

// // Add color and and names
// for (var i = 0; i < names.length; i++) {
//   legend.add(makeRow(palette[i], names[i]));
//   }

// // add legend to map (alternatively you can also print the legend to the console)
// Map.add(legend);