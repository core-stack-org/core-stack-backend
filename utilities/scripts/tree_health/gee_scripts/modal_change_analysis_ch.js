var tree_cover = require('users/corestackdev/core-stack:tree_health/tree_cover');
// This script is for doing a modal analysis while analysing forest cover change over the years between any 2 given years.
// This is the updated modal computation, where we won't have missing data apart from the area where data is missing for all 3 years.
// It also gives the change quantification

// Set year_1 and year_2. These are the 2 years between which comparison has to be made
// Note that year_2 must be > year_1 and both must be of Integer type
var year_1 = 2023;
var year_2 = 2022;

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
var acz = 'Middle Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau & Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
// var acz = 'East Coast Plains & Hills Region';

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

var project_path = 'projects/corestack1-dev-alpha/assets/tree_characteristics';

// var tree_cover_in_0 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_0)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_0 = tree_cover.get_tree_cover(aoi, year_in_0);
tree_cover_in_0 = tree_cover_in_0.rename(['label_' + year_in_0.slice(-2)]);


// var tree_cover_in_1 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_1)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_1 = tree_cover.get_tree_cover(aoi, year_in_1);
tree_cover_in_1 = tree_cover_in_1.rename(['label_' + year_in_1.slice(-2)]);


// var tree_cover_in_2 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_in_2)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_in_2 = tree_cover.get_tree_cover(aoi, year_in_2);
tree_cover_in_2 = tree_cover_in_2.rename(['label_' + year_in_2.slice(-2)]);


// var tree_cover_fi_0 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_0)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_0 = tree_cover.get_tree_cover(aoi, year_fi_0);
tree_cover_fi_0 = tree_cover_fi_0.rename(['label_' + year_fi_0.slice(-2)]);


// var tree_cover_fi_1 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_1)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_1 = tree_cover.get_tree_cover(aoi, year_fi_1);
tree_cover_fi_1 = tree_cover_fi_1.rename(['label_' + year_fi_1.slice(-2)]);


// var tree_cover_fi_2 = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/tree_cover_' + year_fi_2)
//                       .filterBounds(aoi)
//                       .mode()
//                       .clip(aoi);
var tree_cover_fi_2 = tree_cover.get_tree_cover(aoi, year_fi_2);
tree_cover_fi_2 = tree_cover_fi_2.rename(['label_' + year_fi_2.slice(-2)]);

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


// CH visualizations

// var ch_in_0 = ee.ImageCollection(project_path+'/ch_' + year_in_0)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_in_0 = ee.ImageCollection(project_path+'/ch_' + year_in_0).filterBounds(aoi);

ch_in_0 = ee.Algorithms.If(
  ch_in_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_in_0.mean().clip(aoi)
);

ch_in_0 = ee.Image(ch_in_0);

// var ch_in_1 = ee.ImageCollection(project_path+'/ch_' + year_in_1)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_in_1 = ee.ImageCollection(project_path+'/ch_' + year_in_1).filterBounds(aoi);

ch_in_1 = ee.Algorithms.If(
  ch_in_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_in_1.mean().clip(aoi)
);

ch_in_1 = ee.Image(ch_in_1);

// var ch_in_2 = ee.ImageCollection(project_path+'/ch_' + year_in_2)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_in_2 = ee.ImageCollection(project_path+'/ch_' + year_in_2).filterBounds(aoi);

ch_in_2 = ee.Algorithms.If(
  ch_in_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_in_2.mean().clip(aoi)
);

ch_in_2 = ee.Image(ch_in_2);

// var ch_fi_0 = ee.ImageCollection(project_path+'/ch_' + year_fi_0)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_fi_0 = ee.ImageCollection(project_path+'/ch_' + year_fi_0).filterBounds(aoi);

ch_fi_0 = ee.Algorithms.If(
  ch_fi_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_fi_0.mean().clip(aoi)
);

ch_fi_0 = ee.Image(ch_fi_0);

// var ch_fi_1 = ee.ImageCollection(project_path+'/ch_' + year_fi_1)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_fi_1 = ee.ImageCollection(project_path+'/ch_' + year_fi_1).filterBounds(aoi);

ch_fi_1 = ee.Algorithms.If(
  ch_fi_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_fi_1.mean().clip(aoi)
);

ch_fi_1 = ee.Image(ch_fi_1);

// var ch_fi_2 = ee.ImageCollection(project_path+'/ch_' + year_fi_2)
// .filterBounds(aoi)
// .mean()
// .clip(aoi);
var ch_fi_2 = ee.ImageCollection(project_path+'/ch_' + year_fi_2).filterBounds(aoi);

ch_fi_2 = ee.Algorithms.If(
  ch_fi_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_class','rh50_class','rh75_class','rh98_class'])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  ch_fi_2.mean().clip(aoi)
);

ch_fi_2 = ee.Image(ch_fi_2);

// var correction_img = ee.ImageCollection('projects/ee-mtpictd/assets/harsh/corrections_ch')
//                   .filterBounds(aoi)
//                   .mode()
//                   .clip(aoi);

// var correction_in_0 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_0)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_in_0, 'rh98_'+year_in_0, 'rh75_'+year_in_0, 'rh50_'+year_in_0]);

var correction_in_0 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_0).filterBounds(aoi);

correction_in_0 = ee.Algorithms.If(
  correction_in_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_in_0, 'rh98_'+year_in_0, 'rh75_'+year_in_0, 'rh50_'+year_in_0])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_0.mode().clip(aoi).select(['ch_'+year_in_0, 'rh98_'+year_in_0, 'rh75_'+year_in_0, 'rh50_'+year_in_0])
);
correction_in_0 = ee.Image(correction_in_0);

// var correction_in_1 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_1)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_in_1, 'rh98_'+year_in_1, 'rh75_'+year_in_1, 'rh50_'+year_in_1]);
var correction_in_1 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_1).filterBounds(aoi);

correction_in_1 = ee.Algorithms.If(
  correction_in_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_in_1, 'rh98_'+year_in_1, 'rh75_'+year_in_1, 'rh50_'+year_in_1])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_1.mode().clip(aoi).select(['ch_'+year_in_1, 'rh98_'+year_in_1, 'rh75_'+year_in_1, 'rh50_'+year_in_1])
);
correction_in_1 = ee.Image(correction_in_1);

// var correction_in_2 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_2)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_in_2, 'rh98_'+year_in_2, 'rh75_'+year_in_2, 'rh50_'+year_in_2]);

var correction_in_2 = ee.ImageCollection(project_path+'/corrections_ch_' + year_in_2).filterBounds(aoi);

correction_in_2 = ee.Algorithms.If(
  correction_in_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_in_2, 'rh98_'+year_in_2, 'rh75_'+year_in_2, 'rh50_'+year_in_2])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_in_2.mode().clip(aoi).select(['ch_'+year_in_2, 'rh98_'+year_in_2, 'rh75_'+year_in_2, 'rh50_'+year_in_2])
);
correction_in_2 = ee.Image(correction_in_2);

// var correction_fi_0 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_0)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_fi_0, 'rh98_'+year_fi_0, 'rh75_'+year_fi_0, 'rh50_'+year_fi_0]);

var correction_fi_0 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_0).filterBounds(aoi);

correction_fi_0 = ee.Algorithms.If(
  correction_fi_0.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_fi_0, 'rh98_'+year_fi_0, 'rh75_'+year_fi_0, 'rh50_'+year_fi_0])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_0.mode().clip(aoi).select(['ch_'+year_fi_0, 'rh98_'+year_fi_0, 'rh75_'+year_fi_0, 'rh50_'+year_fi_0])
);
correction_fi_0 = ee.Image(correction_fi_0);

// var correction_fi_1 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_1)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_fi_1, 'rh98_'+year_fi_1, 'rh75_'+year_fi_1, 'rh50_'+year_fi_1]);

var correction_fi_1 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_1).filterBounds(aoi);

correction_fi_1 = ee.Algorithms.If(
  correction_fi_1.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_fi_1, 'rh98_'+year_fi_1, 'rh75_'+year_fi_1, 'rh50_'+year_fi_1])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_1.mode().clip(aoi).select(['ch_'+year_fi_1, 'rh98_'+year_fi_1, 'rh75_'+year_fi_1, 'rh50_'+year_fi_1])
);
correction_fi_1 = ee.Image(correction_fi_1);

// var correction_fi_2 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_2)
// .filterBounds(aoi)
// .mode()
// .clip(aoi)
// .select(['ch_'+year_fi_2, 'rh98_'+year_fi_2, 'rh75_'+year_fi_2, 'rh50_'+year_fi_2]);

var correction_fi_2 = ee.ImageCollection(project_path+'/corrections_ch_' + year_fi_2).filterBounds(aoi);

correction_fi_2 = ee.Algorithms.If(
  correction_fi_2.size().eq(0),
  // Dummy masked image
  ee.Image.constant([-9999, -9999, -9999, -9999])
    .rename(['ch_'+year_fi_2, 'rh98_'+year_fi_2, 'rh75_'+year_fi_2, 'rh50_'+year_fi_2])
    .reproject('EPSG:4326', null, 25)
    .clip(aoi)
    .updateMask(ee.Image(1)),
  // Real mean image
  correction_fi_2.mode().clip(aoi).select(['ch_'+year_fi_2, 'rh98_'+year_fi_2, 'rh75_'+year_fi_2, 'rh50_'+year_fi_2])
);
correction_fi_2 = ee.Image(correction_fi_2);

// var correction_in_0 = correction_img.select(['ch_'+year_in_0, 'rh98_'+year_in_0, 'rh75_'+year_in_0, 'rh50_'+year_in_0]);
// var correction_in_1 = correction_img.select(['ch_'+year_in_1, 'rh98_'+year_in_1, 'rh75_'+year_in_1, 'rh50_'+year_in_1]);
// var correction_in_2 = correction_img.select(['ch_'+year_in_2, 'rh98_'+year_in_2, 'rh75_'+year_in_2, 'rh50_'+year_in_2]);
// var correction_fi_0 = correction_img.select(['ch_'+year_fi_0, 'rh98_'+year_fi_0, 'rh75_'+year_fi_0, 'rh50_'+year_fi_0]);
// var correction_fi_1 = correction_img.select(['ch_'+year_fi_1, 'rh98_'+year_fi_1, 'rh75_'+year_fi_1, 'rh50_'+year_fi_1]);
// var correction_fi_2 = correction_img.select(['ch_'+year_fi_2, 'rh98_'+year_fi_2, 'rh75_'+year_fi_2, 'rh50_'+year_fi_2]);


correction_in_0 = correction_in_0.rename(['ch_in_0', 'rh98_in_0', 'rh75_in_0', 'rh50_in_0']);
correction_in_1 = correction_in_1.rename(['ch_in_1', 'rh98_in_1', 'rh75_in_1', 'rh50_in_1']);
correction_in_2 = correction_in_2.rename(['ch_in_2', 'rh98_in_2', 'rh75_in_2', 'rh50_in_2']);
correction_fi_0 = correction_fi_0.rename(['ch_fi_0', 'rh98_fi_0', 'rh75_fi_0', 'rh50_fi_0']);
correction_fi_1 = correction_fi_1.rename(['ch_fi_1', 'rh98_fi_1', 'rh75_fi_1', 'rh50_fi_1']);
correction_fi_2 = correction_fi_2.rename(['ch_fi_2', 'rh98_fi_2', 'rh75_fi_2', 'rh50_fi_2']);


var ch_in_0_ch = ch_in_0.select('ch_class');
ch_in_0_ch = ch_in_0_ch.addBands(correction_in_0);
ch_in_0_ch = ch_in_0_ch.unmask(-9999);
ch_in_0_ch = ch_in_0_ch.expression(
    "((b('ch_in_0')!=b('ch_class')) and (b('ch_in_0')!=-9999)) ? (b('ch_in_0'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_in_0_ch = ch_in_0_ch.updateMask(ch_in_0_ch.neq(-9999));
ch_in_0_ch = ch_in_0_ch.rename(['ch_in_0_ch']);


var ch_in_0_rh98 = ch_in_0.select('rh98_class');
ch_in_0_rh98 = ch_in_0_rh98.addBands(correction_in_0);
ch_in_0_rh98 = ch_in_0_rh98.unmask(-9999);
ch_in_0_rh98 = ch_in_0_rh98.expression(
    "((b('rh98_in_0')!=b('rh98_class')) and (b('rh98_in_0')!=-9999)) ? (b('rh98_in_0'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_in_0_rh98 = ch_in_0_rh98.updateMask(ch_in_0_rh98.neq(-9999));
ch_in_0_rh98 = ch_in_0_rh98.rename(['ch_in_0_rh98']);

var ch_in_0_rh75 = ch_in_0.select('rh75_class');
ch_in_0_rh75 = ch_in_0_rh75.addBands(correction_in_0);
ch_in_0_rh75 = ch_in_0_rh75.unmask(-9999);
ch_in_0_rh75 = ch_in_0_rh75.expression(
    "((b('rh75_in_0')!=b('rh75_class')) and (b('rh75_in_0')!=-9999)) ? (b('rh75_in_0'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_in_0_rh75 = ch_in_0_rh75.updateMask(ch_in_0_rh75.neq(-9999));
ch_in_0_rh75 = ch_in_0_rh75.rename(['ch_in_0_rh75']);ch_in_0_rh75


var ch_in_0_rh50 = ch_in_0.select('rh50_class');
ch_in_0_rh50 = ch_in_0_rh50.addBands(correction_in_0);
ch_in_0_rh50 = ch_in_0_rh50.unmask(-9999);
ch_in_0_rh50 = ch_in_0_rh50.expression(
    "((b('rh50_in_0')!=b('rh50_class')) and (b('rh50_in_0')!=-9999)) ? (b('rh50_in_0'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_in_0_rh50 = ch_in_0_rh50.updateMask(ch_in_0_rh50.neq(-9999));
ch_in_0_rh50 = ch_in_0_rh50.rename(['ch_in_0_rh50']);


ch_in_0 = ch_in_0_ch.addBands(ch_in_0_rh98).addBands(ch_in_0_rh75).addBands(ch_in_0_rh50);


var ch_in_1_ch = ch_in_1.select('ch_class');
ch_in_1_ch = ch_in_1_ch.addBands(correction_in_1);
ch_in_1_ch = ch_in_1_ch.unmask(-9999);
ch_in_1_ch = ch_in_1_ch.expression(
    "((b('ch_in_1')!=b('ch_class')) and (b('ch_in_1')!=-9999)) ? (b('ch_in_1'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_in_1_ch = ch_in_1_ch.updateMask(ch_in_1_ch.neq(-9999));
ch_in_1_ch = ch_in_1_ch.rename(['ch_in_1_ch']);


var ch_in_1_rh98 = ch_in_1.select('rh98_class');
ch_in_1_rh98 = ch_in_1_rh98.addBands(correction_in_1);
ch_in_1_rh98 = ch_in_1_rh98.unmask(-9999);
ch_in_1_rh98 = ch_in_1_rh98.expression(
    "((b('rh98_in_1')!=b('rh98_class')) and (b('rh98_in_1')!=-9999)) ? (b('rh98_in_1'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_in_1_rh98 = ch_in_1_rh98.updateMask(ch_in_1_rh98.neq(-9999));
ch_in_1_rh98 = ch_in_1_rh98.rename(['ch_in_1_rh98']);


var ch_in_1_rh75 = ch_in_1.select('rh75_class');
ch_in_1_rh75 = ch_in_1_rh75.addBands(correction_in_1);
ch_in_1_rh75 = ch_in_1_rh75.unmask(-9999);
ch_in_1_rh75 = ch_in_1_rh75.expression(
    "((b('rh75_in_1')!=b('rh75_class')) and (b('rh75_in_1')!=-9999)) ? (b('rh75_in_1'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_in_1_rh75 = ch_in_1_rh75.updateMask(ch_in_1_rh75.neq(-9999));
ch_in_1_rh75 = ch_in_1_rh75.rename(['ch_in_1_rh75']);ch_in_1_rh75


var ch_in_1_rh50 = ch_in_1.select('rh50_class');
ch_in_1_rh50 = ch_in_1_rh50.addBands(correction_in_1);
ch_in_1_rh50 = ch_in_1_rh50.unmask(-9999);
ch_in_1_rh50 = ch_in_1_rh50.expression(
    "((b('rh50_in_1')!=b('rh50_class')) and (b('rh50_in_1')!=-9999)) ? (b('rh50_in_1'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_in_1_rh50 = ch_in_1_rh50.updateMask(ch_in_1_rh50.neq(-9999));
ch_in_1_rh50 = ch_in_1_rh50.rename(['ch_in_1_rh50']);


ch_in_1 = ch_in_1_ch.addBands(ch_in_1_rh98).addBands(ch_in_1_rh75).addBands(ch_in_1_rh50);

// print(ch_in_1);

var ch_in_2_ch = ch_in_2.select('ch_class');
ch_in_2_ch = ch_in_2_ch.addBands(correction_in_2);
ch_in_2_ch = ch_in_2_ch.unmask(-9999);
ch_in_2_ch = ch_in_2_ch.expression(
    "((b('ch_in_2')!=b('ch_class')) and (b('ch_in_2')!=-9999)) ? (b('ch_in_2'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_in_2_ch = ch_in_2_ch.updateMask(ch_in_2_ch.neq(-9999));
ch_in_2_ch = ch_in_2_ch.rename(['ch_in_2_ch']);


var ch_in_2_rh98 = ch_in_2.select('rh98_class');
ch_in_2_rh98 = ch_in_2_rh98.addBands(correction_in_2);
ch_in_2_rh98 = ch_in_2_rh98.unmask(-9999);
ch_in_2_rh98 = ch_in_2_rh98.expression(
    "((b('rh98_in_2')!=b('rh98_class')) and (b('rh98_in_2')!=-9999)) ? (b('rh98_in_2'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_in_2_rh98 = ch_in_2_rh98.updateMask(ch_in_2_rh98.neq(-9999));
ch_in_2_rh98 = ch_in_2_rh98.rename(['ch_in_2_rh98']);


var ch_in_2_rh75 = ch_in_2.select('rh75_class');
ch_in_2_rh75 = ch_in_2_rh75.addBands(correction_in_2);
ch_in_2_rh75 = ch_in_2_rh75.unmask(-9999);
ch_in_2_rh75 = ch_in_2_rh75.expression(
    "((b('rh75_in_2')!=b('rh75_class')) and (b('rh75_in_2')!=-9999)) ? (b('rh75_in_2'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_in_2_rh75 = ch_in_2_rh75.updateMask(ch_in_2_rh75.neq(-9999));
ch_in_2_rh75 = ch_in_2_rh75.rename(['ch_in_2_rh75']);ch_in_2_rh75


var ch_in_2_rh50 = ch_in_2.select('rh50_class');
ch_in_2_rh50 = ch_in_2_rh50.addBands(correction_in_2);
ch_in_2_rh50 = ch_in_2_rh50.unmask(-9999);
ch_in_2_rh50 = ch_in_2_rh50.expression(
    "((b('rh50_in_2')!=b('rh50_class')) and (b('rh50_in_2')!=-9999)) ? (b('rh50_in_2'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_in_2_rh50 = ch_in_2_rh50.updateMask(ch_in_2_rh50.neq(-9999));
ch_in_2_rh50 = ch_in_2_rh50.rename(['ch_in_2_rh50']);


ch_in_2 = ch_in_2_ch.addBands(ch_in_2_rh98).addBands(ch_in_2_rh75).addBands(ch_in_2_rh50);


var ch_fi_0_ch = ch_fi_0.select('ch_class');
ch_fi_0_ch = ch_fi_0_ch.addBands(correction_fi_0);
ch_fi_0_ch = ch_fi_0_ch.unmask(-9999);
ch_fi_0_ch = ch_fi_0_ch.expression(
    "((b('ch_fi_0')!=b('ch_class')) and (b('ch_fi_0')!=-9999)) ? (b('ch_fi_0'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_fi_0_ch = ch_fi_0_ch.updateMask(ch_fi_0_ch.neq(-9999));
ch_fi_0_ch = ch_fi_0_ch.rename(['ch_fi_0_ch']);


var ch_fi_0_rh98 = ch_fi_0.select('rh98_class');
ch_fi_0_rh98 = ch_fi_0_rh98.addBands(correction_fi_0);
ch_fi_0_rh98 = ch_fi_0_rh98.unmask(-9999);
ch_fi_0_rh98 = ch_fi_0_rh98.expression(
    "((b('rh98_fi_0')!=b('rh98_class')) and (b('rh98_fi_0')!=-9999)) ? (b('rh98_fi_0'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_fi_0_rh98 = ch_fi_0_rh98.updateMask(ch_fi_0_rh98.neq(-9999));
ch_fi_0_rh98 = ch_fi_0_rh98.rename(['ch_fi_0_rh98']);


var ch_fi_0_rh75 = ch_fi_0.select('rh75_class');
ch_fi_0_rh75 = ch_fi_0_rh75.addBands(correction_fi_0);
ch_fi_0_rh75 = ch_fi_0_rh75.unmask(-9999);
ch_fi_0_rh75 = ch_fi_0_rh75.expression(
    "((b('rh75_fi_0')!=b('rh75_class')) and (b('rh75_fi_0')!=-9999)) ? (b('rh75_fi_0'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_fi_0_rh75 = ch_fi_0_rh75.updateMask(ch_fi_0_rh75.neq(-9999));
ch_fi_0_rh75 = ch_fi_0_rh75.rename(['ch_fi_0_rh75']);ch_fi_0_rh75


var ch_fi_0_rh50 = ch_fi_0.select('rh50_class');
ch_fi_0_rh50 = ch_fi_0_rh50.addBands(correction_fi_0);
ch_fi_0_rh50 = ch_fi_0_rh50.unmask(-9999);
ch_fi_0_rh50 = ch_fi_0_rh50.expression(
    "((b('rh50_fi_0')!=b('rh50_class')) and (b('rh50_fi_0')!=-9999)) ? (b('rh50_fi_0'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_fi_0_rh50 = ch_fi_0_rh50.updateMask(ch_fi_0_rh50.neq(-9999));
ch_fi_0_rh50 = ch_fi_0_rh50.rename(['ch_fi_0_rh50']);


ch_fi_0 = ch_fi_0_ch.addBands(ch_fi_0_rh98).addBands(ch_fi_0_rh75).addBands(ch_fi_0_rh50);


var ch_fi_1_ch = ch_fi_1.select('ch_class');
ch_fi_1_ch = ch_fi_1_ch.addBands(correction_fi_1);
ch_fi_1_ch = ch_fi_1_ch.unmask(-9999);
ch_fi_1_ch = ch_fi_1_ch.expression(
    "((b('ch_fi_1')!=b('ch_class')) and (b('ch_fi_1')!=-9999)) ? (b('ch_fi_1'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_fi_1_ch = ch_fi_1_ch.updateMask(ch_fi_1_ch.neq(-9999));
ch_fi_1_ch = ch_fi_1_ch.rename(['ch_fi_1_ch']);


var ch_fi_1_rh98 = ch_fi_1.select('rh98_class');
ch_fi_1_rh98 = ch_fi_1_rh98.addBands(correction_fi_1);
ch_fi_1_rh98 = ch_fi_1_rh98.unmask(-9999);
ch_fi_1_rh98 = ch_fi_1_rh98.expression(
    "((b('rh98_fi_1')!=b('rh98_class')) and (b('rh98_fi_1')!=-9999)) ? (b('rh98_fi_1'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_fi_1_rh98 = ch_fi_1_rh98.updateMask(ch_fi_1_rh98.neq(-9999));
ch_fi_1_rh98 = ch_fi_1_rh98.rename(['ch_fi_1_rh98']);


var ch_fi_1_rh75 = ch_fi_1.select('rh75_class');
ch_fi_1_rh75 = ch_fi_1_rh75.addBands(correction_fi_1);
ch_fi_1_rh75 = ch_fi_1_rh75.unmask(-9999);
ch_fi_1_rh75 = ch_fi_1_rh75.expression(
    "((b('rh75_fi_1')!=b('rh75_class')) and (b('rh75_fi_1')!=-9999)) ? (b('rh75_fi_1'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_fi_1_rh75 = ch_fi_1_rh75.updateMask(ch_fi_1_rh75.neq(-9999));
ch_fi_1_rh75 = ch_fi_1_rh75.rename(['ch_fi_1_rh75']);ch_fi_1_rh75


var ch_fi_1_rh50 = ch_fi_1.select('rh50_class');
ch_fi_1_rh50 = ch_fi_1_rh50.addBands(correction_fi_1);
ch_fi_1_rh50 = ch_fi_1_rh50.unmask(-9999);
ch_fi_1_rh50 = ch_fi_1_rh50.expression(
    "((b('rh50_fi_1')!=b('rh50_class')) and (b('rh50_fi_1')!=-9999)) ? (b('rh50_fi_1'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_fi_1_rh50 = ch_fi_1_rh50.updateMask(ch_fi_1_rh50.neq(-9999));
ch_fi_1_rh50 = ch_fi_1_rh50.rename(['ch_fi_1_rh50']);


ch_fi_1 = ch_fi_1_ch.addBands(ch_fi_1_rh98).addBands(ch_fi_1_rh75).addBands(ch_fi_1_rh50);


var ch_fi_2_ch = ch_fi_2.select('ch_class');
ch_fi_2_ch = ch_fi_2_ch.addBands(correction_fi_2);
ch_fi_2_ch = ch_fi_2_ch.unmask(-9999);
ch_fi_2_ch = ch_fi_2_ch.expression(
    "((b('ch_fi_2')!=b('ch_class')) and (b('ch_fi_2')!=-9999)) ? (b('ch_fi_2'))"+
    ":b('ch_class')"
  ).clip(aoi);
ch_fi_2_ch = ch_fi_2_ch.updateMask(ch_fi_2_ch.neq(-9999));
ch_fi_2_ch = ch_fi_2_ch.rename(['ch_fi_2_ch']);


var ch_fi_2_rh98 = ch_fi_2.select('rh98_class');
ch_fi_2_rh98 = ch_fi_2_rh98.addBands(correction_fi_2);
ch_fi_2_rh98 = ch_fi_2_rh98.unmask(-9999);
ch_fi_2_rh98 = ch_fi_2_rh98.expression(
    "((b('rh98_fi_2')!=b('rh98_class')) and (b('rh98_fi_2')!=-9999)) ? (b('rh98_fi_2'))"+
    ":b('rh98_class')"
  ).clip(aoi);
ch_fi_2_rh98 = ch_fi_2_rh98.updateMask(ch_fi_2_rh98.neq(-9999));
ch_fi_2_rh98 = ch_fi_2_rh98.rename(['ch_fi_2_rh98']);


var ch_fi_2_rh75 = ch_fi_2.select('rh75_class');
ch_fi_2_rh75 = ch_fi_2_rh75.addBands(correction_fi_2);
ch_fi_2_rh75 = ch_fi_2_rh75.unmask(-9999);
ch_fi_2_rh75 = ch_fi_2_rh75.expression(
    "((b('rh75_fi_2')!=b('rh75_class')) and (b('rh75_fi_2')!=-9999)) ? (b('rh75_fi_2'))"+
    ":b('rh75_class')"
  ).clip(aoi);
ch_fi_2_rh75 = ch_fi_2_rh75.updateMask(ch_fi_2_rh75.neq(-9999));
ch_fi_2_rh75 = ch_fi_2_rh75.rename(['ch_fi_2_rh75']);ch_fi_2_rh75


var ch_fi_2_rh50 = ch_fi_2.select('rh50_class');
ch_fi_2_rh50 = ch_fi_2_rh50.addBands(correction_fi_2);
ch_fi_2_rh50 = ch_fi_2_rh50.unmask(-9999);
ch_fi_2_rh50 = ch_fi_2_rh50.expression(
    "((b('rh50_fi_2')!=b('rh50_class')) and (b('rh50_fi_2')!=-9999)) ? (b('rh50_fi_2'))"+
    ":b('rh50_class')"
  ).clip(aoi);
ch_fi_2_rh50 = ch_fi_2_rh50.updateMask(ch_fi_2_rh50.neq(-9999));
ch_fi_2_rh50 = ch_fi_2_rh50.rename(['ch_fi_2_rh50']);


ch_fi_2 = ch_fi_2_ch.addBands(ch_fi_2_rh98).addBands(ch_fi_2_rh75).addBands(ch_fi_2_rh50);

var initial_image = ch_in_0.addBands(ch_in_1).addBands(ch_in_2);
// print(initial_image);

var initial_image_ch = initial_image.select(['ch_in_0_ch', 'ch_in_1_ch', 'ch_in_2_ch']);
initial_image_ch = initial_image_ch.unmask(-9999);
initial_image_ch = initial_image_ch.expression(
    "((b('ch_in_0_ch')==b('ch_in_2_ch')) and (b('ch_in_0_ch')!=-9999)) ? (b('ch_in_0_ch'))"+
    ":((b('ch_in_0_ch')==-9999 or b('ch_in_2_ch')==-9999) and (b('ch_in_1_ch')!=-9999)) ? (b('ch_in_1_ch'))"+
    ":((b('ch_in_0_ch')!=-9999 and b('ch_in_1_ch')==-9999 and b('ch_in_2_ch')==-9999)) ? (b('ch_in_0_ch'))"+
    ":((b('ch_in_1_ch')==-9999 and b('ch_in_2_ch')!=-9999) and (b('ch_in_0_ch')!=b('ch_in_2_ch'))) ? (b('ch_in_2_ch'))"+
    ":(b('ch_in_1_ch'))"
  ).clip(aoi);
initial_image_ch = initial_image_ch.rename(['ch_class']);
initial_image_ch = initial_image_ch.updateMask(initial_image_ch.neq(-9999));

var initial_image_rh98 = initial_image.select(['ch_in_0_rh98', 'ch_in_1_rh98', 'ch_in_2_rh98']);
initial_image_rh98 = initial_image_rh98.unmask(-9999);
initial_image_rh98 = initial_image_rh98.expression(
    "((b('ch_in_0_rh98')==b('ch_in_2_rh98')) and (b('ch_in_0_rh98')!=-9999)) ? (b('ch_in_0_rh98'))"+
    ":((b('ch_in_0_rh98')==-9999 or b('ch_in_2_rh98')==-9999) and (b('ch_in_1_rh98')!=-9999)) ? (b('ch_in_1_rh98'))"+
    ":((b('ch_in_0_rh98')!=-9999 and b('ch_in_1_rh98')==-9999 and b('ch_in_2_rh98')==-9999)) ? (b('ch_in_0_rh98'))"+
    ":((b('ch_in_1_rh98')==-9999 and b('ch_in_2_rh98')!=-9999) and (b('ch_in_0_rh98')!=b('ch_in_2_rh98'))) ? (b('ch_in_2_rh98'))"+
    ":(b('ch_in_1_rh98'))"
  ).clip(aoi);
initial_image_rh98 = initial_image_rh98.rename(['rh98_class']);
initial_image_rh98 = initial_image_rh98.updateMask(initial_image_rh98.neq(-9999));

var initial_image_rh75 = initial_image.select(['ch_in_0_rh75', 'ch_in_1_rh75', 'ch_in_2_rh75']);
initial_image_rh75 = initial_image_rh75.unmask(-9999);
initial_image_rh75 = initial_image_rh75.expression(
    "((b('ch_in_0_rh75')==b('ch_in_2_rh75')) and (b('ch_in_0_rh75')!=-9999)) ? (b('ch_in_0_rh75'))"+
    ":((b('ch_in_0_rh75')==-9999 or b('ch_in_2_rh75')==-9999) and (b('ch_in_1_rh75')!=-9999)) ? (b('ch_in_1_rh75'))"+
    ":((b('ch_in_0_rh75')!=-9999 and b('ch_in_1_rh75')==-9999 and b('ch_in_2_rh75')==-9999)) ? (b('ch_in_0_rh75'))"+
    ":((b('ch_in_1_rh75')==-9999 and b('ch_in_2_rh75')!=-9999) and (b('ch_in_0_rh75')!=b('ch_in_2_rh75'))) ? (b('ch_in_2_rh75'))"+
    ":(b('ch_in_1_rh75'))"
  ).clip(aoi);
initial_image_rh75 = initial_image_rh75.rename(['rh75_class']);
initial_image_rh75 = initial_image_rh75.updateMask(initial_image_rh75.neq(-9999));

var initial_image_rh50 = initial_image.select(['ch_in_0_rh50', 'ch_in_1_rh50', 'ch_in_2_rh50']);
initial_image_rh50 = initial_image_rh50.unmask(-9999);
initial_image_rh50 = initial_image_rh50.expression(
    "((b('ch_in_0_rh50')==b('ch_in_2_rh50')) and (b('ch_in_0_rh50')!=-9999)) ? (b('ch_in_0_rh50'))"+
    ":((b('ch_in_0_rh50')==-9999 or b('ch_in_2_rh50')==-9999) and (b('ch_in_1_rh50')!=-9999)) ? (b('ch_in_1_rh50'))"+
    ":((b('ch_in_0_rh50')!=-9999 and b('ch_in_1_rh50')==-9999 and b('ch_in_2_rh50')==-9999)) ? (b('ch_in_0_rh50'))"+
    ":((b('ch_in_1_rh50')==-9999 and b('ch_in_2_rh50')!=-9999) and (b('ch_in_0_rh50')!=b('ch_in_2_rh50'))) ? (b('ch_in_2_rh50'))"+
    ":(b('ch_in_1_rh50'))"
  ).clip(aoi);
initial_image_rh50 = initial_image_rh50.rename(['rh50_class']);
initial_image_rh50 = initial_image_rh50.updateMask(initial_image_rh50.neq(-9999));

initial_image_ch = initial_image_ch.addBands(tree_cover_initial);
initial_image_ch = initial_image_ch.unmask(-9999);

// 4 denotes missing data
initial_image_ch = initial_image_ch.expression(
  "((b('ch_class') == -9999) and (b('label') != -9999)) ? (4)"+
  ":b('ch_class')"
  ).clip(aoi);
initial_image_ch = initial_image_ch.rename(['ch_class']);
initial_image_ch = initial_image_ch.updateMask(initial_image_ch.neq(-9999));

initial_image = initial_image_ch.addBands(initial_image_rh98).addBands(initial_image_rh75)
                .addBands(initial_image_rh50);


var final_image = ch_fi_0.addBands(ch_fi_1).addBands(ch_fi_2);

var final_image_ch = final_image.select(['ch_fi_0_ch', 'ch_fi_1_ch', 'ch_fi_2_ch']);
final_image_ch = final_image_ch.unmask(-9999);
final_image_ch = final_image_ch.expression(
    "((b('ch_fi_0_ch')==b('ch_fi_2_ch')) and (b('ch_fi_0_ch')!=-9999)) ? (b('ch_fi_0_ch'))"+
    ":((b('ch_fi_0_ch')==-9999 or b('ch_fi_2_ch')==-9999) and (b('ch_fi_1_ch')!=-9999)) ? (b('ch_fi_1_ch'))"+
    ":((b('ch_fi_0_ch')!=-9999 and b('ch_fi_1_ch')==-9999 and b('ch_fi_2_ch')==-9999)) ? (b('ch_fi_0_ch'))"+
    ":((b('ch_fi_1_ch')==-9999 and b('ch_fi_2_ch')!=-9999) and (b('ch_fi_0_ch')!=b('ch_fi_2_ch'))) ? (b('ch_fi_2_ch'))"+
    ":(b('ch_fi_1_ch'))"
  ).clip(aoi);
final_image_ch = final_image_ch.rename(['ch_class']);
final_image_ch = final_image_ch.updateMask(final_image_ch.neq(-9999));

var final_image_rh98 = final_image.select(['ch_fi_0_rh98', 'ch_fi_1_rh98', 'ch_fi_2_rh98']);
final_image_rh98 = final_image_rh98.unmask(-9999);
final_image_rh98 = final_image_rh98.expression(
    "((b('ch_fi_0_rh98')==b('ch_fi_2_rh98')) and (b('ch_fi_0_rh98')!=-9999)) ? (b('ch_fi_0_rh98'))"+
    ":((b('ch_fi_0_rh98')==-9999 or b('ch_fi_2_rh98')==-9999) and (b('ch_fi_1_rh98')!=-9999)) ? (b('ch_fi_1_rh98'))"+
    ":((b('ch_fi_0_rh98')!=-9999 and b('ch_fi_1_rh98')==-9999 and b('ch_fi_2_rh98')==-9999)) ? (b('ch_fi_0_rh98'))"+
    ":((b('ch_fi_1_rh98')==-9999 and b('ch_fi_2_rh98')!=-9999) and (b('ch_fi_0_rh98')!=b('ch_fi_2_rh98'))) ? (b('ch_fi_2_rh98'))"+
    ":(b('ch_fi_1_rh98'))"
  ).clip(aoi);
final_image_rh98 = final_image_rh98.rename(['rh98_class']);
final_image_rh98 = final_image_rh98.updateMask(final_image_rh98.neq(-9999));

var final_image_rh75 = final_image.select(['ch_fi_0_rh75', 'ch_fi_1_rh75', 'ch_fi_2_rh75']);
final_image_rh75 = final_image_rh75.unmask(-9999);
final_image_rh75 = final_image_rh75.expression(
    "((b('ch_fi_0_rh75')==b('ch_fi_2_rh75')) and (b('ch_fi_0_rh75')!=-9999)) ? (b('ch_fi_0_rh75'))"+
    ":((b('ch_fi_0_rh75')==-9999 or b('ch_fi_2_rh75')==-9999) and (b('ch_fi_1_rh75')!=-9999)) ? (b('ch_fi_1_rh75'))"+
    ":((b('ch_fi_0_rh75')!=-9999 and b('ch_fi_1_rh75')==-9999 and b('ch_fi_2_rh75')==-9999)) ? (b('ch_fi_0_rh75'))"+
    ":((b('ch_fi_1_rh75')==-9999 and b('ch_fi_2_rh75')!=-9999) and (b('ch_fi_0_rh75')!=b('ch_fi_2_rh75'))) ? (b('ch_fi_2_rh75'))"+
    ":(b('ch_fi_1_rh75'))"
  ).clip(aoi);
final_image_rh75 = final_image_rh75.rename(['rh75_class']);
final_image_rh75 = final_image_rh75.updateMask(final_image_rh75.neq(-9999));

var final_image_rh50 = final_image.select(['ch_fi_0_rh50', 'ch_fi_1_rh50', 'ch_fi_2_rh50']);
final_image_rh50 = final_image_rh50.unmask(-9999);
final_image_rh50 = final_image_rh50.expression(
    "((b('ch_fi_0_rh50')==b('ch_fi_2_rh50')) and (b('ch_fi_0_rh50')!=-9999)) ? (b('ch_fi_0_rh50'))"+
    ":((b('ch_fi_0_rh50')==-9999 or b('ch_fi_2_rh50')==-9999) and (b('ch_fi_1_rh50')!=-9999)) ? (b('ch_fi_1_rh50'))"+
    ":((b('ch_fi_0_rh50')!=-9999 and b('ch_fi_1_rh50')==-9999 and b('ch_fi_2_rh50')==-9999)) ? (b('ch_fi_0_rh50'))"+
    ":((b('ch_fi_1_rh50')==-9999 and b('ch_fi_2_rh50')!=-9999) and (b('ch_fi_0_rh50')!=b('ch_fi_2_rh50'))) ? (b('ch_fi_2_rh50'))"+
    ":(b('ch_fi_1_rh50'))"
  ).clip(aoi);
final_image_rh50 = final_image_rh50.rename(['rh50_class']);
final_image_rh50 = final_image_rh50.updateMask(final_image_rh50.neq(-9999));

final_image_ch = final_image_ch.addBands(tree_cover_final);
final_image_ch = final_image_ch.unmask(-9999);
// 4 denotes missing data
final_image_ch = final_image_ch.expression(
  "((b('ch_class') == -9999) and (b('label') != -9999)) ? (4)"+
  ":b('ch_class')"
  ).clip(aoi);
final_image_ch = final_image_ch.rename(['ch_class']);
final_image_ch = final_image_ch.updateMask(final_image_ch.neq(-9999));

final_image = final_image_ch.addBands(final_image_rh98).addBands(final_image_rh75)
                .addBands(final_image_rh50);


var palette =['FFA500', 'DEE64C', 'DEE64C','007500', '000000'];
Map.addLayer(initial_image, {bands:['ch_class'], min: 0, max: 4, palette: palette}, 'initial modal image ch');
Map.addLayer(final_image, {bands:['ch_class'], min: 0, max: 4, palette: palette}, 'final modal image ch');

initial_image = initial_image.unmask(-9999);
initial_image = initial_image.expression(
  "((b('ch_class') == 4)) ? (8)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 0) and (b('rh98_class') == 0)) ? (0)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 0) and (b('rh98_class') == 1)) ? (1)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 1) and (b('rh98_class') == 0)) ? (2)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 1) and (b('rh98_class') == 1)) ? (3)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 0) and (b('rh98_class') == 0)) ? (4)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 0) and (b('rh98_class') == 1)) ? (5)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 1) and (b('rh98_class') == 0)) ? (6)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 1) and (b('rh98_class') == 1)) ? (7)"+
  ": ((b('rh50_class') != -9999) or (b('rh75_class') != -9999) and (b('rh98_class') != -9999)) ? (8)"+
  ":-9999"
  ).clip(aoi);
initial_image = initial_image.updateMask(initial_image.neq(-9999));
initial_image = initial_image.rename(['ch_class']);


final_image = final_image.unmask(-9999);
final_image = final_image.expression(
  "((b('ch_class') == 4)) ? (8)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 0) and (b('rh98_class') == 0)) ? (0)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 0) and (b('rh98_class') == 1)) ? (1)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 1) and (b('rh98_class') == 0)) ? (2)"+
  ": ((b('rh50_class') == 0) and (b('rh75_class') == 1) and (b('rh98_class') == 1)) ? (3)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 0) and (b('rh98_class') == 0)) ? (4)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 0) and (b('rh98_class') == 1)) ? (5)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 1) and (b('rh98_class') == 0)) ? (6)"+
  ": ((b('rh50_class') == 1) and (b('rh75_class') == 1) and (b('rh98_class') == 1)) ? (7)"+
  ": ((b('rh50_class') != -9999) or (b('rh75_class') != -9999) and (b('rh98_class') != -9999)) ? (8)"+
  ":-9999"
  ).clip(aoi);
final_image = final_image.updateMask(final_image.neq(-9999));
final_image = final_image.rename(['ch_class']);

var palette =['FFA500', 'FFA500', 'DEE64C', 'DEE64C', 'DEE64C', 'DEE64C', '007500', '007500', '000000'];
Map.addLayer(initial_image, {bands:['ch_class'], min: 0, max: 8, palette: palette}, 'initial modal image ch');
Map.addLayer(final_image, {bands:['ch_class'], min: 0, max: 8, palette: palette}, 'final modal image ch');


var change = initial_image.addBands(final_image);
change = change.unmask(-9999);
// 3 denotes missing data
change = change.expression(
    "((b('ch_class') == -9999) and (b('ch_class_1') != -9999)) ? (2)" +
    ": ((b('ch_class') != -9999) and (b('ch_class_1') == -9999)) ? (-2)"+
    ": ((b('ch_class') == 8) or (b('ch_class_1') == 8)) ? (3)" +
    ": ((b('ch_class') == b('ch_class_1')) and (b('ch_class') != -9999)) ? (0)" +
    ": ((b('ch_class') < b('ch_class_1')) and (b('ch_class') != -9999) and ((b('ch_class') <= 1) or (b('ch_class') >= 4))) ? (1)" +
    ": ((b('ch_class') > b('ch_class_1')) and (b('ch_class_1') != -9999)) ? (-1)" +
    ": ((b('ch_class') == 2) and ((b('ch_class_1') == 3) or (b('ch_class_1') == 6) or (b('ch_class_1') == 7))) ? (1)" +
    ": ((b('ch_class') == 3) and ((b('ch_class_1') == 6) or (b('ch_class_1') == 7))) ? (1)" +
    ": (((b('ch_class') == 2) or (b('ch_class') == 3)) and ((b('ch_class_1') == 4) or (b('ch_class_1') == 5))) ? (-1)"+
    ":-9999"
).clip(aoi);
change = change.updateMask(change.neq(-9999));
// var palette = ['red', 'orange', 'white', 'lightgreen', 'green', 'black'];
var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];
Map.addLayer(change, {min: -2, max: 3, palette: palette}, 'change layer ch');

// Export modal initial image
Export.image.toAsset({
    image: initial_image,
    description: 'ch_' + year_in_1 + '_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/modal_ch_' + year_in_1 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });

// Export modal final image
Export.image.toAsset({
    image: final_image,
    description: 'ch_' + year_fi_1 + '_' + agroclimaticZoneAcronymDict[acz],
    assetId: project_path+'/modal_ch_' + year_fi_1 + '/' + agroclimaticZoneAcronymDict[acz],
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });

// // Export CH change image
// Export.image.toAsset({
//     image: change,
//     description: 'ch_change_' + agroclimaticZoneAcronymDict[acz],
//     assetId: project_path+'/ch_change_' + year_in_1 + '_' + year_fi_1 + '/' + agroclimaticZoneAcronymDict[acz],
//     region: aoi,
//     scale: 25,
//     crs: 'EPSG:4326',
//     maxPixels: 10000000000
//   });

// Quantifying CH change classes statistics
// var change_chm = change;

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


// Adding Legend on GEE map

// set position of panel
var legend = ui.Panel({
  style: {
    position: 'bottom-left',
    padding: '8px 15px'
  }
});

// Create legend title
var legendTitle = ui.Label({
  value: 'Legend',
  style: {
    fontWeight: 'bold',
    fontSize: '18px',
    margin: '0 0 4px 0',
    padding: '0'
    }
});

// Add the title to the panel
legend.add(legendTitle);

// Creates and styles 1 row of the legend.
var makeRow = function(color, name) {

      // Create the label that is actually the colored box.
      var colorBox = ui.Label({
        style: {
          backgroundColor: '#' + color,
          // Use padding to give the box height and width.
          padding: '8px',
          margin: '0 0 4px 0',
          border: '1px solid black'
        }
      });

      // Create the label filled with the description text.
      var description = ui.Label({
        value: name,
        style: {margin: '0 0 4px 6px'}
      });

      // return the panel
      return ui.Panel({
        widgets: [colorBox, description],
        layout: ui.Panel.Layout.Flow('horizontal')
      });
};

//  Palette with the colors
var palette =['FFA500', 'DEE64C', '007500', '000000'];
// var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', '000000'];



// name of the legend
var names = ['Short Trees', 'Medium Height Trees', 'Tall Trees', 'Missing Data'];
// var names = ['Deforestation','Degradation','No Change','Improvement','Afforestation','Missing Data'];

// Add color and and names
for (var i = 0; i < names.length; i++) {
  legend.add(makeRow(palette[i], names[i]));
  }

// add legend to map (alternatively you can also print the legend to the console)
Map.add(legend);