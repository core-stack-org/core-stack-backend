var tree_cover_change = ee.ImageCollection("projects/corestack-trees/assets/tree_characteristics/overall_change_2017_2022"); // public asset
var tcc = tree_cover_change.mosaic();

var lulc_2018 = ee.Image("projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_2018_2019"); // public asset
var lulc_2022 = ee.Image("projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_2022_2023"); // public asset

var tree_mask_2018 = lulc_2018.eq(6);
var tree_mask_2022 = lulc_2022.eq(6);
var tree_mask = tree_mask_2018.or(tree_mask_2022);

var tcc_masked = tcc.updateMask(tree_mask);

var pa = ee.FeatureCollection("projects/ee-aaditeshwar/assets/protected_areas_data_final"); // final asset with pre-computed stats
tcc_masked = tcc_masked.updateMask(
  ee.Image().paint(pa, 1)
);

var palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', 'DEE64C', 'DEE64C', '000000'];
var classValues = [-2, -1, 0, 1, 2, 3, 4, 5];
var classLabels = ['Tree Cover Loss', 'Degradation', 'No Change', 'Improvement', 'Tree Cover Gain', 'Partially Degraded', 'Partially Degraded', 'Missing Data'];

Map.addLayer(
  tcc_masked,
  {min: -2, max: 5, palette: palette},
  'Tree cover change (PA only)'
);

// protected area boundaries
var empty = ee.Image().byte();
var pa_boundaries = empty.paint({
  featureCollection: pa,
  color: 1,
  width: 1
});

Map.addLayer(pa_boundaries, {palette: ['000000']}, 'Protected Area Boundaries');

var legend = ui.Panel({
  style: {
    position: 'bottom-right',
    padding: '8px 15px'
  }
});

legend.add(ui.Label({
  value: 'Tree cover change: 2017-2023',
  style: {
    fontWeight: 'bold',
    fontSize: '14px',
    margin: '0 0 8px 0'
  }
}));

for (var i = 0; i < classLabels.length; i++) {
  var colorBox = ui.Label({
    style: {
      backgroundColor: '#' + palette[i],
      padding: '8px',
      margin: '0 0 4px 0'
    }
  });

  var description = ui.Label({
    value: classLabels[i],
    style: { margin: '0 0 4px 6px' }
  });

  legend.add(
    ui.Panel({
      widgets: [colorBox, description],
      layout: ui.Panel.Layout.Flow('horizontal')
    })
  );
}

Map.add(legend);

var containerPanel = ui.Panel({
  style: {
    position: 'bottom-left',
    width: '300px'
  },
  layout: ui.Panel.Layout.flow('vertical')
});

var staticPanel = ui.Panel({
  style: {
    width: '280px',
    padding: '8px'
  }
});

staticPanel.add(ui.Label({value: 'CoRE stack innovation challenge demo (Jan 2026)',
  style: {
    fontWeight: 'bold'
  }}));
staticPanel.add(ui.Label('Zoom into a protected area to see its tree cover changes. Click on it to see the change stats. Data source:'));
staticPanel.add(ui.Label({value: 'Tree cover change maps',
  targetUrl: 'https://www.cse.iitd.ernet.in/~aseth/forest-health-ictd2024.pdf',
  style: {
    color: 'blue',
    textDecoration: 'underline'
  }}));
staticPanel.add(ui.Label({value: 'Protected area boundaries',
  targetUrl: 'https://pau-database.kalpavriksh.org/',
  style: {
    color: 'blue',
    textDecoration: 'underline'
  }}));

containerPanel.add(staticPanel);

var infoPanel = ui.Panel({
  style: {
    width: '280px',
    padding: '8px'
  }
});

containerPanel.add(infoPanel);

Map.onClick(function(coords) {
  infoPanel.clear();
  infoPanel.add(ui.Label('Loading...', {fontWeight: 'bold'}));

  var point = ee.Geometry.Point(coords.lon, coords.lat);

  var feature = pa.filterBounds(point).first();

  feature.evaluate(function(f) {
    infoPanel.clear();

    if (!f) {
      infoPanel.add(ui.Label('No protected area here'));
      return;
    }

    infoPanel.add(ui.Label(f.properties.name || 'Protected Area'));

    infoPanel.add(ui.Label(
      'No change (ha): ' + (f.properties.area_no_change_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Tree cover gain (ha): ' + (f.properties.area_tree_gain_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Tree cover improvement (ha): ' + (f.properties.area_improvement_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Tree cover loss (ha): ' + (f.properties.area_tree_loss_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Tree cover degradation (ha): ' + (f.properties.area_degradation_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Tree cover partial degradation (ha): ' + (f.properties.area_partially_degraded_ha + f.properties.area_partially_degraded2_ha || 0).toFixed(1)
    ));
    infoPanel.add(ui.Label(
      'Missing data (ha): ' + (f.properties.area_missing_data_ha || 0).toFixed(1)
    ));

  });
});

Map.add(containerPanel)
Map.setCenter(78.9629, 20.5937, 5);
