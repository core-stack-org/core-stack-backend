<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld
  http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">

  <NamedLayer>
    <Name>catchment_area_singleflow</Name>
    <UserStyle>
      <Title>Catchment Area Single Flow Style</Title>
      <Abstract>Color ramp representing catchment area accumulation (single flow)</Abstract>

      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap type="intervals">
              <ColorMapEntry color="#440154" quantity="2" label="≤ 2" opacity="1.0"/>
              <ColorMapEntry color="#45317c" quantity="50" label="2 - 50" opacity="1.0"/>
              <ColorMapEntry color="#375a8c" quantity="100" label="50 - 100" opacity="1.0"/>
              <ColorMapEntry color="#287b8e" quantity="200" label="100 - 200" opacity="1.0"/>
              <ColorMapEntry color="#219889" quantity="500" label="200 - 500" opacity="1.0"/>
              <ColorMapEntry color="#4cbe6c" quantity="1000" label="500 - 1000" opacity="1.0"/>
              <ColorMapEntry color="#e6e32e" quantity="2000" label="1000 - 2000" opacity="1.0"/>
              <ColorMapEntry color="#fde725" quantity="999999" label="> 2000" opacity="1.0"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>

    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>