<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld
  http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">

  <NamedLayer>
    <Name>natural_depression_style</Name>
    <UserStyle>
      <Title>Natural Depression Raster Style</Title>
      <Abstract>Color ramp representing natural depression depth/density levels</Abstract>

      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap type="intervals">
              <ColorMapEntry color="#ffffcc" quantity="1" label="≤ 1" opacity="1.0"/>
              <ColorMapEntry color="#a1dab4" quantity="3" label="1 - 3" opacity="1.0"/>
              <ColorMapEntry color="#2c7fb8" quantity="5" label="3 - 5" opacity="1.0"/>
              <ColorMapEntry color="#253494" quantity="10" label="5 - 10" opacity="1.0"/>
              <ColorMapEntry color="#2e004f" quantity="20" label="10 - 20" opacity="1.0"/>
              <ColorMapEntry color="#67001f" quantity="50" label="20 - 50" opacity="1.0"/>
              <ColorMapEntry color="#990000" quantity="9999" label="> 50" opacity="1.0"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>

    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>