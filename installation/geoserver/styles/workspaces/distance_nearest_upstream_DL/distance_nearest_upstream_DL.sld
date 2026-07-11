<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld
  http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">

  <NamedLayer>
    <Name>distance_to_nearest_upstream_DL</Name>
    <UserStyle>
      <Title>Distance to Nearest Upstream Drainage Line</Title>
      <Abstract>Raster color ramp representing distance (in meters) to the nearest upstream drainage line</Abstract>

      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap type="intervals">
              <ColorMapEntry color="#0e10c7" quantity="0" label="0 m" opacity="1.0"/>
              <ColorMapEntry color="#5fbcf6" quantity="60" label="1–60 m" opacity="1.0"/>
              <ColorMapEntry color="#145626" quantity="120" label="60–120 m" opacity="1.0"/>
              <ColorMapEntry color="#3df03c" quantity="240" label="120–240 m" opacity="1.0"/>
              <ColorMapEntry color="#f0ee26" quantity="500" label="240–500 m" opacity="1.0"/>
              <ColorMapEntry color="#eb8115" quantity="1000" label="500–1000 m" opacity="1.0"/>
              <ColorMapEntry color="#dd0f08" quantity="2000" label="1000–2000 m" opacity="1.0"/>
              <ColorMapEntry color="#7d0b1b" quantity="177944.0625" label="> 2000 m" opacity="1.0"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>

    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>