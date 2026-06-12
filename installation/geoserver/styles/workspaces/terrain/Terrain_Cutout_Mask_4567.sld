<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>terrain_cutout_mask</Name>
    <UserStyle>
      <Title>Terrain Cutout Mask - Transparent for 4, 5, 6, 7</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap type="values">
              <!-- Values 1, 2, 3: Dark overlay (hide site suitability) -->
              <ColorMapEntry quantity="1" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="2" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="3" color="#1a1a2e" opacity="0.9" />
              <!-- Values 4, 5, 6, 7: TRANSPARENT (let site suitability show through) -->
              <ColorMapEntry quantity="4" color="#000000" opacity="0" />
              <ColorMapEntry quantity="5" color="#000000" opacity="0" />
              <ColorMapEntry quantity="6" color="#000000" opacity="0" />
              <ColorMapEntry quantity="7" color="#000000" opacity="0" />
              <!-- Values 8, 9, 10, 11, 12: Dark overlay (hide site suitability) -->
              <ColorMapEntry quantity="8" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="9" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="10" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="11" color="#1a1a2e" opacity="0.9" />
              <ColorMapEntry quantity="12" color="#1a1a2e" opacity="0.9" />
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>