<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>terrain_raster</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry quantity="0" label="Background" color="#f7fcf5" opacity="0.0" />
			<ColorMapEntry quantity="1" label="Crops-Crops" color="#eee05d" opacity="0.7" />
              <ColorMapEntry quantity="2" label="Crops-Built_Up" color="#ff0000" opacity="0.7" />
              <ColorMapEntry quantity="3" label="Crops-Barren" color="#a9a9a9" opacity="0.7" />
              <ColorMapEntry quantity="4" label="Crops-Shrubs_and_Scrubs" color="#eaa4f0" opacity="0.7" />
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>