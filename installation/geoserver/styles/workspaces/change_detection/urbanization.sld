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
			<ColorMapEntry quantity="1" label="Built_Up-Built_Up" color="#ff0000" opacity="0.7" />
              <ColorMapEntry quantity="2" label="Water-Built_Up" color="#1ca3ec" opacity="0.7" />
              <ColorMapEntry quantity="3" label="Trees_or_Crops-Built_Up" color="#73bb53" opacity="0.7" />
              <ColorMapEntry quantity="4" label="Barren_or_Shrubs_and_Scrubs-Built_Up" color="#a9a9a9" opacity="0.7" />
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>