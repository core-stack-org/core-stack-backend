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
                <ColorMapEntry color="#000000" quantity="0.0" label="Background" opacity="0.0" />
              	<ColorMapEntry color="#313695" quantity="1.0" label="Valleys" opacity="0.7		" />
              	<ColorMapEntry color="#1a9850" quantity="2.0" label="Plains" opacity="0.7" />
              	<ColorMapEntry color="#fee08b" quantity="3.0" label="Broad Slopes" opacity="0.7" />
              	<ColorMapEntry color="#fc8d59" quantity="4.0" label="Steep Slopes" opacity="0.7" />
              	<ColorMapEntry color="#d73027" quantity="5.0" label="Ridges" opacity="0.7" />
            </ColorMap>
            
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>