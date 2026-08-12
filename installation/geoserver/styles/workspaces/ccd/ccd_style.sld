<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>ccd_style</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
                <ColorMapEntry color="#ffa500" quantity="0.0" label="Low Density" opacity="0.7" />
              	<ColorMapEntry color="#007500" quantity="1.0" label="High Density" opacity="0.7		" />
              	<ColorMapEntry color="#000000" quantity="2.0" label="Missing Data" opacity="0.7" />
            </ColorMap> 
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>