<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/sld
http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd" version="1.0.0">
  <NamedLayer>
    <Name>ch_style</Name>
    <UserStyle>
      <Title>A raster style</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
                <ColorMapEntry color="#FFA500" quantity="0.0" label="Short Trees" opacity="0.7" />
              	<ColorMapEntry color="#FFA500" quantity="1.0" label="Short Trees" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="2.0" label="Medium Trees" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="3.0" label="Medium Trees" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="4.0" label="Medium Trees" opacity="0.7" />
              	<ColorMapEntry color="#DEE64C" quantity="5.0" label="Medium Trees" opacity="0.7" />
              	<ColorMapEntry color="#007500" quantity="6.0" label="Tall Trees" opacity="0.7" />
              	<ColorMapEntry color="#007500" quantity="7.0" label="Tall Trees" opacity="0.7" />
              	<ColorMapEntry color="#000000" quantity="8.0" label="Missing Data" opacity="0.7" />
            </ColorMap> 
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>